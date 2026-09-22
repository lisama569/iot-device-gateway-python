"""Local simulated-traffic demo (no broker / real devices / credentials).

Runs entirely in-process against the real services:

    uv run python scripts/demo.py

It prints the key inputs (device ids, credential ids, message order
tuples), the firmware version, per-device decisions and the final job
state, so the conflict/terminal-state reasoning is observable.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gateway.devices import DeviceService
from gateway.firmware import FirmwareService
from gateway.models import CreateJobRequest, RegisterDeviceRequest, TelemetryMessage
from gateway.simulator import DeviceSimulator
from gateway.store import DataStore
from gateway.telemetry import TelemetryService

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("demo")


async def main() -> None:
    state_file = os.environ.get(
        "GATEWAY_STATE_FILE", os.path.join(tempfile.gettempdir(), "gateway_demo.json")
    )
    if os.path.exists(state_file):
        os.remove(state_file)
    store = DataStore(state_file)
    await store.load()
    devices = DeviceService(store, default_ttl=0)
    telemetry = TelemetryService(store, devices, quota=100, quota_window=60.0)
    simulator = DeviceSimulator(fail_rate=0.0, delay=0.02)
    firmware = FirmwareService(
        store, devices, simulator, batch_size=2, max_retries=1, dispatch_delay=0.02
    )

    # 1. register
    creds: dict[str, tuple[str, str]] = {}
    for did in ["sensor-a", "sensor-b", "sensor-c"]:
        view, secret = await devices.register(
            RegisterDeviceRequest(device_id=did, name=did, metadata={"site": "local"})
        )
        cid = view.credentials[0].credential_id
        creds[did] = (cid, secret)
        log.info("registered device=%s credential=%s", did, cid)

    # 2. telemetry: in-order, out-of-order, duplicate and time-skewed
    cid, secret = creds["sensor-a"]

    def msg(version, ts, state, seq=0, nonce=None):
        return TelemetryMessage(
            device_id="sensor-a", credential_id=cid, credential_secret=secret,
            version=version, timestamp=ts, seq=seq, nonce=nonce, state=state,
        )

    plan = [
        msg(1, 1.0, {"temp": 20.0}),
        msg(3, 3.0, {"temp": 22.0, "fan": "on"}),
        msg(2, 2.0, {"temp": 21.0}),                    # out of order -> stale
        msg(3, 3.0, {"temp": 22.0, "fan": "on"}),       # exact replay -> duplicate
        msg(3, 2.5, {"temp": 99.0}),                    # older timestamp -> stale
        msg(4, 4.0, {"temp": 22.5}),                    # newer -> merged
    ]
    for m in plan:
        r = await telemetry.ingest(m)
        log.info(
            "telemetry in=(v=%s,t=%s) decision=%-9s reason=%s shadow_version=%s",
            m.version, m.timestamp, r.decision, r.reason, r.current_version,
        )
    shadow = await telemetry.get_shadow("sensor-a")
    log.info("FINAL SHADOW version=%s state=%s", shadow.version, shadow.state)

    # 3. batched firmware upgrade; one device is forced to fail
    targets = ["sensor-a", "sensor-b", "sensor-c"]
    simulator.force_failure("sensor-b", "fw-2.0")
    job = await firmware.create_job(
        CreateJobRequest(
            job_id="demo-upgrade",
            firmware_version="fw-2.0",
            device_ids=targets,
            batch_size=2,
            max_retries=1,
        )
    )
    log.info("created job=%s firmware=%s batches<=%s devices=%s",
             job.job_id, job.firmware_version, job.batch_size, targets)
    await firmware.start_job("demo-upgrade")
    final = await firmware.wait_until_settled("demo-upgrade", timeout=10)
    for did, st in final.device_states.items():
        log.info("device=%s state=%s attempts=%s error=%s",
                 did, st.state.value, st.attempts, st.last_error)
    log.info("JOB terminal=%s reason=%s", final.state.value, final.terminal_reason)

    # 4. cancellation demonstration on a second job
    extra = [f"extra-{i}" for i in range(4)]
    for did in extra:
        await devices.register(RegisterDeviceRequest(device_id=did))
    await firmware.create_job(
        CreateJobRequest(job_id="demo-cancel", firmware_version="fw-3.0",
                         device_ids=extra, batch_size=1, max_retries=0)
    )
    await firmware.start_job("demo-cancel")
    await asyncio.sleep(0.03)
    cancelled = await firmware.cancel_job("demo-cancel", "demo abort")
    final2 = await firmware.wait_until_settled("demo-cancel", timeout=5)
    log.info("JOB terminal=%s reason=%s states=%s",
             final2.state.value,
             final2.terminal_reason,
             {d: s.state.value for d, s in final2.device_states.items()})
    log.info("state file persisted at %s", state_file)


if __name__ == "__main__":
    asyncio.run(main())
