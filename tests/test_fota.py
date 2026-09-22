"""Firmware campaign orchestration tests."""
import pytest

from app.fota import FirmwareOrchestrator
from app.models import ErrorCode, GatewayError
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher


def make_fleet(n=5):
    reg = DeviceRegistry()
    for i in range(n):
        reg.register(f"dev-{i}", "pw")
    return reg


def test_batched_campaign_completes_and_bumps_versions():
    reg = make_fleet(5)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher)
    devices = [f"dev-{i}" for i in range(5)]
    orc.create_campaign("c1", "1.1.0", devices, batch_size=2)
    final = orc.run_until_blocked("c1")
    print(f"[complete] status={final['status']} states={final['device_states']} "
          f"batches_dispatched={final['current_batch']}")
    assert final["status"] == "completed"
    assert all(s == "succeeded" for s in final["device_states"].values())
    assert all(reg.get(d).firmware_version == "1.1.0" for d in devices)
    assert all(flasher.version_of(d) == "1.1.0" for d in devices)


def test_create_is_idempotent_and_conflict_detected():
    reg = make_fleet(2)
    orc = FirmwareOrchestrator(reg, SimulatedFlasher())
    c1 = orc.create_campaign("c1", "1.0.0", ["dev-0"])
    c2 = orc.create_campaign("c1", "1.0.0", ["dev-0"])
    assert c1 == c2
    with pytest.raises(GatewayError) as e:
        orc.create_campaign("c1", "9.9.9", ["dev-0"])
    print(f"[create-conflict] code={e.value.code}")
    assert e.value.code is ErrorCode.TASK_STATE_CONFLICT


def test_failed_device_retries_then_campaign_fails_deterministically():
    reg = make_fleet(4)
    flasher = SimulatedFlasher()
    flasher.fail_next("dev-1", times=10)  # always fails
    orc = FirmwareOrchestrator(reg, flasher, max_retries=2)
    devices = [f"dev-{i}" for i in range(4)]
    orc.create_campaign("c1", "2.0.0", devices, batch_size=2)
    final = orc.run_until_blocked("c1")
    print(f"[fail] status={final['status']} error={final['error']} "
          f"attempts={final['attempts']} states={final['device_states']}")
    assert final["status"] == "failed"
    assert final["device_states"]["dev-1"] == "failed"
    assert final["attempts"]["dev-1"] == 3  # 1 try + 2 retries
    # batch 0 finished, later batches never dispatched
    assert final["device_states"]["dev-0"] == "succeeded"
    assert final["device_states"]["dev-2"] == "skipped"
    assert final["device_states"]["dev-3"] == "skipped"
    # terminal state is final
    with pytest.raises(GatewayError) as e:
        orc.resume("c1")
    assert e.value.code is ErrorCode.TASK_STATE_CONFLICT


def test_retry_succeeds_within_budget():
    reg = make_fleet(2)
    flasher = SimulatedFlasher()
    flasher.fail_next("dev-0", times=1)  # flaky once
    orc = FirmwareOrchestrator(reg, flasher, max_retries=2)
    orc.create_campaign("c1", "2.0.0", ["dev-0", "dev-1"], batch_size=2)
    final = orc.run_until_blocked("c1")
    print(f"[flaky] status={final['status']} attempts={final['attempts']}")
    assert final["status"] == "completed"
    assert final["attempts"]["dev-0"] == 2


def test_pause_resume_cancel():
    reg = make_fleet(4)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher)

    # pause before running: nothing dispatched
    orc.create_campaign("c1", "3.0.0", [f"dev-{i}" for i in range(4)], batch_size=1)
    orc.pause("c1")
    st = orc.status("c1")
    print(f"[pause] status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"
    assert flasher.calls() == []

    # resume runs to completion
    final = orc.resume("c1")
    print(f"[resume] status={final['status']}")
    assert final["status"] == "completed"

    # cancel a second campaign mid-flight
    orc.create_campaign("c2", "4.0.0", [f"dev-{i}" for i in range(4)], batch_size=1)
    orc.pause("c2")
    cancelled = orc.cancel("c2")
    print(f"[cancel] status={cancelled['status']} states={cancelled['device_states']}")
    assert cancelled["status"] == "cancelled"
    assert all(s == "skipped" for s in cancelled["device_states"].values())
    with pytest.raises(GatewayError):
        orc.resume("c2")


def test_device_cannot_be_in_two_live_campaigns():
    reg = make_fleet(3)
    orc = FirmwareOrchestrator(reg, SimulatedFlasher())
    orc.create_campaign("c1", "1.0.0", ["dev-0", "dev-1"])
    orc.pause("c1")  # live but parked
    with pytest.raises(GatewayError) as e:
        orc.create_campaign("c2", "2.0.0", ["dev-1", "dev-2"])
    print(f"[busy] code={e.value.code}")
    assert e.value.code is ErrorCode.DEVICE_BUSY
    # disjoint device set is fine
    orc.create_campaign("c3", "2.0.0", ["dev-2"])


def test_restart_recovery_leaves_nothing_stuck(tmp_path):
    state = tmp_path / "fota.json"
    reg = make_fleet(3)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher, state_path=str(state))
    devices = [f"dev-{i}" for i in range(3)]
    orc.create_campaign("c1", "5.0.0", devices, batch_size=1)
    orc.pause("c1")
    # simulate a crash mid-RUNNING by hand-editing persisted state
    import json
    data = json.loads(state.read_text())
    data[0]["status"] = "running"
    data[0]["device_states"]["dev-0"] = "in_progress"
    state.write_text(json.dumps(data))

    # new process: fresh orchestrator recovers from disk
    orc2 = FirmwareOrchestrator(reg, flasher, state_path=str(state))
    st = orc2.status("c1")
    print(f"[recover] status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"           # not stuck in running
    assert st["device_states"]["dev-0"] == "pending"  # safe to re-dispatch

    final = orc2.resume("c1")
    calls = flasher.calls()
    print(f"[recover-resume] status={final['status']} flash_calls={calls}")
    assert final["status"] == "completed"
    # idempotent flasher: no device got the same version delivered twice
    assert len(calls) == len(set(calls))
