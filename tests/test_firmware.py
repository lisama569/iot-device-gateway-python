"""Firmware orchestration: batching, retries, unique terminal states,
pause/resume/cancel, device mutex and crash recovery. Job identifiers,
firmware versions and per-device states are logged as evidence."""
from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import JobConflictError, JobNotFoundError
from gateway.firmware import FirmwareService
from gateway.models import CreateJobRequest, RegisterDeviceRequest
from gateway.simulator import DeviceSimulator

log = logging.getLogger("tests.firmware")


async def _populate(device_service, n, prefix="d"):
    ids = [f"{prefix}{i}" for i in range(n)]
    for did in ids:
        await device_service.register(RegisterDeviceRequest(device_id=did), now=1000.0)
    return ids


def _summary(view):
    return {d: st.state.value for d, st in sorted(view.device_states.items())}


async def test_batched_completion(registry):
    fw = registry.firmware_service
    ids = await _populate(registry.device_service, 7)
    view = await fw.create_job(
        CreateJobRequest(
            job_id="job-batch",
            firmware_version="fw-1.0",
            device_ids=ids,
            batch_size=3,
            max_retries=0,
        )
    )
    assert view.state.value == "pending"
    await fw.start_job("job-batch")
    final = await fw.wait_until_settled("job-batch", timeout=5)
    log.info("job=%s version=%s final=%s", final.job_id, final.firmware_version, final.state.value)
    assert final.state.value == "completed"
    assert final.terminal_reason == "all devices reached terminal state"
    assert all(st.state.value == "succeeded" for st in final.device_states.values())
    assert final.next_batch_index >= 3  # at least three batches were tracked


async def test_permanent_failure_yields_failed_terminal_with_retries(registry):
    store = registry.store
    ds = registry.device_service
    sim = DeviceSimulator(fail_rate=0.0, delay=0.0)
    fw = FirmwareService(store, ds, sim, batch_size=10, max_retries=2, dispatch_delay=0.0)
    ids = await _populate(ds, 3, prefix="f")
    sim.force_failure("f1", "fw-bad")
    await fw.create_job(
        CreateJobRequest(job_id="job-fail", firmware_version="fw-bad",
                         device_ids=ids, batch_size=10, max_retries=2)
    )
    await fw.start_job("job-fail")
    final = await fw.wait_until_settled("job-fail", timeout=5)
    assert final.state.value == "failed"
    assert "f1" in final.terminal_reason
    assert final.device_states["f1"].attempts == 3
    assert final.device_states["f1"].state.value == "failed"
    assert final.device_states["f0"].state.value == "succeeded"
    log.info(
        "failed job terminal=%s attempts[f1]=%s last_error=%s",
        final.state.value,
        final.device_states["f1"].attempts,
        final.device_states["f1"].last_error,
    )


async def test_transient_failures_are_retried_within_one_job(registry):
    """High fail-rate simulator: some devices need a retry, but no device
    is double-counted and the job still completes when retries succeed."""
    store = registry.store
    ds = registry.device_service
    # Deterministic RNG: pick fail_rate so retries can succeed.
    sim = DeviceSimulator(fail_rate=0.5, delay=0.0)
    fw = FirmwareService(store, ds, sim, batch_size=20, max_retries=5, dispatch_delay=0.0)
    ids = await _populate(ds, 6, prefix="t")
    await fw.create_job(
        CreateJobRequest(job_id="job-retry", firmware_version="fw-x",
                         device_ids=ids, batch_size=20, max_retries=5)
    )
    await fw.start_job("job-retry")
    final = await fw.wait_until_settled("job-retry", timeout=5)
    attempts = {d: st.attempts for d, st in final.device_states.items()}
    log.info("retry job=%s attempts=%s", final.state.value, attempts)
    assert final.state.value == "completed"
    assert all(st.state.value == "succeeded" for st in final.device_states.values())


async def test_pause_blocks_progress_until_resume(registry):
    store = registry.store
    ds = registry.device_service
    sim = DeviceSimulator(fail_rate=0.0, delay=0.0)
    fw = FirmwareService(store, ds, sim, batch_size=2, max_retries=0, dispatch_delay=0.04)
    ids = await _populate(ds, 10, prefix="p")
    await fw.create_job(
        CreateJobRequest(job_id="job-pause", firmware_version="fw-1",
                         device_ids=ids, batch_size=2, max_retries=0)
    )
    await fw.start_job("job-pause")
    for _ in range(200):
        view = await fw.get_job("job-pause")
        done = sum(1 for st in view.device_states.values() if st.state.value == "succeeded")
        pending = sum(1 for st in view.device_states.values() if st.state.value == "pending")
        if done >= 2 and pending:
            break
        await asyncio.sleep(0.005)
    await fw.pause_job("job-pause", "test pause")
    paused = await fw.get_job("job-pause")
    assert paused.state.value == "paused"
    snapshot = _summary(paused)
    await asyncio.sleep(0.15)
    assert _summary(await fw.get_job("job-pause")) == snapshot
    log.info("paused snapshot=%s", snapshot)
    await fw.resume_job("job-pause")
    final = await fw.wait_until_settled("job-pause", timeout=10)
    assert final.state.value == "completed"
    assert all(st.state.value == "succeeded" for st in final.device_states.values())


async def test_cancel_is_terminal_and_unique(registry):
    store = registry.store
    ds = registry.device_service
    sim = DeviceSimulator(fail_rate=0.0, delay=0.0)
    fw = FirmwareService(store, ds, sim, batch_size=3, max_retries=0, dispatch_delay=0.05)
    ids = await _populate(ds, 9, prefix="c")
    await fw.create_job(
        CreateJobRequest(job_id="job-cancel", firmware_version="fw-1",
                         device_ids=ids, batch_size=3, max_retries=0)
    )
    await fw.start_job("job-cancel")
    for _ in range(200):
        view = await fw.get_job("job-cancel")
        done = sum(1 for st in view.device_states.values() if st.state.value == "succeeded")
        pending = sum(1 for st in view.device_states.values() if st.state.value == "pending")
        if done >= 3 and pending:
            break
        await asyncio.sleep(0.005)
    await fw.cancel_job("job-cancel", "abort test")
    final = await fw.wait_until_settled("job-cancel", timeout=5)
    states = {st.state.value for st in final.device_states.values()}
    assert final.state.value == "cancelled"
    assert states <= {"succeeded", "cancelled"}, states
    assert "cancelled" in states
    log.info("cancelled job terminal_reason=%s states=%s", final.terminal_reason, states)
    # idempotent cancel, but resume must be rejected
    assert (await fw.cancel_job("job-cancel")).state.value == "cancelled"
    with pytest.raises(JobConflictError):
        await fw.resume_job("job-cancel")


async def test_device_cannot_join_two_active_jobs(registry):
    store = registry.store
    ds = registry.device_service
    sim = DeviceSimulator(fail_rate=0.0, delay=0.15)
    fw = FirmwareService(store, ds, sim, batch_size=1, max_retries=0, dispatch_delay=0.0)
    await _populate(ds, 2, prefix="m")
    await fw.create_job(
        CreateJobRequest(job_id="job-a", firmware_version="fw-1",
                         device_ids=["m0", "m1"], batch_size=1, max_retries=0)
    )
    await fw.start_job("job-a")
    # m0 is mid-dispatch (in_progress) at this point
    await asyncio.sleep(0.03)
    with pytest.raises(JobConflictError) as exc:
        await fw.create_job(
            CreateJobRequest(job_id="job-b", firmware_version="fw-2", device_ids=["m0"])
        )
    assert exc.value.details["reason"] == "device_busy"
    assert exc.value.details["conflicts"][0]["job_id"] == "job-a"
    log.info("mutex conflict=%s", exc.value.details["conflicts"])
    await fw.wait_until_settled("job-a", timeout=5)
    # terminal job releases the device
    await fw.create_job(
        CreateJobRequest(job_id="job-c", firmware_version="fw-2", device_ids=["m0"])
    )


async def test_job_id_duplicate_and_unknown_device_rejected(registry):
    fw = registry.firmware_service
    await _populate(registry.device_service, 1, prefix="u")
    payload = dict(job_id="job-dup", firmware_version="fw-1", device_ids=["u0"])
    await fw.create_job(CreateJobRequest(**payload))
    with pytest.raises(JobConflictError) as exc:
        await fw.create_job(CreateJobRequest(**payload))
    assert exc.value.details["reason"] == "job_exists"
    with pytest.raises(JobConflictError) as exc:
        await fw.create_job(
            CreateJobRequest(job_id="job-x", firmware_version="fw-1", device_ids=["ghost"])
        )
    assert exc.value.details["reason"] == "unknown_devices"
    with pytest.raises(JobNotFoundError):
        await fw.get_job("missing")


async def test_restart_recovers_in_progress_without_duplicate_dispatch(registry):
    store = registry.store
    ds = registry.device_service
    ids = await _populate(ds, 7, prefix="r")

    # Seed a job that "crashed" mid-flight.
    sim0 = DeviceSimulator(fail_rate=0.0, delay=0.0)
    fw0 = FirmwareService(store, ds, sim0, batch_size=3, max_retries=1, dispatch_delay=0.0)
    await fw0.create_job(
        CreateJobRequest(job_id="job-restart", firmware_version="fw-2",
                         device_ids=ids, batch_size=3, max_retries=1)
    )
    job = store.jobs["job-restart"]
    job["state"] = "in_progress"
    job["round"] = 1
    order = job["device_order"]
    for did in order[:3]:
        st = job["device_states"][did]
        st["state"] = "succeeded"
        st["attempts"] = 1
    job["device_states"][order[3]]["state"] = "in_progress"  # unknown result
    job["device_states"][order[3]]["attempts"] = 1
    for did in order[4:]:
        job["device_states"][did]["state"] = "pending"
    job["queue"] = order[4:]
    await store.save()

    # Simulate a brand-new process over the same file.
    from gateway.config import Settings
    settings = Settings()
    settings.state_file = store.path
    store2 = type(store)(store.path)
    await store2.load()
    sim2 = DeviceSimulator(fail_rate=0.0, delay=0.0)
    fw2 = FirmwareService(store2, ds, sim2, batch_size=3, max_retries=1, dispatch_delay=0.0)
    recovered = await fw2.resume_pending_jobs()
    assert recovered == ["job-restart"]
    final = await fw2.wait_until_settled("job-restart", timeout=5)
    assert final.state.value == "completed", final.state
    assert all(st.state.value == "succeeded" for st in final.device_states.values())
    victim = final.device_states[order[3]]
    log.info("recovered job=%s victim attempts=%s", final.state.value, victim.attempts)
    assert victim.attempts <= 2
    # already-succeeded devices were replayed into the new simulator
    assert sim2.installed_versions(order[0]) == {"fw-2"}
