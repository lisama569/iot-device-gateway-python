"""Firmware upgrade orchestration: batching, retries, pause/resume/cancel.

Durability model
----------------
Every state transition is persisted before the next dispatch, so a
process crash never leaves a device stuck in ``in_progress``: on startup
``resume_pending_jobs`` either restarts a ``pending`` job, or recovers an
``in_progress`` job by re-queueing devices whose dispatch result was
unknown (at-least-once). The simulator treats an already-installed
firmware version as an idempotent success, and the successful device
states themselves are persisted and replayed into the simulator on
restart, so recovered jobs do not double-push.

Terminal-state rules (unique, explainable)
------------------------------------------
* explicit cancel wins immediately -> ``cancelled``;
* otherwise all devices terminal and at least one ``failed`` ->
  ``failed``;
* otherwise -> ``completed`` (``skipped`` devices do not fail the job).
"""
from __future__ import annotations

import asyncio
import itertools
import secrets
import time

from .errors import JobConflictError, JobNotFoundError
from .models import (
    CreateJobRequest,
    DeviceUpgradeState,
    DeviceUpgradeView,
    DeviceStatus,
    JobState,
    JobView,
)
from .store import DataStore

_TERMINAL_JOB = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
_TERMINAL_DEVICE = {
    DeviceUpgradeState.SUCCEEDED,
    DeviceUpgradeState.FAILED,
    DeviceUpgradeState.SKIPPED,
    DeviceUpgradeState.CANCELLED,
}
_ACTIVE_DEVICE = {DeviceUpgradeState.PENDING, DeviceUpgradeState.IN_PROGRESS}


def _now() -> float:
    return time.time()


class FirmwareService:
    def __init__(
        self,
        store: DataStore,
        device_service,
        simulator,
        batch_size: int = 5,
        max_retries: int = 2,
        dispatch_delay: float = 0.01,
    ) -> None:
        self.store = store
        self.devices = device_service
        self.simulator = simulator
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.dispatch_delay = dispatch_delay
        self._job_seq = itertools.count(1)
        self._runner_tasks: dict[str, asyncio.Task] = {}
        self._job_locks: dict[str, asyncio.Lock] = {}
        self._create_lock = asyncio.Lock()
        self._settled: dict[str, asyncio.Event] = {}

    # ------------------------------------------------------------------ create

    async def create_job(
        self, request: CreateJobRequest, *, now: float | None = None
    ) -> JobView:
        """Create a pending job.

        Validates the target set: unknown devices are rejected, and a
        device already participating in another active job causes a
        ``JobConflictError`` whose details name both the device and the
        blocking job. Duplicate ``job_id`` is rejected distinctly.
        """
        now = now if now is not None else _now()
        # de-duplicate target list while preserving order
        seen: set[str] = set()
        targets: list[str] = []
        for did in request.device_ids:
            if did not in seen:
                seen.add(did)
                targets.append(did)

        async with self._create_lock:
            job_id = request.job_id or self._generate_job_id()
            if job_id in self.store.jobs:
                raise JobConflictError(
                    f"job {job_id!r} already exists",
                    details={"job_id": job_id, "reason": "job_exists"},
                )

            unknown = [d for d in targets if d not in self.store.devices]
            if unknown:
                raise JobConflictError(
                    "target devices do not exist",
                    details={"reason": "unknown_devices", "device_ids": unknown},
                )
            deregistered = [
                d
                for d in targets
                if self.store.devices[d]["status"] == DeviceStatus.DEREGISTERED.value
            ]
            if deregistered:
                raise JobConflictError(
                    "target devices are deregistered",
                    details={"reason": "device_deregistered", "device_ids": deregistered},
                )

            blocking = self._find_blocking_jobs(targets)
            if blocking:
                raise JobConflictError(
                    "device already in an active upgrade",
                    details={"reason": "device_busy", "conflicts": blocking},
                )

            device_states: dict[str, dict] = {}
            for did in targets:
                device_states[did] = {
                    "device_id": did,
                    "state": DeviceUpgradeState.PENDING.value,
                    "firmware_version": request.firmware_version,
                    "attempts": 0,
                    "last_error": None,
                    "updated_at": now,
                }
            job = {
                "job_id": job_id,
                "firmware_version": request.firmware_version,
                "state": JobState.PENDING.value,
                "batch_size": request.batch_size,
                "max_retries": request.max_retries,
                "round": 0,
                "queue": list(targets),
                "device_order": list(targets),
                "device_states": device_states,
                "created_at": now,
                "updated_at": now,
                "finished_at": None,
                "terminal_reason": None,
                "revision": 0,
            }
            self.store.jobs[job_id] = job
            await self._persist(job, now, "job_created")
            self._settled[job_id] = asyncio.Event()
            return self._to_view(job)

    def _find_blocking_jobs(self, targets: list[str]) -> list[dict]:
        conflicts: list[dict] = []
        wanted = set(targets)
        for jid, job in self.store.jobs.items():
            if JobState(job["state"]) in _TERMINAL_JOB:
                continue
            for did, st in job["device_states"].items():
                if did in wanted and DeviceUpgradeState(st["state"]) in _ACTIVE_DEVICE:
                    conflicts.append(
                        {"device_id": did, "job_id": jid, "device_state": st["state"]}
                    )
        return conflicts

    def _generate_job_id(self) -> str:
        return f"job_{next(self._job_seq)}_{secrets.token_hex(3)}"

    # ------------------------------------------------------------------ reads

    async def get_job(self, job_id: str) -> JobView:
        job = self.store.jobs.get(job_id)
        if job is None:
            raise JobNotFoundError(details={"job_id": job_id})
        return self._to_view(job)

    # ---------------------------------------------------------------- controls

    async def start_job(self, job_id: str) -> JobView:
        """Start a pending job (or resume a paused one). Idempotent."""
        job = self._require_job(job_id)
        async with self._lock_for(job_id):
            state = JobState(job["state"])
            if state == JobState.PENDING:
                await self._transition(job, JobState.IN_PROGRESS, "job started")
            elif state == JobState.PAUSED:
                await self._transition(job, JobState.IN_PROGRESS, "job resumed")
            elif state == JobState.IN_PROGRESS:
                pass
            else:
                raise JobConflictError(
                    f"cannot start job in state {state.value}",
                    details={"job_id": job_id, "state": state.value},
                )
        self._spawn_runner(job_id)
        return await self.get_job(job_id)

    async def pause_job(self, job_id: str, reason: str = "") -> JobView:
        job = self._require_job(job_id)
        async with self._lock_for(job_id):
            state = JobState(job["state"])
            if state not in (JobState.IN_PROGRESS,):
                if state == JobState.PAUSED:
                    return self._to_view(job)
                raise JobConflictError(
                    f"cannot pause job in state {state.value}",
                    details={"job_id": job_id, "state": state.value},
                )
            await self._transition(job, JobState.PAUSED, reason or "paused by request")
        return await self.get_job(job_id)

    async def resume_job(self, job_id: str) -> JobView:
        return await self.start_job(job_id)

    async def cancel_job(self, job_id: str, reason: str = "") -> JobView:
        job = self._require_job(job_id)
        async with self._lock_for(job_id):
            state = JobState(job["state"])
            if state in _TERMINAL_JOB:
                if state == JobState.CANCELLED:
                    return self._to_view(job)
                raise JobConflictError(
                    f"cannot cancel job in terminal state {state.value}",
                    details={"job_id": job_id, "state": state.value, "reason": "terminal"},
                )
            # Devices not yet dispatched are cancelled immediately.
            now = _now()
            for did, st in job["device_states"].items():
                dst = DeviceUpgradeState(st["state"])
                if dst == DeviceUpgradeState.PENDING:
                    st["state"] = DeviceUpgradeState.CANCELLED.value
                    st["updated_at"] = now
                elif dst == DeviceUpgradeState.IN_PROGRESS:
                    # leave in_progress for the runner to settle, but
                    # record intent via queue truncation/state transition
                    st["last_error"] = "cancelled while in flight"
            await self._transition(job, JobState.CANCELLED, reason or "cancelled by request")
        self._spawn_runner(job_id)  # let runner settle in-flight devices
        return await self.get_job(job_id)

    # ------------------------------------------------------------- runner loop

    def _spawn_runner(self, job_id: str) -> None:
        existing = self._runner_tasks.get(job_id)
        if existing is not None and not existing.done():
            return
        self._runner_tasks[job_id] = asyncio.create_task(self._run_job(job_id))

    async def _run_job(self, job_id: str) -> None:
        try:
            while True:
                job = self.store.jobs.get(job_id)
                if job is None:
                    return
                state = JobState(job["state"])
                if state in _TERMINAL_JOB:
                    await self._settle_inflight(job)
                    self._signal_settled(job_id)
                    return
                if state == JobState.PAUSED or state == JobState.PENDING:
                    # Explicit start/resume is required to continue.
                    return
                await self._run_batch(job)
                if self.dispatch_delay:
                    await asyncio.sleep(self.dispatch_delay)
        finally:
            task = self._runner_tasks.get(job_id)
            if task is asyncio.current_task():
                self._runner_tasks.pop(job_id, None)

    async def _dispatch(self, job: dict, device_id: str, attempt: int) -> tuple[bool, str | None]:
        """Respect the device lifecycle: disabled/deregistered -> failure."""
        device = self.store.devices.get(device_id)
        if device is None:
            return False, "device vanished before dispatch"
        if device["status"] == DeviceStatus.DEREGISTERED.value:
            return False, "device deregistered before dispatch"
        if device["status"] == DeviceStatus.DISABLED.value:
            return False, "device disabled before dispatch"
        return await self.simulator.simulate_upgrade(
            device_id, job["firmware_version"], attempt
        )

    async def _run_batch(self, job: dict) -> None:
        jid = job["job_id"]
        async with self._lock_for(jid):
            if JobState(job["state"]) != JobState.IN_PROGRESS:
                return
            now = _now()
            batch: list[str] = []
            skipped: list[str] = []
            kept: list[str] = []
            for did in job["queue"]:
                st = job["device_states"][did]
                if st["state"] != DeviceUpgradeState.PENDING.value:
                    continue
                device = self.store.devices.get(did)
                if device is not None and device["status"] in (
                    DeviceStatus.DISABLED.value,
                    DeviceStatus.DEREGISTERED.value,
                ):
                    skipped.append(did)
                    continue
                if len(batch) < job["batch_size"]:
                    batch.append(did)
                    st["state"] = DeviceUpgradeState.IN_PROGRESS.value
                    st["attempts"] += 1
                    st["updated_at"] = now
                else:
                    kept.append(did)
            for did in skipped:
                st = job["device_states"][did]
                device = self.store.devices.get(did)
                st["state"] = DeviceUpgradeState.SKIPPED.value
                st["updated_at"] = now
                st["last_error"] = (
                    f"device {device['status'] if device else 'missing'} at dispatch"
                )
            job["queue"] = kept
            if batch or skipped:
                job["round"] += 1
            await self._persist(job, now, f"batch_dispatched:{len(batch)}:skipped:{len(skipped)}")

        if not batch:
            await self._evaluate(job)
            return

        # Dispatch concurrently; the simulator is the local fake device.
        results = await asyncio.gather(
            *[
                self._dispatch(job, did, job["device_states"][did]["attempts"])
                for did in batch
            ]
        )
        await self._record_batch(job, list(zip(batch, results)))

    async def _record_batch(self, job: dict, pairs: list[tuple[str, tuple[bool, str | None]]]) -> None:
        jid = job["job_id"]
        async with self._lock_for(jid):
            now = _now()
            cancelled = JobState(job["state"]) == JobState.CANCELLED
            retries: list[str] = []
            for did, (ok, err) in pairs:
                st = job["device_states"][did]
                st["updated_at"] = now
                if ok:
                    st["state"] = DeviceUpgradeState.SUCCEEDED.value
                    st["last_error"] = None
                    # Survives restart: recovered jobs re-dispatch nothing.
                    self.simulator.mark_installed(did, job["firmware_version"])
                    continue
                st["last_error"] = err
                if cancelled:
                    st["state"] = DeviceUpgradeState.CANCELLED.value
                elif st["attempts"] <= job["max_retries"]:
                    st["state"] = DeviceUpgradeState.PENDING.value
                    retries.append(did)
                else:
                    st["state"] = DeviceUpgradeState.FAILED.value
            if cancelled:
                for did in job["queue"]:
                    st = job["device_states"][did]
                    if st["state"] == DeviceUpgradeState.PENDING.value:
                        st["state"] = DeviceUpgradeState.CANCELLED.value
                        st["updated_at"] = now
                job["queue"] = []
            else:
                # deterministic retry order: original device order
                order = {d: i for i, d in enumerate(job["device_order"])}
                retries.sort(key=order.get)
                job["queue"].extend(retries)
            await self._persist(job, now, f"batch_recorded:{len(pairs)}:retries:{len(retries)}")
            await self._evaluate_locked(job)

    async def _evaluate(self, job: dict) -> None:
        async with self._lock_for(job["job_id"]):
            await self._evaluate_locked(job)

    async def _evaluate_locked(self, job: dict) -> None:
        """Finalize iff every device reached a terminal state."""
        state = JobState(job["state"])
        if state in _TERMINAL_JOB:
            # Defensive: nothing active may linger under a terminal job.
            for st in job["device_states"].values():
                if DeviceUpgradeState(st["state"]) in _ACTIVE_DEVICE:
                    st["state"] = DeviceUpgradeState.CANCELLED.value
            job["queue"] = []
            await self._persist(job, _now(), "terminal_guard")
            return
        active = [
            did
            for did, st in job["device_states"].items()
            if DeviceUpgradeState(st["state"]) in _ACTIVE_DEVICE
        ]
        if active or job["queue"]:
            return
        failed = [
            did
            for did, st in job["device_states"].items()
            if st["state"] == DeviceUpgradeState.FAILED.value
        ]
        if failed:
            await self._transition(
                job, JobState.FAILED, f"{len(failed)} device(s) failed: {','.join(sorted(failed))}"
            )
        else:
            await self._transition(job, JobState.COMPLETED, "all devices reached terminal state")

    async def _settle_inflight(self, job: dict) -> None:
        """Resolve devices left in-flight when entering a terminal state."""
        jid = job["job_id"]
        async with self._lock_for(jid):
            inflight = [
                did
                for did in job["device_order"]
                if job["device_states"][did]["state"] == DeviceUpgradeState.IN_PROGRESS.value
            ]
            queued = [
                did
                for did in job["queue"]
                if job["device_states"][did]["state"] == DeviceUpgradeState.PENDING.value
            ]
        if JobState(job["state"]) != JobState.CANCELLED:
            return  # completed/failed can never legitimately have in-flight work
        if not inflight and not queued:
            return
        # At-least-once for the unknown dispatch; the simulator is idempotent.
        results = (
            await asyncio.gather(
                *[
                    self._dispatch(job, did, job["device_states"][did]["attempts"])
                    for did in inflight
                ]
            )
            if inflight
            else []
        )
        async with self._lock_for(jid):
            now = _now()
            for did, (ok, err) in zip(inflight, results):
                st = job["device_states"][did]
                st["updated_at"] = now
                if ok:
                    st["state"] = DeviceUpgradeState.SUCCEEDED.value
                    st["last_error"] = None
                    self.simulator.mark_installed(did, job["firmware_version"])
                else:
                    st["state"] = DeviceUpgradeState.CANCELLED.value
                    st["last_error"] = err
            for did in queued:
                st = job["device_states"][did]
                st["state"] = DeviceUpgradeState.CANCELLED.value
                st["updated_at"] = now
            job["queue"] = []
            await self._persist(job, now, "inflight_settled_after_cancel")

    async def resume_pending_jobs(self) -> list[str]:
        """Recover jobs left non-terminal by a crash (called on startup).

        * succeeded device versions are replayed into the simulator so a
          redispatch (at-least-once) is an idempotent no-op;
        * ``in_progress`` jobs are resumed; devices caught mid-dispatch
          are re-queued without consuming an extra retry, keeping the
          deterministic order;
        * pending/paused jobs stay put (nothing was dispatched, so
          nothing can be stuck); terminal jobs are marked settled.
        """
        for job in self.store.jobs.values():
            for did, st in job["device_states"].items():
                if st["state"] == DeviceUpgradeState.SUCCEEDED.value:
                    self.simulator.mark_installed(did, job["firmware_version"])
            self._settled.setdefault(job["job_id"], asyncio.Event())

        recovered: list[str] = []
        for jid, job in list(self.store.jobs.items()):
            state = JobState(job["state"])
            if state == JobState.IN_PROGRESS:
                async with self._lock_for(jid):
                    inflight = [
                        did
                        for did in job["device_order"]
                        if job["device_states"][did]["state"]
                        == DeviceUpgradeState.IN_PROGRESS.value
                    ]
                    for did in inflight:
                        job["device_states"][did]["state"] = DeviceUpgradeState.PENDING.value
                    # inflight devices keep their position before the
                    # devices that had never been selected
                    queued = [
                        did
                        for did in job["queue"]
                        if job["device_states"][did]["state"]
                        == DeviceUpgradeState.PENDING.value
                    ]
                    inflight_set = set(inflight)
                    job["queue"] = inflight + [d for d in queued if d not in inflight_set]
                    await self._persist(job, _now(), "recovered_in_progress")
                self._spawn_runner(jid)
                recovered.append(jid)
            elif state in _TERMINAL_JOB:
                await self._settle_inflight(job)
                self._signal_settled(jid)
        return recovered

    async def wait_until_settled(self, job_id: str, timeout: float | None = 30.0) -> JobView:
        """Wait for a job to reach a terminal state."""
        if job_id not in self.store.jobs:
            raise JobNotFoundError(details={"job_id": job_id})
        event = self._settled.setdefault(job_id, asyncio.Event())

        async def _poll() -> None:
            while JobState(self.store.jobs[job_id]["state"]) not in _TERMINAL_JOB:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(asyncio.gather(event.wait(), _poll()), timeout=timeout)
        return await self.get_job(job_id)

    # ------------------------------------------------------------------ utils

    def _require_job(self, job_id: str) -> dict:
        job = self.store.jobs.get(job_id)
        if job is None:
            raise JobNotFoundError(details={"job_id": job_id})
        return job

    def _lock_for(self, job_id: str) -> asyncio.Lock:
        lock = self._job_locks.get(job_id)
        if lock is None:
            lock = asyncio.Lock()
            self._job_locks[job_id] = lock
        return lock

    async def _transition(self, job: dict, new_state: JobState, reason: str) -> None:
        now = _now()
        old = job["state"]
        job["state"] = new_state.value
        job["updated_at"] = now
        job["revision"] += 1
        if new_state in _TERMINAL_JOB:
            job["finished_at"] = now
            job["terminal_reason"] = reason
            job["queue"] = []
        await self._persist(job, now, f"job_transition:{old}->{new_state.value}:{reason}")

    async def _persist(self, job: dict, now: float, kind: str) -> None:
        await self.store.save()
        await self.store.append_audit(
            {
                "ts": now,
                "kind": kind,
                "job_id": job["job_id"],
                "state": job["state"],
                "revision": job["revision"],
                "round": job.get("round"),
            }
        )
        await self.store.save()

    def _signal_settled(self, job_id: str) -> None:
        event = self._settled.get(job_id)
        if event is not None:
            event.set()

    def _to_view(self, job: dict) -> JobView:
        return JobView(
            job_id=job["job_id"],
            firmware_version=job["firmware_version"],
            state=JobState(job["state"]),
            batch_size=job["batch_size"],
            max_retries=job["max_retries"],
            next_batch_index=job["round"],
            total_devices=len(job["device_order"]),
            device_states={
                did: DeviceUpgradeView(
                    device_id=st["device_id"],
                    state=DeviceUpgradeState(st["state"]),
                    firmware_version=st["firmware_version"],
                    attempts=st["attempts"],
                    last_error=st.get("last_error"),
                    updated_at=st["updated_at"],
                )
                for did, st in job["device_states"].items()
            },
            created_at=job["created_at"],
            updated_at=job["updated_at"],
            finished_at=job.get("finished_at"),
            terminal_reason=job.get("terminal_reason"),
        )
