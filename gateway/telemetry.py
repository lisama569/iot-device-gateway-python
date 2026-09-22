"""Telemetry ingestion and device-shadow merging.

Conflict rule (deterministic total order):
  * Higher ``version`` always wins.
  * On equal version, higher ``timestamp`` wins; if timestamps tie, the
    larger ``seq`` wins; if that ties, lexicographically larger ``nonce``
    wins.
  * The comparison tuple and the decision (``merged`` / ``duplicate`` /
    ``stale`` / ``conflict_lost``) are written to the audit log, so every
    accept/drop is traceable.
  * A tuple that has already been applied is an exact duplicate: it is
    acknowledged but never merged or counted twice.

State merge: the winner's reported keys are merged into the shadow
(absent keys are retained); losers leave the shadow untouched, so
out-of-order messages can never roll state back.
"""
from __future__ import annotations

import asyncio
import math
import time

from .errors import (
    DeviceNotFoundError,
    QuotaExceededError,
    ValidationFailureError,
)
from .models import ShadowView, TelemetryMessage, TelemetryResult
from .store import DataStore

# How many recently applied order-tuples to remember per device for
# exact-duplicate detection.
_DEDUPE_CAP = 256
_PRIMITIVE = (str, int, float, bool, type(None))


def _validate_state(state: dict) -> None:
    def walk(value, path: str) -> None:
        if isinstance(value, _PRIMITIVE):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                raise ValidationFailureError(
                    f"non-finite float at {path}", details={"path": path}
                )
            return
        if isinstance(value, dict):
            for k, v in value.items():
                if not isinstance(k, str):
                    raise ValidationFailureError(
                        f"non-string key at {path}", details={"path": path}
                    )
                walk(v, f"{path}.{k}")
            return
        if isinstance(value, list):
            for i, v in enumerate(value):
                walk(v, f"{path}[{i}]")
            return
        raise ValidationFailureError(
            f"unsupported value type at {path}",
            details={"path": path, "type": type(value).__name__},
        )

    walk(state, "$")


class TelemetryService:
    def __init__(
        self,
        store: DataStore,
        device_service,
        quota: int = 1000,
        quota_window: float = 60.0,
    ) -> None:
        self.store = store
        self.devices = device_service
        self.quota = quota
        self.quota_window = quota_window
        self._shadow_locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()
        # device_id -> list[float] accepted-merge timestamps (sliding window)
        self._quota_windows: dict[str, list[float]] = {}

    async def ingest(self, msg: TelemetryMessage, *, now: float | None = None) -> TelemetryResult:
        """Authenticate, validate, quota-check, dedupe and merge."""
        now = now if now is not None else time.time()
        if not math.isfinite(msg.timestamp):
            raise ValidationFailureError(
                "timestamp must be finite", details={"field": "timestamp"}
            )
        _validate_state(msg.state)

        # Authentication enables/disables/credential state immediately.
        await self.devices.authenticate(
            msg.device_id, msg.credential_id, msg.credential_secret, now=now
        )

        lock = await self._lock_for(msg.device_id)
        async with lock:
            shadow = self.store.shadows.get(msg.device_id)
            incoming = self._order_tuple(msg)

            if shadow is None:
                shadow = self._new_shadow(msg.device_id)
                self.store.shadows[msg.device_id] = shadow

            seen = {tuple(t) for t in shadow.get("seen_tuples", [])}
            if incoming in seen:
                await self._audit(now, msg, incoming, shadow, "duplicate", "exact tuple replay")
                return TelemetryResult(
                    accepted=True,
                    device_id=msg.device_id,
                    current_version=shadow["version"],
                    merged_state=dict(shadow["state"]),
                    decision="duplicate",
                    reason="exact tuple replay; state unchanged",
                )

            current = tuple(shadow["last_tuple"]) if shadow.get("last_tuple") else None
            if current is not None:
                if incoming < current:
                    await self._audit(now, msg, incoming, shadow, "stale", "order tuple below current")
                    return TelemetryResult(
                        accepted=False,
                        device_id=msg.device_id,
                        current_version=shadow["version"],
                        merged_state=dict(shadow["state"]),
                        decision="stale",
                        reason=f"incoming {incoming} < applied {current}",
                    )
                if incoming == current:
                    # Defensive: should have been caught by seen-set.
                    await self._audit(now, msg, incoming, shadow, "duplicate", "tuple equal to current")
                    return TelemetryResult(
                        accepted=True,
                        device_id=msg.device_id,
                        current_version=shadow["version"],
                        merged_state=dict(shadow["state"]),
                        decision="duplicate",
                        reason="tuple equal to current",
                    )

            # Quota applies only to messages that actually change the shadow.
            self._enforce_quota(msg.device_id, now)

            shadow["state"].update(msg.state)
            shadow["version"] = msg.version
            shadow["last_source_version"] = msg.version
            shadow["updated_at"] = now
            shadow["last_tuple"] = list(incoming)
            seen.add(incoming)
            trimmed = sorted(seen)[-_DEDUPE_CAP:]
            shadow["seen_tuples"] = [list(t) for t in trimmed]
            self._quota_windows.setdefault(msg.device_id, []).append(now)
            await self.store.save()
            await self._audit(now, msg, incoming, shadow, "merged", "higher order tuple")
            return TelemetryResult(
                accepted=True,
                device_id=msg.device_id,
                current_version=shadow["version"],
                merged_state=dict(shadow["state"]),
                decision="merged",
                reason=f"incoming {incoming} > previous {current}",
            )

    async def get_shadow(self, device_id: str) -> ShadowView:
        if device_id not in self.store.devices:
            raise DeviceNotFoundError(details={"device_id": device_id})
        shadow = self.store.shadows.get(device_id)
        if shadow is None:
            return ShadowView(
                device_id=device_id,
                version=0,
                state={},
                updated_at=0.0,
                last_source_version=None,
            )
        return ShadowView(
            device_id=device_id,
            version=shadow["version"],
            state=dict(shadow["state"]),
            updated_at=shadow["updated_at"],
            last_source_version=shadow.get("last_source_version"),
        )

    async def _lock_for(self, device_id: str) -> asyncio.Lock:
        async with self._guard:
            lock = self._shadow_locks.get(device_id)
            if lock is None:
                lock = asyncio.Lock()
                self._shadow_locks[device_id] = lock
            return lock

    def _enforce_quota(self, device_id: str, now: float) -> None:
        window = self._quota_windows.setdefault(device_id, [])
        cutoff = now - self.quota_window
        i = 0
        for i, ts in enumerate(window):
            if ts > cutoff:
                break
        else:
            i = len(window)
        del window[:i]
        if len(window) >= self.quota:
            retry_after = self.quota_window - (now - window[0])
            raise QuotaExceededError(
                f"quota {self.quota} per {self.quota_window}s exceeded",
                details={
                    "device_id": device_id,
                    "quota": self.quota,
                    "window_seconds": self.quota_window,
                    "retry_after": max(0.0, retry_after),
                },
            )

    async def _audit(
        self, now: float, msg: TelemetryMessage, incoming: tuple, shadow: dict,
        decision: str, reason: str,
    ) -> None:
        await self.store.append_audit(
            {
                "ts": now,
                "kind": "telemetry_decision",
                "device_id": msg.device_id,
                "incoming_tuple": list(incoming),
                "applied_tuple": shadow.get("last_tuple"),
                "decision": decision,
                "reason": reason,
            }
        )

    @staticmethod
    def _new_shadow(device_id: str) -> dict:
        return {
            "device_id": device_id,
            "version": 0,
            "state": {},
            "updated_at": 0.0,
            "last_source_version": None,
            "last_tuple": None,
            "seen_tuples": [],
        }

    @staticmethod
    def _order_tuple(msg: TelemetryMessage) -> tuple:
        """Deterministic total order used for conflict resolution."""
        return (msg.version, msg.timestamp, msg.seq, msg.nonce or "")
