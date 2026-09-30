"""Telemetry ingest + device shadow merge with deterministic conflict rules.

Rules (deterministic, auditable):
- Idempotency: message_id already seen -> "duplicate_message", no state change,
  no quota consumed.
- Validation: payload must be a non-empty dict of scalar values; device_ts must
  not be in the future beyond max_clock_skew_seconds. Failures are recorded in
  the per-device rejected log (never silently dropped).
- Quota: each device may accept at most per_device_quota distinct valid
  messages; excess -> "quota_exceeded", recorded in the rejected log.
- Shadow merge: per-field last-writer-wins ordered by (device_ts, seq).
  A field is applied iff its (device_ts, seq) is strictly greater than the
  stored version, so out-of-order / time-shifted / replayed messages can never
  roll state back or silently overwrite newer data. Every field-level decision
  is returned with the compared versions as the audit trail.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .models import ErrorCode


@dataclass
class FieldVersion:
    """Version stamp used for last-writer-wins per shadow field."""
    device_ts: float
    seq: int

    def as_tuple(self) -> tuple[float, int]:
        return (self.device_ts, self.seq)

    def to_dict(self) -> dict:
        return {"device_ts": self.device_ts, "seq": self.seq}

    @classmethod
    def from_dict(cls, data: dict) -> "FieldVersion":
        device_ts = data["device_ts"]
        seq = data["seq"]
        if (not isinstance(device_ts, (int, float))
                or isinstance(device_ts, bool) or device_ts < 0):
            raise ValueError("field_version.device_ts must be a number >= 0")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValueError("field_version.seq must be an int >= 0")
        return cls(device_ts=float(device_ts), seq=seq)


@dataclass
class MergeDecision:
    field: str
    outcome: str  # "applied" | "stale_rejected"
    reason: str

    def to_dict(self) -> dict:
        return {"field": self.field, "outcome": self.outcome,
                "reason": self.reason}

    @classmethod
    def from_dict(cls, data: dict) -> "MergeDecision":
        field = data["field"]
        outcome = data["outcome"]
        if not isinstance(field, str) or not field:
            raise ValueError("decision.field must be a non-empty string")
        if outcome not in ("applied", "stale_rejected"):
            raise ValueError("decision.outcome must be applied|stale_rejected")
        if not isinstance(data["reason"], str):
            raise ValueError("decision.reason must be a string")
        return cls(field=field, outcome=outcome, reason=data["reason"])


@dataclass
class IngestResult:
    accepted: bool
    code: str  # ErrorCode value or "ok"
    decisions: list[MergeDecision] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"accepted": self.accepted, "code": self.code,
                "decisions": [d.to_dict() for d in self.decisions]}

    @classmethod
    def from_dict(cls, data: dict) -> "IngestResult":
        accepted = data["accepted"]
        code = data["code"]
        if not isinstance(accepted, bool):
            raise ValueError("rejected.accepted must be a bool")
        if not isinstance(code, str) or not code:
            raise ValueError("rejected.code must be a non-empty string")
        decisions = [MergeDecision.from_dict(d) for d in data.get("decisions", [])]
        return cls(accepted=accepted, code=code, decisions=decisions)


class TelemetryStore:
    def __init__(self, per_device_quota: int = 1000,
                 max_clock_skew_seconds: float = 300.0,
                 clock=time.time, state_path: str | None = None):
        self._quota = per_device_quota
        self._max_skew = max_clock_skew_seconds
        self._clock = clock
        self._shadows: dict[str, dict[str, object]] = {}
        self._versions: dict[str, dict[str, FieldVersion]] = {}
        self._seen_ids: dict[str, set[str]] = {}
        self._accepted_count: dict[str, int] = {}
        self._rejected: dict[str, list[IngestResult]] = {}
        self._lock = threading.Lock()
        self._state_path = state_path
        if state_path:
            self.recover()

    # -- helpers ---------------------------------------------------------
    def _reject(self, device_id: str, code: ErrorCode) -> IngestResult:
        result = IngestResult(accepted=False, code=code.value)
        self._rejected.setdefault(device_id, []).append(result)
        self._persist()
        return result

    def _validate(self, device_id: str, message_id: str, seq: int,
                  device_ts: float, payload: object) -> IngestResult | None:
        if not message_id:
            return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        if not isinstance(seq, int) or seq < 0:
            return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        if not isinstance(device_ts, (int, float)) or device_ts < 0:
            return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        if device_ts > self._clock() + self._max_skew:
            return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        if not isinstance(payload, dict) or not payload:
            return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        for key, value in payload.items():
            if not isinstance(key, str) or not key:
                return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
            if not isinstance(value, (str, int, float, bool)) and value is not None:
                return self._reject(device_id, ErrorCode.VALIDATION_FAILED)
        return None

    # -- public API ------------------------------------------------------
    def ingest(self, device_id: str, message_id: str, seq: int,
               device_ts: float, payload: dict) -> IngestResult:
        with self._lock:
            invalid = self._validate(device_id, message_id, seq, device_ts, payload)
            if invalid is not None:
                return invalid

            seen = self._seen_ids.setdefault(device_id, set())
            if message_id in seen:
                # Idempotent replay: success, no state change, no quota spent.
                return IngestResult(accepted=True,
                                    code=ErrorCode.DUPLICATE_MESSAGE.value)

            count = self._accepted_count.get(device_id, 0)
            if count >= self._quota:
                return self._reject(device_id, ErrorCode.QUOTA_EXCEEDED)

            seen.add(message_id)
            self._accepted_count[device_id] = count + 1

            shadow = self._shadows.setdefault(device_id, {})
            versions = self._versions.setdefault(device_id, {})
            incoming = FieldVersion(device_ts=device_ts, seq=seq)
            decisions: list[MergeDecision] = []
            for key, value in payload.items():
                stored = versions.get(key)
                if stored is None or incoming.as_tuple() > stored.as_tuple():
                    shadow[key] = value
                    versions[key] = incoming
                    decisions.append(MergeDecision(
                        field=key, outcome="applied",
                        reason=(f"incoming (ts={device_ts}, seq={seq}) > stored "
                                f"{stored.as_tuple() if stored else None}")))
                else:
                    decisions.append(MergeDecision(
                        field=key, outcome="stale_rejected",
                        reason=(f"incoming (ts={device_ts}, seq={seq}) <= stored "
                                f"{stored.as_tuple()}; kept "
                                f"{shadow[key]!r}")))
            self._persist()
            return IngestResult(accepted=True, code="ok", decisions=decisions)

    def shadow(self, device_id: str) -> dict:
        with self._lock:
            return dict(self._shadows.get(device_id, {}))

    def field_versions(self, device_id: str) -> dict[str, tuple[float, int]]:
        with self._lock:
            return {k: v.as_tuple()
                    for k, v in self._versions.get(device_id, {}).items()}

    def rejected(self, device_id: str) -> list[IngestResult]:
        with self._lock:
            return list(self._rejected.get(device_id, []))

    def accepted_count(self, device_id: str) -> int:
        with self._lock:
            return self._accepted_count.get(device_id, 0)

    # -- backup/restore (caller holds self._lock) ------------------------
    def snapshot_locked(self) -> dict:
        return {"devices": {
            dev: {
                "shadow": dict(self._shadows.get(dev, {})),
                "versions": {k: v.to_dict()
                             for k, v in self._versions.get(dev, {}).items()},
                "seen_ids": sorted(self._seen_ids.get(dev, set())),
                "accepted_count": self._accepted_count.get(dev, 0),
                "rejected": [r.to_dict() for r in self._rejected.get(dev, [])],
            }
            for dev in self._shadows | self._versions | self._seen_ids
            | self._accepted_count | self._rejected
        }}

    @staticmethod
    def _valid_scalar(value: object) -> bool:
        return (value is None
                or isinstance(value, (str, int, float, bool)))

    def restore_locked(self, data: dict) -> int:
        """Replace all telemetry state from validated backup data."""
        shadows: dict[str, dict[str, object]] = {}
        versions: dict[str, dict[str, FieldVersion]] = {}
        seen_ids: dict[str, set[str]] = {}
        counts: dict[str, int] = {}
        rejected: dict[str, list[IngestResult]] = {}
        for dev, rec in data["devices"].items():
            if not isinstance(dev, str) or not dev:
                raise ValueError("telemetry device id must be a non-empty string")
            shadow = rec["shadow"]
            if not isinstance(shadow, dict) or any(
                    not isinstance(k, str) or not k
                    or not self._valid_scalar(v)
                    for k, v in shadow.items()):
                raise ValueError(f"bad shadow for device {dev!r}")
            vmap = rec["versions"]
            if not isinstance(vmap, dict) or any(
                    not isinstance(k, str) or not k for k in vmap):
                raise ValueError(f"bad field_versions for device {dev!r}")
            fv = {k: FieldVersion.from_dict(v) for k, v in vmap.items()}
            if set(fv) != set(shadow):
                raise ValueError(
                    f"shadow/field_versions key mismatch for device {dev!r}")
            ids = rec["seen_ids"]
            if (not isinstance(ids, list) or any(
                    not isinstance(i, str) or not i for i in ids)
                    or len(set(ids)) != len(ids)):
                raise ValueError(f"bad seen_ids for device {dev!r}")
            count = rec["accepted_count"]
            if (not isinstance(count, int) or isinstance(count, bool)
                    or count < 0):
                raise ValueError(f"bad accepted_count for device {dev!r}")
            rej = [IngestResult.from_dict(r) for r in rec["rejected"]]
            shadows[dev] = dict(shadow)
            versions[dev] = fv
            seen_ids[dev] = set(ids)
            counts[dev] = count
            rejected[dev] = rej
        self._shadows = shadows
        self._versions = versions
        self._seen_ids = seen_ids
        self._accepted_count = counts
        self._rejected = rejected
        self._persist()
        return len(shadows)

    # -- persistence (same atomic-temp-replace pattern as the other stores)
    def _persist(self) -> None:
        if not self._state_path:
            return
        import json
        import os
        directory = os.path.dirname(os.path.abspath(self._state_path))
        os.makedirs(directory, exist_ok=True)
        tmp = self._state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.snapshot_locked(), fh, indent=2)
        os.replace(tmp, self._state_path)

    def recover(self) -> None:
        """Reload persisted state after a process restart."""
        import json
        import os
        if not self._state_path or not os.path.exists(self._state_path):
            return
        with open(self._state_path, encoding="utf-8") as fh:
            data = json.load(fh)
        with self._lock:
            self.restore_locked(data)
