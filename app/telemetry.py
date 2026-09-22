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


@dataclass
class MergeDecision:
    field: str
    outcome: str  # "applied" | "stale_rejected"
    reason: str


@dataclass
class IngestResult:
    accepted: bool
    code: str  # ErrorCode value or "ok"
    decisions: list[MergeDecision] = field(default_factory=list)


class TelemetryStore:
    def __init__(self, per_device_quota: int = 1000,
                 max_clock_skew_seconds: float = 300.0,
                 clock=time.time):
        self._quota = per_device_quota
        self._max_skew = max_clock_skew_seconds
        self._clock = clock
        self._shadows: dict[str, dict[str, object]] = {}
        self._versions: dict[str, dict[str, FieldVersion]] = {}
        self._seen_ids: dict[str, set[str]] = {}
        self._accepted_count: dict[str, int] = {}
        self._rejected: dict[str, list[IngestResult]] = {}
        self._lock = threading.Lock()

    # -- helpers ---------------------------------------------------------
    def _reject(self, device_id: str, code: ErrorCode) -> IngestResult:
        result = IngestResult(accepted=False, code=code.value)
        self._rejected.setdefault(device_id, []).append(result)
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
