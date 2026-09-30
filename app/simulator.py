"""Local simulated device fleet: no external broker, devices, or credentials.

- SimulatedFlasher is an idempotent device-side firmware applier: a device
  already at the target version is a no-op success, so re-dispatch after a
  restart never re-delivers firmware. Scripted failures model flaky devices.
- TelemetryGenerator produces deterministic out-of-order / duplicated /
  time-shifted message streams for local verification.
"""
from __future__ import annotations

import threading


class SimulatedFlasher:
    """Idempotent firmware applier with scriptable failures."""

    def __init__(self):
        self._versions: dict[str, str] = {}
        self._failures_left: dict[str, int] = {}
        self._calls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def fail_next(self, device_id: str, times: int) -> None:
        """Script the next `times` flash attempts of a device to fail."""
        with self._lock:
            self._failures_left[device_id] = times

    def version_of(self, device_id: str) -> str | None:
        with self._lock:
            return self._versions.get(device_id)

    def calls(self) -> list[tuple[str, str]]:
        with self._lock:
            return list(self._calls)

    def __call__(self, device_id: str, target_version: str) -> None:
        with self._lock:
            self._calls.append((device_id, target_version))
            if self._versions.get(device_id) == target_version:
                return  # idempotent: already flashed, do not re-deliver
            left = self._failures_left.get(device_id, 0)
            if left > 0:
                self._failures_left[device_id] = left - 1
                raise RuntimeError(f"simulated flash failure on {device_id}")
            self._versions[device_id] = target_version

    # -- backup / restore ------------------------------------------------
    def snapshot(self) -> dict:
        """Only the actually-installed versions are durable runtime state;
        scripted failures and call logs are test scaffolding."""
        with self._lock:
            return {"installed_versions": dict(self._versions)}

    def restore_locked(self, data: dict) -> None:
        raw = data.get("installed_versions")
        if not isinstance(raw, dict):
            raise ValueError("flasher.installed_versions must be an object")
        versions = {}
        for device_id, version in raw.items():
            if not isinstance(device_id, str) or not device_id:
                raise ValueError("malformed installed version: bad device id")
            if not isinstance(version, str) or not version:
                raise ValueError("malformed installed version: bad version")
            versions[device_id] = version
        self._versions = versions
        self._failures_left = {}
        self._calls = []


class TelemetryGenerator:
    """Builds deterministic message streams incl. reorder/duplicates/skew."""

    @staticmethod
    def shuffled_stream(device_id: str, count: int, base_ts: float,
                        duplicates: int = 0, skew_seconds: float = 0.0):
        """Yield (device_id, message_id, seq, device_ts, payload) tuples in
        reversed order (worst-case reordering), with optional exact duplicate
        replays appended and a constant clock offset applied."""
        messages = []
        for seq in range(count):
            messages.append((device_id, f"{device_id}-msg-{seq}", seq,
                             base_ts + seq + skew_seconds,
                             {"seq_gauge": seq, "device": device_id}))
        stream = list(reversed(messages))
        stream.extend(messages[:duplicates])  # replays arrive last
        return stream
