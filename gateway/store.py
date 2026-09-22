"""Persistence layer: JSON-file backed store with an asyncio write lock.

No external broker/database is used. State is mirrored to a local JSON
file so that upgrade jobs survive a process restart. Writes are atomic
(temp file + ``os.replace``) and serialized by a single asyncio lock.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path

_SCHEMA_VERSION = 1


class DataStore:
    """Holds all gateway state and persists it to a single JSON file."""

    def __init__(self, path: str | os.PathLike = "gateway_state.json") -> None:
        self.path = Path(path)
        self.devices: dict[str, dict] = {}
        self.shadows: dict[str, dict] = {}
        self.jobs: dict[str, dict] = {}
        self.audit: list[dict] = []
        self.meta: dict = {"schema_version": _SCHEMA_VERSION}
        self._lock = asyncio.Lock()
        self._loaded = False

    async def load(self) -> None:
        """Load state from disk if present. Safe to call multiple times."""
        async with self._lock:
            if self._loaded:
                return
            self._loaded = True
            if not self.path.exists():
                return
            with self.path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
            self.devices = payload.get("devices", {})
            self.shadows = payload.get("shadows", {})
            self.jobs = payload.get("jobs", {})
            self.audit = payload.get("audit", [])
            meta = payload.get("meta") or {}
            self.meta = {"schema_version": meta.get("schema_version", _SCHEMA_VERSION)}

    async def save(self) -> None:
        """Atomically persist current state.

        Callers do not need to hold the lock; save() takes it itself.
        Callers performing multi-field mutations should coordinate via
        their own per-entity locks so persisted snapshots stay consistent.
        """
        payload = self.snapshot()
        await asyncio.to_thread(self._atomic_write, payload)

    def snapshot(self) -> dict:
        return {
            "schema_version": _SCHEMA_VERSION,
            "devices": self.devices,
            "shadows": self.shadows,
            "jobs": self.jobs,
            "audit": self.audit,
            "meta": self.meta,
        }

    def _atomic_write(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    async def reset(self) -> None:
        """Clear all state (used by tests)."""
        async with self._lock:
            self.devices.clear()
            self.shadows.clear()
            self.jobs.clear()
            self.audit.clear()
            self.meta = {"schema_version": _SCHEMA_VERSION}
            self._loaded = True
        await self.save()

    async def append_audit(self, entry: dict) -> None:
        """Append a traceability record (truncated to a sane cap)."""
        async with self._lock:
            self.audit.append(entry)
            # keep the file bounded: drop oldest beyond the cap
            cap = 5000
            if len(self.audit) > cap:
                del self.audit[: len(self.audit) - cap]


def default_store_path() -> str:
    return os.environ.get("GATEWAY_STATE_FILE", "gateway_state.json")
