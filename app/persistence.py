"""Restart persistence: the whole gateway state survives process restarts.

The on-disk file IS a regular backup package (ZIP with manifest + SHA-256,
see app.backup), so boot-time loading reuses the exact same staging,
cross-component validation and atomic swap as a backup import:

- every state mutation is checkpointed atomically (tmp file + os.replace),
  so a crash mid-write can never truncate the last good checkpoint;
- on boot the checkpoint is either loaded completely or, when any
  validation fails, quarantined aside and reported through status():
  the gateway never runs on half-restored state, never silently drops
  devices/shadows, and stays able to accept a backup import afterwards;
- restore-then-restart works because a successful restore is itself a
  state change and is checkpointed immediately.
"""
from __future__ import annotations

import os
import threading
import time

from .backup import BackupManager
from .models import BackupError, BackupRejection


class StateStore:
    """Checkpoint/restore of the full gateway state at a single file path."""

    def __init__(self, backups: BackupManager, path: str, clock=time.time):
        self._backups = backups
        self._path = path
        self._clock = clock
        self._write_lock = threading.Lock()
        self._state = "attached"  # attached -> absent | loaded | error
        self._last_error: str | None = None
        self._quarantined_to: str | None = None
        self._checkpointed_at: float | None = None

    @property
    def path(self) -> str:
        return self._path

    def attach(self) -> "StateStore":
        """Subscribe to every component's change observer.

        All components notify strictly after releasing their own locks, so
        persist() can safely take the global snapshot lock order.
        """
        self._backups._registry._observer = self.persist
        self._backups._telemetry._observer = self.persist
        self._backups._orchestrator._observer = self.persist
        self._backups._flasher._observer = self.persist
        self._backups._observer = self.persist
        return self

    # -- checkpoint --------------------------------------------------------
    def persist(self) -> None:
        """Write one self-consistent snapshot of all components, atomically."""
        raw = self._backups.export_bytes()  # snapshot taken under the locks
        with self._write_lock:              # concurrent mutations may race
            parent = os.path.dirname(os.path.abspath(self._path))
            os.makedirs(parent, exist_ok=True)
            tmp = f"{self._path}.tmp"
            with open(tmp, "wb") as fh:
                fh.write(raw)
            os.replace(tmp, self._path)
            self._checkpointed_at = self._clock()

    # -- boot --------------------------------------------------------------
    def load(self) -> str:
        """Load the checkpoint at boot. Returns absent | loaded | error.

        A rejected checkpoint is quarantined (renamed, never deleted or
        overwritten) and reported; the gateway keeps running empty so a
        backup can still be imported to recover.
        """
        try:
            with open(self._path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            self._state = "absent"
            return self._state
        except OSError as exc:
            self._state = "error"
            self._last_error = f"state file unreadable: {exc}"
            self._quarantine()
            return self._state

        try:
            self._backups.restore_bytes(raw)
        except BackupError as exc:
            self._state = "error"
            self._last_error = f"{exc.code.value}: {exc.message}"
            if exc.code is not BackupRejection.NONEMPTY_TARGET:
                # a *package* problem: move the file aside so later
                # checkpoints never overwrite the evidence
                self._quarantine()
            return self._state
        self._state = "loaded"
        return self._state

    def _quarantine(self) -> None:
        candidate = f"{self._path}.corrupt"
        i = 0
        while os.path.exists(candidate):
            i += 1
            candidate = f"{self._path}.corrupt.{i}"
        try:
            os.replace(self._path, candidate)
            self._quarantined_to = candidate
        except OSError:
            pass

    # -- introspection -----------------------------------------------------
    def status(self) -> dict:
        return {
            "enabled": True,
            "path": self._path,
            "state": self._state,
            "last_error": self._last_error,
            "quarantined_to": self._quarantined_to,
            # a restore/local mutation may have written a checkpoint even
            # when boot found none (state == "absent")
            "checkpointed_at": self._checkpointed_at,
        }
