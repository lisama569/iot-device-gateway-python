"""Durable full-gateway state across process restarts / redeployments.

Why this exists
---------------
The online backup package covers *every* component (registry, telemetry,
FOTA campaigns, flasher), but restoring one used to leave all but the FOTA
file in memory only: a single restart of the "new machine" produced an
empty gateway whose lone campaigns referenced devices that no longer
existed. This coordinator makes the *whole* snapshot durable:

- low-rate, crash-critical changes (device/credential mutations and each
  per-device campaign step, including the version actually installed) are
  flushed *synchronously* before the mutating call returns;
- high-rate telemetry accepts are *coalesced* by a short debounce window
  (GATEWAY_STATE_FLUSH_INTERVAL_SECONDS, default 0.1s) so ingest keeps up
  with traffic, and are always included in the next urgent snapshot and
  force-flushed on graceful shutdown (SIGTERM/SIGINT -> lifespan stop);
- every snapshot is one complete ZIP written via temp-file + os.replace
  under the fixed global lock order, while `state.zip.bak` always holds
  the previous complete snapshot, so a crash at any instant leaves a
  self-consistent whole -- never a half-written file;
- startup loads the snapshot through the exact same "stage -> validate ->
  swap" pipeline as backup restore. RUNNING campaigns are parked and
  interrupted devices reset exactly as on import;
- if neither the snapshot nor its predecessor can be read or validated,
  the gateway comes up *faulted* (business API refuses with a clear error
  instead of silently serving an empty gateway; importing a backup clears
  the fault). Repeated restarts converge to identical state.
"""
from __future__ import annotations

import atexit
import os
import shutil
import threading
import time

from .models import ErrorCode, GatewayError


class DurableState:
    def __init__(self, backups, path: str | None,
                 debounce_seconds: float = 0.1):
        self._backups = backups
        self._path = path
        self._debounce = max(0.0, float(debounce_seconds))
        self._faulted: str | None = None        # startup-time unusable store
        self._async_error: str | None = None    # last background write error
        self._recovered_from: str | None = None
        self._suspended = False                 # while restore swaps a package
        self._sources = self._candidate_paths(path)

        self._io_lock = threading.RLock()       # serializes snapshot writes
        self._wake = threading.Event()
        self._stopping = False
        self._worker: threading.Thread | None = None
        if path:
            self._worker = threading.Thread(
                target=self._flush_loop, name="gateway-durable-flusher",
                daemon=True)
            self._worker.start()
            atexit.register(self._atexit_flush)

    @staticmethod
    def _candidate_paths(path: str | None) -> list[str]:
        if not path:
            return []
        return [path, path + ".bak"]

    # -- introspection ---------------------------------------------------
    @property
    def faulted(self) -> bool:
        return self._faulted is not None

    @property
    def path(self) -> str | None:
        return self._path

    def status(self) -> dict:
        return {
            "healthy": self._faulted is None,
            "state_path": self._path,
            "recovered_from": self._recovered_from,
            "fault": self._faulted,
        }

    def require_healthy(self) -> None:
        """Gate for business endpoints: never serve traffic on a broken store."""
        if self._faulted is not None:
            raise GatewayError(
                ErrorCode.STATE_UNAVAILABLE,
                "gateway durable state is unusable: " + self._faulted
                + "; restore a backup via POST /admin/backup/restore to recover")

    # -- mutation hooks --------------------------------------------------
    def flush_urgent(self) -> None:
        """Synchronously persist the current whole snapshot.

        Used by registry/credential changes and by every per-device FOTA
        progress step: those are rare and crash-critical. Any telemetry
        coalesced-but-not-yet-written is part of the snapshot too.
        """
        self._flush(raise_async_fault=True)

    def flush_soon(self) -> None:
        """Schedule a coalesced snapshot (high-rate telemetry path).

        Surfaces a previously failed background write immediately so the
        caller can roll the just-applied in-memory change back; a startup
        fault is likewise refused before memory can diverge from disk.
        """
        if not self._path:
            return
        if self._faulted is not None:
            raise GatewayError(
                ErrorCode.STATE_UNAVAILABLE,
                "cannot persist: durable state is faulted (" + self._faulted
                + "); restore a backup to recover")
        if self._async_error is not None:
            err = self._async_error
            raise GatewayError(
                ErrorCode.STATE_PERSIST_FAILED,
                f"previous durable write failed: {err}; refusing further "
                "changes until the state directory is writable again")
        self._wake.set()

    # -- background coalescing -------------------------------------------
    def _flush_loop(self) -> None:
        while not self._stopping:
            # idle sleep (no disk writes while nothing happens); return on
            # first event, then hold for the debounce window to coalesce
            if not self._wake.wait(timeout=0.25):
                continue
            self._wake.clear()
            if self._debounce:
                time.sleep(self._debounce)
            self._wake.clear()
            try:
                self._flush(raise_async_fault=True)
                self._async_error = None
            except GatewayError as exc:
                # don't kill the worker; remember the cause so the next
                # mutation surfaces it synchronously
                self._async_error = exc.message

    def sync(self) -> None:
        """Block until everything scheduled so far has been persisted."""
        if not self._path:
            return
        self._wake.set()
        self._flush(raise_async_fault=True)
        self._wake.clear()
        self._async_error = None

    def shutdown(self) -> None:
        """Graceful stop: force one final complete snapshot out."""
        if not self._path:
            return
        self._stopping = True
        self._wake.set()
        try:
            self._flush(raise_async_fault=False)
        except Exception:
            pass  # best effort: the previous complete snapshot still stands
        if self._worker is not None:
            self._worker.join(timeout=2.0)

    def _atexit_flush(self) -> None:
        try:
            self.shutdown()
        except Exception:
            pass

    def _flush(self, *, raise_async_fault: bool) -> None:
        if not self._path or self._suspended:
            return
        if self._faulted is not None:
            # Never overwrite a store we could not read at startup; an admin
            # restore is the documented way back.
            raise GatewayError(
                ErrorCode.STATE_UNAVAILABLE,
                "cannot persist: durable state is faulted (" + self._faulted
                + "); restore a backup to recover")
        with self._io_lock:
            try:
                raw = self._backups.export_bytes()
                self._write_atomic(self._path, raw)
            except GatewayError:
                raise
            except OSError as exc:
                if raise_async_fault:
                    raise GatewayError(
                        ErrorCode.STATE_PERSIST_FAILED,
                        f"could not persist gateway state to {self._path!r}: "
                        f"{exc}") from exc
            else:
                self._async_error = None

    # -- startup ---------------------------------------------------------
    def load_at_startup(self) -> dict | None:
        """Load & install the best available snapshot at process start.

        Tries the primary file then its .bak predecessor. Returns the
        restore summary of the installed snapshot, or None when there is
        simply no state yet (fresh data directory). When every candidate
        is unreadable/invalid the gateway is marked faulted.
        """
        if not self._path:
            return None
        errors: list[str] = []
        for source in self._sources:
            if not os.path.exists(source):
                continue
            try:
                with open(source, "rb") as fh:
                    raw = fh.read()
                summary = self._backups.install_snapshot_bytes(raw)
            except Exception as exc:  # reported via the fault, not swallowed
                errors.append(f"{os.path.basename(source)}: {exc}")
                continue
            # success: make the normalized whole the new canonical snapshot,
            # and refresh the predecessor so future restarts converge too
            try:
                self._persist_primary_and_bak()
            except OSError:
                pass
            self._recovered_from = source
            self._faulted = None
            self._async_error = None
            return summary
        if errors:
            self._faulted = ("; ".join(errors)
                             or "no readable durable state file found")
        return None

    def _persist_primary_and_bak(self) -> None:
        raw = self._backups.export_bytes()
        self._write_atomic(self._path, raw)
        shutil.copyfile(self._path, self._path + ".bak")

    # -- restore integration ---------------------------------------------
    def commit_package(self, raw: bytes) -> None:
        """Durably install an already-validated backup package's bytes.

        Called by BackupManager between validation and the in-memory swap:
        the durable file is replaced FIRST, so an import that crashes the
        process comes back up with exactly the imported (whole) state -- and
        a disk failure aborts the import before live state is touched.
        """
        if not self._path:
            return
        self._suspended = True
        try:
            with self._io_lock:
                self._write_atomic(self._path, raw)
                shutil.copyfile(self._path, self._path + ".bak")
            self._faulted = None
            self._async_error = None
            self._wake.clear()
        finally:
            self._suspended = False

    # -- internals -------------------------------------------------------
    @staticmethod
    def _write_atomic(path: str, raw: bytes) -> None:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(raw)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
