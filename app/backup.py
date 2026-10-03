"""Hot-backup export / fresh-environment restore for the whole gateway state.

Coverage:
- device lifecycle + credential validity (secret hash, issued/expiry, revoked)
- per-device shadow, field versions, accepted counts, rejected records
- firmware campaign progress (status, per-device states, attempts, batches)
- firmware version actually installed on each (simulated) device

Consistency:
- export takes a single cross-component snapshot under a fixed global lock
  order (orchestrator -> registry -> telemetry -> flasher), the only order in
  which the codebase ever nests them, so writes/flashes concurrent with an
  export can never produce a half-new/half-old package;
- restore first stages and fully validates the replacement state on private
  throwaway component instances and only swaps it in under the same locks,
  so a refused import leaves the running state untouched.

Package layout (ZIP):
    manifest.json -> {"format_version": 1, "created_at": ...,
                      "data_file": "data.json", "sha256": <hex>}
    data.json     -> snapshot payload
"""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import time
import zipfile

from .fota import FirmwareOrchestrator
from .models import BackupError, BackupRejection
from .registry import DeviceRegistry
from .simulator import SimulatedFlasher
from .telemetry import TelemetryStore

FORMAT_VERSION = 1
SUPPORTED_FORMAT_VERSIONS = {1}

_DATA_FILE = "data.json"
_MANIFEST_FILE = "manifest.json"
_SECTIONS = ("registry", "telemetry", "fota", "flasher")


class BackupManager:
    def __init__(self, registry: DeviceRegistry, telemetry: TelemetryStore,
                 orchestrator: FirmwareOrchestrator, flasher: SimulatedFlasher,
                 clock=time.time):
        self._registry = registry
        self._telemetry = telemetry
        self._orchestrator = orchestrator
        self._flasher = flasher
        self._clock = clock
        # Optional change observer (restart persistence), notified after a
        # successful restore once every lock has been released.
        self._observer = None

    def _notify_change(self) -> None:
        observer = self._observer
        if observer is not None:
            observer()

    # -- locking ---------------------------------------------------------
    @contextlib.contextmanager
    def _all_locked(self):
        """Acquire every component lock in the single global nesting order.

        The orchestrator is the only component that ever touches another
        component under lock (it writes registry.firmware_version while
        flashing), so it must always be outermost.
        """
        with self._orchestrator._lock, \
                self._registry._lock, \
                self._telemetry._lock, \
                self._flasher._lock:
            yield

    # -- export ----------------------------------------------------------
    def snapshot(self) -> dict:
        """Build one self-consistent snapshot of all four components."""
        with self._all_locked():
            return {
                "format_version": FORMAT_VERSION,
                "created_at": self._clock(),
                "registry": self._registry._snapshot_locked(),
                "telemetry": self._telemetry._snapshot_locked(),
                "fota": self._orchestrator._snapshot_locked(),
                "flasher": {"installed_versions":
                            dict(self._flasher._versions)},
            }

    def export_bytes(self) -> bytes:
        snapshot = self.snapshot()  # I/O happens only after locks release
        payload = json.dumps(snapshot, indent=2, sort_keys=True).encode("utf-8")
        manifest = {
            "format_version": FORMAT_VERSION,
            "created_at": snapshot["created_at"],
            "data_file": _DATA_FILE,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(_MANIFEST_FILE,
                        json.dumps(manifest, indent=2, sort_keys=True))
            zf.writestr(_DATA_FILE, payload)
        return buf.getvalue()

    def export_to_path(self, path: str) -> str:
        raw = self.export_bytes()
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(raw)
        os.replace(tmp, path)
        return path

    # -- package parsing / verification ----------------------------------
    @staticmethod
    def _read_package(raw: bytes) -> tuple[dict, dict]:
        """Return (manifest, payload). Raises BackupError, never raw errors."""
        if not isinstance(raw, (bytes, bytearray)) or not raw:
            raise BackupError(BackupRejection.TRUNCATED,
                              "backup package is empty")
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
        except (zipfile.BadZipFile, OSError) as exc:
            raise BackupError(BackupRejection.TRUNCATED,
                              f"backup is not a readable archive: {exc}") from exc
        try:
            bad = zf.testzip()
            if bad is not None:
                raise BackupError(
                    BackupRejection.TRUNCATED,
                    f"archive member {bad!r} fails CRC check (truncated?)")
            names = set(zf.namelist())
            for required in (_MANIFEST_FILE, _DATA_FILE):
                if required not in names:
                    raise BackupError(
                        BackupRejection.TRUNCATED,
                        f"required member {required!r} missing from archive")
            try:
                manifest = json.loads(zf.read(_MANIFEST_FILE).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BackupError(
                    BackupRejection.TRUNCATED,
                    f"manifest unreadable: {exc}") from exc
            payload_bytes = zf.read(_DATA_FILE)
        except KeyError as exc:
            raise BackupError(BackupRejection.TRUNCATED,
                              f"archive member missing: {exc}") from exc
        finally:
            zf.close()

        if not isinstance(manifest, dict):
            raise BackupError(BackupRejection.TRUNCATED,
                              "manifest is not a JSON object")
        for key in ("format_version", "data_file", "sha256"):
            if key not in manifest:
                raise BackupError(BackupRejection.TRUNCATED,
                                  f"manifest missing key {key!r}")
        if manifest.get("data_file") != _DATA_FILE:
            raise BackupError(BackupRejection.TRUNCATED,
                              "manifest references an unexpected data file")

        # Format version is decided by the (plaintext) manifest header, so a
        # package from a newer/older build is reported as incompatible even if
        # it was truncated or signed under different rules.
        version = manifest["format_version"]
        if version not in SUPPORTED_FORMAT_VERSIONS:
            raise BackupError(
                BackupRejection.FORMAT_UNSUPPORTED,
                f"backup format version {version!r} is not supported by this "
                f"build (supports: {sorted(SUPPORTED_FORMAT_VERSIONS)})")

        digest = hashlib.sha256(payload_bytes).hexdigest()
        if not isinstance(manifest["sha256"], str) or digest != manifest["sha256"]:
            raise BackupError(
                BackupRejection.INTEGRITY_FAILED,
                "payload checksum mismatch: package was tampered with or "
                "transferred incompletely")

        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            # checksum already matched, so this is structurally malformed data
            raise BackupError(BackupRejection.PAYLOAD_INVALID,
                              f"payload is not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise BackupError(BackupRejection.PAYLOAD_INVALID,
                              "payload is not a JSON object")
        if payload.get("format_version") not in SUPPORTED_FORMAT_VERSIONS:
            raise BackupError(
                BackupRejection.FORMAT_UNSUPPORTED,
                f"payload format version {payload.get('format_version')!r} "
                "is not supported")
        return manifest, payload

    def inspect_package(self, raw: bytes) -> dict:
        """Parse and fully validate a package without touching live state.

        Returns a summary; raises BackupError with a distinct BackupRejection
        code when the package must be refused.
        """
        _manifest, payload = self._read_package(raw)
        staged = self._stage(payload)
        return staged["summary"]

    # -- restore ---------------------------------------------------------
    def _stage(self, payload: dict) -> dict:
        """Build throwaway replacement components + run all validation.

        Nothing here mutates live state; every structural problem raises
        BackupError(PAYLOAD_INVALID / MISSING_SECTION).
        """
        missing = [name for name in _SECTIONS if name not in payload]
        if missing:
            raise BackupError(
                BackupRejection.MISSING_SECTION,
                f"backup payload is missing required section(s): {missing}")

        reg = DeviceRegistry(credential_ttl_seconds=self._registry._ttl,
                             clock=self._registry._clock)
        tel = TelemetryStore(per_device_quota=self._telemetry._quota,
                             max_clock_skew_seconds=self._telemetry._max_skew,
                             clock=self._telemetry._clock)
        flasher = SimulatedFlasher()
        # state_path=None: staging must never touch the live state file
        orc = FirmwareOrchestrator(reg, flasher,
                                   max_retries=self._orchestrator._max_retries,
                                   state_path=None,
                                   clock=self._orchestrator._clock)
        try:
            with reg._lock:
                reg._restore_locked(payload["registry"])
            with flasher._lock:
                flasher.restore_locked(payload["flasher"])
            with tel._lock:
                tel._restore_locked(payload["telemetry"])
            with orc._lock:
                orc._restore_locked(payload["fota"])
        except ValueError as exc:
            raise BackupError(BackupRejection.PAYLOAD_INVALID,
                              f"backup payload failed validation: {exc}") from exc

        self._cross_validate(reg, tel, orc, flasher)

        return {
            "registry": reg,
            "telemetry": tel,
            "orchestrator": orc,
            "flasher": flasher,
            "summary": {
                "format_version": payload["format_version"],
                "created_at": payload.get("created_at"),
                "devices": len(reg._devices),
                "shadow_devices": len(tel._shadows),
                "campaigns": len(orc._campaigns),
                "installed_versions": dict(flasher._versions),
            },
        }

    @staticmethod
    def _cross_validate(reg: DeviceRegistry, tel: TelemetryStore,
                        orc: FirmwareOrchestrator, flasher: SimulatedFlasher):
        """Invariants that span more than one component.

        The flasher runs outside the orchestrator lock, so a snapshot taken
        in the "flash applied, bookkeeping not yet finished" window may show
        device IN_PROGRESS / PENDING with the target version already on the
        device but the registry still at the previous one. That state heals
        itself on resume thanks to the idempotent flasher, and is accepted;
        only contradictions that cannot be explained are refused.
        """
        from .models import CampaignStatus, DeviceUpdateState
        problems: list[str] = []
        device_ids = set(reg._devices)
        reconciled: set[str] = set()

        for campaign in orc._campaigns.values():
            unknown = [d for d in campaign.device_ids if d not in device_ids]
            if unknown:
                problems.append(
                    f"campaign {campaign.campaign_id!r} references unknown "
                    f"device(s) {unknown}")
            for dev, state in campaign.device_states.items():
                if dev not in device_ids:
                    continue
                installed = flasher.version_of(dev)
                reg_version = reg._devices[dev].firmware_version
                if state is DeviceUpdateState.SUCCEEDED:
                    if installed != campaign.target_version:
                        problems.append(
                            f"device {dev!r} marked succeeded at "
                            f"{campaign.target_version!r} but installed "
                            f"version is {installed!r}")
                    if reg_version != campaign.target_version:
                        problems.append(
                            f"device {dev!r} marked succeeded but registry "
                            f"reports {reg_version!r}")
                elif (state is DeviceUpdateState.PENDING
                      and campaign.status not in (CampaignStatus.CANCELLED,
                                                  CampaignStatus.FAILED)
                      and installed == campaign.target_version
                      and reg_version != campaign.target_version):
                    # caught mid-flash; resume no-ops the flasher and finishes
                    reconciled.add(dev)

        for dev, device in reg._devices.items():
            if dev in reconciled:
                continue
            installed = flasher.version_of(dev)
            expected = installed if installed is not None else "0.0.0"
            if device.firmware_version != expected:
                problems.append(
                    f"device {dev!r} registry firmware "
                    f"{device.firmware_version!r} disagrees with actually "
                    f"installed {expected!r}")

        for dev in (set(tel._shadows) | set(tel._versions) | set(tel._seen_ids)
                    | set(tel._accepted_count) | set(tel._rejected)):
            if dev not in device_ids:
                problems.append(
                    f"telemetry exists for unknown device {dev!r}")

        if problems:
            raise BackupError(
                BackupRejection.PAYLOAD_INVALID,
                "backup sections are mutually inconsistent: "
                + "; ".join(problems))

    def is_empty(self) -> bool:
        with self._all_locked():
            return self._is_empty_locked()

    def restore_bytes(self, raw: bytes, *,
                      allow_overwrite: bool = False) -> dict:
        _manifest, payload = self._read_package(raw)
        staged = self._stage(payload)
        with self._all_locked():
            if not allow_overwrite and not self._is_empty_locked():
                raise BackupError(
                    BackupRejection.NONEMPTY_TARGET,
                    "refusing to restore into a gateway that already holds "
                    "state; start from a clean data directory or pass "
                    "allow_overwrite=True")
            self._swap_locked(staged)
        # locks released: the restored state is live; make it durable so a
        # restart from here keeps everything the restore just brought in
        self._notify_change()
        return staged["summary"]

    def restore_from_path(self, path: str, *,
                          allow_overwrite: bool = False) -> dict:
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError as exc:
            raise BackupError(BackupRejection.TRUNCATED,
                              f"cannot read backup file {path!r}: {exc}") from exc
        return self.restore_bytes(raw, allow_overwrite=allow_overwrite)

    def restore_snapshot(self, data: dict, *,
                         allow_overwrite: bool = False) -> dict:
        """Restore from an in-memory snapshot (e.g. snapshot() output)."""
        try:
            raw = json.dumps(data).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise BackupError(BackupRejection.PAYLOAD_INVALID,
                              f"snapshot is not serializable: {exc}") from exc
        return self.restore_bytes(raw, allow_overwrite=allow_overwrite)

    def _is_empty_locked(self) -> bool:
        return (not self._registry._devices
                and not self._orchestrator._campaigns
                and not self._telemetry._shadows
                and not self._telemetry._versions
                and not self._telemetry._seen_ids
                and not self._telemetry._accepted_count
                and not self._telemetry._rejected
                and not self._flasher._versions)

    def _swap_locked(self, staged: dict) -> None:
        """Atomically install pre-validated replacement state."""
        reg = staged["registry"]
        tel = staged["telemetry"]
        orc = staged["orchestrator"]
        flasher = staged["flasher"]

        self._registry._devices = reg._devices
        self._telemetry._shadows = tel._shadows
        self._telemetry._versions = tel._versions
        self._telemetry._seen_ids = tel._seen_ids
        self._telemetry._accepted_count = tel._accepted_count
        self._telemetry._rejected = tel._rejected
        self._orchestrator._campaigns = orc._campaigns
        self._orchestrator._persist()  # keep the state file in sync
        self._flasher._versions = flasher._versions
        self._flasher._failures_left = {}
        self._flasher._calls = []
