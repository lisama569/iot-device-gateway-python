"""Online full-state backup export and restore for gateway migration/DR.

A backup is a single self-contained JSON envelope:

    {
      "format": "iot-gateway-backup",
      "format_version": 1,
      "created_at": <epoch seconds>,
      "checksum": "<sha256 hex of the canonical-JSON payload>",
      "payload": {
        "registry":   {devices: [...]},   # lifecycle, credential validity/expiry/revocation, version
        "telemetry":  {devices: {...}},   # shadow, field versions, seen ids, rejected log
        "campaigns":  {campaigns: [...]}, # FOTA progress per device
        "flasher":    {versions: {...}}   # actual firmware installed on each device
      }
    }

Consistency: export takes every component lock in one fixed global order
(orchestrator -> registry -> telemetry -> flasher), so the snapshot is a
single atomic cross-component state even while devices register, ingest
telemetry or campaigns dispatch; it can never be half-old/half-new.

Safety: restore fully parses, checksum-verifies and cross-validates the
envelope against *throwaway* components before touching any live state, then
swaps all four components under the same lock order. A rejected backup
therefore cannot damage the running state, and re-importing the same file is
a deterministic full-replace (idempotent result).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time

from .fota import FirmwareOrchestrator
from .models import (DeviceUpdateState, ErrorCode, GatewayError,
                     TERMINAL_STATES)
from .registry import DeviceRegistry
from .simulator import SimulatedFlasher
from .telemetry import TelemetryStore

BACKUP_FORMAT = "iot-gateway-backup"
SUPPORTED_FORMAT_VERSIONS = (1,)

# Fixed global lock-acquisition order; must match the nesting direction used
# by the components themselves (fota takes orchestrator->registry; the
# flasher is only ever called with no other component lock held).
_ENVELOPE_KEYS = ("format", "format_version", "created_at", "checksum",
                  "payload")
_PAYLOAD_SECTIONS = ("registry", "telemetry", "campaigns", "flasher")


def _canonical(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def _checksum(payload: dict) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


class BackupCoordinator:
    """Exports/imports the combined state of registry, telemetry and FOTA."""

    def __init__(self, registry: DeviceRegistry, telemetry: TelemetryStore,
                 orchestrator: FirmwareOrchestrator, flasher: SimulatedFlasher,
                 clock=time.time):
        self._registry = registry
        self._telemetry = telemetry
        self._orchestrator = orchestrator
        self._flasher = flasher
        self._clock = clock

    # -- export -----------------------------------------------------------
    def _snapshot_locked(self) -> dict:
        return {
            "registry": self._registry.snapshot_locked(),
            "telemetry": self._telemetry.snapshot_locked(),
            "campaigns": self._orchestrator.snapshot_locked(),
            "flasher": self._flasher.snapshot_locked(),
        }

    def export_bytes(self) -> bytes:
        """Return the backup envelope as canonical JSON bytes."""
        with self._orchestrator._lock, self._registry._lock, \
                self._telemetry._lock, self._flasher._lock:
            payload = self._snapshot_locked()
        envelope = {
            "format": BACKUP_FORMAT,
            "format_version": 1,
            "created_at": self._clock(),
            "checksum": _checksum(payload),
            "payload": payload,
        }
        return json.dumps(envelope, indent=2, ensure_ascii=False).encode("utf-8")

    def export_to_file(self, path: str) -> dict:
        """Write a consistent full backup to `path` via atomic temp+replace."""
        raw = self.export_bytes()
        tmp = path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(raw)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        counts = self._counts(json.loads(raw.decode("utf-8"))["payload"])
        counts["path"] = path
        return counts

    # -- restore ----------------------------------------------------------
    def restore_bytes(self, raw: bytes | str) -> dict:
        """Validate and apply a backup; returns a restore summary.

        Raises GatewayError with a distinct code for each rejection reason:
        backup_corrupted / backup_integrity_failed /
        backup_version_unsupported / backup_incomplete.
        """
        envelope = self._parse_envelope(raw)
        payload = envelope["payload"]
        parsed = self._validate_payload(payload)

        # All checks passed: swap live state atomically under the same global
        # lock order used by export. Nothing above mutated live components.
        with self._orchestrator._lock, self._registry._lock, \
                self._telemetry._lock, self._flasher._lock:
            n_devices = self._registry.restore_locked(payload["registry"])
            n_tel = self._telemetry.restore_locked(payload["telemetry"])
            self._orchestrator.restore_locked(payload["campaigns"])
            n_versions = self._flasher.restore_locked(payload["flasher"])
        return {
            "format_version": envelope["format_version"],
            "restored_at": self._clock(),
            "devices": n_devices,
            "telemetry_devices": n_tel,
            "campaigns": len(parsed.campaigns),
            "installed_versions": n_versions,
        }

    def restore_from_file(self, path: str) -> dict:
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError as exc:
            raise GatewayError(
                ErrorCode.BACKUP_CORRUPTED,
                f"cannot read backup file {path!r}: {exc}") from exc
        return self.restore_bytes(raw)

    # -- validation -------------------------------------------------------
    def _parse_envelope(self, raw: bytes | str) -> dict:
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not isinstance(raw, (bytes, bytearray)):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "backup must be JSON bytes")
        try:
            envelope = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            # Truncated files, binary garbage, broken JSON all land here.
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               f"backup is not valid JSON: {exc}") from exc
        if not isinstance(envelope, dict):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "backup envelope must be a JSON object")
        missing = [k for k in _ENVELOPE_KEYS if k not in envelope]
        if missing:
            raise GatewayError(
                ErrorCode.BACKUP_CORRUPTED,
                f"backup envelope missing key(s): {', '.join(missing)}")
        if envelope["format"] != BACKUP_FORMAT:
            raise GatewayError(
                ErrorCode.BACKUP_CORRUPTED,
                f"unrecognized backup format {envelope['format']!r}")
        checksum = envelope["checksum"]
        if (not isinstance(checksum, str) or len(checksum) != 64
                or any(c not in "0123456789abcdef" for c in checksum)):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "checksum must be a 64-char sha256 hex string")
        if not isinstance(envelope["payload"], dict):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "payload must be a JSON object")
        # Content tampering (payload edited after sealing) is distinguishable
        # from structural corruption: the JSON parses, but the seal is broken.
        actual = _checksum(envelope["payload"])
        if not hmac.compare_digest(actual, checksum):
            raise GatewayError(
                ErrorCode.BACKUP_INTEGRITY_FAILED,
                f"checksum mismatch: expected {checksum}, payload has {actual}")
        version = envelope["format_version"]
        if not isinstance(version, int) or isinstance(version, bool):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "format_version must be an integer")
        if version not in SUPPORTED_FORMAT_VERSIONS:
            raise GatewayError(
                ErrorCode.BACKUP_VERSION_UNSUPPORTED,
                f"backup format_version {version} is not supported; "
                f"supported: {', '.join(map(str, SUPPORTED_FORMAT_VERSIONS))}")
        if not isinstance(envelope["created_at"], (int, float)) \
                or isinstance(envelope["created_at"], bool):
            raise GatewayError(ErrorCode.BACKUP_CORRUPTED,
                               "created_at must be a number")
        return envelope

    def _validate_payload(self, payload: dict) -> "_ParsedBackup":
        missing = [k for k in _PAYLOAD_SECTIONS if k not in payload]
        if missing:
            raise GatewayError(
                ErrorCode.BACKUP_INCOMPLETE,
                f"payload missing required section(s): {', '.join(missing)}")
        # Parse into throwaway components first: any structural or value
        # error rejects the import before live state is touched.
        try:
            tmp_registry = DeviceRegistry()
            n_devices = tmp_registry.restore_locked(payload["registry"])

            tmp_telemetry = TelemetryStore()
            n_tel = tmp_telemetry.restore_locked(payload["telemetry"])

            tmp_flasher = SimulatedFlasher()
            n_versions = tmp_flasher.restore_locked(payload["flasher"])

            tmp_orc = FirmwareOrchestrator(tmp_registry, tmp_flasher)
            tmp_orc.restore_locked(payload["campaigns"])
        except (KeyError, ValueError, TypeError) as exc:
            raise GatewayError(
                ErrorCode.BACKUP_INCOMPLETE,
                f"backup payload is missing or has invalid information: {exc}") \
                from exc

        parsed = _ParsedBackup(
            registry=tmp_registry, telemetry=tmp_telemetry,
            orchestrator=tmp_orc, flasher=tmp_flasher,
            devices=n_devices, telemetry_devices=n_tel,
            installed_versions=n_versions)
        self._cross_validate(parsed)
        return parsed

    def _cross_validate(self, parsed: "_ParsedBackup") -> None:
        """Cross-section referential and invariant checks."""
        device_ids = set(parsed.registry._devices)

        tel_ids = set(parsed.telemetry._shadows)
        unknown = tel_ids - device_ids
        if unknown:
            raise GatewayError(
                ErrorCode.BACKUP_INCOMPLETE,
                f"telemetry references unknown device(s): {sorted(unknown)}")

        flash_ids = set(parsed.flasher._versions)
        unknown = flash_ids - device_ids
        if unknown:
            raise GatewayError(
                ErrorCode.BACKUP_INCOMPLETE,
                f"flasher references unknown device(s): {sorted(unknown)}")

        busy: dict[str, str] = {}
        for campaign in parsed.orchestrator._campaigns.values():
            cid = campaign.campaign_id
            if set(campaign.device_states) != set(campaign.device_ids):
                raise GatewayError(
                    ErrorCode.BACKUP_INCOMPLETE,
                    f"campaign {cid!r} device_states does not cover "
                    "exactly its target devices")
            if set(campaign.attempts) != set(campaign.device_ids):
                raise GatewayError(
                    ErrorCode.BACKUP_INCOMPLETE,
                    f"campaign {cid!r} attempts does not cover its targets")
            if campaign.batch_size < 1:
                raise GatewayError(ErrorCode.BACKUP_INCOMPLETE,
                                   f"campaign {cid!r} has batch_size < 1")
            n_batches = len(campaign.batches())
            if not 0 <= campaign.current_batch <= n_batches:
                raise GatewayError(
                    ErrorCode.BACKUP_INCOMPLETE,
                    f"campaign {cid!r} current_batch out of range")
            unknown = set(campaign.device_ids) - device_ids
            if unknown:
                raise GatewayError(
                    ErrorCode.BACKUP_INCOMPLETE,
                    f"campaign {cid!r} targets unknown device(s): "
                    f"{sorted(unknown)}")

            # Restore must preserve the device-busy domain invariant.
            if campaign.status not in TERMINAL_STATES:
                for dev in campaign.device_ids:
                    state = campaign.device_states[dev]
                    if state in (DeviceUpdateState.PENDING,
                                 DeviceUpdateState.IN_PROGRESS):
                        if dev in busy:
                            raise GatewayError(
                                ErrorCode.BACKUP_INCOMPLETE,
                                f"device {dev!r} pending in two live "
                                f"campaigns {busy[dev]!r} and {cid!r}")
                        busy[dev] = cid

            # A SUCCEEDED device must actually carry the target firmware in
            # both views; otherwise resume could leave it un-flashed forever.
            target = campaign.target_version
            for dev, state in campaign.device_states.items():
                if state is DeviceUpdateState.SUCCEEDED:
                    device = parsed.registry._devices[dev]
                    if device.firmware_version != target:
                        raise GatewayError(
                            ErrorCode.BACKUP_INCOMPLETE,
                            f"campaign {cid!r}: device {dev!r} marked "
                            f"succeeded but registry version "
                            f"{device.firmware_version!r} != target "
                            f"{target!r}")
                    if parsed.flasher._versions.get(dev) != target:
                        raise GatewayError(
                            ErrorCode.BACKUP_INCOMPLETE,
                            f"campaign {cid!r}: device {dev!r} marked "
                            "succeeded but device-side firmware differs "
                            f"from target {target!r}")

    @staticmethod
    def _counts(payload: dict) -> dict:
        return {
            "devices": len(payload["registry"]["devices"]),
            "telemetry_devices": len(payload["telemetry"]["devices"]),
            "campaigns": len(payload["campaigns"]["campaigns"]),
            "installed_versions": len(payload["flasher"]["versions"]),
        }


class _ParsedBackup:
    """Bundle of throwaway components produced during import validation."""

    def __init__(self, registry: DeviceRegistry, telemetry: TelemetryStore,
                 orchestrator: FirmwareOrchestrator, flasher: SimulatedFlasher,
                 devices: int, telemetry_devices: int,
                 installed_versions: int):
        self.registry = registry
        self.telemetry = telemetry
        self.orchestrator = orchestrator
        self.flasher = flasher
        self.campaigns = list(orchestrator._campaigns.values())
        self.devices = devices
        self.telemetry_devices = telemetry_devices
        self.installed_versions = installed_versions
