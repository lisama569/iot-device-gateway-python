"""Device registry: registration (idempotent), credential lifecycle, enable/disable."""
from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass

from .models import DeviceStatus, ErrorCode, GatewayError


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


@dataclass
class Credential:
    secret_hash: str
    issued_at: float
    expires_at: float | None
    revoked: bool = False


@dataclass
class Device:
    device_id: str
    status: DeviceStatus
    credential: Credential
    registered_at: float
    firmware_version: str = "0.0.0"


class DeviceRegistry:
    """In-memory registry.

    - register() is idempotent: same (device_id, secret) -> same Device;
      same device_id with a different secret -> DEVICE_ID_CONFLICT.
    - authenticate() distinguishes: not found / disabled / deregistered /
      wrong secret / expired / revoked.
    - disable/enable/deregister take effect on the very next authenticate().
    """

    def __init__(self, credential_ttl_seconds: float | None = None,
                 clock=time.time):
        self._ttl = credential_ttl_seconds
        self._clock = clock
        self._devices: dict[str, Device] = {}
        self._lock = threading.Lock()

    def register(self, device_id: str, secret: str) -> Device:
        if not device_id or not secret:
            raise GatewayError(ErrorCode.VALIDATION_FAILED,
                               "device_id and secret must be non-empty")
        with self._lock:
            existing = self._devices.get(device_id)
            if existing is not None and existing.status is not DeviceStatus.DEREGISTERED:
                if existing.credential.secret_hash == _hash_secret(secret):
                    return existing  # idempotent replay
                raise GatewayError(ErrorCode.DEVICE_ID_CONFLICT,
                                   f"device_id {device_id!r} already registered "
                                   "with different credential")
            now = self._clock()
            cred = Credential(
                secret_hash=_hash_secret(secret),
                issued_at=now,
                expires_at=None if self._ttl is None else now + self._ttl,
            )
            device = Device(device_id=device_id, status=DeviceStatus.ACTIVE,
                            credential=cred, registered_at=now,
                            firmware_version=(existing.firmware_version
                                              if existing else "0.0.0"))
            self._devices[device_id] = device
            return device

    def authenticate(self, device_id: str, secret: str) -> Device:
        with self._lock:
            device = self._devices.get(device_id)
            if device is None or device.status is DeviceStatus.DEREGISTERED:
                raise GatewayError(ErrorCode.DEVICE_NOT_FOUND,
                                   f"unknown device {device_id!r}")
            if device.status is DeviceStatus.DISABLED:
                raise GatewayError(ErrorCode.DEVICE_DISABLED,
                                   f"device {device_id!r} is disabled")
            cred = device.credential
            if cred.revoked:
                raise GatewayError(ErrorCode.CREDENTIAL_REVOKED,
                                   "credential has been revoked")
            if cred.expires_at is not None and self._clock() >= cred.expires_at:
                raise GatewayError(ErrorCode.CREDENTIAL_EXPIRED,
                                   "credential has expired")
            if cred.secret_hash != _hash_secret(secret):
                raise GatewayError(ErrorCode.CREDENTIAL_INVALID,
                                   "secret mismatch")
            return device

    def _require(self, device_id: str) -> Device:
        device = self._devices.get(device_id)
        if device is None or device.status is DeviceStatus.DEREGISTERED:
            raise GatewayError(ErrorCode.DEVICE_NOT_FOUND,
                               f"unknown device {device_id!r}")
        return device

    def disable(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.status = DeviceStatus.DISABLED
            return device

    def enable(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.status = DeviceStatus.ACTIVE
            return device

    def deregister(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.status = DeviceStatus.DEREGISTERED
            device.credential.revoked = True
            return device

    def revoke_credential(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.credential.revoked = True
            return device

    def rotate_credential(self, device_id: str, new_secret: str) -> Device:
        if not new_secret:
            raise GatewayError(ErrorCode.VALIDATION_FAILED,
                               "new secret must be non-empty")
        with self._lock:
            device = self._require(device_id)
            now = self._clock()
            device.credential = Credential(
                secret_hash=_hash_secret(new_secret),
                issued_at=now,
                expires_at=None if self._ttl is None else now + self._ttl,
            )
            return device

    def get(self, device_id: str) -> Device:
        with self._lock:
            return self._require(device_id)

    # -- backup / restore ------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> dict:
        return {"devices": [{
            "device_id": d.device_id,
            "status": d.status.value,
            "registered_at": d.registered_at,
            "firmware_version": d.firmware_version,
            "credential": {
                "secret_hash": d.credential.secret_hash,
                "issued_at": d.credential.issued_at,
                "expires_at": d.credential.expires_at,
                "revoked": d.credential.revoked,
            },
        } for d in self._devices.values()]}

    def _restore_locked(self, data: dict) -> None:
        """Replace all state with `data`. ValueError on malformed payload."""
        raw_devices = data.get("devices")
        if not isinstance(raw_devices, list):
            raise ValueError("registry.devices must be a list")
        devices: dict[str, Device] = {}
        for item in raw_devices:
            try:
                cred_data = item["credential"]
                device = Device(
                    device_id=item["device_id"],
                    status=DeviceStatus(item["status"]),
                    registered_at=float(item["registered_at"]),
                    firmware_version=str(item["firmware_version"]),
                    credential=Credential(
                        secret_hash=str(cred_data["secret_hash"]),
                        issued_at=float(cred_data["issued_at"]),
                        expires_at=(None if cred_data["expires_at"] is None
                                    else float(cred_data["expires_at"])),
                        revoked=bool(cred_data["revoked"]),
                    ),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"malformed device entry: {exc}") from exc
            if not isinstance(device.device_id, str) or not device.device_id:
                raise ValueError("malformed device entry: empty device_id")
            if device.device_id in devices:
                raise ValueError(
                    f"malformed registry snapshot: duplicate device "
                    f"{device.device_id!r}")
            devices[device.device_id] = device
        self._devices = devices
