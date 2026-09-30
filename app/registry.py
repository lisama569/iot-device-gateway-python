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

    def to_dict(self) -> dict:
        return {"secret_hash": self.secret_hash, "issued_at": self.issued_at,
                "expires_at": self.expires_at, "revoked": self.revoked}

    @classmethod
    def from_dict(cls, data: dict) -> "Credential":
        secret_hash = data["secret_hash"]
        issued_at = data["issued_at"]
        expires_at = data["expires_at"]
        revoked = data["revoked"]
        if (not isinstance(secret_hash, str)
                or len(secret_hash) != 64
                or any(c not in "0123456789abcdef" for c in secret_hash)):
            raise ValueError("credential.secret_hash must be a sha256 hex string")
        if not isinstance(issued_at, (int, float)) or isinstance(issued_at, bool):
            raise ValueError("credential.issued_at must be a number")
        if expires_at is not None and (
                not isinstance(expires_at, (int, float))
                or isinstance(expires_at, bool)):
            raise ValueError("credential.expires_at must be a number or null")
        if not isinstance(revoked, bool):
            raise ValueError("credential.revoked must be a bool")
        return cls(secret_hash=secret_hash, issued_at=float(issued_at),
                   expires_at=(None if expires_at is None else float(expires_at)),
                   revoked=revoked)


@dataclass
class Device:
    device_id: str
    status: DeviceStatus
    credential: Credential
    registered_at: float
    firmware_version: str = "0.0.0"

    def to_dict(self) -> dict:
        return {"device_id": self.device_id, "status": self.status.value,
                "credential": self.credential.to_dict(),
                "registered_at": self.registered_at,
                "firmware_version": self.firmware_version}

    @classmethod
    def from_dict(cls, data: dict) -> "Device":
        device_id = data["device_id"]
        if not isinstance(device_id, str) or not device_id:
            raise ValueError("device.device_id must be a non-empty string")
        registered_at = data["registered_at"]
        if (not isinstance(registered_at, (int, float))
                or isinstance(registered_at, bool)):
            raise ValueError("device.registered_at must be a number")
        firmware_version = data.get("firmware_version", "0.0.0")
        if not isinstance(firmware_version, str) or not firmware_version:
            raise ValueError("device.firmware_version must be a non-empty string")
        return cls(
            device_id=device_id,
            status=DeviceStatus(data["status"]),
            credential=Credential.from_dict(data["credential"]),
            registered_at=float(registered_at),
            firmware_version=firmware_version,
        )


class DeviceRegistry:
    """In-memory registry.

    - register() is idempotent: same (device_id, secret) -> same Device;
      same device_id with a different secret -> DEVICE_ID_CONFLICT.
    - authenticate() distinguishes: not found / disabled / deregistered /
      wrong secret / expired / revoked.
    - disable/enable/deregister take effect on the very next authenticate().
    """

    def __init__(self, credential_ttl_seconds: float | None = None,
                 clock=time.time, state_path: str | None = None):
        self._ttl = credential_ttl_seconds
        self._clock = clock
        self._devices: dict[str, Device] = {}
        self._lock = threading.Lock()
        self._state_path = state_path
        if state_path:
            self.recover()

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
            self._persist()
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
            self._persist()
            return device

    def enable(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.status = DeviceStatus.ACTIVE
            self._persist()
            return device

    def deregister(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.status = DeviceStatus.DEREGISTERED
            device.credential.revoked = True
            self._persist()
            return device

    def revoke_credential(self, device_id: str) -> Device:
        with self._lock:
            device = self._require(device_id)
            device.credential.revoked = True
            self._persist()
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
            self._persist()
            return device

    def get(self, device_id: str) -> Device:
        with self._lock:
            return self._require(device_id)

    # -- backup/restore (caller holds self._lock) ------------------------
    def snapshot_locked(self) -> dict:
        return {"devices": [d.to_dict() for d in self._devices.values()]}

    def restore_locked(self, data: dict) -> int:
        """Replace all device state from validated backup data."""
        devices: dict[str, Device] = {}
        for raw in data["devices"]:
            device = Device.from_dict(raw)
            if device.device_id in devices:
                raise ValueError(f"duplicate device {device.device_id!r}")
            devices[device.device_id] = device
        self._devices = devices
        self._persist()
        return len(devices)

    # -- persistence (same atomic-temp-replace pattern as the FOTA store)
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
