"""Device registration and credential lifecycle service."""
from __future__ import annotations

import asyncio
import hashlib
import time

from .errors import (
    CredentialExpiredError,
    CredentialInvalidError,
    CredentialRevokedError,
    DeviceConflictError,
    DeviceDisabledError,
    DeviceNotFoundError,
)
from .models import (
    CredentialStatus,
    CredentialView,
    DeviceStatus,
    DeviceView,
    RegisterDeviceRequest,
    RotateCredentialRequest,
)
from .store import DataStore

_REVOKED = CredentialStatus.REVOKED.value
_ACTIVE = CredentialStatus.ACTIVE.value


def _fingerprint(name: str, metadata: dict) -> str:
    """Stable fingerprint of the registration identity fields."""
    blob = repr((name, sorted(metadata.items())))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _now_ts() -> float:
    return time.time()


class DeviceService:
    def __init__(self, store: DataStore, default_ttl: int = 3600) -> None:
        self.store = store
        self.default_ttl = default_ttl
        # per-device serialization for state-changing ops
        self._device_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def _lock_for(self, device_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._device_locks.get(device_id)
            if lock is None:
                lock = asyncio.Lock()
                self._device_locks[device_id] = lock
            return lock

    async def register(
        self, request: RegisterDeviceRequest, *, now: float | None = None
    ) -> tuple[DeviceView, str]:
        """Idempotent registration. Returns ``(device, credential_secret)``.

        Rules
        -----
        * First call: creates an enabled device and an active credential.
        * Repeat call (same device_id + same name/metadata): idempotent,
          returns the *same* credential id and secret.
        * Same device_id, different name/metadata: ``DeviceConflictError``
          (reason ``identity_mismatch``).
        * Device was deregistered: ``DeviceConflictError``
          (reason ``device_deregistered``).
        """
        now = now if now is not None else _now_ts()
        ttl = request.credential_ttl_seconds
        if ttl is None:
            ttl = self.default_ttl if self.default_ttl > 0 else None
        lock = await self._lock_for(request.device_id)
        async with lock:
            existing = self.store.devices.get(request.device_id)
            if existing is None:
                cred = self._new_credential(now, ttl)
                device = {
                    "device_id": request.device_id,
                    "name": request.name,
                    "status": DeviceStatus.ENABLED.value,
                    "metadata": dict(request.metadata),
                    "registration_seq": 1,
                    "created_at": now,
                    "updated_at": now,
                    "fingerprint": _fingerprint(request.name, request.metadata),
                    "credentials": [cred],
                }
                self.store.devices[request.device_id] = device
                await self.store.save()
                await self.store.append_audit(
                    {
                        "ts": now,
                        "kind": "device_registered",
                        "device_id": request.device_id,
                        "credential_id": cred["credential_id"],
                        "registration_seq": 1,
                    }
                )
                await self.store.save()
                return self._to_view(device), cred["secret"]

            if existing["status"] == DeviceStatus.DEREGISTERED.value:
                raise DeviceConflictError(
                    f"device {request.device_id!r} has been deregistered",
                    details={"device_id": request.device_id, "reason": "device_deregistered"},
                )

            if existing["fingerprint"] != _fingerprint(request.name, request.metadata):
                raise DeviceConflictError(
                    f"device {request.device_id!r} already exists with different identity",
                    details={"device_id": request.device_id, "reason": "identity_mismatch"},
                )

            # Identical re-registration: return existing active credential.
            cred = self._latest_active_credential(existing, now)
            if cred is None:
                # All credentials unusable (revoked/expired): refuse
                # silently minting a new one via the registration endpoint.
                raise CredentialRevokedError(
                    "no active credential; use credential rotation",
                    details={"device_id": request.device_id},
                )
            return self._to_view(existing), cred["secret"]

    async def get(self, device_id: str) -> DeviceView:
        device = self.store.devices.get(device_id)
        if device is None:
            raise DeviceNotFoundError(details={"device_id": device_id})
        return self._to_view(device)

    async def authenticate(
        self,
        device_id: str,
        credential_id: str,
        secret: str = "",
        *,
        now: float | None = None,
    ) -> dict:
        """Validate device status and credential.

        On success returns the live device dict. Failure modes are
        distinguishable by exception class: DeviceNotFoundError,
        DeviceDisabledError, CredentialInvalidError, CredentialExpiredError,
        CredentialRevokedError.
        """
        now = now if now is not None else _now_ts()
        device = self.store.devices.get(device_id)
        if device is None:
            raise DeviceNotFoundError(details={"device_id": device_id})
        if device["status"] == DeviceStatus.DEREGISTERED.value:
            raise DeviceNotFoundError(
                f"device {device_id!r} is deregistered",
                details={"device_id": device_id, "reason": "deregistered"},
            )
        if device["status"] == DeviceStatus.DISABLED.value:
            raise DeviceDisabledError(details={"device_id": device_id})

        cred = next(
            (c for c in device["credentials"] if c["credential_id"] == credential_id),
            None,
        )
        if cred is None or cred.get("secret") != secret:
            raise CredentialInvalidError(
                "unknown credential or wrong secret",
                details={"device_id": device_id, "credential_id": credential_id},
            )
        if cred["status"] == _REVOKED:
            raise CredentialRevokedError(
                details={
                    "device_id": device_id,
                    "credential_id": credential_id,
                    "revoked_at": cred.get("revoked_at"),
                }
            )
        if cred["status"] == _ACTIVE and cred["expires_at"] is not None and now > cred["expires_at"]:
            raise CredentialExpiredError(
                details={
                    "device_id": device_id,
                    "credential_id": credential_id,
                    "expires_at": cred["expires_at"],
                    "now": now,
                }
            )
        return device

    async def rotate_credential(
        self,
        device_id: str,
        request: RotateCredentialRequest,
        *,
        now: float | None = None,
    ) -> tuple[DeviceView, str]:
        now = now if now is not None else _now_ts()
        ttl = request.credential_ttl_seconds
        if ttl is None:
            ttl = self.default_ttl if self.default_ttl > 0 else None
        lock = await self._lock_for(device_id)
        async with lock:
            device = self.store.devices.get(device_id)
            if device is None:
                raise DeviceNotFoundError(details={"device_id": device_id})
            if device["status"] == DeviceStatus.DEREGISTERED.value:
                raise DeviceConflictError(
                    "device is deregistered",
                    details={"device_id": device_id, "reason": "device_deregistered"},
                )
            if request.revoke_previous:
                for c in device["credentials"]:
                    if c["status"] == _ACTIVE:
                        c["status"] = _REVOKED
                        c["revoked_at"] = now
            cred = self._new_credential(now, ttl)
            device["credentials"].append(cred)
            device["registration_seq"] += 1
            device["updated_at"] = now
            await self.store.save()
            await self.store.append_audit(
                {
                    "ts": now,
                    "kind": "credential_rotated",
                    "device_id": device_id,
                    "credential_id": cred["credential_id"],
                    "revoked_previous": request.revoke_previous,
                    "registration_seq": device["registration_seq"],
                }
            )
            await self.store.save()
            return self._to_view(device), cred["secret"]

    async def revoke_credential(
        self, device_id: str, credential_id: str, *, now: float | None = None
    ) -> DeviceView:
        now = now if now is not None else _now_ts()
        lock = await self._lock_for(device_id)
        async with lock:
            device = self.store.devices.get(device_id)
            if device is None:
                raise DeviceNotFoundError(details={"device_id": device_id})
            cred = next(
                (c for c in device["credentials"] if c["credential_id"] == credential_id),
                None,
            )
            if cred is None:
                raise CredentialInvalidError(
                    details={"device_id": device_id, "credential_id": credential_id}
                )
            if cred["status"] != _REVOKED:
                cred["status"] = _REVOKED
                cred["revoked_at"] = now
                device["updated_at"] = now
                await self.store.save()
                await self.store.append_audit(
                    {
                        "ts": now,
                        "kind": "credential_revoked",
                        "device_id": device_id,
                        "credential_id": credential_id,
                    }
                )
                await self.store.save()
            return self._to_view(device)

    async def set_enabled(
        self, device_id: str, enabled: bool, *, now: float | None = None
    ) -> DeviceView:
        """Enable/disable a device. Takes effect on the *next* request."""
        now = now if now is not None else _now_ts()
        lock = await self._lock_for(device_id)
        async with lock:
            device = self.store.devices.get(device_id)
            if device is None:
                raise DeviceNotFoundError(details={"device_id": device_id})
            new_status = DeviceStatus.ENABLED if enabled else DeviceStatus.DISABLED
            if device["status"] == DeviceStatus.DEREGISTERED.value:
                raise DeviceConflictError(
                    "cannot toggle a deregistered device",
                    details={"device_id": device_id, "reason": "device_deregistered"},
                )
            if device["status"] != new_status.value:
                device["status"] = new_status.value
                device["updated_at"] = now
                await self.store.save()
                await self.store.append_audit(
                    {
                        "ts": now,
                        "kind": "device_enabled" if enabled else "device_disabled",
                        "device_id": device_id,
                    }
                )
                await self.store.save()
            return self._to_view(device)

    async def deregister(
        self, device_id: str, *, now: float | None = None
    ) -> DeviceView:
        now = now if now is not None else _now_ts()
        lock = await self._lock_for(device_id)
        async with lock:
            device = self.store.devices.get(device_id)
            if device is None:
                raise DeviceNotFoundError(details={"device_id": device_id})
            if device["status"] != DeviceStatus.DEREGISTERED.value:
                device["status"] = DeviceStatus.DEREGISTERED.value
                device["updated_at"] = now
                for c in device["credentials"]:
                    if c["status"] == _ACTIVE:
                        c["status"] = _REVOKED
                        c["revoked_at"] = now
                await self.store.save()
                await self.store.append_audit(
                    {"ts": now, "kind": "device_deregistered", "device_id": device_id}
                )
                await self.store.save()
            return self._to_view(device)

    # ---- helpers ----

    @staticmethod
    def _new_credential(now: float, ttl: int | None) -> dict:
        import secrets

        cid = "cred_" + secrets.token_hex(6)
        return {
            "credential_id": cid,
            "secret": secrets.token_urlsafe(24),
            "status": _ACTIVE,
            "issued_at": now,
            "expires_at": (now + ttl) if ttl else None,
            "revoked_at": None,
        }

    @staticmethod
    def _latest_active_credential(device: dict, now: float | None = None) -> dict | None:
        now = now if now is not None else _now_ts()
        actives = [
            c
            for c in device["credentials"]
            if c["status"] == _ACTIVE
            and (c["expires_at"] is None or c["expires_at"] > now)
        ]
        return actives[-1] if actives else None

    def _to_view(self, device: dict) -> DeviceView:
        now = _now_ts()
        return DeviceView(
            device_id=device["device_id"],
            name=device["name"],
            status=DeviceStatus(device["status"]),
            metadata=device["metadata"],
            registration_seq=device["registration_seq"],
            created_at=device["created_at"],
            updated_at=device["updated_at"],
            credentials=[self._credential_view(c, now) for c in device["credentials"]],
        )

    @staticmethod
    def _credential_view(cred: dict, now: float | None = None) -> CredentialView:
        now = now if now is not None else _now_ts()
        status = CredentialStatus(cred["status"])
        if (
            status == CredentialStatus.ACTIVE
            and cred.get("expires_at") is not None
            and now > cred["expires_at"]
        ):
            status = CredentialStatus.EXPIRED
        return CredentialView(
            credential_id=cred["credential_id"],
            status=status,
            expires_at=cred.get("expires_at"),
            issued_at=cred["issued_at"],
            revoked_at=cred.get("revoked_at"),
        )
