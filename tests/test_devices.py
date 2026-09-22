"""Device registration idempotency, conflict classification and the
immediate effect of enable/disable/deregister and credential lifecycle."""
from __future__ import annotations

import logging

import pytest

from gateway.errors import (
    CredentialExpiredError,
    CredentialInvalidError,
    CredentialRevokedError,
    DeviceConflictError,
    DeviceDisabledError,
    DeviceNotFoundError,
)
from gateway.models import RegisterDeviceRequest, RotateCredentialRequest

log = logging.getLogger("tests.devices")

pytestmark = pytest.mark.asyncio


async def test_registration_is_idempotent(registry):
    svc = registry.device_service
    req = RegisterDeviceRequest(device_id="d1", name="sensor", metadata={"floor": 1})
    v1, secret1 = await svc.register(req, now=1000.0)
    v2, secret2 = await svc.register(req, now=1000.0)
    log.info(
        "idempotent register: registration_seq=%s credential_id=%s",
        v1.registration_seq,
        v1.credentials[0].credential_id,
    )
    assert v1.registration_seq == v2.registration_seq == 1
    assert v1.credentials[0].credential_id == v2.credentials[0].credential_id
    assert secret1 == secret2


async def test_registration_conflict_is_distinguishable(registry):
    svc = registry.device_service
    await svc.register(RegisterDeviceRequest(device_id="d1", name="name-a"), now=1000.0)
    with pytest.raises(DeviceConflictError) as exc:
        await svc.register(RegisterDeviceRequest(device_id="d1", name="name-b"), now=1000.0)
    log.info("identity conflict: error=%s details=%s", exc.value.error_code, exc.value.details)
    assert exc.value.error_code == "device_conflict"
    assert exc.value.details["reason"] == "identity_mismatch"


async def test_credential_error_classes_are_distinct(registry):
    svc = registry.device_service
    # short-lived credential -> expired
    view, secret = await svc.register(
        RegisterDeviceRequest(device_id="d1", credential_ttl_seconds=10), now=1000.0
    )
    cid = view.credentials[0].credential_id
    with pytest.raises(CredentialExpiredError) as exc:
        await svc.authenticate("d1", cid, secret, now=2000.0)
    log.info("expired credential: details=%s", exc.value.details)
    assert exc.value.details["expires_at"] == 1010.0

    # bad secret -> invalid (not expired)
    with pytest.raises(CredentialInvalidError):
        await svc.authenticate("d1", cid, "wrong-secret", now=1000.0)
    # unknown credential id -> invalid
    with pytest.raises(CredentialInvalidError):
        await svc.authenticate("d1", "cred_unknown", secret, now=1000.0)
    # revoked -> distinct class
    await svc.revoke_credential("d1", cid, now=1000.0)
    with pytest.raises(CredentialRevokedError) as exc:
        await svc.authenticate("d1", cid, secret, now=1000.0)
    log.info("revoked credential at %s", exc.value.details["revoked_at"])
    assert exc.value.details["revoked_at"] == 1000.0


async def test_rotation_and_expired_re_registration_path(registry):
    svc = registry.device_service
    view, secret = await svc.register(
        RegisterDeviceRequest(device_id="d1", credential_ttl_seconds=10), now=1000.0
    )
    view2, secret2 = await svc.rotate_credential(
        "d1", RotateCredentialRequest(credential_ttl_seconds=10), now=2000.0
    )
    assert view2.registration_seq == 2
    assert len(view2.credentials) == 2
    assert view2.credentials[0].status.value == "revoked"
    # new credential works; old one rejected
    new_cid = view2.credentials[-1].credential_id
    await svc.authenticate("d1", new_cid, secret2, now=2000.0)
    with pytest.raises(CredentialRevokedError):
        await svc.authenticate("d1", view.credentials[0].credential_id, secret, now=2000.0)
    log.info("rotated: old=%s new=%s", view.credentials[0].credential_id, new_cid)


async def test_disable_and_deregister_take_immediate_effect(registry):
    svc = registry.device_service
    view, secret = await svc.register(RegisterDeviceRequest(device_id="d1"), now=1000.0)
    cid = view.credentials[0].credential_id
    await svc.set_enabled("d1", False, now=1000.0)
    with pytest.raises(DeviceDisabledError):
        await svc.authenticate("d1", cid, secret)
    await svc.set_enabled("d1", True, now=1000.0)
    await svc.authenticate("d1", cid, secret)  # works again

    await svc.deregister("d1", now=1000.0)
    with pytest.raises(DeviceNotFoundError):
        await svc.authenticate("d1", cid, secret)
    # re-registration of a deregistered id is a distinct conflict reason
    with pytest.raises(DeviceConflictError) as exc:
        await svc.register(RegisterDeviceRequest(device_id="d1"), now=1000.0)
    assert exc.value.details["reason"] == "device_deregistered"
    log.info("deregistered device rejected with %s", exc.value.details)


async def test_unknown_device_and_status_code_mapping(registry):
    svc = registry.device_service
    with pytest.raises(DeviceNotFoundError) as exc:
        await svc.get("ghost")
    assert exc.value.http_status == 404
