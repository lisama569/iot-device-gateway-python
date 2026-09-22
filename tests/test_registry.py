"""Device registration & credential lifecycle tests."""
import pytest

from app.models import ErrorCode, GatewayError
from app.registry import DeviceRegistry


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def test_register_is_idempotent():
    reg = DeviceRegistry()
    d1 = reg.register("dev-1", "pw-1")
    d2 = reg.register("dev-1", "pw-1")
    print(f"[register] first={d1.registered_at} replay={d2.registered_at}")
    assert d1 is d2
    assert d1.registered_at == d2.registered_at


def test_register_conflicting_secret_rejected():
    reg = DeviceRegistry()
    reg.register("dev-1", "pw-1")
    with pytest.raises(GatewayError) as exc:
        reg.register("dev-1", "pw-OTHER")
    print(f"[conflict] code={exc.value.code}")
    assert exc.value.code is ErrorCode.DEVICE_ID_CONFLICT


def test_authenticate_error_classes_are_distinct():
    clock = FakeClock()
    reg = DeviceRegistry(credential_ttl_seconds=60, clock=clock)
    reg.register("dev-1", "pw-1")
    reg.register("dev-2", "pw-2")
    reg.disable("dev-2")

    cases = {}
    with pytest.raises(GatewayError) as e:
        reg.authenticate("ghost", "pw")
    cases["unknown"] = e.value.code
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-2", "pw-2")
    cases["disabled"] = e.value.code
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "wrong")
    cases["wrong_secret"] = e.value.code
    clock.now += 120  # let the credential expire
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "pw-1")
    cases["expired"] = e.value.code

    print(f"[auth-errors] {cases}")
    assert cases == {
        "unknown": ErrorCode.DEVICE_NOT_FOUND,
        "disabled": ErrorCode.DEVICE_DISABLED,
        "wrong_secret": ErrorCode.CREDENTIAL_INVALID,
        "expired": ErrorCode.CREDENTIAL_EXPIRED,
    }
    assert len(set(cases.values())) == 4  # all distinguishable


def test_revoke_and_rotate():
    reg = DeviceRegistry()
    reg.register("dev-1", "pw-1")
    reg.revoke_credential("dev-1")
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "pw-1")
    print(f"[revoke] code={e.value.code}")
    assert e.value.code is ErrorCode.CREDENTIAL_REVOKED

    reg.rotate_credential("dev-1", "pw-2")
    assert reg.authenticate("dev-1", "pw-2").device_id == "dev-1"
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "pw-1")
    assert e.value.code is ErrorCode.CREDENTIAL_INVALID


def test_disable_enable_deregister_take_effect_immediately():
    reg = DeviceRegistry()
    reg.register("dev-1", "pw-1")
    reg.disable("dev-1")
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "pw-1")
    assert e.value.code is ErrorCode.DEVICE_DISABLED
    reg.enable("dev-1")
    assert reg.authenticate("dev-1", "pw-1")
    reg.deregister("dev-1")
    with pytest.raises(GatewayError) as e:
        reg.authenticate("dev-1", "pw-1")
    print(f"[deregister] code={e.value.code}")
    assert e.value.code is ErrorCode.DEVICE_NOT_FOUND
