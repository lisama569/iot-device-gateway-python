"""Telemetry: out-of-order, duplicate, time-skew, concurrency conflict
resolution, quota rejection and validation failures. All decisions are
logged with their deterministic order-tuples for traceability."""
from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import (
    CredentialExpiredError,
    QuotaExceededError,
    ValidationFailureError,
)
from gateway.models import RegisterDeviceRequest, TelemetryMessage

log = logging.getLogger("tests.telemetry")


def _msg(cid, secret, *, version, timestamp, seq=0, nonce=None, state=None, device_id="d1"):
    return TelemetryMessage(
        device_id=device_id,
        credential_id=cid,
        credential_secret=secret,
        version=version,
        timestamp=timestamp,
        seq=seq,
        nonce=nonce,
        state=state if state is not None else {},
    )


async def _register(registry, device_id="d1", **kw):
    view, secret = await registry.device_service.register(
        RegisterDeviceRequest(device_id=device_id, **kw), now=1000.0
    )
    return view.credentials[0].credential_id, secret


async def test_out_of_order_does_not_roll_state_back(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry)
    r1 = await ts.ingest(_msg(cid, secret, version=1, timestamp=1.0, state={"a": 1}))
    r3 = await ts.ingest(_msg(cid, secret, version=3, timestamp=3.0, state={"b": 3}))
    r2 = await ts.ingest(_msg(cid, secret, version=2, timestamp=2.0, state={"a": 2, "b": 2}))
    log.info("decisions: %s | %s | %s", r1.decision, r3.decision, r2.decision)
    assert (r1.decision, r3.decision, r2.decision) == ("merged", "merged", "stale")
    shadow = await ts.get_shadow("d1")
    log.info("final shadow version=%s state=%s", shadow.version, shadow.state)
    assert shadow.version == 3
    assert shadow.state == {"a": 1, "b": 3}


async def test_duplicate_message_counted_once(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry)
    msg = _msg(cid, secret, version=1, timestamp=1.0, state={"a": 1})
    first = await ts.ingest(msg)
    second = await ts.ingest(msg)
    third = await ts.ingest(msg)
    log.info("replay decisions: %s, %s, %s", first.decision, second.decision, third.decision)
    assert first.decision == "merged"
    assert second.decision == third.decision == "duplicate"
    assert len(ts._quota_windows["d1"]) == 1
    shadow = await ts.get_shadow("d1")
    assert shadow.version == 1 and shadow.state == {"a": 1}


async def test_time_skew_equal_version_uses_timestamp_then_seq_nonce(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry)
    await ts.ingest(_msg(cid, secret, version=1, timestamp=10.0, state={"k": "new"}))
    # skewed clock: older timestamp, same version -> stale
    r_skew = await ts.ingest(_msg(cid, secret, version=1, timestamp=5.0, state={"k": "old"}))
    assert r_skew.decision == "stale"
    # later timestamp wins even on same version
    r_new = await ts.ingest(_msg(cid, secret, version=1, timestamp=11.0, state={"k": "newer"}))
    assert r_new.decision == "merged"
    # tie on version+timestamp: seq, then nonce
    await ts.ingest(_msg(cid, secret, version=2, timestamp=1.0, seq=1, nonce="a", state={"x": "a"}))
    r = await ts.ingest(_msg(cid, secret, version=2, timestamp=1.0, seq=2, nonce="a", state={"x": "b"}))
    assert r.decision == "merged"
    r = await ts.ingest(_msg(cid, secret, version=3, timestamp=1.0, seq=1, nonce="aaa", state={"y": 1}))
    assert r.decision == "merged"
    r = await ts.ingest(_msg(cid, secret, version=3, timestamp=1.0, seq=1, nonce="zzz", state={"y": 2}))
    assert r.decision == "merged"
    shadow = await ts.get_shadow("d1")
    assert shadow.state["k"] == "newer" and shadow.state["x"] == "b" and shadow.state["y"] == 2
    log.info("tie-break final shadow: %s", shadow.state)


async def test_concurrent_writers_reach_deterministic_state(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry)

    async def scenario(device_id, first_nonce):
        cid, secret = await _register(registry, device_id=device_id)
        # distinct tuples for every writer: same version/time, differ by nonce
        nonces = ["n01", "n02", "n03", "n04", "n05", "n06", "n07", "n08"]
        msgs = [
            _msg(cid, secret, version=1, timestamp=1.0, nonce=n, state={"winner": n},
                 device_id=device_id)
            for n in nonces
        ]
        if first_nonce == "high":
            msgs = list(reversed(msgs))
        results = await asyncio.gather(*(ts.ingest(m, now=2000.0) for m in msgs))
        decisions = [r.decision for r in results]
        shadow = await ts.get_shadow(device_id)
        return decisions, shadow

    # Two devices fed in reversed arrival orders converge to the same
    # winning state (highest nonce), proving deterministic resolution.
    d1, s1 = await scenario("da", "low")
    assert s1.state["winner"] == "n08"
    d2, s2 = await scenario("db", "high")
    assert s2.state["winner"] == "n08"
    log.info("concurrent decisions low-first=%s high-first=%s", sorted(d1), sorted(d2))
    assert s1.state == s2.state
    assert "stale" in d1 or "stale" in d2


async def test_quota_exceeded_is_reported_not_silently_dropped(registry):
    ts = TelemetryService_with_quota(registry, 2)
    cid, secret = await _register(registry, device_id="dq")
    base = dict(device_id="dq", credential_id=cid, credential_secret=secret)
    for i in range(2):
        r = await ts.ingest(
            TelemetryMessage(**base, version=i + 1, timestamp=float(i), state={"i": i}),
            now=1000.0,
        )
        assert r.decision == "merged"
    with pytest.raises(QuotaExceededError) as exc:
        await ts.ingest(
            TelemetryMessage(**base, version=3, timestamp=9.0, state={"i": 9}),
            now=1000.0,
        )
    log.info("quota rejection details=%s", exc.value.details)
    assert exc.value.http_status == 429
    assert exc.value.details["quota"] == 2
    # sliding window: once the window has moved, writes are accepted again
    r = await ts.ingest(
        TelemetryMessage(**base, version=3, timestamp=9.0, state={"ok": True}),
        now=1061.0,
    )
    assert r.decision == "merged"


async def test_validation_failures_raise(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry)
    bad_payloads = [
        {"k": float("nan")},
        {"k": float("inf")},
        {"k": object()},
    ]
    for bad in bad_payloads:
        with pytest.raises(ValidationFailureError) as exc:
            await ts.ingest(_msg(cid, secret, version=1, timestamp=1.0, state=bad))
        log.info("rejected payload type=%s error=%s", type(bad["k"]).__name__, exc.value.error_code)
        assert exc.value.http_status == 422


async def test_expired_credential_blocks_ingest(registry):
    ts = registry.telemetry_service
    cid, secret = await _register(registry, credential_ttl_seconds=10)
    with pytest.raises(CredentialExpiredError) as exc:
        await ts.ingest(_msg(cid, secret, version=1, timestamp=1.0), now=2000.0)
    log.info("ingest blocked: %s expires_at=%s", exc.value.error_code, exc.value.details["expires_at"])


def TelemetryService_with_quota(registry, quota):
    from gateway.telemetry import TelemetryService

    return TelemetryService(
        registry.store, registry.device_service, quota=quota, quota_window=60.0
    )
