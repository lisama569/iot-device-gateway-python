"""Backup/restore tests: online export, migration to a fresh environment,
unfinished-campaign continuation, corrupt/incompatible package rejection,
and repeat-import idempotency."""
import io
import json
import threading
import time
import zipfile

import pytest

from app.backup import FORMAT_VERSION, BackupManager
from app.fota import FirmwareOrchestrator
from app.models import BackupError, BackupRejection
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore


class FakeClock:
    def __init__(self, now=1_000.0):
        self.now = now

    def __call__(self):
        return self.now


def make_gateway(ttl=None, quota=1000, state_path=None, clock=time.time):
    reg = DeviceRegistry(credential_ttl_seconds=ttl, clock=clock)
    tel = TelemetryStore(per_device_quota=quota, clock=clock)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher, state_path=state_path, clock=clock)
    return reg, tel, flasher, orc, BackupManager(reg, tel, orc, flasher, clock=clock)


def fresh_gateway(ttl=None, quota=1000, state_path=None, clock=time.time):
    return make_gateway(ttl=ttl, quota=quota, state_path=state_path, clock=clock)


def package_manifest(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        return json.loads(zf.read("manifest.json")), json.loads(zf.read("data.json"))


def repackage(manifest, payload: dict | bytes) -> bytes:
    payload_bytes = (payload if isinstance(payload, bytes)
                     else json.dumps(payload).encode())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest))
        zf.writestr("data.json", payload_bytes)
    return buf.getvalue()


# --- online export --------------------------------------------------------
def test_online_export_covers_full_lifecycle_state():
    clock = FakeClock(now=10_000.0)
    reg, tel, flasher, orc, bm = make_gateway(ttl=60, clock=clock)
    reg.register("d1", "pw1")
    reg.register("d2", "pw2")
    reg.disable("d2")
    reg.register("d3", "pw3")
    reg.revoke_credential("d3")
    reg.register("d4", "pw4")
    reg.deregister("d4")
    tel.ingest("d1", "m1", 1, 9000.0, {"temp": 21, "mode": "auto"})
    tel.ingest("d1", "m2", 2, 9100.0, {"temp": 22})
    tel.ingest("d1", "old", 0, 1.0, {"temp": 0})       # stale, shadow kept
    tel.ingest("d1", "bad", 1, 999_999.0, {"x": 1})    # rejected record
    orc.create_campaign("c1", "1.0.0", ["d1"], batch_size=1)

    raw = bm.export_bytes()
    manifest, payload = package_manifest(raw)
    print(f"[export] manifest={manifest}")
    assert manifest["format_version"] == FORMAT_VERSION
    assert len(manifest["sha256"]) == 64
    devices = {d["device_id"]: d for d in payload["registry"]["devices"]}
    assert devices["d2"]["status"] == "disabled"
    assert devices["d3"]["credential"]["revoked"] is True
    assert devices["d4"]["status"] == "deregistered"
    assert devices["d4"]["credential"]["revoked"] is True
    assert devices["d1"]["credential"]["expires_at"] == pytest.approx(10_060.0)
    tdev = next(d for d in payload["telemetry"]["devices"]
                if d["device_id"] == "d1")
    assert tdev["shadow"] == {"temp": 22, "mode": "auto"}
    assert tdev["field_versions"]["temp"] == {"device_ts": 9100.0, "seq": 2}
    # the stale message is still accepted (quota) + dedup-tracked; only its
    # field merge is rejected
    assert tdev["accepted_count"] == 3
    assert sorted(tdev["seen_message_ids"]) == ["m1", "m2", "old"]
    assert [r["code"] for r in tdev["rejected"]] == ["validation_failed"]
    camp = payload["fota"]["campaigns"][0]
    assert camp["campaign_id"] == "c1" and camp["status"] == "pending"


def test_export_during_concurrent_writes_is_always_restorable():
    """Writes/flashes concurrent with exports can never yield a torn package:
    every package taken during the storm must restore cleanly and describe a
    self-consistent state."""
    packages: list[bytes] = []
    stop = threading.Event()
    started = threading.Event()

    class SlowFlasher(SimulatedFlasher):
        """Stretches the dispatch window so online exports overlap an
        in-flight upgrade rather than racing a 20 ms whole-campaign run."""
        def __call__(self, device_id, target_version):
            time.sleep(0.005)
            return super().__call__(device_id, target_version)

    reg, tel, flasher, orc, bm = make_gateway()
    slow = SlowFlasher()
    orc = FirmwareOrchestrator(reg, slow)
    bm = BackupManager(reg, tel, orc, slow)
    n = 40
    for i in range(n):
        reg.register(f"d{i}", "pw")
    orc.create_campaign("c-storm", "2.0.0",
                        [f"d{i}" for i in range(n)], batch_size=1)

    def writer():
        started.set()
        for i in range(400):
            dev = f"d{i % n}"
            tel.ingest(dev, f"m{i}", i, 9000.0 + i, {"gauge": i})

    def exporter():
        started.wait(timeout=2)
        while not stop.is_set():
            packages.append(bm.export_bytes())
            time.sleep(0.002)  # sample across the whole write/run window

    te = threading.Thread(target=exporter)
    tw = threading.Thread(target=writer)
    te.start()
    tw.start()
    # campaign runs concurrently as well -> packages may catch RUNNING/
    # IN_PROGRESS states, which must normalize safely on restore
    final = orc.run_until_blocked("c-storm")
    tw.join()
    stop.set()
    te.join()
    assert final["status"] == "completed"
    assert len(packages) > 10
    print(f"[storm] captured {len(packages)} online packages")

    for raw in packages:
        r2, t2, f2, o2, b2 = fresh_gateway()
        summary = b2.restore_bytes(raw)  # raises on any torn package
        # every shadow in the package must be internally coherent
        for dev in [f"d{i}" for i in range(n)]:
            assert t2.accepted_count(dev) == len(_seen_ids(t2, dev))
        # campaign continuation must work from any captured point. A package
        # taken before run flipped PENDING->RUNNING legitimately shows pending.
        st = o2.status("c-storm")
        assert st["status"] in ("pending", "paused", "completed")
        if st["status"] != "completed":
            done = [d for d, s in st["device_states"].items()
                    if s == "succeeded"]
            resumed = o2.resume("c-storm")
            assert resumed["status"] == "completed"
            # devices already succeeded are never re-dispatched
            calls = [d for d, _ in f2.calls()]
            assert not any(calls.count(d) > 1 for d in done)
        assert summary["devices"] == n


def _seen_ids(store: TelemetryStore, device_id):
    return store._seen_ids.get(device_id, set())


# --- restore on a fresh environment --------------------------------------
def test_restore_onto_fresh_gateway_keeps_credentials_and_shadow(tmp_path):
    clock = FakeClock(now=10_000.0)
    reg, tel, flasher, orc, bm = make_gateway(ttl=60, clock=clock)
    reg.register("d1", "pw1")
    reg.register("d2", "pw2")
    reg.rotate_credential("d2", "pw2-new")
    tel.ingest("d1", "m1", 1, 9000.0, {"temp": 21})
    tel.ingest("d1", "m2", 2, 9100.0, {"temp": 22})
    tel.ingest("d1", "m1", 1, 9000.0, {"temp": 21})      # duplicate
    tel.ingest("d1", "bad", 1, 999_999.0, {"x": 1})     # rejected
    path = tmp_path / "backup.zip"
    bm.export_to_path(str(path))

    # "new machine / new data directory": fresh components, fota state file
    # in the new directory, credential TTL config identical
    out = tmp_path / "newdir" / "fota.json"
    r2, t2, f2, o2, b2 = fresh_gateway(ttl=60, state_path=str(out), clock=clock)
    summary = b2.restore_from_path(str(path))
    print(f"[migrate] summary={summary}")
    assert summary["devices"] == 2 and summary["shadow_devices"] == 1

    # old credential rules still hold exactly
    assert r2.authenticate("d1", "pw1").device_id == "d1"
    assert r2.authenticate("d2", "pw2-new").device_id == "d2"
    # expiry semantics survive: advance past TTL -> expired, not invalid
    clock.now = 10_000.0 + 61
    from app.models import ErrorCode, GatewayError
    with pytest.raises(GatewayError) as e:
        r2.authenticate("d1", "pw1")
    print(f"[migrate-expiry] code={e.value.code}")
    assert e.value.code is ErrorCode.CREDENTIAL_EXPIRED

    # shadow + versions + rejected log + dedup memory all identical
    assert t2.shadow("d1") == {"temp": 22}
    assert t2.field_versions("d1") == {"temp": (9100.0, 2)}
    assert [r.code for r in t2.rejected("d1")] == ["validation_failed"]
    dup = t2.ingest("d1", "m1", 1, 9000.0, {"temp": 21})
    assert dup.code == "duplicate_message"
    assert t2.accepted_count("d1") == 2

    # orchestrator restored and its new state file was (re)written
    assert out.exists()


def test_restored_revoked_and_disabled_states_are_enforced():
    reg, tel, flasher, orc, bm = make_gateway()
    reg.register("d1", "pw")
    reg.revoke_credential("d1")
    reg.register("d2", "pw")
    reg.disable("d2")
    raw = bm.export_bytes()

    r2, t2, f2, o2, b2 = fresh_gateway()
    b2.restore_bytes(raw)
    from app.models import ErrorCode, GatewayError
    with pytest.raises(GatewayError) as e:
        r2.authenticate("d1", "pw")
    assert e.value.code is ErrorCode.CREDENTIAL_REVOKED
    with pytest.raises(GatewayError) as e:
        r2.authenticate("d2", "pw")
    assert e.value.code is ErrorCode.DEVICE_DISABLED


# --- unfinished campaign continuation -------------------------------------
def test_unfinished_campaign_resumes_and_skips_succeeded(tmp_path):
    old_state = tmp_path / "old" / "fota.json"
    reg, tel, flasher, orc, bm = make_gateway(state_path=str(old_state))
    for i in range(4):
        reg.register(f"d{i}", "pw")
    orc.create_campaign("c1", "3.0.0",
                        [f"d{i}" for i in range(4)], batch_size=1)
    orc.pause("c1")

    # craft a genuine mid-flight checkpoint via the state file (the same
    # shape a crash/export can catch): d0 done, rest never dispatched
    data = json.loads(old_state.read_text())
    camp = data[0]
    camp["status"] = "paused"
    camp["device_states"]["d0"] = "succeeded"
    for i in range(1, 4):
        camp["device_states"][f"d{i}"] = "pending"
    camp["attempts"] = {"d0": 1, "d1": 0, "d2": 0, "d3": 0}
    camp["current_batch"] = 1
    old_state.write_text(json.dumps(data))
    # rebuild orchestrator from that checkpoint and flash d0 for real
    orc = FirmwareOrchestrator(reg, flasher, state_path=str(old_state))
    flasher("d0", "3.0.0")
    reg.get("d0").firmware_version = "3.0.0"
    bm = BackupManager(reg, tel, orc, flasher)
    raw = bm.export_bytes()

    # restore into the clean environment
    r2, t2, f2, o2, b2 = fresh_gateway(
        state_path=str(tmp_path / "new" / "fota.json"))
    summary = b2.restore_bytes(raw)
    assert summary["installed_versions"] == {"d0": "3.0.0"}
    st = o2.status("c1")
    print(f"[continue] restored status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"
    assert st["device_states"]["d0"] == "succeeded"
    assert st["device_states"]["d1"] == "pending"
    assert r2.get("d0").firmware_version == "3.0.0"

    # pause/resume/cancel all keep working; only unfinished devices dispatch
    o2.pause("c1")  # idempotent on an already parked campaign
    final = o2.resume("c1")
    print(f"[continue] final={final['status']} calls={f2.calls()}")
    assert final["status"] == "completed"
    assert set(f2.calls()) == {("d1", "3.0.0"), ("d2", "3.0.0"),
                               ("d3", "3.0.0")}
    assert all(r2.get(f"d{i}").firmware_version == "3.0.0" for i in range(4))


def test_campaign_caught_running_normalizes_and_can_be_cancelled():
    reg, tel, flasher, orc, bm = make_gateway()
    for i in range(3):
        reg.register(f"d{i}", "pw")
    orc.create_campaign("c1", "4.0.0",
                        [f"d{i}" for i in range(3)], batch_size=1)
    orc.pause("c1")
    raw = bm.export_bytes()
    # hand-edit the package as if the export caught a crash mid-dispatch
    _manifest, payload = package_manifest(raw)
    payload["fota"]["campaigns"][0]["status"] = "running"
    payload["fota"]["campaigns"][0]["device_states"]["d0"] = "in_progress"
    manifest, _ = package_manifest(raw)
    evil = repackage(manifest, payload)
    # checksum now differs -> refuses. Re-sign to model a consistent snapshot
    # captured while RUNNING (the cross-component locks allow this only with
    # status running + devices in_progress/...)
    payload_bytes = json.dumps(payload).encode()
    manifest["sha256"] = __import__("hashlib").sha256(payload_bytes).hexdigest()
    raw2 = repackage(manifest, payload_bytes)

    r2, t2, f2, o2, b2 = fresh_gateway()
    b2.restore_bytes(raw2)
    st = o2.status("c1")
    print(f"[running-caught] status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"          # never stuck running
    assert st["device_states"]["d0"] == "pending"
    cancelled = o2.cancel("c1")
    assert cancelled["status"] == "cancelled"
    assert cancelled["device_states"]["d0"] == "skipped"
    assert f2.calls() == []                  # nothing was dispatched yet


# --- rejection: truncated / tampered / incompatible / incomplete ---------
def _expect_rejected(raw, code):
    bm = BackupManager(DeviceRegistry(), TelemetryStore(),
                       FirmwareOrchestrator(DeviceRegistry(), SimulatedFlasher()),
                       SimulatedFlasher())
    with pytest.raises(BackupError) as e:
        bm.inspect_package(raw)
    print(f"[reject] expected={code.value} got={e.value.code.value} "
          f"msg={e.value.message}")
    assert e.value.code is code
    return e.value


def test_truncated_package_is_rejected():
    raw = BackupManager(DeviceRegistry(), TelemetryStore(),
                        FirmwareOrchestrator(DeviceRegistry(), SimulatedFlasher()),
                        SimulatedFlasher()).export_bytes()
    _expect_rejected(b"", BackupRejection.TRUNCATED)
    _expect_rejected(raw[: len(raw) // 2], BackupRejection.TRUNCATED)
    _expect_rejected(b"not a zip at all", BackupRejection.TRUNCATED)


def test_tampered_payload_fails_integrity():
    reg, tel, flasher, orc, bm = make_gateway()
    reg.register("d1", "pw")
    raw = bm.export_bytes()
    manifest, payload = package_manifest(raw)
    # flip the secret hash but keep the manifest/checksum -> tampering
    dev = payload["registry"]["devices"][0]
    dev["credential"]["secret_hash"] = "0" * 64
    tampered = repackage(manifest, payload)
    _expect_rejected(tampered, BackupRejection.INTEGRITY_FAILED)


def test_unsupported_format_version_is_rejected():
    reg, tel, flasher, orc, bm = make_gateway()
    raw = bm.export_bytes()
    manifest, payload = package_manifest(raw)
    # manifest version unknown -> rejected before even trusting the checksum
    m99 = dict(manifest, format_version=999)
    _expect_rejected(repackage(m99, payload),
                     BackupRejection.FORMAT_UNSUPPORTED)
    # manifest v1 but payload claims v2 -> still incompatible
    payload_v2 = dict(payload, format_version=2)
    pb = json.dumps(payload_v2).encode()
    m2 = dict(manifest, sha256=__import__("hashlib").sha256(pb).hexdigest())
    _expect_rejected(repackage(m2, pb), BackupRejection.FORMAT_UNSUPPORTED)


def test_missing_section_and_invalid_payload_are_rejected():
    reg, tel, flasher, orc, bm = make_gateway()
    reg.register("d1", "pw")
    tel.ingest("d1", "m1", 1, 9.0, {"a": 1})
    raw = bm.export_bytes()
    manifest, payload = package_manifest(raw)

    no_flasher = {k: v for k, v in payload.items() if k != "flasher"}
    pb = json.dumps(no_flasher).encode()
    m = dict(manifest, sha256=__import__("hashlib").sha256(pb).hexdigest())
    _expect_rejected(repackage(m, pb), BackupRejection.MISSING_SECTION)

    bad_registry = dict(payload, registry={"devices": "not-a-list"})
    pb = json.dumps(bad_registry).encode()
    m = dict(manifest, sha256=__import__("hashlib").sha256(pb).hexdigest())
    _expect_rejected(repackage(m, pb), BackupRejection.PAYLOAD_INVALID)

    # cross-section contradiction: campaign says succeeded but installed wrong
    orc.create_campaign("c1", "9.9.9", ["d1"])
    raw2 = bm.export_bytes()
    manifest2, payload2 = package_manifest(raw2)
    camp = payload2["fota"]["campaigns"][0]
    camp["status"] = "completed"
    camp["device_states"]["d1"] = "succeeded"
    camp["current_batch"] = 1
    pb = json.dumps(payload2).encode()
    m = dict(manifest2,
             sha256=__import__("hashlib").sha256(pb).hexdigest())
    _expect_rejected(repackage(m, pb), BackupRejection.PAYLOAD_INVALID)


def test_failed_import_never_touches_live_state():
    reg, tel, flasher, orc, bm = make_gateway()
    reg.register("live-1", "pw")
    tel.ingest("live-1", "m1", 1, 9.0, {"v": 1})

    good = bm.export_bytes()
    manifest, payload = package_manifest(good)
    tampered = repackage(manifest,
                         dict(payload, registry={"devices": "broken"}))
    # checksum failure path
    with pytest.raises(BackupError):
        bm.restore_bytes(tampered)
    # payload-invalid path (re-signed)
    pb = json.dumps(dict(payload, registry={"devices": "broken"})).encode()
    m = dict(manifest, sha256=__import__("hashlib").sha256(pb).hexdigest())
    with pytest.raises(BackupError):
        bm.restore_bytes(repackage(m, pb))
    with pytest.raises(BackupError) as e:
        bm.restore_bytes(good)  # non-empty target refused
    assert e.value.code is BackupRejection.NONEMPTY_TARGET

    # live state fully intact
    assert reg.authenticate("live-1", "pw").device_id == "live-1"
    assert tel.shadow("live-1") == {"v": 1}
    assert tel.accepted_count("live-1") == 1


# --- repeat imports -------------------------------------------------------
def test_repeated_imports_are_identical_and_overwrite_works():
    reg, tel, flasher, orc, bm = make_gateway()
    reg.register("d1", "pw1")
    tel.ingest("d1", "m1", 1, 9.0, {"a": 1})
    orc.create_campaign("c1", "1.0.0", ["d1"])
    orc.run_until_blocked("c1")
    raw = bm.export_bytes()

    def restored_state():
        r2, t2, f2, o2, b2 = fresh_gateway()
        b2.restore_bytes(raw)
        return (r2.snapshot(), t2.snapshot(),
                f2.snapshot(), o2.list_campaigns())

    s1 = restored_state()
    s2 = restored_state()
    assert s1 == s2  # same package, same result every time
    print("[repeat] two independent imports produced identical state")

    # importing again into the same gateway is refused; with overwrite it
    # converges back to exactly the package state
    r3, t3, f3, o3, b3 = fresh_gateway()
    b3.restore_bytes(raw)
    t3.ingest("d1", "extra", 2, 10.0, {"b": 2})  # local divergence
    with pytest.raises(BackupError) as e:
        b3.restore_bytes(raw)
    assert e.value.code is BackupRejection.NONEMPTY_TARGET
    b3.restore_bytes(raw, allow_overwrite=True)
    assert t3.shadow("d1") == {"a": 1}
    assert t3.accepted_count("d1") == 1
