"""Online backup / restore tests: export coverage, migration, campaign
continuation, corruption rejection and import idempotency."""
import hashlib
import json
import os
import threading
import time

import pytest

from app.backup import BackupCoordinator, _canonical
from app.fota import FirmwareOrchestrator
from app.models import ErrorCode, GatewayError
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def make_components(tmp_path=None, ttl=3600.0, clock=None,
                    state_path: str | None = False):
    clock = clock or FakeClock()
    path = None
    if state_path is not False:
        path = state_path
    elif tmp_path is not None:
        path = str(tmp_path / "fota.json")
    base_dir = os.path.dirname(path) if path else None
    reg_path = (os.path.join(base_dir, "registry.json") if base_dir
                else (str(tmp_path / "registry.json") if tmp_path else None))
    tel_path = (os.path.join(base_dir, "telemetry.json") if base_dir
                else (str(tmp_path / "telemetry.json") if tmp_path else None))
    fl_path = (os.path.join(base_dir, "devices.json") if base_dir
               else (str(tmp_path / "devices.json") if tmp_path else None))
    reg = DeviceRegistry(credential_ttl_seconds=ttl, clock=clock,
                         state_path=reg_path)
    tel = TelemetryStore(clock=clock, state_path=tel_path,
                         max_clock_skew_seconds=10**12)
    fl = SimulatedFlasher(state_path=fl_path)
    orc = FirmwareOrchestrator(reg, fl, state_path=path, clock=clock)
    coord = BackupCoordinator(reg, tel, orc, fl, clock=clock)
    return reg, tel, fl, orc, coord


def populate(reg, tel, fl, orc):
    """Create a state exercising every covered dimension."""
    reg.register("dev-active", "pw-active")
    reg.register("dev-disabled", "pw-disabled")
    reg.disable("dev-disabled")
    reg.register("dev-revoked", "pw-revoked")
    reg.revoke_credential("dev-revoked")
    reg.register("dev-rotated", "old-secret")
    reg.rotate_credential("dev-rotated", "new-secret")
    reg.register("dev-gone", "pw-gone")
    reg.deregister("dev-gone")

    # shadow with out-of-order merge: latest value wins per (ts, seq)
    tel.ingest("dev-active", "m1", 1, 100.0, {"temp": 20})
    tel.ingest("dev-active", "m2", 2, 200.0, {"temp": 22, "hum": 50})
    tel.ingest("dev-active", "m-late", 3, 150.0, {"temp": 99})  # stale
    tel.ingest("dev-disabled", "d1", 1, 100.0, {"x": 1})
    # rejected record (validation failure) must be backed up too
    tel.ingest("dev-active", "", 1, 100.0, {"bad": 1})

    # completed campaign: flasher + registry versions both at target
    orc.create_campaign("c-done", "1.0.0", ["dev-active"], batch_size=1)
    orc.run_until_blocked("c-done")


class PausingFlasher:
    """Wrap SimulatedFlasher; pause a campaign right after the first flash so
    the campaign is captured mid-batch (succeeded + pending devices)."""

    def __init__(self, inner: SimulatedFlasher, orc: FirmwareOrchestrator,
                 campaign_id: str, after: int = 1):
        self.inner = inner
        self.orc = orc
        self.campaign_id = campaign_id
        self.after = after
        self.calls = 0

    def __call__(self, device_id, target_version):
        self.inner(device_id, target_version)
        self.calls += 1
        if self.calls == self.after:
            self.orc.pause(self.campaign_id)


# --------------------------------------------------------------------------
# coverage + online export
# --------------------------------------------------------------------------
def test_online_export_covers_full_lifecycle_state(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path)
    populate(reg, tel, fl, orc)
    raw = coord.export_bytes()
    env = json.loads(raw)

    assert env["format"] == "iot-gateway-backup"
    assert env["format_version"] == 1
    assert env["checksum"] == hashlib.sha256(_canonical(env["payload"])).hexdigest()
    payload = env["payload"]
    assert set(payload) == {"registry", "telemetry", "campaigns", "flasher"}

    devices = {d["device_id"]: d for d in payload["registry"]["devices"]}
    assert devices["dev-active"]["status"] == "active"
    assert devices["dev-disabled"]["status"] == "disabled"
    assert devices["dev-gone"]["status"] == "deregistered"
    # credential validity / expiry / revocation all present
    cred = devices["dev-active"]["credential"]
    assert cred["expires_at"] == cred["issued_at"] + 3600.0
    assert cred["revoked"] is False
    assert devices["dev-revoked"]["credential"]["revoked"] is True
    # rotated secret hash differs from the old one
    assert devices["dev-rotated"]["credential"]["secret_hash"] == \
        hashlib.sha256("new-secret".encode()).hexdigest()
    # current installed firmware version
    assert devices["dev-active"]["firmware_version"] == "1.0.0"

    active_tel = payload["telemetry"]["devices"]["dev-active"]
    assert active_tel["shadow"] == {"temp": 22, "hum": 50}  # no rollback
    assert active_tel["versions"]["temp"] == {"device_ts": 200.0, "seq": 2}
    assert set(active_tel["seen_ids"]) == {"m1", "m2", "m-late"}
    assert [r["code"] for r in active_tel["rejected"]] == ["validation_failed"]

    assert payload["flasher"]["versions"] == {"dev-active": "1.0.0"}
    cdone = payload["campaigns"]["campaigns"][0]
    assert cdone["device_states"] == {"dev-active": "succeeded"}
    print(f"[export] devices={sorted(devices)} flasher={payload['flasher']}")


def test_online_export_self_consistent_under_concurrent_writes(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path, ttl=None,
                                               clock=time.time)
    stop = threading.Event()
    errors = []

    def writer(worker):
        i = 0
        base = time.time()
        while not stop.is_set() and i < 40:
            dev = f"w{worker}-{i}"
            reg.register(dev, "pw")
            r = tel.ingest(dev, f"{dev}-msg", i, base + i * 0.001, {"v": i})
            assert r.accepted
            i += 1

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    [t.start() for t in threads]
    try:
        for _ in range(50):
            raw = coord.export_bytes()
            # every snapshot taken mid-write must restore without any
            # cross-section inconsistency (unknown device refs, etc.)
            r2, t2, f2, o2, c2 = make_components()
            try:
                c2.restore_bytes(raw)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    finally:
        stop.set()
        [t.join() for t in threads]
    assert not errors, errors
    print(f"[concurrent-export] 50 snapshots all self-consistent")


# --------------------------------------------------------------------------
# restore to a clean environment
# --------------------------------------------------------------------------
def test_restore_clean_env_keeps_credentials_shadow_and_rejected(tmp_path):
    clock = FakeClock(1000.0)
    reg, tel, fl, orc, coord = make_components(tmp_path, clock=clock)
    populate(reg, tel, fl, orc)
    expected_shadow = tel.shadow("dev-active")
    expected_versions = tel.field_versions("dev-active")
    expected_rejected = [r.code for r in tel.rejected("dev-active")]
    raw = coord.export_bytes()

    # brand new environment (empty directory), clock continues forward
    clock2 = FakeClock(1500.0)
    reg2, tel2, fl2, orc2, coord2 = make_components(clock=clock2, ttl=3600.0)
    summary = coord2.restore_bytes(raw)
    print(f"[restore] summary={summary}")
    assert summary["devices"] == 5

    # old credential rules still hold
    assert reg2.authenticate("dev-active", "pw-active").device_id == "dev-active"
    assert reg2.authenticate("dev-rotated", "new-secret").device_id == "dev-rotated"
    with pytest.raises(GatewayError) as e:
        reg2.authenticate("dev-rotated", "old-secret")
    assert e.value.code is ErrorCode.CREDENTIAL_INVALID
    with pytest.raises(GatewayError):
        reg2.authenticate("dev-revoked", "pw-revoked")
    assert reg2.get("dev-revoked").credential.revoked is True
    with pytest.raises(GatewayError):
        reg2.authenticate("dev-disabled", "pw-disabled")
    with pytest.raises(GatewayError):
        reg2.authenticate("dev-gone", "pw-gone")

    # expiry is an absolute timestamp: same rules at same clock position
    assert reg2.get("dev-active").credential.expires_at == 4600.0
    clock2.t = 4600.0
    with pytest.raises(GatewayError) as e:
        reg2.authenticate("dev-active", "pw-active")
    assert e.value.code is ErrorCode.CREDENTIAL_EXPIRED

    # shadow, versions and rejected log identical to pre-backup
    assert tel2.shadow("dev-active") == expected_shadow
    assert tel2.field_versions("dev-active") == expected_versions
    assert [r.code for r in tel2.rejected("dev-active")] == expected_rejected
    assert tel2.shadow("dev-disabled") == {"x": 1}
    # idempotency keys survive: replay after restore is still a duplicate
    dup = tel2.ingest("dev-active", "m2", 2, 200.0, {"temp": 22, "hum": 50})
    assert dup.code == ErrorCode.DUPLICATE_MESSAGE.value


def test_restore_via_file_across_directories_and_restart(tmp_path):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir(); dir_b.mkdir()
    state_a = str(dir_a / "fota.json")
    state_b = str(dir_b / "fota.json")

    reg, tel, fl, orc, coord = make_components(state_path=state_a)
    devices = ["dev-active", "dev-1", "dev-2"]
    reg.register("dev-active", "pw")
    for d in devices[1:]:
        reg.register(d, "pw")
    tel.ingest("dev-active", "m1", 1, 10.0, {"temp": 1})
    orc.create_campaign("c-mig", "6.0.0", devices, batch_size=1)
    orc.run_until_blocked("c-mig")
    assert fl.calls() == [(d, "6.0.0") for d in devices]
    backup_path = dir_a / "backup.json"
    coord.export_to_file(str(backup_path))
    assert backup_path.exists()

    # clean environment B, FOTA state path does not exist yet
    reg2, tel2, fl2, orc2, coord2 = make_components(state_path=state_b)
    coord2.restore_from_file(str(backup_path))
    import os
    for name in ("fota.json", "registry.json", "telemetry.json",
                 "devices.json"):
        assert os.path.exists(os.path.join(str(dir_b), name))
    assert tel2.shadow("dev-active") == {"temp": 1}
    assert all(reg2.get(d).firmware_version == "6.0.0" for d in devices)

    # a third process restart from B's persisted state sees the same world
    reg3, tel3, fl3, orc3, coord3 = make_components(state_path=state_b)
    assert reg3.authenticate("dev-active", "pw").device_id == "dev-active"
    assert tel3.shadow("dev-active") == {"temp": 1}
    st = orc3.status("c-mig")
    assert st["status"] == "completed"
    assert all(v == "succeeded" for v in st["device_states"].values())
    # completed campaign is terminal and the flasher never re-dispatches it
    assert fl3.calls() == []


# --------------------------------------------------------------------------
# unfinished campaigns continue; succeeded devices not re-dispatched
# --------------------------------------------------------------------------
def test_mid_campaign_backup_resumes_without_redispatch(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path)
    devices = [f"dev-{i}" for i in range(4)]
    for d in devices:
        reg.register(d, "pw")
    orc.create_campaign("c1", "2.0.0", devices, batch_size=2)

    # run, but the campaign parks after the first device of the first batch;
    # the rest of batch 0 still finishes, batch 1 stays pending
    pause_fl = PausingFlasher(fl, orc, "c1", after=1)
    orc._flasher = pause_fl
    st = orc.run_until_blocked("c1")
    orc._flasher = fl
    print(f"[mid] status={st['status']} states={st['device_states']} "
          f"batch={st['current_batch']}")
    assert st["status"] == "paused"
    assert st["current_batch"] == 1
    assert st["device_states"]["dev-0"] == "succeeded"
    assert st["device_states"]["dev-1"] == "succeeded"
    assert st["device_states"]["dev-2"] == "pending"
    assert st["device_states"]["dev-3"] == "pending"

    raw = coord.export_bytes()

    # restore into a clean environment
    reg2, tel2, fl2, orc2, coord2 = make_components(tmp_path / "b")
    coord2.restore_bytes(raw)
    st2 = orc2.status("c1")
    assert st2["status"] == "paused"
    assert fl2.version_of("dev-0") == "2.0.0"  # device-side truth restored
    assert fl2.version_of("dev-2") is None

    # pause/resume/cancel keep working; resume finishes only pending devices
    orc2.pause("c1")  # idempotent on an already parked campaign
    final = orc2.resume("c1")
    print(f"[mid-resume] states={final['device_states']} calls={fl2.calls()}")
    assert final["status"] == "completed"
    called = {dev for dev, _ in fl2.calls()}
    assert called == {"dev-2", "dev-3"}  # succeeded devices not re-dispatched
    assert all(reg2.get(d).firmware_version == "2.0.0" for d in devices)


def test_running_in_progress_normalized_then_resume_and_cancel(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path)
    for d in ("a", "b", "c"):
        reg.register(d, "pw")
    orc.create_campaign("c-run", "3.0.0", ["a", "b"], batch_size=1)
    orc.run_until_blocked("c-run")  # both succeeded
    orc.create_campaign("c-park", "4.0.0", ["c"], batch_size=1)
    orc.pause("c-park")  # pending, never dispatched
    raw = coord.export_bytes()

    # simulate capture at the exact moment a campaign was mid-dispatch
    env = json.loads(raw)
    for c in env["payload"]["campaigns"]["campaigns"]:
        if c["campaign_id"] == "c-park":
            c["status"] = "running"
            c["device_states"]["c"] = "in_progress"
    env["checksum"] = hashlib.sha256(_canonical(env["payload"])).hexdigest()
    reg2, tel2, fl2, orc2, coord2 = make_components(tmp_path / "b")
    coord2.restore_bytes(json.dumps(env).encode())
    st = orc2.status("c-park")
    print(f"[normalize] status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"            # never stuck running
    assert st["device_states"]["c"] == "pending"
    final = orc2.resume("c-park")
    assert final["status"] == "completed"
    assert fl2.calls() == [("c", "4.0.0")]     # exactly one dispatch

    # a cancelled-before-dispatch campaign stays cancellable after restore
    reg3, tel3, fl3, orc3, coord3 = make_components(tmp_path / "c")
    coord3.restore_bytes(raw)
    cancelled = orc3.cancel("c-park")
    assert cancelled["status"] == "cancelled"
    assert cancelled["device_states"] == {"c": "skipped"}
    assert fl3.calls() == []
    with pytest.raises(GatewayError) as e:
        orc3.resume("c-park")
    assert e.value.code is ErrorCode.TASK_STATE_CONFLICT


# --------------------------------------------------------------------------
# corruption / incompatibility rejection with distinct reasons
# --------------------------------------------------------------------------
def good_envelope(coord) -> dict:
    return json.loads(coord.export_bytes())


def clone(env: dict) -> dict:
    """Deep copy so one rejection case cannot mutate later cases' envelope."""
    return json.loads(json.dumps(env))


def seal(env: dict) -> bytes:
    env["checksum"] = hashlib.sha256(_canonical(env["payload"])).hexdigest()
    return json.dumps(env).encode()


def test_rejected_backups_have_distinct_reasons(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path)
    populate(reg, tel, fl, orc)
    env = good_envelope(coord)

    reg2, tel2, fl2, orc2, coord2 = make_components()

    def reject(raw, code):
        with pytest.raises(GatewayError) as e:
            coord2.restore_bytes(raw)
        print(f"[reject] expected={code.value} got={e.value.code.value}")
        assert e.value.code is code

    reject(b"this is not json {{{", ErrorCode.BACKUP_CORRUPTED)
    reject(json.dumps(env).encode()[:80], ErrorCode.BACKUP_CORRUPTED)  # truncated
    no_checksum = clone(env); del no_checksum["checksum"]
    reject(json.dumps(no_checksum).encode(), ErrorCode.BACKUP_CORRUPTED)
    wrong_format = clone(env); wrong_format["format"] = "something-else"
    reject(seal(wrong_format), ErrorCode.BACKUP_CORRUPTED)
    bad_checksum = clone(env); bad_checksum["checksum"] = "0" * 64
    reject(json.dumps(bad_checksum).encode(),
           ErrorCode.BACKUP_INTEGRITY_FAILED)
    # payload edited without re-sealing: seal is broken
    tampered = clone(env)
    tampered["payload"]["registry"]["devices"][0]["firmware_version"] = "9.9.9"
    reject(json.dumps(tampered).encode(),
           ErrorCode.BACKUP_INTEGRITY_FAILED)
    # payload edited AND re-sealed by an attacker: structural/cross-reference
    # validation still rejects it as incomplete
    re_sealed = clone(env)
    re_sealed["payload"]["registry"]["devices"][0]["firmware_version"] = "9.9.9"
    reject(seal(re_sealed), ErrorCode.BACKUP_INCOMPLETE)
    bad_version = clone(env); bad_version["format_version"] = 99
    reject(seal(bad_version), ErrorCode.BACKUP_VERSION_UNSUPPORTED)
    missing_section = clone(env)
    del missing_section["payload"]["flasher"]
    reject(seal(missing_section), ErrorCode.BACKUP_INCOMPLETE)

    # telemetry pointing at an unknown device
    cross = clone(env)
    cross["payload"]["telemetry"]["devices"]["ghost"] = {
        "shadow": {"v": 1},
        "versions": {"v": {"device_ts": 1.0, "seq": 0}},
        "seen_ids": ["g1"], "accepted_count": 1, "rejected": []}
    reject(seal(cross), ErrorCode.BACKUP_INCOMPLETE)

    # succeeded device whose device-side firmware disagrees
    bad_succ = clone(env)
    bad_succ["payload"]["flasher"]["versions"]["dev-active"] = "0.0.9"
    reject(seal(bad_succ), ErrorCode.BACKUP_INCOMPLETE)


def test_failed_import_leaves_live_state_intact(tmp_path):
    # target environment already serving traffic
    reg2, tel2, fl2, orc2, coord2 = make_components()
    reg2.register("live-dev", "live-pw")
    tel2.ingest("live-dev", "lm", 1, 10.0, {"live": 1})
    orc2.create_campaign("live-c", "1.0.0", ["live-dev"])
    orc2.pause("live-c")

    reg, tel, fl, orc, coord = make_components(tmp_path)
    populate(reg, tel, fl, orc)
    env = good_envelope(coord)

    for corrupt in (b"{not json", seal(dict(env, format_version=99)),
                    json.dumps(dict(env, checksum="f" * 64)).encode()):
        with pytest.raises(GatewayError):
            coord2.restore_bytes(corrupt)

    # current usable state untouched
    assert reg2.authenticate("live-dev", "live-pw").device_id == "live-dev"
    assert tel2.shadow("live-dev") == {"live": 1}
    assert orc2.status("live-c")["status"] == "paused"
    print("[atomicity] live state intact after rejected imports")


def test_repeated_import_is_idempotent(tmp_path):
    reg, tel, fl, orc, coord = make_components(tmp_path)
    populate(reg, tel, fl, orc)
    raw = coord.export_bytes()

    reg2, tel2, fl2, orc2, coord2 = make_components()
    s1 = coord2.restore_bytes(raw)
    snapshot1 = {
        "devices": {d: reg2.get(d).to_dict()
                    for d in ("dev-active", "dev-revoked")},
        "shadow": tel2.shadow("dev-active"),
        "campaign": orc2.status("c-done"),
        "flasher": dict(fl2._versions),
    }
    s2 = coord2.restore_bytes(raw)
    snapshot2 = {
        "devices": {d: reg2.get(d).to_dict()
                    for d in ("dev-active", "dev-revoked")},
        "shadow": tel2.shadow("dev-active"),
        "campaign": orc2.status("c-done"),
        "flasher": dict(fl2._versions),
    }
    assert s1["devices"] == s2["devices"] == 5
    assert s1["campaigns"] == s2["campaigns"]
    assert snapshot1 == snapshot2
    print("[idempotent] second identical import produced identical state")
