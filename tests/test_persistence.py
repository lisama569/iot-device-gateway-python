"""Restart persistence: a gateway restored from a backup (or built locally)
keeps working after the process restarts.

Business areas covered (see the test classes):
- TestRestoreThenRestart: restore on a clean machine, then restart -> device
  access with old credentials, distinguishable credential rules, shadow /
  dedup / rejected-log / campaign progress / installed firmware all intact.
- TestUnfinishedCampaignAcrossRestart: unfinished campaigns survive the
  restart, pause/resume/cancel keep working, only genuinely unfinished
  devices are dispatched again, nothing gets stuck.
- TestCorruptStateOnBoot: a damaged checkpoint is reported and quarantined,
  never half-loaded; the gateway stays usable and backups still import.
- TestRepeatImportAndRestartConsistency: same backup imported repeatedly and
  consecutive restarts all converge to identical state.
"""
import hashlib
import io
import json
import os
import time
import zipfile

import pytest

from app.backup import BackupManager
from app.fota import FirmwareOrchestrator
from app.models import BackupError, BackupRejection, ErrorCode, GatewayError
from app.persistence import StateStore
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore


class FakeClock:
    def __init__(self, now=1_000.0):
        self.now = now

    def __call__(self):
        return self.now


def make_gateway(ttl=None, quota=1000, clock=time.time):
    """A plain in-memory gateway (the 'old machine')."""
    reg = DeviceRegistry(credential_ttl_seconds=ttl, clock=clock)
    tel = TelemetryStore(per_device_quota=quota, clock=clock)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher, clock=clock)
    return reg, tel, flasher, orc, BackupManager(reg, tel, orc, flasher,
                                                 clock=clock)


def boot_gateway(state_path, ttl=None, quota=1000, clock=time.time):
    """Simulate a process start: fresh components + load the checkpoint."""
    reg = DeviceRegistry(credential_ttl_seconds=ttl, clock=clock)
    tel = TelemetryStore(per_device_quota=quota, clock=clock)
    flasher = SimulatedFlasher()
    orc = FirmwareOrchestrator(reg, flasher, clock=clock)
    bm = BackupManager(reg, tel, orc, flasher, clock=clock)
    store = StateStore(bm, str(state_path), clock=clock).attach()
    outcome = store.load()
    return reg, tel, flasher, orc, bm, store, outcome


def component_state(reg, tel, flasher, orc):
    return (reg.snapshot(), tel.snapshot(), flasher.snapshot(),
            orc.list_campaigns())


def package_parts(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        return (json.loads(zf.read("manifest.json")),
                json.loads(zf.read("data.json")))


def repackage(manifest, payload) -> bytes:
    payload_bytes = (payload if isinstance(payload, bytes)
                     else json.dumps(payload).encode())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest))
        zf.writestr("data.json", payload_bytes)
    return buf.getvalue()


def resigned(manifest, payload) -> bytes:
    payload_bytes = json.dumps(payload).encode()
    manifest = dict(manifest,
                    sha256=hashlib.sha256(payload_bytes).hexdigest())
    return repackage(manifest, payload_bytes)


def build_rich_old_machine(clock):
    """The 'old machine': credentials in every lifecycle state, telemetry
    with dedup/rejected history, a finished and an unfinished campaign."""
    reg, tel, flasher, orc, bm = make_gateway(ttl=60, clock=clock)
    reg.register("d-active", "pw-a")
    reg.register("d-disabled", "pw-d")
    reg.disable("d-disabled")
    reg.register("d-revoked", "pw-r")
    reg.revoke_credential("d-revoked")
    reg.register("d-rotated", "pw-old")
    reg.rotate_credential("d-rotated", "pw-new")
    tel.ingest("d-active", "m1", 1, 9_000.0, {"temp": 21, "mode": "auto"})
    tel.ingest("d-active", "m2", 2, 9_100.0, {"temp": 22})
    tel.ingest("d-active", "m1", 1, 9_000.0, {"temp": 21})   # duplicate
    tel.ingest("d-active", "bad", 1, 999_999.0, {"x": 1})    # rejected
    orc.create_campaign("c-done", "1.0.0", ["d-active"], batch_size=1)
    orc.run_until_blocked("c-done")
    orc.create_campaign("c-todo", "2.0.0", ["d-rotated"], batch_size=1)
    orc.pause("c-todo")
    return reg, tel, flasher, orc, bm


# ---------------------------------------------------------------------------
class TestRestoreThenRestart:
    """恢复后重启：接入、凭据规则、影子、判重、拒绝记录、任务与实装版本。"""

    def test_restart_after_restore_keeps_access_and_query_state(self, tmp_path):
        clock = FakeClock(now=10_000.0)
        *_ , bm = build_rich_old_machine(clock)
        raw = bm.export_bytes()

        # clean environment: restore, then restart the process
        state = tmp_path / "new" / "gateway-state.zip"
        r1, t1, f1, o1, b1, s1, out1 = boot_gateway(state, ttl=60, clock=clock)
        assert out1 == "absent"
        b1.restore_bytes(raw)
        assert state.exists()  # restore itself is checkpointed

        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, ttl=60, clock=clock)
        print(f"[restart] boot outcome={out2} status={s2.status()}")
        assert out2 == "loaded"

        # old credentials connect; every credential rule stays distinguishable
        assert r2.authenticate("d-active", "pw-a").device_id == "d-active"
        assert r2.authenticate("d-rotated", "pw-new").device_id == "d-rotated"
        for dev, secret, code in [
                ("d-disabled", "pw-d", ErrorCode.DEVICE_DISABLED),
                ("d-revoked", "pw-r", ErrorCode.CREDENTIAL_REVOKED),
                ("d-rotated", "pw-old", ErrorCode.CREDENTIAL_INVALID),
                ("d-active", "wrong", ErrorCode.CREDENTIAL_INVALID),
                ("ghost", "pw", ErrorCode.DEVICE_NOT_FOUND)]:
            with pytest.raises(GatewayError) as e:
                r2.authenticate(dev, secret)
            print(f"[restart-auth] {dev}: {e.value.code.value}")
            assert e.value.code is code
        # expiry semantics survive the restart (expired != invalid)
        clock.now = 10_000.0 + 61
        with pytest.raises(GatewayError) as e:
            r2.authenticate("d-active", "pw-a")
        print(f"[restart-auth] d-active after TTL: {e.value.code.value}")
        assert e.value.code is ErrorCode.CREDENTIAL_EXPIRED

        # shadow, dedup memory, rejected log, accepted counts
        assert t2.shadow("d-active") == {"temp": 22, "mode": "auto"}
        assert t2.field_versions("d-active") == {"temp": (9_100.0, 2),
                                                 "mode": (9_000.0, 1)}
        assert [r.code for r in t2.rejected("d-active")] == ["validation_failed"]
        dup = t2.ingest("d-active", "m1", 1, 9_000.0, {"temp": 21})
        print(f"[restart-dedup] replayed m1 -> {dup.code}")
        assert dup.code == "duplicate_message"
        assert t2.accepted_count("d-active") == 2

        # campaign progress and actually-installed firmware versions
        done = o2.status("c-done")
        todo = o2.status("c-todo")
        print(f"[restart-fota] c-done={done['status']} c-todo={todo['status']}")
        assert done["status"] == "completed"
        assert done["device_states"] == {"d-active": "succeeded"}
        assert todo["status"] == "paused"
        assert f2.version_of("d-active") == "1.0.0"
        assert r2.get("d-active").firmware_version == "1.0.0"

    def test_consecutive_restarts_converge_to_identical_state(self, tmp_path):
        clock = FakeClock(now=10_000.0)
        *_, bm = build_rich_old_machine(clock)
        raw = bm.export_bytes()
        state = tmp_path / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, ttl=60, clock=clock)
        b1.restore_bytes(raw)
        first = component_state(r1, t1, f1, o1)
        for reboot in (2, 3):
            rN, tN, fN, oN, bN, sN, outN = boot_gateway(state, ttl=60,
                                                        clock=clock)
            assert outN == "loaded"
            assert component_state(rN, tN, fN, oN) == first
            print(f"[multi-restart] boot #{reboot}: state identical")


# ---------------------------------------------------------------------------
class PausingFlasher(SimulatedFlasher):
    """Pauses the campaign from inside the flash call, producing a genuine
    mid-flight checkpoint (2 devices done, rest never dispatched)."""

    def __init__(self, campaign_id, after):
        super().__init__()
        self._campaign_id = campaign_id
        self._after = after
        self.orchestrator = None

    def __call__(self, device_id, target_version):
        super().__call__(device_id, target_version)
        if len(self._versions) >= self._after and self.orchestrator:
            self.orchestrator.pause(self._campaign_id)


class TestUnfinishedCampaignAcrossRestart:
    """带未完成任务恢复后重启：续跑只补未完成部分，控制操作全部可用。"""

    def _gateway_with_midflight_campaign(self, state_path, clock):
        reg = DeviceRegistry(clock=clock)
        tel = TelemetryStore(clock=clock)
        flasher = PausingFlasher("c1", after=2)
        orc = FirmwareOrchestrator(reg, flasher, clock=clock)
        flasher.orchestrator = orc
        bm = BackupManager(reg, tel, orc, flasher, clock=clock)
        store = StateStore(bm, str(state_path), clock=clock).attach()
        store.load()
        for i in range(4):
            reg.register(f"d{i}", "pw")
        orc.create_campaign("c1", "3.0.0",
                            [f"d{i}" for i in range(4)], batch_size=1)
        result = orc.run_until_blocked("c1")
        assert result["status"] == "paused"  # stopped mid-flight by the hook
        return reg, tel, flasher, orc, bm, store

    def test_restore_restart_then_resume_only_dispatches_unfinished(
            self, tmp_path):
        clock = FakeClock(now=50_000.0)
        reg, tel, flasher, orc, bm, _ = \
            self._gateway_with_midflight_campaign(tmp_path / "x", clock)
        assert flasher.version_of("d0") == "3.0.0"
        assert flasher.version_of("d1") == "3.0.0"
        raw = bm.export_bytes()

        # clean machine: restore, restart, then drive the campaign
        state = tmp_path / "migrated" / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, clock=clock)
        b1.restore_bytes(raw)
        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, clock=clock)
        assert out2 == "loaded"
        st = o2.status("c1")
        print(f"[resume-restart] restored: status={st['status']} "
              f"states={st['device_states']}")
        assert st["status"] == "paused"
        assert st["device_states"]["d0"] == "succeeded"
        assert st["device_states"]["d1"] == "succeeded"
        assert st["device_states"]["d2"] == "pending"

        # pause is idempotent on the parked campaign; resume finishes it and
        # only the genuinely unfinished devices are dispatched
        o2.pause("c1")
        final = o2.resume("c1")
        print(f"[resume-restart] final={final['status']} calls={f2.calls()}")
        assert final["status"] == "completed"
        assert set(f2.calls()) == {("d2", "3.0.0"), ("d3", "3.0.0")}
        assert all(r2.get(f"d{i}").firmware_version == "3.0.0"
                   for i in range(4))
        assert all(f2.version_of(f"d{i}") == "3.0.0" for i in range(4))

        # the finished campaign stays finished across yet another restart
        r3, t3, f3, o3, b3, s3, out3 = boot_gateway(state, clock=clock)
        assert out3 == "loaded"
        assert o3.status("c1")["status"] == "completed"
        with pytest.raises(GatewayError) as e:
            o3.pause("c1")  # terminal campaigns reject control operations
        assert e.value.code is ErrorCode.TASK_STATE_CONFLICT
        print("[resume-restart] terminal campaign rejects pause after restart")

    def test_cancel_after_restart_and_no_stuck_tasks(self, tmp_path):
        clock = FakeClock(now=50_000.0)
        reg, tel, flasher, orc, bm, _ = \
            self._gateway_with_midflight_campaign(tmp_path / "y", clock)
        raw = bm.export_bytes()

        state = tmp_path / "m2" / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, clock=clock)
        b1.restore_bytes(raw)
        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, clock=clock)
        assert out2 == "loaded"
        cancelled = o2.cancel("c1")
        print(f"[cancel-restart] status={cancelled['status']} "
              f"states={cancelled['device_states']}")
        assert cancelled["status"] == "cancelled"
        assert cancelled["device_states"]["d0"] == "succeeded"
        assert cancelled["device_states"]["d2"] == "skipped"
        assert f2.calls() == []  # nothing dispatched on the new process
        # cancellation is durable across the next restart too
        _, _, _, o3, _, _, out3 = boot_gateway(state, clock=clock)
        assert out3 == "loaded"
        assert o3.status("c1")["status"] == "cancelled"

    def test_crash_mid_campaign_checkpoint_normalizes_on_boot(self, tmp_path):
        """A checkpoint captured while RUNNING (crash window) must boot into
        paused/pending, then resume only the unfinished devices."""
        clock = FakeClock(now=50_000.0)
        state = tmp_path / "crash" / "gw.zip"
        reg, tel, flasher, orc, bm, _ = \
            self._gateway_with_midflight_campaign(state, clock)
        # rewrite the checkpoint as if the process died mid-dispatch of d2
        raw = state.read_bytes() if hasattr(state, "read_bytes") \
            else open(state, "rb").read()
        manifest, payload = package_parts(raw)
        camp = payload["fota"]["campaigns"][0]
        camp["status"] = "running"
        camp["device_states"]["d2"] = "in_progress"
        camp["attempts"]["d2"] = 1
        state.write_bytes(resigned(manifest, payload))

        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, clock=clock)
        print(f"[crash-boot] outcome={out2} status={s2.status()['state']}")
        assert out2 == "loaded"
        st = o2.status("c1")
        print(f"[crash-boot] normalized: status={st['status']} "
              f"states={st['device_states']}")
        assert st["status"] == "paused"           # never stuck running
        assert st["device_states"]["d2"] == "pending"
        final = o2.resume("c1")
        assert final["status"] == "completed"
        # d0/d1 already had the firmware; only d2/d3 are dispatched now
        assert set(f2.calls()) == {("d2", "3.0.0"), ("d3", "3.0.0")}
        print(f"[crash-boot] resumed, calls={f2.calls()}")


# ---------------------------------------------------------------------------
class TestCorruptStateOnBoot:
    """重启期间状态异常：明确报错、隔离坏档、不半恢复、备份仍可导入。"""

    def test_truncated_checkpoint_is_quarantined_and_gateway_usable(
            self, tmp_path):
        clock = FakeClock(now=10_000.0)
        *_, bm = build_rich_old_machine(clock)
        good_backup = bm.export_bytes()

        state = tmp_path / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, ttl=60, clock=clock)
        b1.restore_bytes(good_backup)
        # crash / disk failure truncates the checkpoint
        raw = state.read_bytes()
        state.write_bytes(raw[: len(raw) // 2])

        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, ttl=60, clock=clock)
        print(f"[corrupt] outcome={out2} status={s2.status()}")
        assert out2 == "error"
        assert "backup_truncated" in s2.status()["last_error"]
        assert not state.exists()               # moved aside, not loaded
        quarantined = s2.status()["quarantined_to"]
        assert quarantined and os.path.exists(quarantined)

        # gateway is empty but fully usable: backups still import
        summary = b2.restore_bytes(good_backup)
        print(f"[corrupt] re-imported backup after quarantine: {summary}")
        assert r2.authenticate("d-active", "pw-a").device_id == "d-active"
        # and the next restart loads the recovered state
        r3, t3, f3, o3, b3, s3, out3 = boot_gateway(state, ttl=60, clock=clock)
        assert out3 == "loaded"
        assert r3.authenticate("d-active", "pw-a").device_id == "d-active"
        assert t3.shadow("d-active") == {"temp": 22, "mode": "auto"}

    def test_tampered_checkpoint_is_reported_not_half_loaded(self, tmp_path):
        clock = FakeClock(now=10_000.0)
        state = tmp_path / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, ttl=60, clock=clock)
        b1.restore_bytes(build_rich_old_machine(clock)[4].export_bytes())
        manifest, payload = package_parts(state.read_bytes())
        payload["registry"]["devices"][0]["credential"]["secret_hash"] = \
            "0" * 64
        state.write_bytes(repackage(manifest, payload))  # checksum now stale

        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, ttl=60, clock=clock)
        print(f"[tampered] outcome={out2} error={s2.status()['last_error']}")
        assert out2 == "error"
        assert "backup_integrity_failed" in s2.status()["last_error"]
        # nothing half-loaded: the gateway holds zero devices, not a subset
        assert r2.snapshot()["devices"] == []
        assert t2.snapshot()["devices"] == []

    def test_half_written_tmp_file_does_not_shadow_last_good_checkpoint(
            self, tmp_path):
        clock = FakeClock(now=10_000.0)
        state = tmp_path / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, ttl=60, clock=clock)
        b1.restore_bytes(build_rich_old_machine(clock)[4].export_bytes())
        # crash mid-persist leaves a garbage tmp file next to the good one
        with open(f"{state}.tmp", "wb") as fh:
            fh.write(b"partial-write")

        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, ttl=60, clock=clock)
        print(f"[tmp-leftover] outcome={out2}")
        assert out2 == "loaded"
        assert r2.authenticate("d-active", "pw-a").device_id == "d-active"

    def test_failed_import_clobbers_neither_checkpoint_nor_live_state(
            self, tmp_path):
        clock = FakeClock(now=10_000.0)
        state = tmp_path / "gw.zip"
        r1, t1, f1, o1, b1, s1, _ = boot_gateway(state, ttl=60, clock=clock)
        b1.restore_bytes(build_rich_old_machine(clock)[4].export_bytes())
        checkpoint_before = state.read_bytes()

        manifest, payload = package_parts(checkpoint_before)
        payload["registry"]["devices"] = "broken"
        bad = resigned(manifest, payload)
        with pytest.raises(BackupError) as e:
            b1.restore_bytes(bad, allow_overwrite=True)
        print(f"[failed-import] refused with {e.value.code.value}")
        assert e.value.code is BackupRejection.PAYLOAD_INVALID
        # live state and on-disk checkpoint both untouched
        assert r1.authenticate("d-active", "pw-a").device_id == "d-active"
        assert state.read_bytes() == checkpoint_before
        r2, t2, f2, o2, b2, s2, out2 = boot_gateway(state, ttl=60, clock=clock)
        assert out2 == "loaded"
        assert r2.authenticate("d-active", "pw-a").device_id == "d-active"


# ---------------------------------------------------------------------------
class TestRepeatImportAndRestartConsistency:
    """同一份备份重复导入、连续多次重启，结果都必须一致。"""

    def test_repeated_imports_and_restarts_are_identical(self, tmp_path):
        clock = FakeClock(now=10_000.0)
        raw = build_rich_old_machine(clock)[4].export_bytes()

        states = []
        for i in (1, 2):  # two independent imports of the same package
            r, t, f, o, b, s, _ = boot_gateway(tmp_path / f"gw{i}.zip",
                                               ttl=60, clock=clock)
            b.restore_bytes(raw)
            states.append(component_state(r, t, f, o))
        assert states[0] == states[1]
        print("[repeat] two imports of one backup -> identical state")

        # and restarts never drift away from it
        r, t, f, o, b, s, out = boot_gateway(tmp_path / "gw1.zip", ttl=60,
                                             clock=clock)
        assert out == "loaded"
        assert component_state(r, t, f, o) == states[0]
        print("[repeat] restart after import -> still identical")
