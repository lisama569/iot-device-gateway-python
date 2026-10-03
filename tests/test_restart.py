"""Restart-persistence tests for backup restore (the bug fixed in this round).

A backup restored into a clean environment used to survive only until the
next process restart: registry/telemetry/flasher state was memory-only and
the lone FOTA state file then referenced devices that no longer existed.
These tests build a *full durable gateway* pointed at a state file, kill it,
and construct a brand new component set from the same file -- the in-process
equivalent of restarting the process / redeploying onto a new machine.
"""
import io
import json
import time
import zipfile

import pytest

from app.backup import BackupManager
from app.durable import DurableState
from app.fota import FirmwareOrchestrator
from app.models import (BackupError, CampaignStatus, DeviceUpdateState,
                        ErrorCode, GatewayError)
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore


# --- a "process" is one fresh set of components over the same state file ---
class GatewayProcess:
    def __init__(self, state_path, ttl=None, quota=1000, clock=time.time):
        self.state_path = state_path
        self.durable = DurableState(None, state_path)
        # deterministic unit harness: every component change is flushed
        # synchronously (coalesced telemetry behavior is tested separately)
        urgent = self.durable.flush_urgent
        self.registry = DeviceRegistry(credential_ttl_seconds=ttl,
                                       clock=clock, on_change=urgent)
        self.telemetry = TelemetryStore(per_device_quota=quota,
                                        clock=clock, on_change=urgent)
        self.flasher = SimulatedFlasher()
        self.orchestrator = FirmwareOrchestrator(
            self.registry, self.flasher, clock=clock, on_change=urgent)
        self.backups = BackupManager(self.registry, self.telemetry,
                                     self.orchestrator, self.flasher,
                                     clock=clock)
        self.durable._backups = self.backups
        self.backups._durable = self.durable

    def start(self):
        """Boot: load durable state (none on the very first start)."""
        self.startup_summary = self.durable.load_at_startup()
        return self


def boot(state_path, *, clock=time.time, **kw):
    """A cold start: new process over the same durable file."""
    return GatewayProcess(state_path, clock=clock, **kw).start()


def _repack(manifest, payload: dict) -> bytes:
    pb = json.dumps(payload).encode()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest))
        zf.writestr("data.json", pb)
    return buf.getvalue()


# === 业务一：恢复后重启仍能接入与查询 ======================================
def test_restart_after_restore_keeps_access_shadow_dedup_and_rejects(tmp_path):
    """老凭据/撤销停用过期规则、影子、判重、拒绝记录、实装版本在重启后一致。"""
    state = str(tmp_path / "durable" / "state.zip")

    class Clock:
        def __init__(self, t=10_000.0): self.t = t
        def __call__(self): return self.t

    clock = Clock()

    # --- "old machine": build a backup through the export surface ---
    old = boot(None, clock=clock)
    old.registry.register("keep", "snow")
    old.registry.register("rev", "r1")
    old.registry.revoke_credential("rev")
    old.registry.register("off", "d1")
    old.registry.disable("off")
    old.registry.register("gone", "x1")
    old.registry.deregister("gone")
    old.telemetry.ingest("keep", "m1", 1, 9_900.0, {"rpm": 1200, "mode": "auto"})
    old.telemetry.ingest("keep", "m2", 2, 10_000.0, {"rpm": 1300})
    old.telemetry.ingest("keep", "m1", 1, 9_900.0, {"rpm": 1200})  # duplicate
    old.telemetry.ingest("keep", "bad", 1, 999_999.0, {"x": 1})    # rejected
    raw = old.backups.export_bytes()

    # --- "new machine": restore once, then COLD RESTART (same timeline) ---
    gw = boot(state, clock=clock)
    summary = gw.backups.restore_bytes(raw)
    print(f"[restart-access] restore summary={summary}")
    assert summary["devices"] == 4

    gw = boot(state, clock=clock)  # <-- the op that used to wipe everything
    assert gw.durable.status()["healthy"] is True
    print(f"[restart-access] health={gw.durable.status()}")

    # 1) old credentials still connect; revoked / disabled / deregistered
    #    keep producing the SAME distinguishable errors as pre-restart
    assert gw.registry.authenticate("keep", "snow").device_id == "keep"
    with pytest.raises(GatewayError) as e:
        gw.registry.authenticate("rev", "r1")
    assert e.value.code is ErrorCode.CREDENTIAL_REVOKED
    with pytest.raises(GatewayError) as e:
        gw.registry.authenticate("off", "d1")
    assert e.value.code is ErrorCode.DEVICE_DISABLED
    with pytest.raises(GatewayError) as e:
        gw.registry.authenticate("gone", "x1")
    assert e.value.code is ErrorCode.DEVICE_NOT_FOUND
    with pytest.raises(GatewayError) as e:
        gw.registry.authenticate("keep", "wrong")
    assert e.value.code is ErrorCode.CREDENTIAL_INVALID
    print("[restart-access] revoked/disabled/not-found/invalid all distinct")

    # 2) shadow + per-field versions identical
    assert gw.telemetry.shadow("keep") == {"rpm": 1300, "mode": "auto"}
    assert gw.telemetry.field_versions("keep") == {
        "rpm": (10_000.0, 2), "mode": (9_900.0, 1)}

    # 3) dedup memory survives: replay still a duplicate, count unchanged
    dup = gw.telemetry.ingest("keep", "m1", 1, 100.0, {"rpm": 1200})
    assert dup.code == "duplicate_message"
    assert gw.telemetry.accepted_count("keep") == 2

    # 4) rejected records survive
    assert [r.code for r in gw.telemetry.rejected("keep")] == [
        "validation_failed"]
    print("[restart-access] shadow/versions/dedup/rejected all preserved")


def test_credential_expiry_rule_survives_restart(tmp_path):
    """凭据的过期语义（绝对过期时间随包恢复）重启后仍然成立。"""
    state = str(tmp_path / "d" / "state.zip")

    class Clock:
        def __init__(self, t): self.t = t
        def __call__(self): return self.t

    clock = Clock(10_000.0)
    old = boot(None, ttl=60, clock=clock)
    old.registry.register("d1", "pw")
    raw = old.backups.export_bytes()

    gw = boot(state, ttl=60, clock=clock)
    gw.backups.restore_bytes(raw)
    # restart with the same (absolute) timeline in effect
    gw = boot(state, ttl=60, clock=clock)
    clock.t = 10_061.0  # past TTL in the restored timeline
    with pytest.raises(GatewayError) as e:
        gw.registry.authenticate("d1", "pw")
    print(f"[restart-expiry] after restart code={e.value.code.value}")
    assert e.value.code is ErrorCode.CREDENTIAL_EXPIRED
    clock.t = 10_059.0
    assert gw.registry.authenticate("d1", "pw").device_id == "d1"


def test_installed_firmware_version_survives_restart(tmp_path):
    """每台设备实际刷上的固件版本在重启后保持（不会退回 0.0.0）。"""
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    gw.orchestrator.create_campaign("c1", "7.7.7", ["d1"], batch_size=1)
    final = gw.orchestrator.run_until_blocked("c1")
    assert final["status"] == "completed"

    gw = boot(state)
    assert gw.flasher.version_of("d1") == "7.7.7"
    assert gw.registry.get("d1").firmware_version == "7.7.7"
    st = gw.orchestrator.status("c1")
    assert st["status"] == "completed"
    assert st["device_states"]["d1"] == "succeeded"
    print("[restart-version] installed 7.7.7 + completed campaign survive")


# === 业务二：带未完成任务恢复后重启再续跑 ================================
def test_unfinished_campaign_resumes_after_restart_without_redelivering(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    for i in range(4):
        gw.registry.register(f"d{i}", "pw")
    gw.orchestrator.create_campaign("c-mid", "9.0.0",
                                    [f"d{i}" for i in range(4)], batch_size=2)
    # genuine mid-flight durable checkpoint: d0 done, rest not dispatched
    gw.orchestrator.pause("c-mid")
    gw.orchestrator._campaigns["c-mid"].device_states["d0"] = \
        DeviceUpdateState.SUCCEEDED
    gw.orchestrator._campaigns["c-mid"].attempts["d0"] = 1
    gw.flasher("d0", "9.0.0")
    gw.registry.get("d0").firmware_version = "9.0.0"
    gw.durable.flush_urgent()

    # restart, restart again: must stay parked, never stuck running
    gw = boot(state)
    st = gw.orchestrator.status("c-mid")
    print(f"[restart-continue] after 1st restart: {st['status']} "
          f"{st['device_states']}")
    assert st["status"] == "paused"
    assert st["device_states"]["d0"] == "succeeded"
    gw = boot(state)
    assert gw.orchestrator.status("c-mid")["status"] == "paused"

    # pause/resume/cancel semantics all alive after restart
    gw.orchestrator.pause("c-mid")  # idempotent parking
    final = gw.orchestrator.resume("c-mid")
    assert final["status"] == "completed"
    # succeeded d0 is NOT re-dispatched; only the real remainder flashes
    assert set(gw.flasher.calls()) == {
        ("d1", "9.0.0"), ("d2", "9.0.0"), ("d3", "9.0.0")}
    assert all(gw.registry.get(f"d{i}").firmware_version == "9.0.0"
               for i in range(4))
    print(f"[restart-continue] completed, calls={gw.flasher.calls()}")


def test_campaign_caught_running_normalizes_on_restart_and_cancels(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    for i in range(3):
        gw.registry.register(f"d{i}", "pw")
    gw.orchestrator.create_campaign("c-run", "4.0.0",
                                    [f"d{i}" for i in range(3)], batch_size=1)
    gw.orchestrator.pause("c-run")
    # export a package caught with status running + in_progress device
    raw = gw.backups.export_bytes()
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        payload = json.loads(zf.read("data.json"))
    payload["fota"]["campaigns"][0]["status"] = "running"
    payload["fota"]["campaigns"][0]["device_states"]["d0"] = "in_progress"
    payload_bytes = json.dumps(payload).encode()
    manifest["sha256"] = __import__("hashlib").sha256(payload_bytes).hexdigest()
    pkg = _repack(manifest, payload)

    # clean-environment restore (fresh process, no startup state yet)
    gw2 = GatewayProcess(state)
    gw2.backups.restore_bytes(pkg)
    gw2 = boot(state)  # restart
    st = gw2.orchestrator.status("c-run")
    print(f"[restart-running] status={st['status']} states={st['device_states']}")
    assert st["status"] == "paused"
    assert st["device_states"]["d0"] == "pending"
    cancelled = gw2.orchestrator.cancel("c-run")
    assert cancelled["status"] == "cancelled"
    assert cancelled["device_states"]["d0"] == "skipped"
    assert gw2.flasher.calls() == []


def test_running_campaign_survives_a_crash_anywhere_in_dispatch(tmp_path):
    """Crash after d0 of batch 2 flashes: d0 durable, rest resume, no dup."""
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    for i in range(4):
        gw.registry.register(f"d{i}", "pw")
    gw.orchestrator.create_campaign("c-crash", "6.1.0",
                                    [f"d{i}" for i in range(4)], batch_size=10)
    # mark running + d0 succeeded (per-device flush point), then "power loss"
    camp = gw.orchestrator._campaigns["c-crash"]
    camp.status = CampaignStatus.RUNNING
    camp.device_states["d0"] = DeviceUpdateState.SUCCEEDED
    camp.attempts["d0"] = 1
    gw.flasher("d0", "6.1.0")
    gw.registry.get("d0").firmware_version = "6.1.0"
    gw.durable.flush_urgent()

    gw = boot(state)
    final = gw.orchestrator.resume("c-crash")
    assert final["status"] == "completed"
    assert gw.flasher.calls().count(("d0", "6.1.0")) == 0
    assert set(gw.flasher.calls()) == {
        ("d1", "6.1.0"), ("d2", "6.1.0"), ("d3", "6.1.0")}
    print(f"[restart-crash] no redelivery, calls={gw.flasher.calls()}")


# === 业务三：重启期间状态异常 ============================================
def test_corrupt_primary_falls_back_to_predecessor(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    gw.telemetry.ingest("d1", "m1", 1, 9.0, {"a": 1})
    # force the predecessor to exist with this good state
    import shutil
    shutil.copyfile(state, state + ".bak")
    with open(state, "wb") as fh:
        fh.write(b"GARBAGE NOT A ZIP")

    gw = boot(state)
    assert gw.durable.status()["healthy"] is True
    assert gw.durable.status()["recovered_from"].endswith(".bak")
    assert gw.registry.authenticate("d1", "pw").device_id == "d1"
    print(f"[restart-corrupt] fell back to predecessor, healthy again")


def test_all_state_files_unusable_gateway_faults_and_reports_clearly(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    with open(state, "wb") as fh:
        fh.write(b"broken-primary")
    with open(state + ".bak", "wb") as fh:
        fh.write(b"broken-backup")

    gw = boot(state)
    assert gw.durable.faulted is True
    print(f"[restart-fault] fault={gw.durable.status()['fault'][:100]}")
    # not an empty gateway: the devices are NOT loaded, business calls are
    # refused with a distinct, explainable error -- both at the explicit
    # health gate and through the component mutation hook
    with pytest.raises(GatewayError) as e:
        gw.durable.require_healthy()
    assert e.value.code is ErrorCode.STATE_UNAVAILABLE
    with pytest.raises(GatewayError) as e:
        gw.registry.register("d2", "pw")  # hook refuses to persist on fault
    assert e.value.code is ErrorCode.STATE_UNAVAILABLE
    assert "d2" not in gw.registry._devices  # nothing partially written
    # importing a good backup is the escape hatch and clears the fault
    good = BackupManager(
        DeviceRegistry(), TelemetryStore(),
        FirmwareOrchestrator(DeviceRegistry(), SimulatedFlasher()),
        SimulatedFlasher()).export_bytes()
    gw.backups.restore_bytes(good, allow_overwrite=True)
    assert gw.durable.faulted is False
    gw.durable.require_healthy()
    # and that recovery itself survives yet another restart
    gw = boot(state)
    assert gw.durable.faulted is False
    print("[restart-fault] restore cleared fault and survived a restart")


def test_failed_import_never_damages_working_gateway_or_durable_file(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("live", "pw")
    gw.telemetry.ingest("live", "m1", 1, 9.0, {"v": 1})
    good = gw.backups.export_bytes()
    with zipfile.ZipFile(io.BytesIO(good)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        payload = json.loads(zf.read("data.json"))

    # 1) checksum-broken package (signature kept) -> integrity refused
    tampered = dict(payload)
    tampered["registry"] = {"devices": "broken"}
    with pytest.raises(BackupError):
        gw.backups.restore_bytes(_repack(manifest, tampered),
                                 allow_overwrite=True)
    # 2) structurally broken but re-signed -> payload invalid, still refused
    pb = json.dumps(tampered).encode()
    m2 = dict(manifest,
              sha256=__import__("hashlib").sha256(pb).hexdigest())
    with pytest.raises(BackupError):
        gw.backups.restore_bytes(_repack(m2, json.loads(pb)),
                                 allow_overwrite=True)

    # live in-memory state fully intact
    assert gw.registry.authenticate("live", "pw").device_id == "live"
    assert gw.telemetry.shadow("live") == {"v": 1}
    # durable file intact: a cold restart still returns the working state
    gw2 = boot(state)
    assert gw2.registry.authenticate("live", "pw").device_id == "live"
    assert gw2.telemetry.shadow("live") == {"v": 1}
    print("[restart-badimport] live state and durable file both intact")


def test_repeated_restarts_and_repeated_imports_converge(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    gw.telemetry.ingest("d1", "m1", 1, 9.0, {"a": 1})
    gw.orchestrator.create_campaign("c1", "1.0.0", ["d1"])
    gw.orchestrator.pause("c1")
    raw = gw.backups.export_bytes()

    def cold():
        g = boot(state)
        snap = g.backups.snapshot()
        snap.pop("created_at", None)
        return snap

    s1, s2, s3 = cold(), cold(), cold()
    assert s1 == s2 == s3
    print("[restart-converge] three cold restarts produced identical state")

    def fresh_import():
        g = boot(None)
        g.backups.restore_bytes(raw)
        snap = g.backups.snapshot()
        snap.pop("created_at", None)
        return snap

    assert fresh_import() == fresh_import()
    print("[restart-converge] repeated imports produced identical state")


# === 业务四：持久化写入失败时不产生半状态 =================================
def test_memory_change_is_rolled_back_when_snapshot_cannot_be_persisted(tmp_path):
    """落盘失败的注册/遥测变更不能留在内存里（要么整份成功，要么没有）。"""
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    gw.telemetry.ingest("d1", "m0", 1, 9.0, {"k": 0})

    def failing_write(path, raw):
        raise OSError("simulated disk outage")

    # components call the durable object's dynamically-looked-up writer, so
    # patching it simulates an outage for every change notification
    gw.durable._write_atomic = failing_write  # type: ignore[assignment]

    # register / rotate / disable all roll back
    with pytest.raises(GatewayError) as e:
        gw.registry.register("d2", "pw2")
    assert e.value.code is ErrorCode.STATE_PERSIST_FAILED
    assert "d2" not in gw.registry._devices
    with pytest.raises(GatewayError):
        gw.registry.rotate_credential("d1", "newpw")
    assert gw.registry.authenticate("d1", "pw").device_id == "d1"  # old still works
    with pytest.raises(GatewayError):
        gw.registry.disable("d1")
    assert gw.registry.authenticate("d1", "pw").device_id == "d1"  # still active

    # accepted telemetry is rolled back too; after recovery the message id
    # was never consumed, so it still ingests normally
    with pytest.raises(GatewayError):
        gw.telemetry.ingest("d1", "m1", 2, 10.0, {"k": 1})
    assert gw.telemetry.shadow("d1") == {"k": 0}
    assert gw.telemetry.accepted_count("d1") == 1

    # rejected (durable) records likewise roll back
    with pytest.raises(GatewayError):
        gw.telemetry.ingest("d1", "bad", 1, 999_999.0, {"x": 1})
    assert [r.code for r in gw.telemetry.rejected("d1")] == []

    # once disk works again, state re-syncs and converges
    del gw.durable._write_atomic
    gw.registry.register("d2", "pw2")
    gw.telemetry.ingest("d1", "m1", 2, 10.0, {"k": 1})
    gw2 = boot(state)
    assert gw2.registry.authenticate("d2", "pw2").device_id == "d2"
    assert gw2.telemetry.shadow("d1") == {"k": 1}
    print("[restart-persistfail] failed writes rolled back, then re-synced")


def test_export_refused_while_faulted_and_restore_clears(tmp_path):
    """faulted 时禁止导出残缺状态；导入恢复后导出与业务都恢复。"""
    state = str(tmp_path / "d" / "state.zip")
    gw = boot(state)
    gw.registry.register("d1", "pw")
    with open(state, "wb") as fh:
        fh.write(b"bad")
    with open(state + ".bak", "wb") as fh:
        fh.write(b"bad")
    gw = boot(state)
    assert gw.durable.faulted

    # export would only propagate a broken/unclear store -> the HTTP layer
    # refuses it while faulted; the component-level gate is what endpoints
    # call (covered over HTTP in test_api/test_restart_http too)
    with pytest.raises(GatewayError) as e:
        gw.durable.require_healthy()
    assert e.value.code is ErrorCode.STATE_UNAVAILABLE

    good = BackupManager(
        DeviceRegistry(), TelemetryStore(),
        FirmwareOrchestrator(DeviceRegistry(), SimulatedFlasher()),
        SimulatedFlasher()).export_bytes()
    gw.backups.restore_bytes(good, allow_overwrite=True)
    assert not gw.durable.faulted
    gw.durable.require_healthy()
    assert gw.backups.export_bytes()[:2] == b"PK"
    print("[restart-fault-export] export gated while faulted, restored after")


# === 业务五：高频遥测的合并落盘与优雅停机 =================================
def _coalesced_gateway(state_path, debounce=0.1, **kw):
    """Like GatewayProcess but telemetry rides the coalescing path, exactly
    as main.py wires it in production."""
    g = GatewayProcess.__new__(GatewayProcess)
    g.state_path = state_path
    g.durable = DurableState(None, state_path, debounce_seconds=debounce)
    urgent = g.durable.flush_urgent
    g.registry = DeviceRegistry(on_change=urgent)
    g.telemetry = TelemetryStore(on_change=g.durable.flush_soon)
    g.flasher = SimulatedFlasher()
    g.orchestrator = FirmwareOrchestrator(g.registry, g.flasher,
                                          on_change=urgent)
    g.backups = BackupManager(g.registry, g.telemetry, g.orchestrator,
                              g.flasher)
    g.durable._backups = g.backups
    g.backups._durable = g.durable
    g.startup_summary = g.durable.load_at_startup()
    return g


def test_coalesced_telemetry_durable_after_debounce_and_shutdown(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = _coalesced_gateway(state, debounce=0.1)
    gw.registry.register("d1", "pw")
    for i in range(100):  # burst: these share (a few) coalesced writes
        gw.telemetry.ingest("d1", f"m{i}", i, float(i), {"gauge": i})

    # a restart *immediately* may only see what was durably written; after
    # the debounce window the whole burst is on disk
    time.sleep(0.35)
    gw.durable.shutdown()
    gw2 = _coalesced_gateway(state, debounce=0.1)
    assert gw2.telemetry.accepted_count("d1") == 100
    assert gw2.telemetry.shadow("d1") == {"gauge": 99}
    print("[restart-coalesce] 100-message burst survived shutdown/restart")


def test_urgent_campaign_flush_also_captures_pending_coalesced_telemetry(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = _coalesced_gateway(state, debounce=5.0)  # telemetry would wait forever
    gw.registry.register("d1", "pw")
    gw.telemetry.ingest("d1", "m1", 1, 9.0, {"a": 1})  # scheduled, not written
    # an urgent campaign step snapshots the *whole* state, telemetry included
    gw.orchestrator.create_campaign("c1", "1.0.0", ["d1"], batch_size=1)
    gw.orchestrator.run_until_blocked("c1")
    gw.durable.shutdown()

    gw2 = _coalesced_gateway(state)
    assert gw2.telemetry.shadow("d1") == {"a": 1}
    assert gw2.orchestrator.status("c1")["status"] == "completed"
    print("[restart-coalesce] urgent FOTA snapshot captured pending telemetry")


def test_coalesced_write_failure_surfaces_and_rolls_back_next_change(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = _coalesced_gateway(state, debounce=0.0)
    gw.registry.register("d1", "pw")

    def failing_write(path, raw):
        raise OSError("disk full")

    gw.durable._write_atomic = failing_write  # type: ignore[assignment]
    gw.telemetry.ingest("d1", "m1", 1, 9.0, {"a": 1})  # schedules a failed write
    time.sleep(0.15)
    # the next change surfaces the failed background write and rolls back
    with pytest.raises(GatewayError) as e:
        gw.telemetry.ingest("d1", "m2", 2, 10.0, {"a": 2})
    assert e.value.code is ErrorCode.STATE_PERSIST_FAILED
    assert gw.telemetry.shadow("d1") == {"a": 1}  # m2 rolled back
    assert gw.telemetry.accepted_count("d1") == 1
    print("[restart-coalesce] background write failure surfaced + rolled back")


def test_sync_drains_coalesced_backlog_before_returning(tmp_path):
    state = str(tmp_path / "d" / "state.zip")
    gw = _coalesced_gateway(state, debounce=5.0)
    gw.registry.register("d1", "pw")
    gw.telemetry.ingest("d1", "m1", 1, 9.0, {"a": 1})
    gw.durable.sync()  # explicit barrier: everything before it is durable now
    gw.durable.shutdown()
    gw2 = _coalesced_gateway(state)
    assert gw2.telemetry.shadow("d1") == {"a": 1}
    print("[restart-coalesce] sync() barrier drained the backlog")
