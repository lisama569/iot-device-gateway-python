"""End-to-end tests through the FastAPI surface using simulated device traffic."""
import io
import json
import zipfile

from fastapi.testclient import TestClient

import main
from main import app, orchestrator, registry, telemetry

client = TestClient(app)


def test_full_device_lifecycle_and_telemetry_flow():
    # register (twice: idempotent)
    r1 = client.post("/devices/register", json={"device_id": "api-1", "secret": "s1"})
    r2 = client.post("/devices/register", json={"device_id": "api-1", "secret": "s1"})
    assert r1.status_code == 200 and r1.json() == r2.json()
    # conflicting credential
    r3 = client.post("/devices/register", json={"device_id": "api-1", "secret": "sX"})
    print(f"[api-conflict] {r3.status_code} {r3.json()['detail']['code']}")
    assert r3.status_code == 409
    assert r3.json()["detail"]["code"] == "device_id_conflict"

    # telemetry with auth; out-of-order + duplicate via simulated stream
    headers = {"X-Device-Secret": "s1"}
    msgs = [("m2", 2, 9200.0, {"temp": 22}),
            ("m1", 1, 9100.0, {"temp": 21}),   # late, older
            ("m2", 2, 9200.0, {"temp": 22})]   # retry duplicate
    codes = []
    for mid, seq, ts, payload in msgs:
        resp = client.post("/devices/api-1/telemetry",
                           json={"message_id": mid, "seq": seq,
                                 "device_ts": ts, "payload": payload},
                           headers=headers)
        codes.append(resp.json()["code"])
    print(f"[api-telemetry] codes={codes}")
    assert codes == ["ok", "ok", "duplicate_message"]
    shadow = client.get("/devices/api-1/shadow").json()
    print(f"[api-shadow] {shadow}")
    assert shadow["shadow"]["temp"] == 22  # no rollback from the late message

    # disable blocks further ingest immediately
    client.post("/devices/api-1/disable")
    resp = client.post("/devices/api-1/telemetry",
                       json={"message_id": "m3", "seq": 3,
                             "device_ts": 9300.0, "payload": {"temp": 23}},
                       headers=headers)
    print(f"[api-disabled] {resp.status_code} {resp.json()['detail']['code']}")
    assert resp.status_code == 403
    assert resp.json()["detail"]["code"] == "device_disabled"


def test_campaign_over_http():
    for i in range(3):
        client.post("/devices/register",
                    json={"device_id": f"fw-{i}", "secret": "pw"})
    resp = client.post("/campaigns", json={
        "campaign_id": "api-c1", "target_version": "7.0.0",
        "device_ids": ["fw-0", "fw-1", "fw-2"], "batch_size": 2})
    assert resp.status_code == 200
    final = client.post("/campaigns/api-c1/run").json()
    print(f"[api-campaign] status={final['status']} states={final['device_states']}")
    assert final["status"] == "completed"
    st = client.get("/campaigns/api-c1").json()
    assert st["status"] == "completed"
    # unknown campaign -> 404 with distinct code
    resp = client.get("/campaigns/nope")
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "task_not_found"


# --- backup / restore over HTTP ------------------------------------------
def test_backup_export_inspect_restore_over_http():
    # build some real lifecycle/shadow state
    client.post("/devices/register", json={"device_id": "bk-1", "secret": "k"})
    client.post("/devices/register", json={"device_id": "bk-2", "secret": "k"})
    client.post("/devices/bk-1/telemetry",
                json={"message_id": "bm1", "seq": 1, "device_ts": 100.0,
                      "payload": {"temp": 9}},
                headers={"X-Device-Secret": "k"})
    resp = client.post("/campaigns", json={
        "campaign_id": "bk-c1", "target_version": "1.2.3",
        "device_ids": ["bk-1", "bk-2"], "batch_size": 1})
    assert resp.status_code == 200

    exported = client.post("/admin/backup/export")
    assert exported.status_code == 200
    assert exported.headers["content-type"] == "application/zip"
    raw = exported.content
    print(f"[api-backup] package {len(raw)} bytes")

    # inspect works on an already-populated gateway (no state change)
    insp = client.post("/admin/backup/inspect", content=raw)
    assert insp.status_code == 200, insp.text
    body = insp.json()
    print(f"[api-inspect] {body}")
    assert body["devices"] >= 2 and body["campaigns"] >= 1

    # restoring into the non-empty gateway is refused, state is untouched
    refused = client.post("/admin/backup/restore", content=raw)
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "backup_nonempty_target"

    # truncated / tampered packages are distinguished
    assert (client.post("/admin/backup/inspect", content=raw[:20]).json()
            ["detail"]["code"] == "backup_truncated")
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        payload = json.loads(zf.read("data.json"))
    payload["registry"]["devices"][0]["credential"]["secret_hash"] = "f" * 64
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf2:
        zf2.writestr("manifest.json", json.dumps(manifest))
        zf2.writestr("data.json", json.dumps(payload))
    tampered = client.post("/admin/backup/inspect", content=buf.getvalue())
    assert tampered.json()["detail"]["code"] == "backup_integrity_failed"

    # after wiping the in-process gateway, restore succeeds and everything is
    # observable through the same HTTP surface as before
    main.registry._devices.clear()
    for attr in ("_shadows", "_versions", "_seen_ids",
                 "_accepted_count", "_rejected"):
        getattr(main.telemetry, attr).clear()
    main.orchestrator._campaigns.clear()
    main.flasher._versions.clear()

    restored = client.post("/admin/backup/restore", content=raw)
    assert restored.status_code == 200, restored.text
    print(f"[api-restore] {restored.json()}")
    # old credential still authenticates; shadow identical
    ok = client.post("/devices/bk-1/telemetry",
                     json={"message_id": "bm2", "seq": 2, "device_ts": 200.0,
                           "payload": {"temp": 10}},
                     headers={"X-Device-Secret": "k"})
    assert ok.status_code == 200
    shadow = client.get("/devices/bk-1/shadow").json()
    assert shadow["shadow"] == {"temp": 10}
    # unfinished campaign survives and can be driven to completion
    st = client.get("/campaigns/bk-c1").json()
    assert st["status"] == "pending"
    final = client.post("/campaigns/bk-c1/run").json()
    print(f"[api-restore-campaign] {final['status']} {final['device_states']}")
    assert final["status"] == "completed"
