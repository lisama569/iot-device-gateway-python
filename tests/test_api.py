"""End-to-end tests through the FastAPI surface using simulated device traffic."""
from fastapi.testclient import TestClient

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
