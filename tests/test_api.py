"""End-to-end HTTP tests: error codes must be distinguishable and the
full register -> telemetry -> upgrade flow must work over real routes."""
from __future__ import annotations

import logging
import time

log = logging.getLogger("tests.api")


def _wait_terminal(client, job_id, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        body = client.get(f"/upgrades/{job_id}").json()
        if body["state"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.01)
    raise AssertionError(body)


def test_register_conflict_and_credential_errors_over_http(client):
    r = client.post(
        "/devices/register",
        json={"device_id": "d1", "name": "temp", "metadata": {"room": 1}},
    )
    assert r.status_code == 201
    cred = r.json()["credentials"][0]
    secret = r.json()["credential_secret"]
    log.info("registered credential_id=%s", cred["credential_id"])

    # idempotent
    r2 = client.post(
        "/devices/register",
        json={"device_id": "d1", "name": "temp", "metadata": {"room": 1}},
    )
    assert r2.status_code == 201
    assert r2.json()["credential_secret"] == secret

    # identity conflict
    r3 = client.post("/devices/register", json={"device_id": "d1", "name": "humid"})
    assert r3.status_code == 409
    assert r3.json()["error"] == "device_conflict"
    assert r3.json()["details"]["reason"] == "identity_mismatch"

    base = {
        "device_id": "d1",
        "credential_id": cred["credential_id"],
        "credential_secret": secret,
    }
    assert client.post("/telemetry", json={**base, "version": 1, "timestamp": 1.0,
        "state": {"t": 21}}).json()["decision"] == "merged"

    # invalid secret -> 401 credential_invalid
    r = client.post("/telemetry", json={**base, "credential_secret": "nope",
        "version": 2, "timestamp": 2.0})
    assert r.status_code == 401 and r.json()["error"] == "credential_invalid"

    # disable -> 403 device_disabled
    assert client.post("/devices/d1/disable").status_code == 200
    r = client.post("/telemetry", json={**base, "version": 2, "timestamp": 2.0})
    assert r.status_code == 403 and r.json()["error"] == "device_disabled"
    client.post("/devices/d1/enable")

    # revoke -> 401 credential_revoked
    assert client.post(f"/devices/d1/credentials/{cred['credential_id']}/revoke").status_code == 200
    r = client.post("/telemetry", json={**base, "version": 2, "timestamp": 2.0})
    assert r.status_code == 401 and r.json()["error"] == "credential_revoked"
    log.info("error classification verified over HTTP")


def test_telemetry_validation_error_is_not_dropped(client):
    body = client.post("/devices/register", json={"device_id": "dv"}).json()
    cred = body["credentials"][0]
    # Pydantic rejects structurally invalid messages (version must be >= 1)
    r = client.post(
        "/telemetry",
        json={
            "device_id": "dv",
            "credential_id": cred["credential_id"],
            "credential_secret": body["credential_secret"],
            "version": 0,
            "timestamp": 1.0,
            "state": {},
        },
    )
    assert r.status_code == 422, r.text
    log.info("schema validation rejected status=%s", r.status_code)


def test_upgrade_lifecycle_and_cancel_over_http(client):
    n = 6
    for i in range(n):
        assert client.post("/devices/register", json={"device_id": f"u{i}"}).status_code == 201
    r = client.post(
        "/upgrades",
        json={
            "job_id": "job-http",
            "firmware_version": "fw-9.9",
            "device_ids": [f"u{i}" for i in range(n)],
            "batch_size": 2,
            "max_retries": 0,
        },
    )
    assert r.status_code == 201
    final = _wait_terminal(client, "job-http")
    assert final["state"] == "completed"
    assert all(s["state"] == "succeeded" for s in final["device_states"].values())
    log.info("job=%s firmware=%s state=%s", final["job_id"], final["firmware_version"], final["state"])

    # cancel flow on a second job
    for i in range(6):
        client.post("/devices/register", json={"device_id": f"c{i}"})
    client.post(
        "/upgrades",
        json={"job_id": "job-c", "firmware_version": "fw-x",
              "device_ids": [f"c{i}" for i in range(6)], "batch_size": 2, "max_retries": 0},
    )
    rr = client.post("/upgrades/job-c/pause", json={"reason": "hold"})
    assert rr.status_code == 200 and rr.json()["state"] == "paused"
    rr = client.post("/upgrades/job-c/cancel", json={"reason": "abort"})
    assert rr.status_code == 200
    final = _wait_terminal(client, "job-c")
    assert final["state"] == "cancelled"
    assert set(s["state"] for s in final["device_states"].values()) <= {
        "succeeded", "cancelled"
    }
    log.info("cancelled over HTTP: %s", final["terminal_reason"])


def test_upgrade_unknown_device_is_conflict(client):
    r = client.post(
        "/upgrades",
        json={"firmware_version": "fw", "device_ids": ["nobody"]},
    )
    assert r.status_code == 409
    assert r.json()["details"]["reason"] == "unknown_devices"
