"""End-to-end restart test over real HTTP.

Launches the actual uvicorn `main:app` process twice (two "generations")
against the SAME GATEWAY_STATE_PATH -- exactly what restarting the process
or redeploying onto a new machine looks like in production -- and proves the
restored gateway keeps serving after the relaunch, including resuming a
campaign that was unfinished when the package was imported.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error
from contextlib import contextmanager

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health",
                                        timeout=1) as r:
                if r.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"server on {port} did not become ready: {last}")


@contextmanager
def server(state_path: str | None, port: int):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT
    if state_path is None:
        env.pop("GATEWAY_STATE_PATH", None)
    else:
        env["GATEWAY_STATE_PATH"] = state_path
    env["GATEWAY_FOTA_STATE_PATH"] = ""  # only the whole-snapshot is in play
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_ready(port)
        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _req(base: str, method: str, path: str, body=None, headers=None,
         raw: bytes | None = None):
    url = base + path
    data = raw
    hdrs = headers or {}
    if body is not None:
        data = json.dumps(body).encode()
        hdrs = {"Content-Type": "application/json", **hdrs}
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def test_restore_then_real_process_restart_keeps_gateway_alive(tmp_path):
    state = str(tmp_path / "durable" / "state.zip")
    port_old, port_new = _free_port(), _free_port()

    # --- generation 0: build a backup on the "old machine" (no durability) ---
    with server(None, port_old) as old:
        _req(old, "POST", "/devices/register",
             body={"device_id": "rt-1", "secret": "snow"})
        _req(old, "POST", "/devices/register",
             body={"device_id": "rt-2", "secret": "ice"})
        _req(old, "POST", "/devices/rt-1/telemetry",
             body={"message_id": "tm1", "seq": 1, "device_ts": 100.0,
                   "payload": {"rpm": 1200}},
             headers={"X-Device-Secret": "snow"})
        _req(old, "POST", "/campaigns",
             body={"campaign_id": "rt-c", "target_version": "8.8.8",
                   "device_ids": ["rt-1", "rt-2"], "batch_size": 1})
        code, pkg, _ = _req(old, "POST", "/admin/backup/export")
        assert code == 200 and len(pkg) > 0
        print(f"[http-restart] exported {len(pkg)} bytes from old machine")

    # --- generation 1 on the "new machine": restore while unfinished --------
    with server(state, port_new) as new:
        code, body, _ = _req(new, "POST", "/admin/backup/restore", raw=pkg)
        assert code == 200, body
        print(f"[http-restart] generation-1 restore: {json.loads(body)}")
        # campaign starts pending (never run) -- leave it unfinished, then
        # the process ends (context manager terminates it): simulates a
        # restart/redepoly BEFORE anyone drove it to completion
        st, b, _ = _req(new, "GET", "/campaigns/rt-c")
        assert st == 200 and json.loads(b)["status"] == "pending"

    # --- generation 2: SAME durable path, brand new process ("restart") -----
    with server(state, port_new) as new2:
        h, b, _ = _req(new2, "GET", "/health")
        health = json.loads(b)
        print(f"[http-restart] generation-2 health: {health}")
        assert health["healthy"] is True
        assert health["recovered_from"] == state

        # old credential still connects; shadow identical; dedup alive
        code, b, _ = _req(new2, "POST", "/devices/rt-1/telemetry",
                          body={"message_id": "tm1", "seq": 1,
                                "device_ts": 100.0, "payload": {"rpm": 1200}},
                          headers={"X-Device-Secret": "snow"})
        assert code == 200 and json.loads(b)["code"] == "duplicate_message"
        _, b, _ = _req(new2, "GET", "/devices/rt-1/shadow")
        assert json.loads(b)["shadow"] == {"rpm": 1200}

        # unfinished campaign is visible and completes after restart
        _, b, _ = _req(new2, "POST", "/campaigns/rt-c/run")
        final = json.loads(b)
        print(f"[http-restart] campaign after restart -> {final['status']} "
              f"{final['device_states']}")
        assert final["status"] == "completed"
        assert set(final["device_states"]) == {"rt-1", "rt-2"}

        # the device actually installed the firmware, and that is queryable
        for dev in ("rt-1", "rt-2"):
            assert final["device_states"][dev] == "succeeded"

    # --- generation 3: restart AGAIN; terminal outcome stays put -------------
    with server(state, port_new) as new3:
        _, b, _ = _req(new3, "GET", "/campaigns/rt-c")
        assert json.loads(b)["status"] == "completed"
        _, b, _ = _req(new3, "GET", "/health")
        assert json.loads(b)["healthy"] is True
        print("[http-restart] second restart converged to the same state")


def test_corrupt_state_after_restart_reports_503_and_recovers(tmp_path):
    """重启时两份状态文件都损坏：业务接口明确 503，导入备份后自愈。"""
    state = str(tmp_path / "durable" / "state.zip")
    port = _free_port()

    with server(state, port) as g:
        _req(g, "POST", "/devices/register",
             body={"device_id": "hx-1", "secret": "pw"})
        _req(g, "POST", "/devices/hx-1/telemetry",
             body={"message_id": "h1", "seq": 1, "device_ts": 1.0,
                   "payload": {"v": 1}},
             headers={"X-Device-Secret": "pw"})
        code, pkg, _ = _req(g, "POST", "/admin/backup/export")
        assert code == 200

    # corrupt both the snapshot and its predecessor before the next boot
    with open(state, "wb") as fh:
        fh.write(b"broken-state-primary")
    with open(state + ".bak", "wb") as fh:
        fh.write(b"broken-state-backup")

    with server(state, port) as g:
        _, b, _ = _req(g, "GET", "/health")
        health = json.loads(b)
        print(f"[http-fault] health={health}")
        assert health["healthy"] is False and health["fault"]

        # business calls are refused with a clear, distinguishable error
        code, b, _ = _req(g, "GET", "/devices/hx-1/shadow")
        assert code == 503
        assert json.loads(b)["detail"]["code"] == "state_unavailable"
        code, b, _ = _req(g, "POST", "/devices/register",
                          body={"device_id": "x", "secret": "y"})
        assert code == 503
        # export of the broken store is refused, but inspect still works
        code, b, _ = _req(g, "POST", "/admin/backup/export")
        assert code == 503
        assert json.loads(b)["detail"]["code"] == "state_unavailable"
        code, b, _ = _req(g, "POST", "/admin/backup/inspect", raw=pkg)
        assert code == 200

        # admin restore is the escape hatch -> healthy within same process
        code, b, _ = _req(g, "POST", "/admin/backup/restore", raw=pkg,
                          headers={"X-Allow-Overwrite": "true"})
        assert code == 200, b
        _, b, _ = _req(g, "GET", "/health")
        assert json.loads(b)["healthy"] is True
        _, b, _ = _req(g, "GET", "/devices/hx-1/shadow")
        assert json.loads(b)["shadow"] == {"v": 1}
        print("[http-fault] recovered via restore without leaving the process")

    # and the recovery survives yet another restart
    with server(state, port) as g:
        _, b, _ = _req(g, "GET", "/health")
        assert json.loads(b)["healthy"] is True
        _, b, _ = _req(g, "GET", "/devices/hx-1/shadow")
        assert json.loads(b)["shadow"] == {"v": 1}
        print("[http-fault] recovery survived another restart")
