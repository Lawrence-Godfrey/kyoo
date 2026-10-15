"""Hub sync end to end: a hub process, a daemon pointed at it, and catch-up after the hub was down."""

import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest

from kyoo.client import Client

ADMIN = "admin-secret"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for(pred, timeout=20.0, msg="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred():
                return
        except (httpx.HTTPError, KeyError, IndexError):
            pass
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {msg}")


class Hub:
    def __init__(self, hub_dir):
        self.port, self.dir, self.proc = _free_port(), hub_dir, None
        self.url = f"http://127.0.0.1:{self.port}"

    def start(self):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "kyoo", "hub", "--port", str(self.port), "--hub-dir", str(self.dir)],
            env={**os.environ, "KYOO_HUB_ADMIN_TOKEN": ADMIN},
        )
        _wait_for(lambda: httpx.get(f"{self.url}/health", timeout=1).json()["ok"], msg="hub health")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        self.proc.wait(10)
        _wait_for(lambda: not _port_open(self.port), msg="hub port closed")

    def register(self, name):
        r = httpx.post(
            f"{self.url}/machines", json={"name": name}, headers={"Authorization": f"Bearer {ADMIN}"}
        )
        r.raise_for_status()
        return r.json()

    def jobs(self, machine_id, token):
        r = httpx.get(f"{self.url}/machines/{machine_id}/jobs", headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        return {j["id"]: j for j in r.json()}


def _port_open(port) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


@pytest.fixture
def hub(tmp_path):
    h = Hub(tmp_path / "hub")
    h.start()
    yield h
    if h.proc.poll() is None:
        h.stop()


@pytest.fixture
def client(tmp_path, hub, monkeypatch):
    machine = hub.register("test-box")
    tmp_path.joinpath("hub.json").write_text(json.dumps({"url": hub.url, "token": machine["token"]}))
    monkeypatch.setenv("KYOO_PORT", str(_free_port()))
    c = Client(data_dir=tmp_path)
    c.ensure()
    c.machine = machine
    yield c
    pid = httpx.get(f"{c.base_url}/health", timeout=2).json()["pid"]
    os.kill(pid, signal.SIGTERM)


def test_jobs_mirror_to_hub(client, hub, tmp_path):
    m = client.machine
    result = tmp_path / "r.json"
    job = client.enqueue(
        command=f"sleep 1 && echo '{{\"k\": 1}}' > {result}", cwd=str(tmp_path), result_file=str(result)
    )
    _wait_for(
        lambda: hub.jobs(m["machine_id"], m["token"])[job["id"]]["status"] == "running", msg="running on hub"
    )
    client.wait(job["id"], timeout=20)
    _wait_for(
        lambda: hub.jobs(m["machine_id"], m["token"])[job["id"]]["status"] == "succeeded",
        msg="succeeded on hub",
    )
    mirrored = hub.jobs(m["machine_id"], m["token"])[job["id"]]
    assert mirrored["exit_code"] == 0 and mirrored["result"] == {"k": 1}
    _wait_for(
        lambda: httpx.get(f"{client.base_url}/health").json()["hub"]["pending"] == 0, msg="outbox drained"
    )


def test_catch_up_after_hub_downtime(client, hub, tmp_path):
    m = client.machine
    hub.stop()
    job = client.enqueue(command="echo offline", cwd=str(tmp_path))
    client.wait(job["id"], timeout=20)
    health = httpx.get(f"{client.base_url}/health").json()["hub"]
    assert health["pending"] >= 1 and health["last_error"] is not None
    hub.start()
    _wait_for(
        lambda: hub.jobs(m["machine_id"], m["token"])[job["id"]]["status"] == "succeeded",
        timeout=30,
        msg="catch-up",
    )
    _wait_for(
        lambda: httpx.get(f"{client.base_url}/health").json()["hub"]["pending"] == 0, msg="outbox drained"
    )


def test_tenant_isolation(hub):
    a, b = hub.register("a"), hub.register("b")
    httpx.post(
        f"{hub.url}/ingest",
        json={"events": [{"job": {"id": 1, "status": "queued"}, "ts": time.time()}]},
        headers={"Authorization": f"Bearer {a['token']}"},
    ).raise_for_status()
    assert list(hub.jobs(a["machine_id"], a["token"])) == [1]
    r = httpx.get(
        f"{hub.url}/machines/{a['machine_id']}/jobs", headers={"Authorization": f"Bearer {b['token']}"}
    )
    assert r.status_code == 403
    assert (
        httpx.post(
            f"{hub.url}/ingest", json={"events": []}, headers={"Authorization": "Bearer nope"}
        ).status_code
        == 401
    )
    assert (
        httpx.post(
            f"{hub.url}/machines", json={"name": "x"}, headers={"Authorization": f"Bearer {a['token']}"}
        ).status_code
        == 403
    )
