"""End-to-end tests: a real daemon in a temp data dir on a free port, driven through the client."""

import os
import signal
import socket
import time

import httpx
import pytest

from kyoo.client import Client


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("KYOO_PORT", str(_free_port()))
    c = Client(data_dir=tmp_path)
    c.ensure()
    yield c
    pid = httpx.get(f"{c.base_url}/health", timeout=2).json()["pid"]
    os.kill(pid, signal.SIGTERM)


def test_autostart_and_run(client, tmp_path):
    result = tmp_path / "r.json"
    job = client.enqueue(
        command=f"echo hello && echo '{{\"mrr\": 0.5}}' > {result}",
        cwd=str(tmp_path),
        result_file=str(result),
        description="hello",
    )
    done = client.wait(job["id"], timeout=20)
    assert done["status"] == "succeeded" and done["exit_code"] == 0
    assert done["result"] == {"mrr": 0.5}
    assert "hello" in client.log(job["id"])


def test_failure_and_dependency_skip(client, tmp_path):
    bad = client.enqueue(command="exit 3", cwd=str(tmp_path))
    child = client.enqueue(command="echo never", cwd=str(tmp_path), after=[bad["id"]])
    assert client.wait(bad["id"], timeout=20)["status"] == "failed"
    assert client.wait(child["id"], timeout=20)["status"] == "skipped"


def test_dependency_runs_after_success(client, tmp_path):
    first = client.enqueue(command="sleep 1 && echo one", cwd=str(tmp_path))
    second = client.enqueue(command="echo two", cwd=str(tmp_path), after=[first["id"]])
    s = client.wait(second["id"], timeout=20)
    f = client.get(first["id"])
    assert s["status"] == "succeeded" and s["started_at"] >= f["finished_at"]


def test_lock_serialises(client, tmp_path):
    a = client.enqueue(command="sleep 2", cwd=str(tmp_path), locks=["gpu0"])
    b = client.enqueue(command="sleep 0.1", cwd=str(tmp_path), locks=["gpu0"])
    time.sleep(1)
    assert client.get(a["id"])["status"] == "running" and client.get(b["id"])["status"] == "queued"
    held = client.locks()["gpu0"]["held_by"]
    assert held == [a["id"]]
    done = client.wait(b["id"], timeout=20)
    assert done["started_at"] >= client.get(a["id"])["finished_at"]


def test_lock_capacity(client, tmp_path):
    client.set_lock("cpu", 2)
    a = client.enqueue(command="sleep 2", cwd=str(tmp_path), locks=["cpu"])
    b = client.enqueue(command="sleep 2", cwd=str(tmp_path), locks=["cpu"])
    time.sleep(1)
    assert {client.get(a["id"])["status"], client.get(b["id"])["status"]} == {"running"}


def test_urgent_goes_first(client, tmp_path):
    blocker = client.enqueue(command="sleep 1.5", cwd=str(tmp_path), locks=["x"])
    normal = client.enqueue(command="echo n", cwd=str(tmp_path), locks=["x"])
    urgent = client.enqueue(command="echo u", cwd=str(tmp_path), locks=["x"], priority=1)
    client.wait(normal["id"], timeout=20)
    assert client.get(urgent["id"])["started_at"] < client.get(normal["id"])["started_at"]
    assert blocker["id"] < urgent["id"]


def test_preflight_holds_then_fails_is_reported(client, tmp_path):
    job = client.enqueue(command="echo run", cwd=str(tmp_path), preflight="echo not-yet >&2; exit 1")
    time.sleep(1.5)
    held = client.get(job["id"])
    assert held["status"] == "held" and "not-yet" in held["error"] and held["preflight_attempts"] == 1


def test_preflight_passes(client, tmp_path):
    job = client.enqueue(command="echo run", cwd=str(tmp_path), preflight="true")
    assert client.wait(job["id"], timeout=20)["status"] == "succeeded"


def test_cancel_running(client, tmp_path):
    job = client.enqueue(command="sleep 30", cwd=str(tmp_path))
    time.sleep(1)
    assert client.get(job["id"])["status"] == "running"
    client.cancel(job["id"])
    done = client.wait(job["id"], timeout=20)
    assert done["status"] == "cancelled"


def test_cancel_queued(client, tmp_path):
    a = client.enqueue(command="sleep 5", cwd=str(tmp_path), locks=["y"])
    b = client.enqueue(command="echo b", cwd=str(tmp_path), locks=["y"])
    assert client.cancel(b["id"])["status"] == "cancelled"
    client.cancel(a["id"])


def test_env_and_cwd(client, tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    job = client.enqueue(command="pwd && echo $GREETING", cwd=str(sub), env={"GREETING": "hi"})
    client.wait(job["id"], timeout=20)
    assert str(sub) in client.log(job["id"]) and "hi" in client.log(job["id"])


def test_bad_requests(client, tmp_path):
    with pytest.raises(RuntimeError, match="cwd does not exist"):
        client.enqueue(command="true", cwd="/no/such/dir")
    with pytest.raises(RuntimeError, match="does not exist"):
        client.enqueue(command="true", cwd=str(tmp_path), after=[999999])
    assert (
        httpx.post(f"{client.base_url}/jobs", json={"command": "true", "cwd": str(tmp_path)}).status_code
        == 401
    )


def test_ui_served(client):
    html = httpx.get(f"{client.base_url}/", timeout=5).text
    assert "<title>kyoo</title>" in html


def test_daemon_restart_adopts_running_job(client, tmp_path):
    result = tmp_path / "r.json"
    job = client.enqueue(
        command=f"sleep 3 && echo '{{\"ok\": 1}}' > {result}", cwd=str(tmp_path), result_file=str(result)
    )
    dep = client.enqueue(command="echo after", cwd=str(tmp_path), after=[job["id"]])
    time.sleep(1)
    pid = httpx.get(f"{client.base_url}/health", timeout=2).json()["pid"]
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + 10
    while client.alive() and time.time() < deadline:
        time.sleep(0.1)
    client.ensure()
    assert client.get(job["id"])["status"] == "running"
    done = client.wait(job["id"], timeout=20)
    assert done["status"] == "succeeded" and done["exit_code"] == 0 and done["result"] == {"ok": 1}
    assert client.wait(dep["id"], timeout=20)["status"] == "succeeded"
