"""HTTP client for the daemon, with auto-start: if nothing answers, spawn `rota serve` detached and wait for it."""

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from rota import db, paths


class Client:
    def __init__(self, data_dir: Path | None = None, autostart: bool = True) -> None:
        self.data_dir = data_dir or paths.data_dir()
        self.autostart = autostart
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.token = paths.token_in(self.data_dir)

    @property
    def port(self) -> int:
        try:
            return int((self.data_dir / "daemon.port").read_text().strip())
        except (FileNotFoundError, ValueError):
            return int(os.environ.get("ROTA_PORT", paths.DEFAULT_PORT))

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def alive(self) -> bool:
        try:
            return httpx.get(f"{self.base_url}/health", timeout=1.0).status_code == 200
        except httpx.HTTPError:
            return False

    def ensure(self) -> None:
        if self.alive():
            return
        if not self.autostart:
            raise RuntimeError(f"no rota daemon at {self.base_url}; run `rota start`")
        self.start_daemon()

    def start_daemon(self, wait: float = 15.0) -> None:
        env = {**os.environ, "ROTA_HOME": str(self.data_dir)}
        with open(self.data_dir / "daemon.log", "ab") as log:
            subprocess.Popen(  # the daemon keeps its own copy of the descriptor
                [sys.executable, "-m", "rota", "serve", "--port", str(self.port)],
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=env,
            )
        deadline = time.time() + wait
        while time.time() < deadline:
            if self.alive():
                return
            time.sleep(0.2)
        raise RuntimeError(f"rota daemon did not come up within {wait}s; see {self.data_dir / 'daemon.log'}")

    # ---- API ------------------------------------------------------------------------------------

    def _request(self, method: str, path: str, **kw: Any) -> httpx.Response:
        self.ensure()
        r = httpx.request(
            method,
            f"{self.base_url}{path}",
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=30.0,
            **kw,
        )
        if r.status_code >= 400:
            detail = (
                r.json().get("detail")
                if r.headers.get("content-type", "").startswith("application/json")
                else r.text
            )
            raise RuntimeError(f"{method} {path}: {r.status_code} {detail}")
        return r

    def enqueue(self, **job: Any) -> dict[str, Any]:
        return self._request("POST", "/jobs", json=job).json()

    def list(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        return self._request("GET", "/jobs", params=params).json()

    def get(self, job_id: int) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}").json()

    def log(self, job_id: int, tail: int = 50) -> str:
        return self._request("GET", f"/jobs/{job_id}/log", params={"tail": tail}).text

    def cancel(self, job_id: int) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/cancel").json()

    def locks(self) -> dict[str, Any]:
        return self._request("GET", "/locks").json()

    def set_lock(self, name: str, capacity: int) -> dict[str, Any]:
        return self._request("PUT", f"/locks/{name}", json={"capacity": capacity}).json()

    def wait(self, job_id: int, timeout: float | None = None, poll: float = 1.0) -> dict[str, Any]:
        deadline = time.time() + timeout if timeout else None
        while True:
            job = self.get(job_id)
            if job["status"] in db.TERMINAL:
                return job
            if deadline and time.time() > deadline:
                return job
            time.sleep(poll)
