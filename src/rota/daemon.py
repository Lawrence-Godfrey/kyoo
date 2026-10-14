"""The daemon: a scheduler thread that runs jobs, and an HTTP API the CLI, MCP server and web page talk to."""

import json
import os
import shlex
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse

from rota import db, paths

PREFLIGHT_TIMEOUT = 120
PREFLIGHT_BACKOFF = 60
PREFLIGHT_MAX_ATTEMPTS = 10
KILL_GRACE = 10


class Scheduler:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        (data_dir / "logs").mkdir(parents=True, exist_ok=True)
        self.conn = db.connect(data_dir / "rota.db")
        self.processes: dict[int, subprocess.Popen] = {}
        self.cancelling: dict[int, float] = {}
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self._recover()

    def _recover(self) -> None:
        # Jobs marked running by a previous daemon are gone with it; the log says how far they got.
        for job in db.running_jobs(self.conn):
            db.update_job(
                self.conn,
                job["id"],
                status="failed",
                finished_at=time.time(),
                error="daemon restarted while the job was running",
            )

    # ---- job lifecycle --------------------------------------------------------------------------

    def enqueue(self, **fields: Any) -> dict[str, Any]:
        with self.lock:
            for dep in fields.get("after") or []:
                if db.get_job(self.conn, dep) is None:
                    raise ValueError(f"dependency job {dep} does not exist")
            job_id = db.insert_job(self.conn, **fields)
            return db.get_job(self.conn, job_id)  # type: ignore[return-value]

    def cancel(self, job_id: int) -> dict[str, Any]:
        with self.lock:
            job = db.get_job(self.conn, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] in ("queued", "held"):
                db.update_job(self.conn, job_id, status="cancelled", finished_at=time.time())
            elif job["status"] == "running" and job_id in self.processes:
                os.killpg(self.processes[job_id].pid, signal.SIGTERM)
                self.cancelling[job_id] = time.time()
            return db.get_job(self.conn, job_id)  # type: ignore[return-value]

    def _deps_state(self, job: dict[str, Any]) -> str:
        states = [(db.get_job(self.conn, dep) or {}).get("status") for dep in job["after"]]
        if any(s in ("failed", "cancelled", "skipped") for s in states):
            return "blocked"
        return "ready" if all(s == "succeeded" for s in states) else "waiting"

    def _locks_free(self, job: dict[str, Any], usage: dict[str, int]) -> bool:
        return all(usage.get(name, 0) < db.lock_capacity(self.conn, name) for name in job["locks"])

    def _run_preflight(self, job: dict[str, Any]) -> bool:
        try:
            proc = subprocess.run(
                job["preflight"],
                shell=True,
                cwd=job["cwd"],
                env={**os.environ, **job["env"]},
                capture_output=True,
                text=True,
                timeout=PREFLIGHT_TIMEOUT,
            )
            ok, output = proc.returncode == 0, (proc.stdout + proc.stderr).strip()
        except subprocess.TimeoutExpired:
            ok, output = False, f"preflight timed out after {PREFLIGHT_TIMEOUT}s"
        if ok:
            return True
        attempts = job["preflight_attempts"] + 1
        if attempts >= PREFLIGHT_MAX_ATTEMPTS:
            db.update_job(
                self.conn,
                job["id"],
                status="failed",
                finished_at=time.time(),
                preflight_attempts=attempts,
                error=f"preflight failed {attempts} times; last output: {output[-500:]}",
            )
        else:
            db.update_job(
                self.conn,
                job["id"],
                status="held",
                preflight_attempts=attempts,
                next_attempt_at=time.time() + PREFLIGHT_BACKOFF,
                error=f"preflight failed: {output[-500:]}",
            )
        return False

    def _start(self, job: dict[str, Any]) -> None:
        log = open(self.data_dir / "logs" / f"{job['id']}.log", "ab")
        log.write(f"# rota job {job['id']}: {job['command']}\n# cwd {job['cwd']}\n".encode())
        log.flush()
        proc = subprocess.Popen(
            job["command"],
            shell=True,
            cwd=job["cwd"],
            env={**os.environ, **job["env"]},
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        log.close()
        self.processes[job["id"]] = proc
        db.update_job(
            self.conn, job["id"], status="running", started_at=time.time(), pid=proc.pid, error=None
        )

    def _finish(self, job_id: int, proc: subprocess.Popen) -> None:
        job = db.get_job(self.conn, job_id) or {}
        result = None
        if job.get("result_file"):
            try:
                result = json.loads(Path(job["result_file"]).read_text())
            except (OSError, ValueError) as e:
                result = {"error": f"could not read result file: {e}"}
        if job_id in self.cancelling:
            status = "cancelled"
        else:
            status = "succeeded" if proc.returncode == 0 else "failed"
        db.update_job(
            self.conn,
            job_id,
            status=status,
            finished_at=time.time(),
            exit_code=proc.returncode,
            result=result,
            error=None
            if status == "succeeded"
            else f"exit code {proc.returncode}"
            if status == "failed"
            else "cancelled",
        )
        self.processes.pop(job_id, None)
        self.cancelling.pop(job_id, None)

    def tick(self) -> None:
        with self.lock:
            for job_id, proc in list(self.processes.items()):
                if proc.poll() is not None:
                    self._finish(job_id, proc)
                elif job_id in self.cancelling and time.time() - self.cancelling[job_id] > KILL_GRACE:
                    os.killpg(proc.pid, signal.SIGKILL)
            usage: dict[str, int] = {}
            for running in db.running_jobs(self.conn):
                for name in running["locks"]:
                    usage[name] = usage.get(name, 0) + 1
            for job in db.runnable_candidates(self.conn, time.time()):
                deps = self._deps_state(job)
                if deps == "blocked":
                    db.update_job(
                        self.conn,
                        job["id"],
                        status="skipped",
                        finished_at=time.time(),
                        error="a dependency failed or was cancelled",
                    )
                    continue
                if deps == "waiting" or not self._locks_free(job, usage):
                    continue
                if job["preflight"] and not self._run_preflight(job):
                    continue
                self._start(job)
                for name in job["locks"]:
                    usage[name] = usage.get(name, 0) + 1

    def run(self, interval: float = 0.5) -> None:
        while not self.stop.is_set():
            try:
                self.tick()
            except Exception as e:  # the scheduler must outlive any one bad job
                print(f"scheduler error: {e!r}", flush=True)
            self.stop.wait(interval)


# ---- HTTP API ---------------------------------------------------------------------------------------


def create_app(scheduler: Scheduler, token: str) -> FastAPI:
    app = FastAPI(title="rota")
    ui_dir = Path(__file__).parent / "ui"

    def authed(request: Request) -> None:
        if request.headers.get("authorization") != f"Bearer {token}":
            raise HTTPException(401, "missing or wrong token")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "pid": os.getpid(), "running": len(scheduler.processes)}

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(ui_dir / "index.html")

    @app.post("/jobs", dependencies=[Depends(authed)])
    def enqueue(body: dict[str, Any]) -> dict[str, Any]:
        required = {"command", "cwd"}
        if missing := required - body.keys():
            raise HTTPException(422, f"missing fields: {sorted(missing)}")
        cwd = Path(body["cwd"]).expanduser()
        if not cwd.is_dir():
            raise HTTPException(422, f"cwd does not exist: {cwd}")
        fields = {
            "command": body["command"],
            "cwd": str(cwd),
            "env": body.get("env") or {},
            "description": body.get("description") or "",
            "locks": body.get("locks") or [],
            "after": [int(i) for i in body.get("after") or []],
            "priority": int(body.get("priority") or 0),
            "preflight": body.get("preflight"),
            "result_file": body.get("result_file"),
            "queued_by": body.get("queued_by") or "",
        }
        try:
            return scheduler.enqueue(**fields)
        except ValueError as e:
            raise HTTPException(422, str(e))

    @app.get("/jobs", dependencies=[Depends(authed)])
    def list_jobs(status: str | None = Query(None), limit: int = 100) -> list[dict[str, Any]]:
        statuses = tuple(s for s in status.split(",") if s) if status else None
        return db.list_jobs(scheduler.conn, statuses, limit)

    @app.get("/jobs/{job_id}", dependencies=[Depends(authed)])
    def get_job(job_id: int) -> dict[str, Any]:
        job = db.get_job(scheduler.conn, job_id)
        if job is None:
            raise HTTPException(404, f"no job {job_id}")
        return job

    @app.get("/jobs/{job_id}/log", dependencies=[Depends(authed)], response_class=PlainTextResponse)
    def job_log(job_id: int, tail: int = 50) -> str:
        path = scheduler.data_dir / "logs" / f"{job_id}.log"
        if not path.exists():
            return ""
        lines = path.read_text(errors="replace").splitlines()
        return "\n".join(lines[-tail:] if tail > 0 else lines)

    @app.post("/jobs/{job_id}/cancel", dependencies=[Depends(authed)])
    def cancel(job_id: int) -> dict[str, Any]:
        try:
            return scheduler.cancel(job_id)
        except KeyError:
            raise HTTPException(404, f"no job {job_id}")

    @app.get("/locks", dependencies=[Depends(authed)])
    def locks() -> dict[str, Any]:
        return db.lock_usage(scheduler.conn)

    @app.put("/locks/{name}", dependencies=[Depends(authed)])
    def set_lock(name: str, body: dict[str, Any]) -> dict[str, Any]:
        db.set_lock_capacity(scheduler.conn, name, int(body["capacity"]))
        return {name: db.lock_capacity(scheduler.conn, name)}

    return app


def serve(port: int, data_dir: Path | None = None, host: str = "127.0.0.1") -> None:
    data_dir = data_dir or paths.data_dir()
    scheduler = Scheduler(data_dir)
    threading.Thread(target=scheduler.run, name="scheduler", daemon=True).start()
    app = create_app(scheduler, paths.token_in(data_dir))
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    (data_dir / "daemon.port").write_text(str(port))
    try:
        server.run()
    finally:
        scheduler.stop.set()


def quote(command: list[str]) -> str:
    return " ".join(shlex.quote(c) for c in command)
