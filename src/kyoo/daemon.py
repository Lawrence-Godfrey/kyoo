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

from kyoo import db, paths
from kyoo.sync import HubSync

PREFLIGHT_TIMEOUT = 120
PREFLIGHT_BACKOFF = 60
PREFLIGHT_MAX_ATTEMPTS = 10
KILL_GRACE = 10


class _Adopted:
    """A running job's process group inherited from a previous daemon: pollable, exit code from the exit file."""

    def __init__(self, pid: int, exit_file: Path) -> None:
        self.pid, self.exit_file, self.returncode = pid, exit_file, None

    def poll(self) -> int | None:
        try:
            os.getpgid(self.pid)  # the job's shell leads its own process group, so this fails once it is gone
            return None
        except ProcessLookupError:
            text = self.exit_file.read_text().strip() if self.exit_file.exists() else ""
            self.returncode = int(text) if text else None
            return self.returncode if self.returncode is not None else -1


class Scheduler:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        (data_dir / "logs").mkdir(parents=True, exist_ok=True)
        self.conn = db.connect(data_dir / "kyoo.db")
        self.processes: dict[int, subprocess.Popen | _Adopted] = {}
        self.cancelling: dict[int, float] = {}
        self.lock = threading.RLock()  # guards self.conn, which every thread shares
        self.stop = threading.Event()
        self.last_error: str | None = None
        self.thread: threading.Thread | None = None
        self.sync: HubSync | None = (
            HubSync(data_dir, self.conn, self.lock) if HubSync.configured(data_dir) else None
        )
        self._recover()

    def _emit(self, job_id: int) -> None:
        # Snapshot the job for the hub sync; cheap when no hub is configured (the outbox is drained unread).
        job = db.get_job(self.conn, job_id)
        if job is not None and self.sync is not None:
            db.outbox_add(self.conn, {"job": job, "ts": time.time()})
            self.sync.wake()

    def _recover(self) -> None:
        # Jobs run in their own session, so they outlive the daemon; adopt the ones still alive.
        for job in db.running_jobs(self.conn):
            handle = _Adopted(job["pid"], self.data_dir / "logs" / f"{job['id']}.exit")
            if handle.poll() is None or handle.returncode is not None:
                self.processes[job["id"]] = handle
            else:
                self._finish(job["id"], handle)

    # ---- job lifecycle --------------------------------------------------------------------------

    def enqueue(self, **fields: Any) -> dict[str, Any]:
        with self.lock:
            for dep in fields.get("after") or []:
                if db.get_job(self.conn, dep) is None:
                    raise ValueError(f"dependency job {dep} does not exist")
            job_id = db.insert_job(self.conn, **fields)
            self._emit(job_id)
            return db.get_job(self.conn, job_id)  # type: ignore[return-value]

    def cancel(self, job_id: int) -> dict[str, Any]:
        with self.lock:
            job = db.get_job(self.conn, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] in ("queued", "held"):
                db.update_job(self.conn, job_id, status="cancelled", finished_at=time.time())
                self._emit(job_id)
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
            proc = subprocess.run(  # noqa: PLW1510 - a non-zero exit is the signal, handled below
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
            self._emit(job["id"])
        else:
            db.update_job(
                self.conn,
                job["id"],
                status="held",
                preflight_attempts=attempts,
                next_attempt_at=time.time() + PREFLIGHT_BACKOFF,
                error=f"preflight failed: {output[-500:]}",
            )
            self._emit(job["id"])
        return False

    def _start(self, job: dict[str, Any]) -> None:
        with open(self.data_dir / "logs" / f"{job['id']}.log", "ab") as log:
            log.write(f"# kyoo job {job['id']}: {job['command']}\n# cwd {job['cwd']}\n".encode())
            log.flush()
            exit_file = self.data_dir / "logs" / f"{job['id']}.exit"
            exit_file.unlink(missing_ok=True)
            proc = subprocess.Popen(  # the child keeps its own copy of the descriptor
                f"({job['command']}); rc=$?; echo $rc > {shlex.quote(str(exit_file))}; exit $rc",
                shell=True,
                cwd=job["cwd"],
                env={**os.environ, **job["env"]},
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        self.processes[job["id"]] = proc
        db.update_job(
            self.conn, job["id"], status="running", started_at=time.time(), pid=proc.pid, error=None
        )
        self._emit(job["id"])

    def _finish(self, job_id: int, proc: subprocess.Popen | _Adopted) -> None:
        job = db.get_job(self.conn, job_id) or {}
        result = None
        if job.get("result_file"):
            try:
                result = json.loads(Path(job["result_file"]).read_text())
            except (OSError, ValueError) as e:
                result = {"error": f"could not read result file: {e}"}
        if job_id in self.cancelling:
            status, error = "cancelled", "cancelled"
        elif (
            proc.returncode is None
        ):  # adopted from a daemon that predates exit files: trust only a result file
            ok = bool(job.get("result_file")) and isinstance(result, dict) and "error" not in result
            status = "succeeded" if ok else "failed"
            error = None if ok else "finished while the daemon was down; exit code unknown"
        else:
            status = "succeeded" if proc.returncode == 0 else "failed"
            error = None if status == "succeeded" else f"exit code {proc.returncode}"
        db.update_job(
            self.conn,
            job_id,
            status=status,
            finished_at=time.time(),
            exit_code=proc.returncode,
            result=result,
            error=error,
        )
        self._emit(job_id)
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
                    self._emit(job["id"])
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
            except Exception as e:  # noqa: BLE001 - the scheduler must outlive any one bad job
                self.last_error = f"{type(e).__name__}: {e}"[:300]
                try:
                    print(f"scheduler error: {e!r}", flush=True)
                except OSError:  # a full disk fails the log write too; that must not take the loop down
                    pass
            self.stop.wait(interval)


# ---- HTTP API ---------------------------------------------------------------------------------------


def create_app(scheduler: Scheduler, token: str) -> FastAPI:
    app = FastAPI(title="kyoo")
    ui_dir = Path(__file__).parent / "ui"

    def authed(request: Request) -> None:
        if request.headers.get("authorization") != f"Bearer {token}":
            raise HTTPException(401, "missing or wrong token")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "pid": os.getpid(),
            "running": len(scheduler.processes),
            "scheduler_alive": scheduler.thread.is_alive() if scheduler.thread else None,
            "last_error": scheduler.last_error,
            "hub": scheduler.sync.status() if scheduler.sync else None,
        }

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
        with scheduler.lock:
            return db.list_jobs(scheduler.conn, statuses, limit)

    @app.get("/jobs/{job_id}", dependencies=[Depends(authed)])
    def get_job(job_id: int) -> dict[str, Any]:
        with scheduler.lock:
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
        with scheduler.lock:
            return db.lock_usage(scheduler.conn)

    @app.put("/locks/{name}", dependencies=[Depends(authed)])
    def set_lock(name: str, body: dict[str, Any]) -> dict[str, Any]:
        with scheduler.lock:
            db.set_lock_capacity(scheduler.conn, name, int(body["capacity"]))
            return {name: db.lock_capacity(scheduler.conn, name)}

    return app


def serve(port: int, data_dir: Path | None = None, host: str = "127.0.0.1") -> None:
    data_dir = data_dir or paths.data_dir()
    scheduler = Scheduler(data_dir)
    scheduler.thread = threading.Thread(target=scheduler.run, name="scheduler", daemon=True)
    scheduler.thread.start()
    if scheduler.sync is not None:
        threading.Thread(target=scheduler.sync.run, name="hub-sync", daemon=True).start()
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
