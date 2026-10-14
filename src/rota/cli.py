"""The `rota` command."""

import json
import os
import sys
import time
from pathlib import Path
from typing import Annotated

import typer

from rota import __version__, paths
from rota.client import Client

app = typer.Typer(
    help="A local job queue: enqueue commands with locks, dependencies and preflight checks.",
    no_args_is_help=True,
)


def _client() -> Client:
    return Client()


def _fmt_age(ts: float | None) -> str:
    if not ts:
        return "-"
    s = int(time.time() - ts)
    return f"{s}s" if s < 60 else f"{s // 60}m" if s < 3600 else f"{s // 3600}h{(s % 3600) // 60:02d}"


def _fmt_dur(job: dict) -> str:
    if not job.get("started_at"):
        return "-"
    end = job.get("finished_at") or time.time()
    s = int(end - job["started_at"])
    return f"{s}s" if s < 60 else f"{s // 60}m{s % 60:02d}"


@app.command()
def serve(port: int = typer.Option(None, help="Port to listen on (default ROTA_PORT or 7777)")) -> None:
    """Run the daemon in the foreground."""
    from rota.daemon import serve as _serve

    _serve(port or int(os.environ.get("ROTA_PORT", paths.DEFAULT_PORT)))


@app.command()
def start() -> None:
    """Start the daemon in the background if it is not running."""
    c = _client()
    if c.alive():
        typer.echo(f"daemon already running at {c.base_url}")
        return
    c.start_daemon()
    typer.echo(f"daemon started at {c.base_url}")


@app.command()
def stop() -> None:
    """Stop the daemon (running jobs keep running)."""
    import signal

    import httpx

    c = _client()
    if not c.alive():
        typer.echo("daemon is not running")
        return
    pid = httpx.get(f"{c.base_url}/health", timeout=2).json()["pid"]
    os.kill(pid, signal.SIGTERM)
    typer.echo(f"sent SIGTERM to daemon pid {pid}")


@app.command()
def status() -> None:
    """Daemon state and lock usage."""
    c = _client()
    if not c.alive():
        typer.echo(f"daemon not running (would listen at {c.base_url}); state in {c.data_dir}")
        raise typer.Exit(1)
    typer.echo(f"daemon at {c.base_url}  state {c.data_dir}")
    for name, info in c.locks().items():
        typer.echo(f"  lock {name}: {len(info['held_by'])}/{info['capacity']} held {info['held_by'] or ''}")


@app.command()
def add(
    command: Annotated[str, typer.Argument(help="Shell command to run")],
    cwd: Annotated[Path | None, typer.Option(help="Working directory (default: current)")] = None,
    description: Annotated[str, typer.Option("--description", "-d", help="What this job is for")] = "",
    lock: Annotated[
        list[str] | None, typer.Option("--lock", "-l", help="Lock to hold while running; repeatable")
    ] = None,
    after: Annotated[
        list[int] | None, typer.Option("--after", help="Job id that must succeed first; repeatable")
    ] = None,
    urgent: Annotated[bool, typer.Option("--urgent", help="Run before other queued jobs")] = False,
    preflight: Annotated[
        str | None, typer.Option(help="Command that must exit 0 before the job starts")
    ] = None,
    result: Annotated[
        Path | None, typer.Option(help="JSON file the job writes; recorded with the job")
    ] = None,
    env: Annotated[
        list[str] | None, typer.Option("--env", "-e", help="KEY=VALUE for the job's environment; repeatable")
    ] = None,
    wait: Annotated[bool, typer.Option("--wait", help="Block until the job finishes")] = False,
) -> None:
    """Queue a command."""
    environment = dict(kv.split("=", 1) for kv in env or [])
    job = _client().enqueue(
        command=command,
        cwd=str((cwd or Path.cwd()).resolve()),
        description=description,
        locks=lock or [],
        after=after or [],
        priority=1 if urgent else 0,
        preflight=preflight,
        result_file=str(result.resolve()) if result else None,
        env=environment,
        queued_by=os.environ.get("ROTA_QUEUED_BY", f"cli:{os.getlogin() if sys.stdin.isatty() else 'cli'}"),
    )
    typer.echo(f"queued job {job['id']}: {job['description'] or job['command']}")
    if wait:
        job = _client().wait(job["id"])
        typer.echo(
            f"job {job['id']} {job['status']}"
            + (f" (exit {job['exit_code']})" if job.get("exit_code") is not None else "")
        )
        if job["status"] != "succeeded":
            raise typer.Exit(1)


@app.command("ls")
def list_jobs(
    status: Annotated[str | None, typer.Option(help="Comma-separated statuses to show")] = None,
    limit: int = 30,
) -> None:
    """List jobs, newest first."""
    jobs = _client().list(status=status, limit=limit)
    if not jobs:
        typer.echo("no jobs")
        return
    typer.echo(f"{'id':>5}  {'status':9}  {'age':>6}  {'took':>7}  {'locks':12}  description / command")
    for j in jobs:
        locks = ",".join(j["locks"])[:12]
        typer.echo(
            f"{j['id']:>5}  {j['status']:9}  {_fmt_age(j['created_at']):>6}  {_fmt_dur(j):>7}  {locks:12}  {j['description'] or j['command'][:70]}"
        )


@app.command()
def get(job_id: int, tail: int = 20) -> None:
    """Everything about one job, plus the end of its log."""
    c = _client()
    job = c.get(job_id)
    for key in (
        "id",
        "status",
        "description",
        "command",
        "cwd",
        "locks",
        "after",
        "priority",
        "preflight",
        "exit_code",
        "error",
        "queued_by",
    ):
        if job.get(key) not in (None, "", [], 0):
            typer.echo(f"{key:12} {job[key]}")
    typer.echo(f"{'created':12} {_fmt_age(job['created_at'])} ago   {'ran':5} {_fmt_dur(job)}")
    if job.get("result"):
        typer.echo(f"{'result':12} {json.dumps(job['result'])}")
    if tail:
        log = c.log(job_id, tail=tail)
        if log:
            typer.echo(f"--- last {tail} log lines ---")
            typer.echo(log)


@app.command()
def log(job_id: int, tail: int = 50, follow: Annotated[bool, typer.Option("--follow", "-f")] = False) -> None:
    """Print a job's log."""
    c = _client()
    typer.echo(c.log(job_id, tail=tail))
    if follow:
        seen = len(c.log(job_id, tail=0).splitlines())
        while c.get(job_id)["status"] in ("queued", "held", "running"):
            time.sleep(1)
            lines = c.log(job_id, tail=0).splitlines()
            for line in lines[seen:]:
                typer.echo(line)
            seen = len(lines)


@app.command()
def wait(job_id: int, timeout: float | None = None) -> None:
    """Block until a job finishes; exit 0 only if it succeeded."""
    job = _client().wait(job_id, timeout=timeout)
    typer.echo(f"job {job_id} {job['status']}")
    if job["status"] != "succeeded":
        raise typer.Exit(1)


@app.command()
def cancel(job_id: int) -> None:
    """Cancel a queued job or stop a running one."""
    job = _client().cancel(job_id)
    typer.echo(f"job {job_id} {job['status']}")


@app.command()
def lock(name: str, capacity: int) -> None:
    """Set how many jobs may hold a lock at once (default 1)."""
    typer.echo(_client().set_lock(name, capacity))


@app.command()
def ui(open_browser: Annotated[bool, typer.Option("--open/--no-open")] = True) -> None:
    """Print (and open) the web page."""
    c = _client()
    c.ensure()
    url = f"{c.base_url}/?token={c.token}"
    typer.echo(url)
    if open_browser:
        import webbrowser

        webbrowser.open(url)


@app.command()
def mcp() -> None:
    """Run the MCP server on stdio (register with `claude mcp add rota -- rota mcp`)."""
    from rota.mcp_server import main

    main()


@app.command("install-service")
def install_service() -> None:
    """Install a systemd user service so the daemon outlives logins and reboots (Linux)."""
    import shutil
    import subprocess

    if sys.platform != "linux":
        typer.echo("only Linux systemd is supported for now; on macOS run `rota start` after login")
        raise typer.Exit(1)
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit = unit_dir / "rota.service"
    unit.write_text(
        "[Unit]\nDescription=rota job queue daemon\n\n[Service]\n"
        f"ExecStart={sys.executable} -m rota serve\nRestart=on-failure\nEnvironment=ROTA_HOME={paths.data_dir()}\n\n"
        "[Install]\nWantedBy=default.target\n"
    )
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", "--now", "rota.service"], check=True)
    if shutil.which("loginctl"):
        subprocess.run(["loginctl", "enable-linger"], check=False)
    typer.echo(f"installed and started {unit}")


@app.command()
def version() -> None:
    typer.echo(__version__)
