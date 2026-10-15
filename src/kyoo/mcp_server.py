"""MCP server (stdio) exposing the queue to agents. Every tool returns compact JSON-able data."""

import logging
import os
from typing import Any

try:  # mcp 2.x renamed FastMCP to MCPServer; the tool decorator and run() are the same
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

from kyoo.client import Client

logging.getLogger("httpx").setLevel(logging.WARNING)  # keep request lines out of the agent's stderr

mcp = _Server(
    "kyoo",
    instructions=(
        "kyoo is a local job queue. Use enqueue for any command that takes more than a minute or needs a "
        "shared resource; name the resource as a lock (e.g. gpu0, worktree:myrepo) and the queue serialises "
        "it. Use wait or get to collect results instead of sleeping. Jobs run on the machine the daemon runs on."
    ),
)

_SUMMARY_KEYS = (
    "id",
    "status",
    "description",
    "command",
    "cwd",
    "locks",
    "after",
    "priority",
    "exit_code",
    "error",
    "result",
    "created_at",
    "started_at",
    "finished_at",
    "queued_by",
)


def _summary(job: dict[str, Any]) -> dict[str, Any]:
    return {k: job[k] for k in _SUMMARY_KEYS if job.get(k) not in (None, "", [], 0) or k in ("id", "status")}


@mcp.tool()
def enqueue(
    command: str,
    cwd: str,
    description: str = "",
    locks: list[str] | None = None,
    after: list[int] | None = None,
    urgent: bool = False,
    preflight: str | None = None,
    result_file: str | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Queue a shell command. locks: names of resources it needs exclusively (capacity is per lock, default 1).
    after: job ids that must succeed first; the job is skipped if any of them fails. preflight: a command that must
    exit 0 right before the job starts (retried with backoff if it fails, e.g. an auth check). result_file: a JSON
    file the job writes; its contents are returned by get_job when the job finishes."""
    job = Client().enqueue(
        command=command,
        cwd=cwd,
        description=description,
        locks=locks or [],
        after=after or [],
        priority=1 if urgent else 0,
        preflight=preflight,
        result_file=result_file,
        env=env or {},
        queued_by=os.environ.get("KYOO_QUEUED_BY", "mcp"),
    )
    return _summary(job)


@mcp.tool()
def list_jobs(status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Jobs newest first. status: comma-separated filter from queued, held, running, succeeded, failed, cancelled, skipped."""
    return [_summary(j) for j in Client().list(status=status, limit=limit)]


@mcp.tool()
def get_job(job_id: int, log_tail: int = 0) -> dict[str, Any]:
    """One job with its result; log_tail > 0 appends the last N log lines."""
    c = Client()
    out = _summary(c.get(job_id))
    if log_tail:
        out["log_tail"] = c.log(job_id, tail=log_tail)
    return out


@mcp.tool()
def wait_job(job_id: int, timeout_seconds: float = 600) -> dict[str, Any]:
    """Block until the job reaches a terminal state or the timeout passes; returns the job either way."""
    return _summary(Client().wait(job_id, timeout=timeout_seconds))


@mcp.tool()
def tail_log(job_id: int, lines: int = 50) -> str:
    """The last N lines of a job's log."""
    return Client().log(job_id, tail=lines)


@mcp.tool()
def cancel_job(job_id: int) -> dict[str, Any]:
    """Cancel a queued job or stop a running one (SIGTERM, then SIGKILL after 10 s)."""
    return _summary(Client().cancel(job_id))


@mcp.tool()
def locks() -> dict[str, Any]:
    """Lock capacities and which running jobs hold them."""
    return Client().locks()


@mcp.tool()
def set_lock_capacity(name: str, capacity: int) -> dict[str, Any]:
    """How many jobs may hold a lock at once."""
    return Client().set_lock(name, capacity)


def main() -> None:
    mcp.run()
