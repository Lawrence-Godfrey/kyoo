"""SQLite state: jobs and locks. The daemon is the only writer."""

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

TERMINAL = ("succeeded", "failed", "cancelled", "skipped")
STATUSES = ("queued", "held", "running", *TERMINAL)

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    env TEXT NOT NULL DEFAULT '{}',
    description TEXT NOT NULL DEFAULT '',
    locks TEXT NOT NULL DEFAULT '[]',
    after TEXT NOT NULL DEFAULT '[]',
    preflight TEXT,
    preflight_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    result_file TEXT,
    result TEXT,
    exit_code INTEGER,
    pid INTEGER,
    queued_by TEXT NOT NULL DEFAULT '',
    error TEXT
);
CREATE TABLE IF NOT EXISTS locks (
    name TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL DEFAULT 1
);
"""

JSON_COLUMNS = ("env", "locks", "after", "result")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def row_to_job(row: sqlite3.Row) -> dict[str, Any]:
    job = dict(row)
    for col in JSON_COLUMNS:
        if job.get(col) is not None:
            job[col] = json.loads(job[col])
    return job


def insert_job(conn: sqlite3.Connection, **fields: Any) -> int:
    fields.setdefault("created_at", time.time())
    fields.setdefault("status", "queued")
    for col in JSON_COLUMNS:
        if col in fields and fields[col] is not None:
            fields[col] = json.dumps(fields[col])
    cols = ", ".join(fields)
    marks = ", ".join("?" for _ in fields)
    cur = conn.execute(f"INSERT INTO jobs ({cols}) VALUES ({marks})", list(fields.values()))
    return int(cur.lastrowid)


def update_job(conn: sqlite3.Connection, job_id: int, **fields: Any) -> None:
    for col in JSON_COLUMNS:
        if col in fields and fields[col] is not None:
            fields[col] = json.dumps(fields[col])
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE jobs SET {sets} WHERE id = ?", [*fields.values(), job_id])


def get_job(conn: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return row_to_job(row) if row else None


def list_jobs(
    conn: sqlite3.Connection, statuses: tuple[str, ...] | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    if statuses:
        marks = ", ".join("?" for _ in statuses)
        rows = conn.execute(
            f"SELECT * FROM jobs WHERE status IN ({marks}) ORDER BY id DESC LIMIT ?", (*statuses, limit)
        )
    else:
        rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,))
    return [row_to_job(r) for r in rows]


def runnable_candidates(conn: sqlite3.Connection, now: float) -> list[dict[str, Any]]:
    # Highest priority first, then oldest; held jobs only once their retry time has come.
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'queued' OR (status = 'held' AND next_attempt_at <= ?) "
        "ORDER BY priority DESC, id ASC",
        (now,),
    )
    return [row_to_job(r) for r in rows]


def running_jobs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [row_to_job(r) for r in conn.execute("SELECT * FROM jobs WHERE status = 'running' ORDER BY id")]


def lock_capacity(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT capacity FROM locks WHERE name = ?", (name,)).fetchone()
    return int(row["capacity"]) if row else 1


def set_lock_capacity(conn: sqlite3.Connection, name: str, capacity: int) -> None:
    conn.execute(
        "INSERT INTO locks (name, capacity) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET capacity = excluded.capacity",
        (name, capacity),
    )


def lock_usage(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    usage: dict[str, dict[str, Any]] = {
        r["name"]: {"capacity": r["capacity"], "held_by": []} for r in conn.execute("SELECT * FROM locks")
    }
    for job in running_jobs(conn):
        for name in job["locks"]:
            usage.setdefault(name, {"capacity": 1, "held_by": []})["held_by"].append(job["id"])
    return usage
