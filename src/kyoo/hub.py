"""The hub: receives job snapshots from daemons and serves them per machine. One account per machine token.

State lives in <hub_dir>/hub.db. Creating a machine needs the admin token (KYOO_HUB_ADMIN_TOKEN); until the
device-code link flow exists that is how a daemon gets its token. Every row is scoped by machine, every
machine by account, so adding sign-in later changes who may read, not what is stored.
"""

import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Annotated, Any

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS machines (
    id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL REFERENCES accounts(id),
    name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL, created_at REAL NOT NULL, last_seen_at REAL
);
CREATE TABLE IF NOT EXISTS jobs (
    machine_id INTEGER NOT NULL REFERENCES machines(id), job_id INTEGER NOT NULL,
    status TEXT NOT NULL, data TEXT NOT NULL, updated_at REAL NOT NULL,
    PRIMARY KEY (machine_id, job_id)
);
"""


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    if conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO accounts (email, created_at) VALUES (NULL, ?)", (time.time(),)
        )  # the owner, until sign-in exists
    return conn


def create_app(conn: sqlite3.Connection, admin_token: str) -> FastAPI:
    app = FastAPI(title="kyoo hub")

    def bearer(request: Request) -> str:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(401, "missing bearer token")
        return auth[7:]

    def admin(token: str = Depends(bearer)) -> None:
        if not secrets.compare_digest(token, admin_token):
            raise HTTPException(403, "admin token required")

    def machine(token: str = Depends(bearer)) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM machines WHERE token_hash = ?", (_hash(token),)).fetchone()
        if row is None:
            raise HTTPException(401, "unknown machine token")
        return dict(row)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "machines": conn.execute("SELECT COUNT(*) FROM machines").fetchone()[0]}

    @app.post("/machines", dependencies=[Depends(admin)])
    def create_machine(body: dict[str, Any]) -> dict[str, Any]:
        token = secrets.token_urlsafe(32)
        cur = conn.execute(
            "INSERT INTO machines (account_id, name, token_hash, created_at) VALUES (1, ?, ?, ?)",
            (body.get("name") or "machine", _hash(token), time.time()),
        )
        return {"machine_id": cur.lastrowid, "token": token}

    @app.post("/ingest")
    def ingest(body: dict[str, Any], m: Annotated[dict[str, Any], Depends(machine)]) -> dict[str, Any]:
        events = body.get("events") or []
        now = time.time()
        for event in events:
            job = event["job"]
            conn.execute(
                "INSERT INTO jobs (machine_id, job_id, status, data, updated_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(machine_id, job_id) DO UPDATE SET status = excluded.status, data = excluded.data, updated_at = excluded.updated_at "
                "WHERE excluded.updated_at >= jobs.updated_at",
                (m["id"], int(job["id"]), job["status"], json.dumps(job), float(event.get("ts") or now)),
            )
        conn.execute("UPDATE machines SET last_seen_at = ? WHERE id = ?", (now, m["id"]))
        return {"accepted": len(events)}

    @app.get("/machines/{machine_id}/jobs")
    def list_jobs(
        machine_id: int, token: Annotated[str, Depends(bearer)], limit: int = 100
    ) -> list[dict[str, Any]]:
        own = conn.execute(
            "SELECT id FROM machines WHERE token_hash = ? AND id = ?", (_hash(token), machine_id)
        ).fetchone()
        if own is None and not secrets.compare_digest(token, admin_token):
            raise HTTPException(403, "not your machine")
        rows = conn.execute(
            "SELECT data FROM jobs WHERE machine_id = ? ORDER BY job_id DESC LIMIT ?", (machine_id, limit)
        )
        return [json.loads(r["data"]) for r in rows]

    return app


def serve(port: int, hub_dir: Path, host: str = "127.0.0.1") -> None:
    hub_dir.mkdir(parents=True, exist_ok=True)
    admin_token = os.environ.get("KYOO_HUB_ADMIN_TOKEN")
    if not admin_token:
        raise SystemExit("KYOO_HUB_ADMIN_TOKEN must be set: it authorises creating machines")
    app = create_app(connect(hub_dir / "hub.db"), admin_token)
    uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="warning")).run()
