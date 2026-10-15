"""Outbound sync to a hub: job snapshots queue in the local outbox and are posted in batches, in order, with retry.

Configured by <data_dir>/hub.json: {"url": "https://kyoo.example", "token": "<machine token>"}. The hub being
unreachable loses nothing; the outbox holds the snapshots until it answers again.
"""

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from kyoo import db

BATCH = 100
MAX_BACKOFF = 60.0


class HubSync:
    def __init__(self, data_dir: Path, conn: sqlite3.Connection, lock: threading.RLock) -> None:
        self.config_path = data_dir / "hub.json"
        self.conn, self.lock = conn, lock
        self.wakeup = threading.Event()
        self.stop = threading.Event()
        self.last_error: str | None = None
        self.last_sent_at: float | None = None

    @staticmethod
    def configured(data_dir: Path) -> bool:
        return (data_dir / "hub.json").exists()

    def _config(self) -> dict[str, str]:
        return json.loads(self.config_path.read_text())

    def wake(self) -> None:
        self.wakeup.set()

    def status(self) -> dict[str, Any]:
        with self.lock:
            pending = db.outbox_size(self.conn)
        return {
            "url": self._config().get("url"),
            "pending": pending,
            "last_sent_at": self.last_sent_at,
            "last_error": self.last_error,
        }

    def flush_once(self) -> bool:
        """Send one batch. Returns True when something was sent and more may be waiting."""
        with self.lock:
            batch = db.outbox_peek(self.conn, BATCH)
        if not batch:
            return False
        cfg = self._config()
        r = httpx.post(
            f"{cfg['url'].rstrip('/')}/ingest",
            json={"events": [p for _, p in batch]},
            headers={"Authorization": f"Bearer {cfg['token']}"},
            timeout=30.0,
        )
        r.raise_for_status()
        with self.lock:
            db.outbox_delete(self.conn, [i for i, _ in batch])
        self.last_sent_at, self.last_error = time.time(), None
        return len(batch) == BATCH

    def run(self) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            try:
                while self.flush_once():
                    pass
                backoff = 1.0
                self.wakeup.wait(timeout=5.0)
                self.wakeup.clear()
            except (httpx.HTTPError, OSError, ValueError) as e:
                self.last_error = f"{type(e).__name__}: {e}"[:200]
                self.stop.wait(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
