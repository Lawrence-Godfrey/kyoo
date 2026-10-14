"""Where rota keeps its state. One directory per user; ROTA_HOME overrides it."""

import os
import secrets
import sys
from pathlib import Path

DEFAULT_PORT = 7777


def data_dir() -> Path:
    if env := os.environ.get("ROTA_HOME"):
        path = Path(env)
    elif sys.platform == "darwin":
        path = Path.home() / "Library" / "Application Support" / "rota"
    else:
        path = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "rota"
    path.mkdir(parents=True, exist_ok=True)
    (path / "logs").mkdir(exist_ok=True)
    return path


def db_path() -> Path:
    return data_dir() / "rota.db"


def log_path(job_id: int) -> Path:
    return data_dir() / "logs" / f"{job_id}.log"


def token_in(directory: Path) -> str:
    # Shared secret between the daemon and its clients so another user on the machine cannot use the queue.
    path = directory / "token"
    if not path.exists():
        path.write_text(secrets.token_urlsafe(32))
        path.chmod(0o600)
    return path.read_text().strip()


def token() -> str:
    return token_in(data_dir())


def port_file() -> Path:
    return data_dir() / "daemon.port"


def read_port() -> int | None:
    try:
        return int(port_file().read_text().strip())
    except (FileNotFoundError, ValueError):
        return None
