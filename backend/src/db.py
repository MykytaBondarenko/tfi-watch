"""SQLite helpers for the static GTFS database."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def get_sqlite_connection(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def check_sqlite_health(db_path: Path | str) -> bool:
    """Return True if the DB opens, answers SELECT 1, and is in WAL mode."""
    try:
        conn = get_sqlite_connection(db_path)
    except (sqlite3.Error, OSError):
        return False
    try:
        if conn.execute("SELECT 1").fetchone() != (1,):
            return False
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        return str(mode).lower() == "wal"
    except sqlite3.Error:
        return False
    finally:
        conn.close()
