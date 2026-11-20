"""SQLite helpers for the static GTFS database."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class StaticDataNotReady(RuntimeError):
    """The GTFS Static database is missing or has not been ingested yet."""


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


def get_readonly_connection(db_path: Path | str) -> sqlite3.Connection:
    """Open the static DB read-only. Raises StaticDataNotReady if it does not exist."""
    path = Path(db_path)
    if not path.is_file():
        raise StaticDataNotReady(f"GTFS database not found at {path}")
    uri = f"{path.resolve().as_uri()}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise StaticDataNotReady(f"Cannot open GTFS database at {path}: {exc}") from exc


def read_ingested_at(conn: sqlite3.Connection) -> str | None:
    """Return the ingestion timestamp, or None if the DB has not been ingested."""
    try:
        row = conn.execute("SELECT value FROM metadata WHERE key = 'ingested_at'").fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def open_ingested_db(db_path: Path | str) -> sqlite3.Connection:
    """Open the static DB read-only and verify it contains ingested GTFS data."""
    conn = get_readonly_connection(db_path)
    if read_ingested_at(conn) is None:
        conn.close()
        raise StaticDataNotReady(
            "GTFS static data has not been ingested yet. Run: python scripts/ingest_static.py"
        )
    return conn


def get_ingested_at(db_path: Path | str) -> str | None:
    """Return the ingestion timestamp for the DB at ``db_path`` (None if not ready)."""
    try:
        conn = open_ingested_db(db_path)
    except StaticDataNotReady:
        return None
    try:
        return read_ingested_at(conn)
    finally:
        conn.close()


def is_ingested(db_path: Path | str) -> bool:
    return get_ingested_at(db_path) is not None
