import pytest

from src.db import check_sqlite_health, get_sqlite_connection

pytestmark = pytest.mark.unit


def test_creates_nested_db_in_wal_mode(tmp_path):
    db_path = tmp_path / "nested" / "dir" / "gtfs.db"
    conn = get_sqlite_connection(db_path)
    try:
        assert db_path.exists()
        assert conn.execute("PRAGMA journal_mode;").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1
    finally:
        conn.close()
    assert check_sqlite_health(db_path) is True


def test_health_check_false_when_path_unusable(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    assert check_sqlite_health(blocker / "gtfs.db") is False
