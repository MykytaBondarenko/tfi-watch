import sqlite3

import pytest

from src.ingestion.static_loader import REQUIRED_FILES, ingest_gtfs_directory
from tests.support import TripSpec, one_stop, write_gtfs_dir

pytestmark = pytest.mark.unit

TRIPS = [
    TripSpec(
        "T1",
        "C3",
        [("S1", 1, "08:00:00", 0), ("S2", 2, "08:10:00", 0), ("S3", 3, "08:20:00", 1)],
    ),
    one_stop("T2", "66", "24:15:00"),
]


def _query(db_path, sql, *params):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def test_ingest_creates_tables_indexes_and_metadata(tmp_path):
    gtfs_dir = write_gtfs_dir(
        tmp_path / "gtfs", TRIPS, calendar_date_rows=[("WK", "20261225", 2)]
    )
    db_path = tmp_path / "data" / "gtfs.db"

    summary = ingest_gtfs_directory(gtfs_dir, db_path)

    assert summary["counts"] == {
        "stops": 3,
        "routes": 2,
        "trips": 2,
        "stop_times": 4,
        "calendar": 1,
        "calendar_dates": 1,
        "stop_routes": 3,
    }
    assert summary["elapsed_seconds"] >= 0
    assert _query(db_path, "PRAGMA journal_mode")[0][0].lower() == "wal"
    names = {row[0] for row in _query(db_path, "SELECT name FROM sqlite_master WHERE type='index'")}
    assert {
        "idx_stops_code",
        "idx_stops_name",
        "idx_stop_times_stop_dep",
        "idx_stop_times_trip_id",
        "idx_calendar_dates_date",
        "idx_stop_routes_stop_id",
    } <= names
    meta = dict(_query(db_path, "SELECT key, value FROM metadata"))
    assert meta["ingested_at"] == summary["ingested_at"]
    assert meta["count_stop_times"] == "4"
    # no staging leftovers
    assert [p.name for p in db_path.parent.iterdir()] == ["gtfs.db"]


def test_post_midnight_times_and_pickup_filtering(tmp_path):
    gtfs_dir = write_gtfs_dir(tmp_path / "gtfs", TRIPS)
    db_path = tmp_path / "gtfs.db"
    ingest_gtfs_directory(gtfs_dir, db_path)

    assert _query(db_path, "SELECT departure_secs FROM stop_times WHERE trip_id='T2'") == [
        (87300,)
    ]
    routes = _query(db_path, "SELECT stop_id, route_short_name FROM stop_routes ORDER BY 1, 2")
    # S3 is drop-off only (pickup_type=1) so it must not list route C3
    assert routes == [("S1", "66"), ("S1", "C3"), ("S2", "C3")]


def test_handles_nta_style_line_endings(tmp_path):
    gtfs_dir = write_gtfs_dir(tmp_path / "gtfs", TRIPS, lineterminator="\r\r\n")
    summary = ingest_gtfs_directory(gtfs_dir, tmp_path / "gtfs.db")
    assert summary["counts"]["stops"] == 3
    assert summary["counts"]["stop_times"] == 4


def test_malformed_rows_are_skipped(tmp_path):
    gtfs_dir = write_gtfs_dir(tmp_path / "gtfs", TRIPS)
    with (gtfs_dir / "stops.txt").open("a", newline="") as fh:
        fh.write("BAD,bad-code,Bad Stop,,not-a-number,-6.5\n")
    with (gtfs_dir / "stop_times.txt").open("a", newline="") as fh:
        fh.write("T1,,,S1,9,,0,0\n")  # no times at all
        fh.write("T1,09:00:00,,S1,10,,0,0\n")  # arrival only -> departure falls back
    summary = ingest_gtfs_directory(gtfs_dir, tmp_path / "gtfs.db")
    assert summary["counts"]["stops"] == 3
    assert summary["counts"]["stop_times"] == 5


def test_reingest_atomically_replaces_data(tmp_path):
    db_path = tmp_path / "gtfs.db"
    ingest_gtfs_directory(write_gtfs_dir(tmp_path / "v1", TRIPS), db_path)
    v2 = write_gtfs_dir(tmp_path / "v2", [one_stop("NEW", "X1", "10:00:00")])
    ingest_gtfs_directory(v2, db_path)

    assert _query(db_path, "SELECT trip_id FROM trips") == [("NEW",)]
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith("gtfs.db")] == ["gtfs.db"]


def test_missing_file_fails_without_touching_existing_db(tmp_path):
    db_path = tmp_path / "gtfs.db"
    ingest_gtfs_directory(write_gtfs_dir(tmp_path / "v1", TRIPS), db_path)
    before = db_path.read_bytes()

    broken = write_gtfs_dir(tmp_path / "v2", [one_stop("NEW", "X1", "10:00:00")])
    (broken / "routes.txt").unlink()
    with pytest.raises(FileNotFoundError, match="routes.txt"):
        ingest_gtfs_directory(broken, db_path)

    assert db_path.read_bytes() == before


def test_failure_mid_load_preserves_existing_db_and_cleans_staging(tmp_path):
    db_path = tmp_path / "gtfs.db"
    ingest_gtfs_directory(write_gtfs_dir(tmp_path / "v1", TRIPS), db_path)

    broken = write_gtfs_dir(tmp_path / "v2", [one_stop("NEW", "X1", "10:00:00")])
    (broken / "stops.txt").write_text("stop_id,stop_code\nS1,1\n")  # stop_name etc. missing
    with pytest.raises(ValueError, match="missing required columns"):
        ingest_gtfs_directory(broken, db_path)

    assert _query(db_path, "SELECT COUNT(*) FROM trips") == [(2,)]
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith("gtfs.db")] == ["gtfs.db"]


def test_required_files_constant_matches_phase1_expectations():
    assert set(REQUIRED_FILES) == {
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar.txt",
        "calendar_dates.txt",
    }
