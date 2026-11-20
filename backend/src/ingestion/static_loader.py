"""Streaming GTFS Static -> SQLite loader with an atomic swap into place.

``stop_times.txt`` is hundreds of MB, so every CSV is streamed and inserted in batches into a
staging database (``<target>.tmp``) which is only swapped over the live database once complete.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import time
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

REQUIRED_FILES = (
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
)
BATCH_SIZE = 50_000

SCHEMA_SQL = """
CREATE TABLE stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_lat REAL NOT NULL,
    stop_lon REAL NOT NULL
);
CREATE TABLE routes (
    route_id TEXT PRIMARY KEY,
    route_short_name TEXT NOT NULL,
    route_long_name TEXT
);
CREATE TABLE trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_headsign TEXT,
    direction_id INTEGER
);
CREATE TABLE stop_times (
    trip_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    arrival_secs INTEGER NOT NULL,
    departure_secs INTEGER NOT NULL,
    stop_headsign TEXT,
    pickup_type INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL
);
CREATE TABLE calendar_dates (
    service_id TEXT NOT NULL,
    date TEXT NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (service_id, date)
);
CREATE TABLE stop_routes (
    stop_id TEXT NOT NULL,
    route_short_name TEXT NOT NULL,
    PRIMARY KEY (stop_id, route_short_name)
);
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

STOP_ROUTES_SQL = """
INSERT OR IGNORE INTO stop_routes (stop_id, route_short_name)
SELECT DISTINCT st.stop_id, r.route_short_name
FROM stop_times st
JOIN trips t ON t.trip_id = st.trip_id
JOIN routes r ON r.route_id = t.route_id
WHERE st.pickup_type != 1 AND r.route_short_name != ''
"""

INDEX_STATEMENTS = (
    "CREATE INDEX idx_stops_code ON stops(stop_code)",
    "CREATE INDEX idx_stops_name ON stops(stop_name COLLATE NOCASE)",
    "CREATE INDEX idx_routes_short_name ON routes(route_short_name)",
    "CREATE INDEX idx_trips_service_id ON trips(service_id)",
    "CREATE INDEX idx_trips_route_id ON trips(route_id)",
    "CREATE INDEX idx_stop_times_stop_dep ON stop_times(stop_id, departure_secs)",
    "CREATE INDEX idx_stop_times_trip_id ON stop_times(trip_id)",
    "CREATE INDEX idx_calendar_dates_date ON calendar_dates(date)",
    "CREATE INDEX idx_stop_routes_stop_id ON stop_routes(stop_id)",
)

COUNTED_TABLES = (
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
    "stop_routes",
)


def _read_rows(path: Path, columns: Sequence[str], required: Sequence[str]) -> Iterator[list[str]]:
    """Stream selected CSV columns as stripped strings ('' when a column is absent).

    Uses ``csv.reader`` and skips blank rows, which also copes with the NTA feed's odd
    ``\\r\\r\\n`` line endings.
    """
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"{path.name} is empty")
        index = {name.strip(): position for position, name in enumerate(header)}
        missing = [name for name in required if name not in index]
        if missing:
            raise ValueError(f"{path.name} is missing required columns: {missing}")
        positions = [index.get(name, -1) for name in columns]
        for row in reader:
            if not row:
                continue
            size = len(row)
            yield [row[p].strip() if 0 <= p < size else "" for p in positions]


def _secs(value: str) -> int:
    hours, minutes, seconds = value.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def _to_int(value: str, default: int | None = None) -> int | None:
    try:
        return int(value)
    except ValueError:
        return default


def _stop_rows(path: Path) -> Iterator[tuple]:
    columns = ("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon")
    required = ("stop_id", "stop_name", "stop_lat", "stop_lon")
    for stop_id, code, name, lat, lon in _read_rows(path, columns, required):
        if not stop_id:
            continue
        try:
            yield (stop_id, code or None, name, float(lat), float(lon))
        except ValueError:
            continue


def _route_rows(path: Path) -> Iterator[tuple]:
    columns = ("route_id", "route_short_name", "route_long_name")
    for route_id, short_name, long_name in _read_rows(path, columns, ("route_id",)):
        if route_id:
            yield (route_id, short_name, long_name or None)


def _trip_rows(path: Path) -> Iterator[tuple]:
    columns = ("trip_id", "route_id", "service_id", "trip_headsign", "direction_id")
    required = ("trip_id", "route_id", "service_id")
    for trip_id, route_id, service_id, headsign, direction in _read_rows(path, columns, required):
        if trip_id and route_id and service_id:
            yield (trip_id, route_id, service_id, headsign or None, _to_int(direction))


def _stop_time_rows(path: Path) -> Iterator[tuple]:
    columns = (
        "trip_id",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
        "stop_headsign",
        "pickup_type",
    )
    required = ("trip_id", "stop_id", "stop_sequence")
    for trip_id, stop_id, seq, arrival, departure, headsign, pickup in _read_rows(
        path, columns, required
    ):
        try:
            sequence = int(seq)
            arrival_secs = _secs(arrival) if arrival else None
            departure_secs = _secs(departure) if departure else None
        except ValueError:
            continue
        if arrival_secs is None and departure_secs is None:
            continue
        if departure_secs is None:
            departure_secs = arrival_secs
        if arrival_secs is None:
            arrival_secs = departure_secs
        yield (
            trip_id,
            stop_id,
            sequence,
            arrival_secs,
            departure_secs,
            headsign or None,
            _to_int(pickup, 0) if pickup else 0,
        )


def _calendar_rows(path: Path) -> Iterator[tuple]:
    days = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    columns = ("service_id", *days, "start_date", "end_date")
    for row in _read_rows(path, columns, columns):
        if not row[0]:
            continue
        flags = [_to_int(value, 0) for value in row[1:8]]
        yield (row[0], *flags, row[8], row[9])


def _calendar_date_rows(path: Path) -> Iterator[tuple]:
    columns = ("service_id", "date", "exception_type")
    for service_id, date, exception_type in _read_rows(path, columns, columns):
        parsed = _to_int(exception_type)
        if service_id and date and parsed in (1, 2):
            yield (service_id, date, parsed)


def _bulk_insert(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple]) -> int:
    batch: list[tuple] = []
    total = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            conn.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        total += len(batch)
    return total


def _remove_db_files(path: Path) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        path.with_name(path.name + suffix).unlink(missing_ok=True)


def _build_database(gtfs_dir: Path, db_path: Path) -> tuple[dict[str, int], str]:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-64000")
        conn.executescript(SCHEMA_SQL)

        _bulk_insert(
            conn,
            "INSERT OR IGNORE INTO stops VALUES (?,?,?,?,?)",
            _stop_rows(gtfs_dir / "stops.txt"),
        )
        _bulk_insert(
            conn,
            "INSERT OR IGNORE INTO routes VALUES (?,?,?)",
            _route_rows(gtfs_dir / "routes.txt"),
        )
        _bulk_insert(
            conn,
            "INSERT OR IGNORE INTO trips VALUES (?,?,?,?,?)",
            _trip_rows(gtfs_dir / "trips.txt"),
        )
        _bulk_insert(
            conn,
            "INSERT OR REPLACE INTO calendar VALUES (?,?,?,?,?,?,?,?,?,?)",
            _calendar_rows(gtfs_dir / "calendar.txt"),
        )
        _bulk_insert(
            conn,
            "INSERT OR REPLACE INTO calendar_dates VALUES (?,?,?)",
            _calendar_date_rows(gtfs_dir / "calendar_dates.txt"),
        )
        _bulk_insert(
            conn,
            "INSERT INTO stop_times VALUES (?,?,?,?,?,?,?)",
            _stop_time_rows(gtfs_dir / "stop_times.txt"),
        )

        conn.execute(STOP_ROUTES_SQL)
        conn.commit()
        for statement in INDEX_STATEMENTS:
            conn.execute(statement)
        conn.commit()

        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            for table in COUNTED_TABLES
        }
        conn.execute("ANALYZE")
        ingested_at = datetime.now(UTC).isoformat(timespec="seconds")
        metadata = [("ingested_at", ingested_at)]
        metadata += [(f"count_{table}", str(count)) for table, count in counts.items()]
        conn.executemany("INSERT OR REPLACE INTO metadata VALUES (?,?)", metadata)
        conn.commit()

        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return counts, ingested_at
    finally:
        conn.close()


def ingest_gtfs_directory(gtfs_dir: Path, target_db_path: Path) -> dict:
    """Load the GTFS CSVs in ``gtfs_dir`` into SQLite and atomically swap it into place.

    The existing database at ``target_db_path`` is untouched unless the whole load succeeds.
    """
    gtfs_dir = Path(gtfs_dir)
    target = Path(target_db_path)
    missing = [name for name in REQUIRED_FILES if not (gtfs_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"GTFS directory {gtfs_dir} is missing required files: {missing}")

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(target.name + ".tmp")
    _remove_db_files(staging)
    started = time.monotonic()
    try:
        counts, ingested_at = _build_database(gtfs_dir, staging)
        # Stale sidecars from the previous DB must never be paired with the new file.
        for suffix in ("-wal", "-shm"):
            target.with_name(target.name + suffix).unlink(missing_ok=True)
        os.replace(staging, target)
    except BaseException:
        _remove_db_files(staging)
        raise
    return {
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "counts": counts,
        "ingested_at": ingested_at,
    }
