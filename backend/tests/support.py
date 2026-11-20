"""Shared test helpers: fake Redis, stub GTFS-R service, GTFS fixture and feed builders."""

from __future__ import annotations

import asyncio
import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2
from redis.exceptions import ConnectionError as RedisConnectionError

from src.ingestion.static_loader import ingest_gtfs_directory

DUBLIN = ZoneInfo("Europe/Dublin")
# A Friday in winter (GMT, UTC+0).
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=DUBLIN)


# --------------------------------------------------------------------------- Redis fakes


class FakeRedis:
    """Minimal in-memory stand-in for ``redis.asyncio.Redis`` (records TTLs, yields to the loop)."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.mget_calls = 0
        self.pipeline_batches: list[int] = []

    async def get(self, key):
        await asyncio.sleep(0)
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        await asyncio.sleep(0)
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def exists(self, *keys):
        await asyncio.sleep(0)
        return sum(1 for key in keys if key in self.store)

    async def mget(self, keys):
        await asyncio.sleep(0)
        self.mget_calls += 1
        return [self.store.get(key) for key in keys]

    async def ttl(self, key):
        return self.ttls.get(key, -1) if key in self.store else -2

    async def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)
            self.ttls.pop(key, None)

    async def ping(self):
        return True

    async def aclose(self):
        return None

    def pipeline(self, transaction=True):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._queued: list[tuple[str, str, int | None]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def set(self, key, value, ex=None):
        self._queued.append((key, value, ex))
        return self

    async def execute(self):
        await asyncio.sleep(0)
        self._redis.pipeline_batches.append(len(self._queued))
        for key, value, ex in self._queued:
            self._redis.store[key] = value
            if ex is not None:
                self._redis.ttls[key] = ex
        self._queued.clear()
        return []


class FailingRedis(FakeRedis):
    """Every operation fails like a dropped connection."""

    async def get(self, key):
        raise RedisConnectionError("redis is down")

    async def exists(self, *keys):
        raise RedisConnectionError("redis is down")

    async def mget(self, keys):
        raise RedisConnectionError("redis is down")

    async def ping(self):
        raise RedisConnectionError("redis is down")


class StubGtfsr:
    """Drop-in for GtfsrCacheService returning canned live updates."""

    def __init__(self, updates=None, available=True, meta=None) -> None:
        self.updates = updates or {}
        self.available = available
        self.meta = meta
        self.calls: list[list[str]] = []

    async def get_trip_updates(self, trip_ids):
        self.calls.append(list(trip_ids))
        return {tid: self.updates[tid] for tid in trip_ids if tid in self.updates}, self.available

    async def peek_meta(self):
        return self.meta


# ------------------------------------------------------------------- GTFS-R feed builder


def build_feed(entities: list[dict], *, timestamp: int = 1_700_000_000) -> bytes:
    """Serialise a FeedMessage. Entity dict: trip_id, sr?, updates: [{seq, stop_id, ...}]."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp
    for index, spec in enumerate(entities):
        entity = feed.entity.add()
        entity.id = str(index)
        trip_update = entity.trip_update
        trip_update.trip.trip_id = spec["trip_id"]
        if "sr" in spec:
            trip_update.trip.schedule_relationship = getattr(
                gtfs_realtime_pb2.TripDescriptor, spec["sr"]
            )
        for upd in spec.get("updates", []):
            stu = trip_update.stop_time_update.add()
            if "seq" in upd:
                stu.stop_sequence = upd["seq"]
            if "stop_id" in upd:
                stu.stop_id = upd["stop_id"]
            if "arr_delay" in upd:
                stu.arrival.delay = upd["arr_delay"]
            if "dep_delay" in upd:
                stu.departure.delay = upd["dep_delay"]
            if "arr_time" in upd:
                stu.arrival.time = upd["arr_time"]
            if "dep_time" in upd:
                stu.departure.time = upd["dep_time"]
            if "sr" in upd:
                stu.schedule_relationship = getattr(
                    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate, upd["sr"]
                )
    return feed.SerializeToString()


# ---------------------------------------------------------------- GTFS static fixtures


@dataclass
class TripSpec:
    trip_id: str
    route: str
    # (stop_id, stop_sequence, "HH:MM:SS", pickup_type)
    stops: list[tuple[str, int, str, int]]
    headsign: str = "Dublin"
    service_id: str = "WK"
    stop_headsign: str = ""
    route_long: str = field(default="")


def one_stop(
    trip_id: str,
    route: str,
    time: str,
    *,
    stop: str = "S1",
    seq: int = 1,
    pickup: int = 0,
    headsign: str = "Dublin",
) -> TripSpec:
    return TripSpec(trip_id, route, [(stop, seq, time, pickup)], headsign=headsign)


def fmt_secs(total: int) -> str:
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


def _write_csv(path: Path, header: list[str], rows: list[list], lineterminator: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator=lineterminator)
        writer.writerow(header)
        writer.writerows(rows)


def write_gtfs_dir(
    directory: Path,
    trips: list[TripSpec],
    *,
    stop_rows: list[tuple] | None = None,
    calendar_rows: list[tuple] | None = None,
    calendar_date_rows: list[tuple] | None = None,
    lineterminator: str = "\n",
) -> Path:
    """Write a minimal but realistic GTFS feed (the six files the loader requires)."""
    directory.mkdir(parents=True, exist_ok=True)
    if stop_rows is None:
        stop_ids = sorted({stop[0] for trip in trips for stop in trip.stops})
        stop_rows = [(sid, f"code-{sid}", f"Stop {sid}", 53.38, -6.59) for sid in stop_ids]
    _write_csv(
        directory / "stops.txt",
        ["stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon"],
        [[sid, code, name, "", lat, lon] for sid, code, name, lat, lon in stop_rows],
        lineterminator,
    )

    routes: dict[str, str] = {}
    for trip in trips:
        routes.setdefault(trip.route, trip.route_long or f"{trip.route} route")
    _write_csv(
        directory / "routes.txt",
        ["route_id", "agency_id", "route_short_name", "route_long_name"],
        [[f"R_{short}", "A1", short, long] for short, long in routes.items()],
        lineterminator,
    )
    _write_csv(
        directory / "trips.txt",
        ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"],
        [[f"R_{t.route}", t.service_id, t.trip_id, t.headsign, 0] for t in trips],
        lineterminator,
    )
    _write_csv(
        directory / "stop_times.txt",
        [
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "stop_headsign",
            "pickup_type",
            "drop_off_type",
        ],
        [
            [t.trip_id, time, time, stop_id, seq, t.stop_headsign, pickup, 0]
            for t in trips
            for stop_id, seq, time, pickup in t.stops
        ],
        lineterminator,
    )
    if calendar_rows is None:
        calendar_rows = [("WK", 1, 1, 1, 1, 1, 1, 1, "20260101", "20261231")]
    _write_csv(
        directory / "calendar.txt",
        [
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
        [list(row) for row in calendar_rows],
        lineterminator,
    )
    _write_csv(
        directory / "calendar_dates.txt",
        ["service_id", "date", "exception_type"],
        [list(row) for row in (calendar_date_rows or [])],
        lineterminator,
    )
    return directory


def make_db(tmp_path: Path, trips: list[TripSpec], **kwargs) -> Path:
    """Write a GTFS fixture under tmp_path and ingest it into ``tmp_path/gtfs.db``."""
    gtfs_dir = write_gtfs_dir(tmp_path / "gtfs", trips, **kwargs)
    db_path = tmp_path / "gtfs.db"
    ingest_gtfs_directory(gtfs_dir, db_path)
    return db_path
