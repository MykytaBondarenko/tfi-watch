"""Departure joining engine: static schedule window + live GTFS-R delays."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src.db import open_ingested_db
from src.ingestion.gtfs_time import get_active_service_ids, gtfs_service_datetime
from src.ingestion.gtfsr_cache import StopTimeLive, TripLiveUpdate
from src.models.schemas import Departure

LOOKBACK_SECONDS = 30 * 60
PAST_GRACE_SECONDS = 60
DEFAULT_TZ = "Europe/Dublin"

_CANDIDATE_SQL = """
SELECT st.trip_id, st.stop_sequence, st.departure_secs, st.stop_headsign,
       t.trip_headsign, t.service_id, r.route_short_name, r.route_long_name
FROM stop_times st
JOIN trips t ON t.trip_id = st.trip_id
JOIN routes r ON r.route_id = t.route_id
WHERE st.stop_id = ?
  AND st.pickup_type != 1
  AND st.departure_secs BETWEEN ? AND ?
"""


@dataclass(frozen=True)
class StaticCandidate:
    trip_id: str
    stop_id: str
    stop_sequence: int
    service_date: date
    departure_secs: int
    route: str
    headsign: str


@dataclass(frozen=True)
class DeparturesResult:
    departures: list[Departure]
    realtime_available: bool


def parse_route_filter(routes: str | list[str] | None) -> list[str] | None:
    """Normalise a comma-separated route filter (or list) to upper-case, de-duplicated names."""
    if routes is None:
        return None
    parts = routes.split(",") if isinstance(routes, str) else routes
    cleaned = list(dict.fromkeys(part.strip().upper() for part in parts if part and part.strip()))
    return cleaned or None


def fetch_static_candidates(
    conn: sqlite3.Connection,
    stop_id: str,
    now: datetime,
    window_secs: int,
    routes: list[str] | None,
) -> list[StaticCandidate]:
    """Scheduled stops for yesterday/today/tomorrow's service dates that fall in the window.

    Looking at neighbouring service dates handles post-midnight GTFS times (``24:25:00``) and
    the midnight rollover; the lookback catches buses scheduled in the past but running late.
    """
    tz = now.tzinfo
    route_clause = ""
    route_params: list[str] = []
    if routes:
        route_clause = f" AND UPPER(r.route_short_name) IN ({','.join('?' * len(routes))})"
        route_params = list(routes)

    candidates: list[StaticCandidate] = []
    for day_offset in (-1, 0, 1):
        service_date = now.date() + timedelta(days=day_offset)
        midnight = gtfs_service_datetime(service_date, 0, tz)
        offset = int((now - midnight).total_seconds())
        low, high = offset - LOOKBACK_SECONDS, offset + window_secs
        if high < 0:
            continue
        active = get_active_service_ids(conn, service_date)
        if not active:
            continue
        rows = conn.execute(
            _CANDIDATE_SQL + route_clause, (stop_id, low, high, *route_params)
        ).fetchall()
        for trip_id, seq, dep_secs, stop_headsign, trip_headsign, service_id, short, long in rows:
            if service_id not in active:
                continue
            candidates.append(
                StaticCandidate(
                    trip_id=trip_id,
                    stop_id=stop_id,
                    stop_sequence=seq,
                    service_date=service_date,
                    departure_secs=dep_secs,
                    route=short,
                    headsign=stop_headsign or trip_headsign or long or "",
                )
            )
    return candidates


def _find_exact(updates: tuple[StopTimeLive, ...], candidate: StaticCandidate):
    for update in updates:
        if update.seq is not None and update.seq == candidate.stop_sequence:
            return update
    for update in updates:
        if update.seq is None and update.stop_id == candidate.stop_id:
            return update
    return None


def _propagated_delay(updates: tuple[StopTimeLive, ...], stop_sequence: int) -> int | None:
    """GTFS-R delay propagation: reuse the delay of the closest earlier stop with one."""
    delay = None
    for update in updates:
        if update.seq is None or update.seq >= stop_sequence or update.sr == "SKIPPED":
            continue
        candidate_delay = update.dep_delay if update.dep_delay is not None else update.arr_delay
        if candidate_delay is not None:
            delay = candidate_delay
    return delay


def resolve_prediction(
    candidate: StaticCandidate,
    scheduled: datetime,
    live: TripLiveUpdate | None,
    tz: ZoneInfo,
) -> tuple[datetime, int, bool, bool]:
    """Return ``(predicted_time, delay_seconds, is_live, is_cancelled)`` for a candidate."""
    if live is None:
        return scheduled, 0, False, False
    if live.sr == "CANCELED":
        return scheduled, 0, True, True

    exact = _find_exact(live.updates, candidate)
    if exact is not None:
        if exact.sr == "SKIPPED":
            return scheduled, 0, True, True
        if exact.sr == "NO_DATA":
            return scheduled, 0, False, False
        timestamp = exact.dep_time if exact.dep_time is not None else exact.arr_time
        if timestamp is not None:
            predicted = datetime.fromtimestamp(timestamp, tz)
            return predicted, int((predicted - scheduled).total_seconds()), True, False
        delay = exact.dep_delay if exact.dep_delay is not None else exact.arr_delay
        if delay is not None:
            return scheduled + timedelta(seconds=delay), delay, True, False

    propagated = _propagated_delay(live.updates, candidate.stop_sequence)
    if propagated is not None:
        return scheduled + timedelta(seconds=propagated), propagated, True, False
    return scheduled, 0, False, False


def merge_live(
    candidates: list[StaticCandidate],
    live_updates: dict[str, TripLiveUpdate],
    now: datetime,
    tz: ZoneInfo,
) -> list[Departure]:
    cutoff = now - timedelta(seconds=PAST_GRACE_SECONDS)
    departures: list[Departure] = []
    for candidate in candidates:
        scheduled = gtfs_service_datetime(candidate.service_date, candidate.departure_secs, tz)
        predicted, delay, is_live, is_cancelled = resolve_prediction(
            candidate, scheduled, live_updates.get(candidate.trip_id), tz
        )
        if predicted < cutoff:
            continue
        departures.append(
            Departure(
                trip_id=candidate.trip_id,
                route=candidate.route,
                headsign=candidate.headsign,
                scheduled_time=scheduled,
                predicted_time=predicted,
                minutes_until=max(0, int((predicted - now).total_seconds() // 60)),
                delay_seconds=delay,
                is_live=is_live,
                is_cancelled=is_cancelled,
            )
        )
    departures.sort(key=lambda d: (d.predicted_time, d.route, d.trip_id))
    return departures


def _load_candidates(
    db_path: Path, stop_id: str, now: datetime, window_secs: int, routes: list[str] | None
) -> list[StaticCandidate]:
    conn = open_ingested_db(db_path)
    try:
        return fetch_static_candidates(conn, stop_id, now, window_secs, routes)
    finally:
        conn.close()


async def get_departures(
    db_path: Path,
    stop_id: str,
    gtfsr,
    *,
    routes: str | list[str] | None = None,
    limit: int = 10,
    window_minutes: int = 90,
    now: datetime | None = None,
    tz: ZoneInfo | None = None,
) -> DeparturesResult:
    """Next departures at ``stop_id``, with live delays merged in when available."""
    tz = tz or (now.tzinfo if now is not None else ZoneInfo(DEFAULT_TZ))
    now = now.astimezone(tz) if now is not None else datetime.now(tz)
    route_filter = parse_route_filter(routes)

    candidates = await asyncio.to_thread(
        _load_candidates, Path(db_path), stop_id, now, window_minutes * 60, route_filter
    )
    live_updates, realtime_available = await gtfsr.get_trip_updates(
        [candidate.trip_id for candidate in candidates]
    )
    departures = merge_live(candidates, live_updates, now, tz)
    return DeparturesResult(departures[:limit], realtime_available)
