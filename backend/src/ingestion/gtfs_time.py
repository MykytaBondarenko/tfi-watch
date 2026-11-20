"""GTFS time parsing and service-date calendar logic."""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

WEEKDAY_COLUMNS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)

_TIME_RE = re.compile(r"^(\d{1,3}):([0-5]\d):([0-5]\d)$")


def parse_gtfs_time_to_seconds(value: str) -> int:
    """Parse ``HH:MM:SS`` (hours may be >= 24) into seconds since service-day midnight."""
    match = _TIME_RE.match(value.strip()) if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"Invalid GTFS time: {value!r}")
    hours, minutes, seconds = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def gtfs_service_datetime(
    service_date: date, seconds_since_midnight: int, tz: ZoneInfo
) -> datetime:
    """Convert a service date + GTFS time offset into an aware datetime.

    Per the GTFS spec, times are measured from "noon minus 12h" on the service date, which keeps
    them correct on DST-change days. Python's aware-datetime arithmetic is wall-clock based, so the
    maths is done in UTC.
    """
    noon_local = datetime(service_date.year, service_date.month, service_date.day, 12, tzinfo=tz)
    base_utc = noon_local.astimezone(UTC) - timedelta(hours=12)
    return (base_utc + timedelta(seconds=seconds_since_midnight)).astimezone(tz)


def get_active_service_ids(conn: sqlite3.Connection, service_date: date) -> set[str]:
    """Service IDs running on ``service_date`` (calendar.txt + calendar_dates.txt exceptions)."""
    date_str = service_date.strftime("%Y%m%d")
    weekday_column = WEEKDAY_COLUMNS[service_date.weekday()]
    rows = conn.execute(
        f"SELECT service_id FROM calendar "  # noqa: S608 - column comes from a fixed tuple
        f"WHERE start_date <= ? AND end_date >= ? AND {weekday_column} = 1",
        (date_str, date_str),
    )
    active = {row[0] for row in rows}
    exceptions = conn.execute(
        "SELECT service_id, exception_type FROM calendar_dates WHERE date = ?", (date_str,)
    )
    for service_id, exception_type in exceptions:
        if exception_type == 1:
            active.add(service_id)
        elif exception_type == 2:
            active.discard(service_id)
    return active
