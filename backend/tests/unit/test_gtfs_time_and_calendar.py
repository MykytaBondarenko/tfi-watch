import sqlite3
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from src.ingestion.gtfs_time import (
    get_active_service_ids,
    gtfs_service_datetime,
    parse_gtfs_time_to_seconds,
)

pytestmark = pytest.mark.unit

DUBLIN = ZoneInfo("Europe/Dublin")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("00:00:00", 0),
        ("08:30:00", 30600),
        ("24:15:00", 87300),
        ("25:30:00", 91800),
        ("26:05:10", 93910),
        (" 5:30:00", 19800),
    ],
)
def test_parse_valid_times(text, expected):
    assert parse_gtfs_time_to_seconds(text) == expected


@pytest.mark.parametrize(
    "text", ["", "8:30", "08:60:00", "08:30:61", "ab:cd:ef", "08:30:00:00", "-1:00:00"]
)
def test_parse_malformed_times_raise(text):
    with pytest.raises(ValueError):
        parse_gtfs_time_to_seconds(text)


def test_service_datetime_normal_day_and_post_midnight():
    assert gtfs_service_datetime(date(2026, 11, 20), 8 * 3600, DUBLIN) == datetime(
        2026, 11, 20, 8, 0, tzinfo=DUBLIN
    )
    assert gtfs_service_datetime(date(2026, 11, 20), 87300, DUBLIN) == datetime(
        2026, 11, 21, 0, 15, tzinfo=DUBLIN
    )


def test_service_datetime_spring_forward_day():
    # Clocks go forward at 01:00 UTC on 29 March 2026 (GMT -> IST).
    morning = gtfs_service_datetime(date(2026, 3, 29), 8 * 3600, DUBLIN)
    assert (morning.hour, morning.minute) == (8, 0)
    assert morning.utcoffset().total_seconds() == 3600
    # The next day's post-midnight trip is still correct.
    late = gtfs_service_datetime(date(2026, 3, 29), 24 * 3600 + 15 * 60, DUBLIN)
    assert late == datetime(2026, 3, 30, 0, 15, tzinfo=DUBLIN)


def test_service_datetime_fall_back_day():
    # Clocks go back at 01:00 UTC on 25 October 2026 (IST -> GMT).
    morning = gtfs_service_datetime(date(2026, 10, 25), 8 * 3600, DUBLIN)
    assert (morning.hour, morning.minute) == (8, 0)
    assert morning.utcoffset().total_seconds() == 0
    early = gtfs_service_datetime(date(2026, 10, 25), 1 * 3600 + 30 * 60, DUBLIN)
    assert (early.hour, early.minute) == (1, 30)
    assert early.utcoffset().total_seconds() == 0


@pytest.fixture
def calendar_conn():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE calendar (service_id TEXT PRIMARY KEY, monday INT, tuesday INT, wednesday INT,
            thursday INT, friday INT, saturday INT, sunday INT, start_date TEXT, end_date TEXT);
        CREATE TABLE calendar_dates (service_id TEXT, date TEXT, exception_type INT);
        INSERT INTO calendar VALUES ('A',1,1,1,1,1,0,0,'20261101','20261130');
        INSERT INTO calendar VALUES ('SAT',0,0,0,0,0,1,0,'20260101','20261231');
        """
    )
    yield conn
    conn.close()


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 11, 20), {"A"}),  # Friday inside the range
        (date(2026, 11, 21), {"SAT"}),  # Saturday: A does not run on weekends
        (date(2026, 11, 1), set()),  # Sunday inside range, but no weekday flag
        (date(2026, 11, 2), {"A"}),  # first Monday (start boundary)
        (date(2026, 11, 30), {"A"}),  # end_date boundary (Monday)
        (date(2026, 12, 1), set()),  # day after end_date
        (date(2026, 10, 30), set()),  # Friday before start_date
    ],
)
def test_active_service_ids_weekday_and_range(calendar_conn, day, expected):
    assert get_active_service_ids(calendar_conn, day) == expected


def test_calendar_dates_add_and_remove(calendar_conn):
    calendar_conn.executemany(
        "INSERT INTO calendar_dates VALUES (?,?,?)",
        [("A", "20261120", 2), ("EXTRA", "20261121", 1), ("EXTRA", "20261120", 2)],
    )
    assert get_active_service_ids(calendar_conn, date(2026, 11, 20)) == set()  # A removed
    assert get_active_service_ids(calendar_conn, date(2026, 11, 21)) == {"SAT", "EXTRA"}
