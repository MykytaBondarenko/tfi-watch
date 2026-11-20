from datetime import datetime, timedelta

import pytest

from src.ingestion.gtfsr_cache import StopTimeLive, TripLiveUpdate
from src.services.departures import get_departures, parse_route_filter
from tests.support import DUBLIN, NOW, StubGtfsr, TripSpec, make_db, one_stop

pytestmark = pytest.mark.unit


def live(trip_id, updates=(), sr="SCHEDULED"):
    return TripLiveUpdate(trip_id=trip_id, sr=sr, updates=tuple(updates))


def upd(**kwargs):
    return StopTimeLive(**kwargs)


async def run(db, updates=None, *, now=NOW, available=True, stop="S1", **kwargs):
    return await get_departures(
        db, stop, StubGtfsr(updates or {}, available), now=now, tz=DUBLIN, **kwargs
    )


async def test_live_delay_and_early_arrival_applied(tmp_path):
    db = make_db(tmp_path, [one_stop("A", "C3", "12:10:00"), one_stop("B", "C3", "12:20:00")])
    updates = {
        "A": live("A", [upd(seq=1, dep_delay=300)]),
        "B": live("B", [upd(seq=1, dep_delay=-120)]),
    }
    result = await run(db, updates)

    a, b = result.departures
    assert result.realtime_available is True
    assert (a.trip_id, a.delay_seconds, a.is_live, a.is_cancelled) == ("A", 300, True, False)
    assert a.predicted_time == NOW + timedelta(minutes=15)
    assert a.scheduled_time == NOW + timedelta(minutes=10)
    assert a.minutes_until == 15
    assert (b.delay_seconds, b.minutes_until) == (-120, 18)


async def test_explicit_predicted_time_wins_over_delay(tmp_path):
    db = make_db(tmp_path, [one_stop("A", "C3", "12:10:00")])
    predicted = int((NOW + timedelta(minutes=17)).timestamp())
    updates = {"A": live("A", [upd(seq=1, dep_time=predicted, dep_delay=60)])}
    (dep,) = (await run(db, updates)).departures
    assert dep.predicted_time == NOW + timedelta(minutes=17)
    assert dep.delay_seconds == 420
    assert dep.is_live


async def test_late_bus_scheduled_in_the_past_still_listed(tmp_path):
    db = make_db(tmp_path, [one_stop("LATE", "C3", "11:50:00"), one_stop("GONE", "C3", "11:50:00")])
    updates = {"LATE": live("LATE", [upd(seq=1, dep_delay=900)])}
    result = await run(db, updates)
    assert [d.trip_id for d in result.departures] == ["LATE"]
    assert result.departures[0].minutes_until == 5  # 11:50 + 15 min = 12:05


async def test_delay_propagates_from_previous_stop(tmp_path):
    trip = TripSpec(
        "P",
        "C3",
        [("X1", 1, "12:15:00", 0), ("X2", 3, "12:25:00", 0), ("S1", 7, "12:30:00", 0)],
    )
    db = make_db(tmp_path, [trip])
    updates = {"P": live("P", [upd(seq=3, dep_delay=240)])}
    (dep,) = (await run(db, updates)).departures
    assert (dep.delay_seconds, dep.is_live) == (240, True)
    assert dep.predicted_time == NOW + timedelta(minutes=34)


async def test_propagation_falls_back_to_arrival_delay(tmp_path):
    trip = TripSpec("P", "C3", [("X2", 3, "12:25:00", 0), ("S1", 7, "12:30:00", 0)])
    db = make_db(tmp_path, [trip])
    updates = {"P": live("P", [upd(seq=3, arr_delay=180)])}
    (dep,) = (await run(db, updates)).departures
    assert dep.delay_seconds == 180


async def test_exact_stop_update_beats_propagation(tmp_path):
    trip = TripSpec("P", "C3", [("X2", 3, "12:25:00", 0), ("S1", 7, "12:30:00", 0)])
    db = make_db(tmp_path, [trip])
    updates = {"P": live("P", [upd(seq=3, dep_delay=240), upd(seq=7, dep_delay=60)])}
    (dep,) = (await run(db, updates)).departures
    assert dep.delay_seconds == 60


async def test_updates_only_for_later_stops_fall_back_to_schedule(tmp_path):
    trip = TripSpec("P", "C3", [("S1", 2, "12:30:00", 0), ("X9", 9, "12:50:00", 0)])
    db = make_db(tmp_path, [trip])
    updates = {"P": live("P", [upd(seq=9, dep_delay=600)])}
    (dep,) = (await run(db, updates)).departures
    assert (dep.is_live, dep.delay_seconds) == (False, 0)


async def test_cancelled_trip_and_skipped_stop_are_flagged(tmp_path):
    db = make_db(
        tmp_path,
        [one_stop("CAN", "C3", "12:10:00"), one_stop("SKIP", "C4", "12:20:00")],
    )
    updates = {
        "CAN": live("CAN", sr="CANCELED"),
        "SKIP": live("SKIP", [upd(seq=1, sr="SKIPPED")]),
    }
    cancelled, skipped = (await run(db, updates)).departures
    for dep in (cancelled, skipped):
        assert dep.is_cancelled is True
        assert dep.is_live is True
        assert dep.delay_seconds == 0


async def test_drop_off_only_stop_is_excluded(tmp_path):
    db = make_db(
        tmp_path,
        [one_stop("TERM", "C3", "12:10:00", pickup=1), one_stop("OK", "C3", "12:15:00")],
    )
    assert [d.trip_id for d in (await run(db)).departures] == ["OK"]


async def test_midnight_rollover_includes_yesterdays_post_midnight_trips(tmp_path):
    db = make_db(
        tmp_path,
        [
            one_stop("Y", "C3", "24:25:00"),  # yesterday's service, 00:25 today
            one_stop("Z", "C3", "00:40:00"),
            one_stop("W", "C3", "23:50:00"),  # yesterday 23:50 -> already gone
        ],
    )
    now = datetime(2026, 11, 21, 0, 10, tzinfo=DUBLIN)
    result = await run(db, now=now)
    assert [d.trip_id for d in result.departures] == ["Y", "Z"]
    y = result.departures[0]
    assert y.scheduled_time == datetime(2026, 11, 21, 0, 25, tzinfo=DUBLIN)
    assert y.minutes_until == 15


async def test_midnight_rollover_includes_tomorrows_early_trips(tmp_path):
    db = make_db(tmp_path, [one_stop("V", "C3", "00:15:00"), one_stop("U", "C3", "23:45:00")])
    now = datetime(2026, 11, 20, 23, 30, tzinfo=DUBLIN)
    result = await run(db, now=now)
    assert [d.trip_id for d in result.departures] == ["U", "V"]
    v = result.departures[1]
    assert v.scheduled_time == datetime(2026, 11, 21, 0, 15, tzinfo=DUBLIN)
    assert v.minutes_until == 45


async def test_route_filter_applies_before_slicing(tmp_path):
    trips = [one_stop(f"X{i}", "X", f"12:{5 + i:02d}:00") for i in range(6)]
    c_times = ["12:40:00", "12:50:00", "13:00:00"]
    trips += [one_stop(f"C{i}", "C3", t) for i, t in enumerate(c_times)]
    db = make_db(tmp_path, trips)

    filtered = await run(db, routes=["C3"], limit=3)
    assert [d.trip_id for d in filtered.departures] == ["C0", "C1", "C2"]

    mixed_case = await run(db, routes=" c3 ,, ", limit=3)
    assert [d.trip_id for d in mixed_case.departures] == ["C0", "C1", "C2"]

    unfiltered = await run(db, limit=3)
    assert [d.route for d in unfiltered.departures] == ["X", "X", "X"]

    everything = await run(db, limit=20)
    assert len(everything.departures) == 9
    predicted = [d.predicted_time for d in everything.departures]
    assert predicted == sorted(predicted)


async def test_recently_departed_buses_filtered_with_one_minute_grace(tmp_path):
    db = make_db(
        tmp_path,
        [one_stop("JUST", "C3", "11:59:30"), one_stop("OLD", "C3", "11:58:30")],
    )
    result = await run(db)
    assert [d.trip_id for d in result.departures] == ["JUST"]
    assert result.departures[0].minutes_until == 0


async def test_window_and_other_stops_are_respected(tmp_path):
    db = make_db(
        tmp_path,
        [
            one_stop("IN", "C3", "13:25:00"),
            one_stop("OUT", "C3", "14:00:00"),
            one_stop("ELSEWHERE", "C3", "12:30:00", stop="S2"),
        ],
    )
    assert [d.trip_id for d in (await run(db, window_minutes=90)).departures] == ["IN"]
    wide = await run(db, window_minutes=180)
    assert [d.trip_id for d in wide.departures] == ["IN", "OUT"]


async def test_headsign_fallback_chain(tmp_path):
    trips = [
        TripSpec(
            "H1", "C3", [("S1", 1, "12:10:00", 0)], headsign="Trip Head", stop_headsign="Stop Head"
        ),
        TripSpec("H2", "C3", [("S1", 1, "12:20:00", 0)], headsign="Trip Head"),
        TripSpec("H3", "C4", [("S1", 1, "12:30:00", 0)], headsign="", route_long="Long Name"),
    ]
    db = make_db(tmp_path, trips)
    headsigns = [d.headsign for d in (await run(db)).departures]
    assert headsigns == ["Stop Head", "Trip Head", "Long Name"]


async def test_service_calendar_is_respected(tmp_path):
    trips = [
        one_stop("RUNS", "C3", "12:10:00"),
        TripSpec("WEEKEND", "C3", [("S1", 1, "12:15:00", 0)], service_id="SAT"),
        TripSpec("EXTRA", "C3", [("S1", 1, "12:20:00", 0)], service_id="EXTRA"),
    ]
    db = make_db(
        tmp_path,
        trips,
        calendar_rows=[
            ("WK", 1, 1, 1, 1, 1, 1, 1, "20260101", "20261231"),
            ("SAT", 0, 0, 0, 0, 0, 1, 0, "20260101", "20261231"),
        ],
        calendar_date_rows=[("EXTRA", "20261120", 1)],
    )
    assert [d.trip_id for d in (await run(db)).departures] == ["RUNS", "EXTRA"]


async def test_static_only_when_realtime_unavailable(tmp_path):
    db = make_db(tmp_path, [one_stop("A", "C3", "12:10:00")])
    result = await run(db, available=False)
    assert result.realtime_available is False
    assert [(d.trip_id, d.is_live) for d in result.departures] == [("A", False)]


def test_parse_route_filter():
    assert parse_route_filter(None) is None
    assert parse_route_filter("") is None
    assert parse_route_filter(" c3, C4 ,,c3") == ["C3", "C4"]
    assert parse_route_filter(["x1", " "]) == ["X1"]
