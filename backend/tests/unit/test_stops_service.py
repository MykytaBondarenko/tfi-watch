import pytest

from src.db import open_ingested_db
from src.services.stops import resolve_stop, route_sort_key, search_stops
from tests.support import TripSpec, make_db, one_stop

pytestmark = pytest.mark.unit

STOP_ROWS = [
    ("S1", "103431", "Maynooth University", 53.382, -6.598),
    ("S2", "103432", "Maynooth Station", 53.381, -6.594),
    ("S3", "200001", "Old Maynooth Road", 53.390, -6.600),
    ("S4", "300001", "100% Real_Stop", 53.0, -6.0),
]


@pytest.fixture
def conn(tmp_path):
    trips = [
        one_stop(f"T{i}", route, f"12:{i:02d}:00")
        for i, route in enumerate(["115", "66", "X10", "C4", "C3", "N4", "X9", "66A"])
    ]
    trips.append(TripSpec("TS2", "C3", [("S2", 1, "12:30:00", 0)]))
    db = make_db(tmp_path, trips, stop_rows=STOP_ROWS)
    connection = open_ingested_db(db)
    yield connection
    connection.close()


def test_route_sort_key_orders_naturally():
    routes = ["X10", "C4", "115", "66A", "C3", "66", "N4", "X9"]
    assert sorted(routes, key=route_sort_key) == [
        "66",
        "66A",
        "115",
        "C3",
        "C4",
        "N4",
        "X9",
        "X10",
    ]


def test_resolve_by_stop_id_and_stop_code(conn):
    by_id = resolve_stop(conn, "S1")
    by_code = resolve_stop(conn, "103431")
    assert by_id == by_code
    assert by_id.stop_name == "Maynooth University"
    assert by_id.routes == ["66", "66A", "115", "C3", "C4", "N4", "X9", "X10"]
    assert resolve_stop(conn, "S2").routes == ["C3"]


def test_resolve_unknown_stop_returns_none(conn):
    assert resolve_stop(conn, "NOPE") is None


def test_search_ranks_code_match_then_prefix_then_substring(conn):
    assert [s.stop_id for s in search_stops(conn, "103431")] == ["S1"]
    assert [s.stop_id for s in search_stops(conn, "maynooth")] == ["S2", "S1", "S3"]
    assert [s.stop_id for s in search_stops(conn, "MAYNOOTH", limit=1)] == ["S2"]


def test_search_escapes_like_wildcards(conn):
    assert [s.stop_id for s in search_stops(conn, "100%")] == ["S4"]
    assert [s.stop_id for s in search_stops(conn, "l_s")] == ["S4"]
    assert search_stops(conn, "Mayn_oth") == []
    assert search_stops(conn, "Mayn%th") == []
    assert search_stops(conn, "   ") == []
    assert search_stops(conn, "zzz") == []


def test_search_attaches_routes(conn):
    stops = {s.stop_id: s for s in search_stops(conn, "maynooth")}
    assert stops["S2"].routes == ["C3"]
    assert stops["S3"].routes == []
