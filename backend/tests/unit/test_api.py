import sqlite3

import httpx
import pytest

from src.config import Settings
from src.ingestion.gtfsr_cache import StopTimeLive, TripLiveUpdate
from src.main import create_app
from tests.support import NOW, FailingRedis, FakeRedis, StubGtfsr, make_db, one_stop

pytestmark = pytest.mark.unit

STOP_ROWS = [
    ("8260B1034301", "103431", "Maynooth University", 53.382175, -6.598822),
    ("8260B1034302", "103432", "Maynooth Station", 53.381, -6.594),
]
TRIPS = [
    one_stop("A", "C3", "12:10:00", stop="8260B1034301", headsign="Ringsend"),
    one_stop("B", "C4", "12:20:00", stop="8260B1034301"),
    one_stop("C", "66", "12:30:00", stop="8260B1034301"),
    one_stop("D", "115", "12:40:00", stop="8260B1034301"),
]


@pytest.fixture
def db(tmp_path):
    return make_db(tmp_path, TRIPS, stop_rows=STOP_ROWS)


def make_client(db_path, *, app_api_key="", updates=None, redis=None, available=True):
    settings = Settings(
        _env_file=None, sqlite_db_path=db_path, app_api_key=app_api_key, nta_api_key="x"
    )
    app = create_app(
        settings=settings,
        redis_client=redis or FakeRedis(),
        gtfsr=StubGtfsr(
            updates or {}, available, meta={"fetched_at": 1_795_188_630.4, "entity_count": 7}
        ),
        clock=lambda: NOW,
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_health_ok(db):
    async with make_client(db) as client:
        response = await client.get("/health")
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["sqlite_ok"] is True and body["redis_ok"] is True
    assert body["gtfs_ingested_at"]
    assert body["gtfsr_last_fetched_at"] == 1_795_188_630
    assert body["gtfsr_entity_count"] == 7


async def test_health_degraded_when_db_missing_or_redis_down(tmp_path, db):
    async with make_client(tmp_path / "missing.db") as client:
        missing = (await client.get("/health")).json()
    async with make_client(db, redis=FailingRedis()) as client:
        redis_down = (await client.get("/health")).json()
    assert (missing["status"], missing["sqlite_ok"], missing["redis_ok"]) == (
        "degraded",
        False,
        True,
    )
    assert (redis_down["status"], redis_down["sqlite_ok"], redis_down["redis_ok"]) == (
        "degraded",
        True,
        False,
    )


@pytest.mark.parametrize("ref", ["8260B1034301", "103431"])
async def test_stop_lookup_by_id_and_code(db, ref):
    async with make_client(db) as client:
        response = await client.get(f"/stops/{ref}")
    body = response.json()
    assert response.status_code == 200
    assert body == {
        "stop_id": "8260B1034301",
        "stop_code": "103431",
        "stop_name": "Maynooth University",
        "lat": 53.382175,
        "lon": -6.598822,
        "routes": ["66", "115", "C3", "C4"],
    }


async def test_unknown_stop_is_404(db):
    async with make_client(db) as client:
        assert (await client.get("/stops/NONEXISTENT")).status_code == 404
        assert (await client.get("/stops/NONEXISTENT/departures")).status_code == 404


async def test_search_by_name_and_code(db):
    async with make_client(db) as client:
        by_name = (await client.get("/stops/search", params={"q": "Maynooth"})).json()
        by_code = (await client.get("/stops/search", params={"q": "103431"})).json()
        limited = (await client.get("/stops/search", params={"q": "Maynooth", "limit": 1})).json()
    assert {s["stop_id"] for s in by_name["stops"]} == {"8260B1034301", "8260B1034302"}
    assert [s["stop_id"] for s in by_code["stops"]] == ["8260B1034301"]
    assert by_code["stops"][0]["routes"] == ["66", "115", "C3", "C4"]
    assert len(limited["stops"]) == 1


@pytest.mark.parametrize(
    "params",
    [{}, {"q": ""}, {"q": "x", "limit": 0}, {"q": "x", "limit": 51}],
)
async def test_search_validation(db, params):
    async with make_client(db) as client:
        assert (await client.get("/stops/search", params=params)).status_code == 422


async def test_departures_schema_and_live_data(db):
    updates = {"A": TripLiveUpdate("A", "SCHEDULED", (StopTimeLive(seq=1, dep_delay=180),))}
    async with make_client(db, updates=updates) as client:
        response = await client.get("/stops/103431/departures")
    body = response.json()
    assert response.status_code == 200
    assert body["stop"] == {
        "stop_id": "8260B1034301",
        "stop_code": "103431",
        "stop_name": "Maynooth University",
        "lat": 53.382175,
        "lon": -6.598822,
    }
    assert body["realtime_available"] is True
    assert body["generated_at"].startswith("2026-11-20T12:00:00")
    first = body["departures"][0]
    assert set(first) == {
        "trip_id",
        "route",
        "headsign",
        "scheduled_time",
        "predicted_time",
        "minutes_until",
        "delay_seconds",
        "is_live",
        "is_cancelled",
    }
    assert (first["route"], first["headsign"], first["delay_seconds"], first["minutes_until"]) == (
        "C3",
        "Ringsend",
        180,
        13,
    )
    assert first["is_live"] is True
    assert [d["route"] for d in body["departures"]] == ["C3", "C4", "66", "115"]


async def test_departures_route_filter_is_case_insensitive(db):
    async with make_client(db) as client:
        response = await client.get("/stops/8260B1034301/departures", params={"routes": "c3,C4"})
    assert [d["route"] for d in response.json()["departures"]] == ["C3", "C4"]


async def test_departures_limit_and_window(db):
    async with make_client(db) as client:
        limited = (await client.get("/stops/103431/departures", params={"limit": 2})).json()
        narrow = (
            await client.get("/stops/103431/departures", params={"window_minutes": 15})
        ).json()
    assert len(limited["departures"]) == 2
    assert [d["route"] for d in narrow["departures"]] == ["C3"]


@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 100}, {"window_minutes": 1}, {"window_minutes": 241}],
)
async def test_departures_validation(db, params):
    async with make_client(db) as client:
        assert (await client.get("/stops/103431/departures", params=params)).status_code == 422


async def test_departures_report_realtime_unavailable(db):
    async with make_client(db, available=False) as client:
        body = (await client.get("/stops/103431/departures")).json()
    assert body["realtime_available"] is False
    assert all(d["is_live"] is False for d in body["departures"])


async def test_auth_disabled_by_default(db):
    async with make_client(db) as client:
        assert (await client.get("/stops/103431")).status_code == 200


async def test_auth_enabled_requires_matching_key(db):
    async with make_client(db, app_api_key="secret") as client:
        assert (await client.get("/stops/103431")).status_code == 401
        bad = await client.get("/stops/103431", headers={"X-API-Key": "wrong"})
        assert bad.status_code == 401
        assert (await client.get("/stops/103431/departures")).status_code == 401
        assert (await client.get("/stops/search", params={"q": "m"})).status_code == 401
        ok = await client.get("/stops/103431", headers={"X-API-Key": "secret"})
        assert ok.status_code == 200
        dep = await client.get("/stops/103431/departures", headers={"X-API-Key": "secret"})
        assert dep.status_code == 200
        assert (await client.get("/health")).status_code == 200  # never needs a key


async def test_503_when_database_missing_or_not_ingested(tmp_path):
    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()  # exists but holds no GTFS data
    for path in (tmp_path / "missing.db", empty):
        async with make_client(path) as client:
            for url in ("/stops/103431", "/stops/103431/departures", "/stops/search?q=a"):
                response = await client.get(url)
                assert response.status_code == 503, url
                detail = response.json()["detail"].lower()
                assert "ingest" in detail or "not found" in detail
