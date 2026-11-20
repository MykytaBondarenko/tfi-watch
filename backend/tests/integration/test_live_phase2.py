import time
from contextlib import asynccontextmanager

import httpx
import pytest

from src.config import get_settings
from src.db import is_ingested
from src.main import create_app

_SETTINGS = get_settings()

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not (
            _SETTINGS.has_valid_nta_api_key()
            and _SETTINGS.test_stop_id
            and is_ingested(_SETTINGS.sqlite_db_path)
        ),
        reason=(
            "Needs NTA_API_KEY, TEST_STOP_ID and an ingested backend/data/gtfs.db "
            "(python scripts/ingest_static.py)"
        ),
    ),
]


@asynccontextmanager
async def live_client():
    app = create_app(settings=get_settings())
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        await app.state.redis.aclose()


async def test_stop_metadata_by_id_and_by_code():
    stop_id = get_settings().test_stop_id
    async with live_client() as client:
        by_id = await client.get(f"/stops/{stop_id}")
        assert by_id.status_code == 200
        body = by_id.json()
        assert 51 < body["lat"] < 56 and -11 < body["lon"] < -5
        assert body["routes"], "stop should be served by at least one route"

        assert body["stop_code"], "test stop needs a public stop_code for this check"
        by_code = await client.get(f"/stops/{body['stop_code']}")
        assert by_code.status_code == 200
        assert by_code.json()["stop_id"] == body["stop_id"]


async def test_live_departures_and_warm_latency():
    stop_id = get_settings().test_stop_id
    async with live_client() as client:
        first = await client.get(f"/stops/{stop_id}/departures")
        assert first.status_code == 200
        body = first.json()
        assert body["realtime_available"] is True
        for departure in body["departures"]:
            assert {"route", "headsign", "predicted_time", "minutes_until", "is_live"} <= set(
                departure
            )
        print(f"\nlive departures for {stop_id}:")
        for departure in body["departures"][:5]:
            print(
                f"  {departure['route']:>5} {departure['headsign'][:28]:<28} "
                f"{departure['minutes_until']:>3} min live={departure['is_live']}"
            )

        started = time.perf_counter()
        warm = await client.get(f"/stops/{stop_id}/departures")
        elapsed_ms = (time.perf_counter() - started) * 1000
        assert warm.status_code == 200
        assert elapsed_ms < 100, f"warm request took {elapsed_ms:.1f} ms"


async def test_route_filter_returns_only_requested_route():
    stop_id = get_settings().test_stop_id
    async with live_client() as client:
        routes = (await client.get(f"/stops/{stop_id}")).json()["routes"]
        wanted = routes[0]
        response = await client.get(f"/stops/{stop_id}/departures", params={"routes": wanted})
        assert response.status_code == 200
        assert {d["route"] for d in response.json()["departures"]} <= {wanted}
