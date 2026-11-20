import time
import uuid

import httpx
import pytest
import respx

from src.cache import create_redis_client
from src.config import Settings, get_settings
from src.ingestion.gtfsr_cache import GtfsrCacheService
from src.main import create_app
from tests.support import NOW, build_feed, make_db, one_stop

pytestmark = pytest.mark.integration

URL = "https://nta.test/gtfsr/v2/TripUpdates"
STOP = "8260B1034301"


@respx.mock
async def test_full_app_with_real_redis_cold_then_warm(tmp_path):
    db = make_db(
        tmp_path,
        [one_stop("A", "C3", "12:10:00", stop=STOP), one_stop("B", "C4", "12:20:00", stop=STOP)],
        stop_rows=[(STOP, "103431", "Maynooth University", 53.38, -6.59)],
    )
    feed = build_feed(
        [{"trip_id": "A", "updates": [{"seq": 1, "stop_id": STOP, "dep_delay": 120}]}]
    )
    nta = respx.get(URL).mock(return_value=httpx.Response(200, content=feed))

    redis_client = create_redis_client(get_settings().redis_url)
    prefix = f"gtfsr-test-{uuid.uuid4().hex[:8]}"
    settings = Settings(_env_file=None, sqlite_db_path=db, nta_api_key="k")
    gtfsr = GtfsrCacheService(
        redis_client, url=URL, api_key="k", ttl_seconds=60, request_timeout=5, key_prefix=prefix
    )
    app = create_app(settings=settings, redis_client=redis_client, gtfsr=gtfsr, clock=lambda: NOW)

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            cold = await client.get(f"/stops/{STOP}/departures")
            assert cold.status_code == 200
            assert cold.json()["realtime_available"] is True
            assert cold.json()["departures"][0]["delay_seconds"] == 120
            assert nta.call_count == 1

            started = time.perf_counter()
            warm = await client.get(f"/stops/{STOP}/departures")
            elapsed_ms = (time.perf_counter() - started) * 1000

            assert warm.status_code == 200
            assert warm.json()["departures"] == cold.json()["departures"]
            assert nta.call_count == 1  # served from Redis, NTA not called again
            assert elapsed_ms < 100, f"warm request took {elapsed_ms:.1f} ms"

            health = (await client.get("/health")).json()
            assert health["status"] == "ok"
            assert health["gtfsr_entity_count"] == 1
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f"{prefix}:*")]
        if keys:
            await redis_client.delete(*keys)
        await redis_client.aclose()
