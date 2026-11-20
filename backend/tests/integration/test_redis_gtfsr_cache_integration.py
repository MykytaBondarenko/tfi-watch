import uuid

import httpx
import pytest
import respx

from src.cache import create_redis_client
from src.config import get_settings
from src.ingestion.gtfsr_cache import GtfsrCacheService
from tests.support import build_feed

pytestmark = pytest.mark.integration

URL = "https://nta.test/gtfsr/v2/TripUpdates"
FEED = build_feed(
    [
        {"trip_id": "T1", "updates": [{"seq": 3, "stop_id": "S3", "dep_delay": 240}]},
        {"trip_id": "T2", "sr": "CANCELED"},
    ]
)


@pytest.fixture
async def redis_and_prefix():
    client = create_redis_client(get_settings().redis_url)
    prefix = f"gtfsr-test-{uuid.uuid4().hex[:8]}"
    yield client, prefix
    keys = [key async for key in client.scan_iter(match=f"{prefix}:*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


@respx.mock
async def test_real_redis_roundtrip_with_ttls(redis_and_prefix):
    client, prefix = redis_and_prefix
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    service = GtfsrCacheService(
        client, url=URL, api_key="k", ttl_seconds=60, request_timeout=5, key_prefix=prefix
    )

    status = await service.ensure_fresh()
    assert status.realtime_available is True

    assert 0 < await client.ttl(f"{prefix}:meta") <= 60
    assert 60 < await client.ttl(f"{prefix}:trip:T1") <= 90

    updates, available = await service.get_trip_updates(["T1", "T2", "MISSING"])
    assert available is True
    assert set(updates) == {"T1", "T2"}
    assert updates["T1"].updates[0].dep_delay == 240
    assert updates["T2"].sr == "CANCELED"

    again = await service.ensure_fresh()
    assert again.from_cache is True
    assert route.call_count == 1


@respx.mock
async def test_keys_expire_and_trigger_a_refetch(redis_and_prefix):
    client, prefix = redis_and_prefix
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    service = GtfsrCacheService(
        client, url=URL, api_key="k", ttl_seconds=1, request_timeout=5, key_prefix=prefix
    )
    await service.ensure_fresh()
    assert await client.exists(f"{prefix}:meta") == 1

    import asyncio

    await asyncio.sleep(1.2)
    assert await client.exists(f"{prefix}:meta") == 0  # real Redis expired the key
    await service.ensure_fresh()
    assert route.call_count == 2
