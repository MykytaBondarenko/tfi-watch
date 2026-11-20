import asyncio
import json

import httpx
import pytest
import respx

from src.ingestion.gtfsr_cache import GtfsrCacheService, extract_trip_updates
from tests.support import FailingRedis, FakeRedis, build_feed

pytestmark = pytest.mark.unit

URL = "https://nta.test/gtfsr/v2/TripUpdates"

FEED = build_feed(
    [
        {
            "trip_id": "T1",
            "updates": [
                {"seq": 3, "stop_id": "S3", "dep_delay": 240, "arr_delay": 200},
                {"seq": 5, "stop_id": "S5", "dep_time": 1_700_000_900},
            ],
        },
        {"trip_id": "T2", "sr": "CANCELED"},
        {"trip_id": "T3", "updates": [{"seq": 2, "stop_id": "S2", "sr": "SKIPPED"}]},
    ]
)


def make_service(redis, *, api_key="key", ttl=60, backoff=15):
    return GtfsrCacheService(
        redis,
        url=URL,
        api_key=api_key,
        ttl_seconds=ttl,
        request_timeout=5.0,
        failure_backoff_seconds=backoff,
    )


@respx.mock
async def test_cache_miss_populates_then_cache_hit_skips_http():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    redis = FakeRedis()
    service = make_service(redis)

    first = await service.ensure_fresh()
    assert first.realtime_available and not first.from_cache
    assert route.call_count == 1
    assert route.calls.last.request.headers["x-api-key"] == "key"
    assert first.entity_count == 3
    assert first.feed_timestamp == 1_700_000_000

    assert redis.ttls["gtfsr:meta"] == 60
    assert redis.ttls["gtfsr:trip:T1"] == 90  # ttl + 30s grace
    assert set(redis.store) == {"gtfsr:meta", "gtfsr:trip:T1", "gtfsr:trip:T2", "gtfsr:trip:T3"}
    assert json.loads(redis.store["gtfsr:meta"])["entity_count"] == 3

    second = await service.ensure_fresh()
    assert second.realtime_available and second.from_cache
    assert route.call_count == 1  # zero extra NTA calls


@respx.mock
async def test_refetches_after_meta_expires():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    redis = FakeRedis()
    service = make_service(redis)
    await service.ensure_fresh()
    await redis.delete("gtfsr:meta")  # simulate the 60s TTL elapsing
    await service.ensure_fresh()
    assert route.call_count == 2


@respx.mock
async def test_concurrent_requests_trigger_a_single_nta_fetch():
    async def slow_nta(request):
        await asyncio.sleep(0.05)
        return httpx.Response(200, content=FEED)

    route = respx.get(URL).mock(side_effect=slow_nta)
    service = make_service(FakeRedis())

    results = await asyncio.gather(*(service.ensure_fresh() for _ in range(5)))

    assert route.call_count == 1
    assert all(result.realtime_available for result in results)


@respx.mock
async def test_trips_are_written_in_pipelined_batches():
    feed = build_feed([{"trip_id": f"T{i}"} for i in range(1200)])
    respx.get(URL).mock(return_value=httpx.Response(200, content=feed))
    redis = FakeRedis()
    await make_service(redis).ensure_fresh()
    assert redis.pipeline_batches == [500, 500, 200]


@respx.mock
async def test_get_trip_updates_uses_one_mget_and_decodes_values():
    respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    redis = FakeRedis()
    service = make_service(redis)

    updates, available = await service.get_trip_updates(["T1", "T2", "T3", "UNKNOWN"])

    assert available is True
    assert redis.mget_calls == 1
    assert set(updates) == {"T1", "T2", "T3"}
    t1 = updates["T1"]
    assert [(u.seq, u.stop_id, u.dep_delay, u.arr_delay, u.dep_time) for u in t1.updates] == [
        (3, "S3", 240, 200, None),
        (5, "S5", None, None, 1_700_000_900),
    ]
    assert updates["T2"].sr == "CANCELED"
    assert updates["T3"].updates[0].sr == "SKIPPED"


@respx.mock
async def test_get_trip_updates_with_no_ids():
    respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    updates, available = await make_service(FakeRedis()).get_trip_updates([])
    assert (updates, available) == ({}, True)


def test_extract_trip_updates_parses_all_fields():
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(FEED)
    trips = extract_trip_updates(feed)
    assert trips["T1"].sr == "SCHEDULED"
    assert trips["T2"].sr == "CANCELED"
    assert trips["T1"].updates[0].arr_delay == 200
    assert trips["T1"].updates[1].dep_time == 1_700_000_900


@respx.mock
@pytest.mark.parametrize(
    "mock_kwargs",
    [
        {"return_value": httpx.Response(500)},
        {"return_value": httpx.Response(401)},
        {"return_value": httpx.Response(200, content=b"")},
        {"return_value": httpx.Response(200, content=b"\xff\xff\xffgarbage")},
        {"side_effect": httpx.ReadTimeout("slow")},
        {"side_effect": httpx.ConnectError("down")},
    ],
)
async def test_nta_failures_degrade_gracefully(mock_kwargs):
    respx.get(URL).mock(**mock_kwargs)
    redis = FakeRedis()
    service = make_service(redis)

    status = await service.ensure_fresh()
    assert status.realtime_available is False
    assert "gtfsr:meta" not in redis.store

    updates, available = await service.get_trip_updates(["T1"])
    assert (updates, available) == ({}, False)


@respx.mock
async def test_failure_backoff_prevents_hammering_nta():
    route = respx.get(URL).mock(return_value=httpx.Response(500))
    service = make_service(FakeRedis())
    await service.ensure_fresh()
    await service.ensure_fresh()
    await service.ensure_fresh()
    assert route.call_count == 1


@respx.mock
async def test_missing_api_key_skips_http_entirely():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    status = await make_service(FakeRedis(), api_key="").ensure_fresh()
    assert status.realtime_available is False
    assert route.call_count == 0


@respx.mock
async def test_redis_outage_degrades_gracefully():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=FEED))
    service = make_service(FailingRedis())
    status = await service.ensure_fresh()
    assert status.realtime_available is False
    assert route.call_count == 0
    assert await service.peek_meta() is None
