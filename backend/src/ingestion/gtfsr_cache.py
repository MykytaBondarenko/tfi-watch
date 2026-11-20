"""Cache-aside GTFS-Realtime fetcher backed by Redis.

There is no background poller: an API request that finds the cached feed expired triggers a single
NTA fetch (guarded by a lock so concurrent requests never stampede NTA) and the parsed trip
updates are written to Redis for ``ttl_seconds``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import httpx
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

DEFAULT_KEY_PREFIX = "gtfsr"
TRIP_TTL_GRACE_SECONDS = 30
PIPELINE_BATCH_SIZE = 500

_TRIP_SR = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
_STU_SR = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship


@dataclass(frozen=True)
class StopTimeLive:
    seq: int | None = None
    stop_id: str | None = None
    arr_delay: int | None = None
    dep_delay: int | None = None
    arr_time: int | None = None
    dep_time: int | None = None
    sr: str = "SCHEDULED"


@dataclass(frozen=True)
class TripLiveUpdate:
    trip_id: str
    sr: str = "SCHEDULED"
    updates: tuple[StopTimeLive, ...] = ()


@dataclass(frozen=True)
class GtfsrCacheStatus:
    realtime_available: bool
    fetched_at: float | None = None
    feed_timestamp: int | None = None
    entity_count: int | None = None
    from_cache: bool = False
    reason: str | None = None


def _optional(message, field: str) -> int | None:
    return getattr(message, field) if message.HasField(field) else None


def extract_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> dict[str, TripLiveUpdate]:
    """Pull every trip_update out of a FeedMessage, keyed by trip_id."""
    trips: dict[str, TripLiveUpdate] = {}
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        if not trip_id:
            continue
        updates = []
        for stu in trip_update.stop_time_update:
            arrival = stu.arrival if stu.HasField("arrival") else None
            departure = stu.departure if stu.HasField("departure") else None
            updates.append(
                StopTimeLive(
                    seq=_optional(stu, "stop_sequence"),
                    stop_id=stu.stop_id or None,
                    arr_delay=_optional(arrival, "delay") if arrival is not None else None,
                    dep_delay=_optional(departure, "delay") if departure is not None else None,
                    arr_time=_optional(arrival, "time") if arrival is not None else None,
                    dep_time=_optional(departure, "time") if departure is not None else None,
                    sr=_STU_SR.Name(stu.schedule_relationship),
                )
            )
        updates.sort(key=lambda u: (u.seq is None, u.seq or 0))
        trips[trip_id] = TripLiveUpdate(
            trip_id=trip_id,
            sr=_TRIP_SR.Name(trip_update.trip.schedule_relationship),
            updates=tuple(updates),
        )
    return trips


def trip_to_json(trip: TripLiveUpdate) -> str:
    payload = {
        "sr": trip.sr,
        "updates": [
            {
                "seq": u.seq,
                "stop_id": u.stop_id,
                "arr_delay": u.arr_delay,
                "dep_delay": u.dep_delay,
                "arr_time": u.arr_time,
                "dep_time": u.dep_time,
                "sr": u.sr,
            }
            for u in trip.updates
        ],
    }
    return json.dumps(payload, separators=(",", ":"))


def trip_from_json(trip_id: str, raw: str) -> TripLiveUpdate:
    data = json.loads(raw)
    return TripLiveUpdate(
        trip_id=trip_id,
        sr=data.get("sr", "SCHEDULED"),
        updates=tuple(
            StopTimeLive(
                seq=u.get("seq"),
                stop_id=u.get("stop_id"),
                arr_delay=u.get("arr_delay"),
                dep_delay=u.get("dep_delay"),
                arr_time=u.get("arr_time"),
                dep_time=u.get("dep_time"),
                sr=u.get("sr", "SCHEDULED"),
            )
            for u in data.get("updates", [])
        ),
    )


class GtfsrCacheService:
    def __init__(
        self,
        redis,
        *,
        url: str,
        api_key: str,
        ttl_seconds: int = 60,
        request_timeout: float = 10.0,
        failure_backoff_seconds: int = 15,
        key_prefix: str = DEFAULT_KEY_PREFIX,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._redis = redis
        self._url = url
        self._api_key = api_key
        self._ttl = ttl_seconds
        self._timeout = request_timeout
        self._failure_backoff = failure_backoff_seconds
        self._client = http_client
        self._lock = asyncio.Lock()
        self._meta_key = f"{key_prefix}:meta"
        self._fail_key = f"{key_prefix}:fail"
        self._trip_prefix = f"{key_prefix}:trip:"

    # ----------------------------------------------------------------- public API

    async def ensure_fresh(self) -> GtfsrCacheStatus:
        """Make sure Redis holds a fresh feed; never raises when NTA or Redis is unavailable."""
        try:
            cached = await self._cached_status()
            if cached is not None:
                return cached
        except RedisError as exc:
            logger.warning("Redis unavailable while reading GTFS-R cache: %s", exc)
            return GtfsrCacheStatus(False, reason="redis unavailable")

        async with self._lock:
            try:
                cached = await self._cached_status()
                if cached is not None:
                    return cached
            except RedisError as exc:
                logger.warning("Redis unavailable while reading GTFS-R cache: %s", exc)
                return GtfsrCacheStatus(False, reason="redis unavailable")
            return await self._fetch_and_store()

    async def get_trip_updates(
        self, trip_ids: list[str]
    ) -> tuple[dict[str, TripLiveUpdate], bool]:
        """Return live updates for the given trips and whether realtime data is available."""
        status = await self.ensure_fresh()
        if not status.realtime_available:
            return {}, False
        if not trip_ids:
            return {}, True
        unique_ids = list(dict.fromkeys(trip_ids))
        try:
            raw_values = await self._redis.mget([self._trip_prefix + tid for tid in unique_ids])
        except RedisError as exc:
            logger.warning("Redis unavailable while reading trip updates: %s", exc)
            return {}, False
        updates = {
            trip_id: trip_from_json(trip_id, raw)
            for trip_id, raw in zip(unique_ids, raw_values, strict=True)
            if raw is not None
        }
        return updates, True

    async def peek_meta(self) -> dict | None:
        """Return cache metadata without triggering a fetch (used by /health)."""
        try:
            raw = await self._redis.get(self._meta_key)
        except RedisError:
            return None
        return json.loads(raw) if raw else None

    # ------------------------------------------------------------------ internals

    async def _cached_status(self) -> GtfsrCacheStatus | None:
        raw = await self._redis.get(self._meta_key)
        if raw:
            meta = json.loads(raw)
            return GtfsrCacheStatus(
                True,
                fetched_at=meta.get("fetched_at"),
                feed_timestamp=meta.get("feed_timestamp"),
                entity_count=meta.get("entity_count"),
                from_cache=True,
            )
        if await self._redis.exists(self._fail_key):
            return GtfsrCacheStatus(False, reason="recent fetch failure, backing off")
        return None

    async def _fetch_and_store(self) -> GtfsrCacheStatus:
        if not self._api_key:
            logger.warning("NTA_API_KEY is not configured; serving static schedule only")
            return GtfsrCacheStatus(False, reason="NTA_API_KEY not configured")

        try:
            response = await self._http_get()
        except httpx.HTTPError as exc:
            return await self._fail(f"request failed: {exc!r}")
        if response.status_code != 200:
            return await self._fail(f"NTA returned HTTP {response.status_code}")
        if not response.content:
            return await self._fail("NTA returned an empty body")

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response.content)
        except DecodeError as exc:
            return await self._fail(f"invalid GTFS-R payload: {exc}")

        trips = extract_trip_updates(feed)
        fetched_at = time.time()
        feed_timestamp = int(feed.header.timestamp)
        try:
            await self._store(trips, fetched_at, feed_timestamp, len(feed.entity))
        except RedisError as exc:
            logger.warning("Redis unavailable while storing GTFS-R feed: %s", exc)
            return GtfsrCacheStatus(False, reason="redis unavailable")
        logger.info("Cached %d GTFS-R trip updates", len(trips))
        return GtfsrCacheStatus(
            True,
            fetched_at=fetched_at,
            feed_timestamp=feed_timestamp,
            entity_count=len(feed.entity),
            from_cache=False,
        )

    async def _http_get(self) -> httpx.Response:
        client = self._client or httpx.AsyncClient(timeout=self._timeout)
        try:
            return await client.get(
                self._url, headers={"x-api-key": self._api_key}, timeout=self._timeout
            )
        finally:
            if self._client is None:
                await client.aclose()

    async def _store(
        self,
        trips: dict[str, TripLiveUpdate],
        fetched_at: float,
        feed_timestamp: int,
        entity_count: int,
    ) -> None:
        items = list(trips.items())
        trip_ttl = self._ttl + TRIP_TTL_GRACE_SECONDS
        for start in range(0, len(items), PIPELINE_BATCH_SIZE):
            async with self._redis.pipeline(transaction=False) as pipe:
                for trip_id, trip in items[start : start + PIPELINE_BATCH_SIZE]:
                    pipe.set(self._trip_prefix + trip_id, trip_to_json(trip), ex=trip_ttl)
                await pipe.execute()
        meta = {
            "fetched_at": fetched_at,
            "feed_timestamp": feed_timestamp,
            "entity_count": entity_count,
        }
        await self._redis.set(self._meta_key, json.dumps(meta), ex=self._ttl)

    async def _fail(self, reason: str) -> GtfsrCacheStatus:
        logger.warning("GTFS-R fetch failed (%s); serving static schedule only", reason)
        try:
            await self._redis.set(self._fail_key, reason[:200], ex=self._failure_backoff)
        except RedisError:
            pass
        return GtfsrCacheStatus(False, reason=reason)
