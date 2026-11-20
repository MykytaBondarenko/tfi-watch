"""Redis helpers for the live GTFS-R cache."""

from __future__ import annotations

import redis.asyncio as redis
from redis.exceptions import RedisError


def create_redis_client(redis_url: str) -> redis.Redis:
    return redis.from_url(redis_url, decode_responses=True)


async def check_redis_health(redis_url: str) -> bool:
    client = create_redis_client(redis_url)
    try:
        return bool(await client.ping())
    except (RedisError, OSError):
        return False
    finally:
        await client.aclose()
