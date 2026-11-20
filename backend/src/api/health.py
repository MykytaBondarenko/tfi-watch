"""GET /health (unauthenticated)."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request

from src.api.deps import get_settings_from_request
from src.db import get_ingested_at
from src.models.schemas import HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    settings = get_settings_from_request(request)
    ingested_at = await asyncio.to_thread(get_ingested_at, settings.sqlite_db_path)

    redis_ok = False
    meta = None
    try:
        redis_ok = bool(await request.app.state.redis.ping())
        if redis_ok:
            meta = await request.app.state.gtfsr.peek_meta()
    except Exception as exc:
        logger.warning("Redis health check failed: %s", exc)

    sqlite_ok = ingested_at is not None
    fetched_at = meta.get("fetched_at") if meta else None
    return HealthResponse(
        status="ok" if sqlite_ok and redis_ok else "degraded",
        sqlite_ok=sqlite_ok,
        redis_ok=redis_ok,
        gtfs_ingested_at=ingested_at,
        gtfsr_last_fetched_at=int(fetched_at) if fetched_at is not None else None,
        gtfsr_entity_count=meta.get("entity_count") if meta else None,
    )
