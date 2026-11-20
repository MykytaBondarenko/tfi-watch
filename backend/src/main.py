"""FastAPI application entrypoint (``uvicorn src.main:app``)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api import health, stops
from src.cache import create_redis_client
from src.config import Settings, get_settings
from src.db import StaticDataNotReady
from src.ingestion.gtfsr_cache import GtfsrCacheService
from src.ingestion.scheduler import run_daily_static_refresh

logging.basicConfig(level=logging.INFO)


def create_app(
    settings: Settings | None = None,
    redis_client=None,
    gtfsr=None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Build the app. Collaborators can be injected (used by the tests)."""
    settings = settings or get_settings()
    owns_redis = redis_client is None
    redis_client = redis_client or create_redis_client(settings.redis_url)
    if gtfsr is None:
        gtfsr = GtfsrCacheService(
            redis_client,
            url=settings.nta_gtfsr_url,
            api_key=settings.nta_api_key if settings.has_valid_nta_api_key() else "",
            ttl_seconds=settings.gtfsr_ttl_seconds,
            request_timeout=settings.gtfsr_request_timeout_seconds,
        )
    tz = ZoneInfo(settings.timezone)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        refresh_task = None
        if settings.static_refresh_enabled:
            refresh_task = asyncio.create_task(run_daily_static_refresh(settings))
        try:
            yield
        finally:
            if refresh_task is not None:
                refresh_task.cancel()
                with suppress(asyncio.CancelledError):
                    await refresh_task
            if owns_redis:
                await redis_client.aclose()

    app = FastAPI(title="TFI Live Backend", lifespan=lifespan)
    app.state.settings = settings
    app.state.redis = redis_client
    app.state.gtfsr = gtfsr
    app.state.clock = clock or (lambda: datetime.now(tz))

    @app.exception_handler(StaticDataNotReady)
    async def _static_data_not_ready(_: Request, exc: StaticDataNotReady) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    app.include_router(health.router)
    app.include_router(stops.router)
    return app


app = create_app()
