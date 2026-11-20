"""Daily in-app refresh of the GTFS Static database."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from src.config import GTFS_STATIC_DIR, GTFS_STATIC_ZIP, Settings
from src.ingestion.static_loader import ingest_gtfs_directory

logger = logging.getLogger(__name__)


def seconds_until_next_run(now_utc: datetime, hour_utc: int) -> float:
    """Seconds from ``now_utc`` until the next ``hour_utc``:00 UTC (always in the future)."""
    target = now_utc.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    if target <= now_utc:
        target += timedelta(days=1)
    return (target - now_utc).total_seconds()


def refresh_static_data(settings: Settings) -> dict:
    """Download, validate, extract and ingest the latest GTFS Static archive (blocking)."""
    from scripts.fetch_static_gtfs import download_gtfs_static, extract_and_validate_gtfs

    download_gtfs_static(settings.nta_gtfs_static_url, GTFS_STATIC_ZIP)
    extract_and_validate_gtfs(GTFS_STATIC_ZIP, GTFS_STATIC_DIR)
    return ingest_gtfs_directory(GTFS_STATIC_DIR, settings.sqlite_db_path)


async def run_daily_static_refresh(
    settings: Settings,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    refresh: Callable[[Settings], dict] = refresh_static_data,
) -> None:
    """Sleep until the next refresh hour, refresh in a worker thread, repeat forever.

    Errors are logged and never kill the loop; cancellation (app shutdown) propagates.
    """
    while True:
        delay = seconds_until_next_run(datetime.now(UTC), settings.static_refresh_hour_utc)
        logger.info("Next GTFS static refresh in %.0f seconds", delay)
        await sleep(delay)
        try:
            summary = await asyncio.to_thread(refresh, settings)
            logger.info(
                "GTFS static refresh finished in %ss (%s)",
                summary.get("elapsed_seconds"),
                summary.get("counts"),
            )
        except Exception:
            logger.exception("GTFS static refresh failed; keeping the existing database")
