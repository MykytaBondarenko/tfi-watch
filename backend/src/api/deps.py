"""Shared API dependencies and helpers."""

from __future__ import annotations

import asyncio
import hmac
from collections.abc import Callable
from typing import Any

from fastapi import Header, HTTPException, Request

from src.config import Settings
from src.db import open_ingested_db


def get_settings_from_request(request: Request) -> Settings:
    return request.app.state.settings


async def verify_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    """Require ``X-API-Key`` to match ``APP_API_KEY`` when one is configured."""
    expected = get_settings_from_request(request).app_api_key
    if not expected:
        return
    if x_api_key is None or not hmac.compare_digest(x_api_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


async def run_with_db(settings: Settings, func: Callable[..., Any], *args: Any) -> Any:
    """Run ``func(conn, *args)`` on a read-only ingested DB connection in a worker thread."""

    def _call() -> Any:
        conn = open_ingested_db(settings.sqlite_db_path)
        try:
            return func(conn, *args)
        finally:
            conn.close()

    return await asyncio.to_thread(_call)
