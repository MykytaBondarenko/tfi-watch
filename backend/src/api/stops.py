"""GET /stops/search, /stops/{stop_ref}, /stops/{stop_ref}/departures."""

from __future__ import annotations

from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.auth import verify_api_key
from src.api.deps import get_settings_from_request, run_with_db
from src.models.schemas import (
    DeparturesResponse,
    StopInfo,
    StopSearchResponse,
    StopSummary,
)
from src.services.departures import get_departures
from src.services.stops import resolve_stop, search_stops

router = APIRouter(prefix="/stops", tags=["stops"], dependencies=[Depends(verify_api_key)])


def _not_found(stop_ref: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"Stop '{stop_ref}' not found")


@router.get("/search", response_model=StopSearchResponse)
async def search(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=50),
) -> StopSearchResponse:
    settings = get_settings_from_request(request)
    stops = await run_with_db(settings, search_stops, q, limit)
    return StopSearchResponse(stops=stops)


@router.get("/{stop_ref}", response_model=StopInfo)
async def stop_detail(request: Request, stop_ref: str) -> StopInfo:
    settings = get_settings_from_request(request)
    stop = await run_with_db(settings, resolve_stop, stop_ref)
    if stop is None:
        raise _not_found(stop_ref)
    return stop


@router.get("/{stop_ref}/departures", response_model=DeparturesResponse)
async def departures(
    request: Request,
    stop_ref: str,
    routes: str | None = Query(default=None, description="Comma-separated route numbers"),
    limit: int | None = Query(default=None, ge=1, le=30),
    window_minutes: int | None = Query(default=None, ge=5, le=240),
) -> DeparturesResponse:
    settings = get_settings_from_request(request)
    stop = await run_with_db(settings, resolve_stop, stop_ref)
    if stop is None:
        raise _not_found(stop_ref)

    now = request.app.state.clock()
    result = await get_departures(
        settings.sqlite_db_path,
        stop.stop_id,
        request.app.state.gtfsr,
        routes=routes,
        limit=limit or settings.default_departures_limit,
        window_minutes=window_minutes or settings.default_window_minutes,
        now=now,
        tz=ZoneInfo(settings.timezone),
    )
    return DeparturesResponse(
        stop=StopSummary(**stop.model_dump(exclude={"routes"})),
        generated_at=now,
        realtime_available=result.realtime_available,
        departures=result.departures,
    )
