"""Pydantic response models."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class StopSummary(BaseModel):
    stop_id: str
    stop_code: str | None = None
    stop_name: str
    lat: float
    lon: float


class StopInfo(StopSummary):
    routes: list[str] = Field(default_factory=list)


class StopSearchResponse(BaseModel):
    stops: list[StopInfo]


class Departure(BaseModel):
    trip_id: str
    route: str
    headsign: str
    scheduled_time: datetime
    predicted_time: datetime
    minutes_until: int
    delay_seconds: int
    is_live: bool
    is_cancelled: bool


class DeparturesResponse(BaseModel):
    stop: StopSummary
    generated_at: datetime
    realtime_available: bool
    departures: list[Departure]


class HealthResponse(BaseModel):
    status: str
    sqlite_ok: bool
    redis_ok: bool
    gtfs_ingested_at: str | None = None
    gtfsr_last_fetched_at: int | None = None
    gtfsr_entity_count: int | None = None
