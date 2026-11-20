"""Application settings loaded from backend/.env and process environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BACKEND_DIR / "data"
GTFS_STATIC_ZIP = DATA_DIR / "gtfs_static.zip"
GTFS_STATIC_DIR = DATA_DIR / "gtfs_static"

API_KEY_PLACEHOLDER = "your_nta_primary_api_key_here"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BACKEND_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    nta_api_key: str = ""
    nta_gtfsr_url: str = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
    nta_gtfs_static_url: str = (
        "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
    )
    redis_url: str = "redis://localhost:6379/0"
    sqlite_db_path: Path = Path("data/gtfs.db")
    test_stop_id: str = ""

    # --- Phase 2 ---
    # Optional shared secret for X-API-Key auth on /stops/*. Empty = auth disabled.
    app_api_key: str = ""
    # Freshness TTL of the cached GTFS-R feed; per-trip keys get a +30s grace period.
    gtfsr_ttl_seconds: int = Field(default=60, ge=1)
    gtfsr_request_timeout_seconds: float = Field(default=10.0, gt=0)
    default_window_minutes: int = Field(default=90, ge=5, le=240)
    default_departures_limit: int = Field(default=10, ge=1, le=30)
    static_refresh_enabled: bool = False
    static_refresh_hour_utc: int = Field(default=4, ge=0, le=23)
    timezone: str = "Europe/Dublin"

    @field_validator("sqlite_db_path")
    @classmethod
    def _resolve_db_path(cls, value: Path) -> Path:
        """Resolve relative paths against backend/ so cwd does not matter."""
        return value if value.is_absolute() else BACKEND_DIR / value

    def has_valid_nta_api_key(self) -> bool:
        key = self.nta_api_key.strip()
        return bool(key) and key != API_KEY_PLACEHOLDER


@lru_cache
def get_settings() -> Settings:
    return Settings()
