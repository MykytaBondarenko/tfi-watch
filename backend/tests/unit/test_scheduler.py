import asyncio
from datetime import UTC, datetime

import pytest

from src.config import Settings
from src.ingestion.scheduler import run_daily_static_refresh, seconds_until_next_run

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("now", "hour", "expected"),
    [
        (datetime(2026, 11, 20, 3, 0, tzinfo=UTC), 4, 3600),
        (datetime(2026, 11, 20, 4, 0, tzinfo=UTC), 4, 86400),  # exactly now -> tomorrow
        (datetime(2026, 11, 20, 5, 30, tzinfo=UTC), 4, 22.5 * 3600),
        (datetime(2026, 11, 20, 23, 59, 30, tzinfo=UTC), 0, 30),
    ],
)
def test_seconds_until_next_run(now, hour, expected):
    assert seconds_until_next_run(now, hour) == expected


async def test_loop_survives_refresh_errors_and_stops_on_cancel():
    settings = Settings(_env_file=None, static_refresh_hour_utc=4)
    sleeps: list[float] = []
    calls: list[int] = []

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) == 3:
            raise asyncio.CancelledError

    def flaky_refresh(_settings):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("download failed")
        return {"elapsed_seconds": 1, "counts": {"stops": 1}}

    with pytest.raises(asyncio.CancelledError):
        await run_daily_static_refresh(settings, sleep=fake_sleep, refresh=flaky_refresh)

    assert len(calls) == 2  # first failed, loop carried on, second succeeded
    assert all(0 < delay <= 86400 for delay in sleeps)
