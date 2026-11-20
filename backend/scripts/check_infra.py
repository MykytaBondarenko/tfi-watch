"""Check that local infrastructure (Redis + SQLite) is healthy. Exit 0 only if both pass."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.cache import check_redis_health  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.db import check_sqlite_health  # noqa: E402


async def run_checks(redis_url: str, db_path: Path) -> dict[str, bool]:
    return {
        "redis": await check_redis_health(redis_url),
        "sqlite": check_sqlite_health(db_path),
    }


def main() -> int:
    settings = get_settings()
    results = asyncio.run(run_checks(settings.redis_url, settings.sqlite_db_path))
    for name, ok in results.items():
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
