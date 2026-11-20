import csv
import time

import pytest

from scripts.check_gtfsr import fetch_and_parse_gtfsr
from scripts.fetch_static_gtfs import REQUIRED_GTFS_FILES
from src.config import GTFS_STATIC_DIR, get_settings

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not get_settings().has_valid_nta_api_key(),
        reason="Set NTA_API_KEY in backend/.env to run live NTA checks",
    ),
]

MAX_FEED_AGE_SECONDS = 15 * 60


def test_live_gtfsr_feed_is_fresh_and_non_empty():
    settings = get_settings()
    summary = fetch_and_parse_gtfsr(settings.nta_gtfsr_url, settings.nta_api_key)
    assert summary["entity_count"] > 0
    assert abs(time.time() - summary["timestamp"]) < MAX_FEED_AGE_SECONDS


def test_static_gtfs_files_present():
    if not (GTFS_STATIC_DIR / "stops.txt").exists():
        pytest.skip("Run scripts/fetch_static_gtfs.py --download first")
    for name in REQUIRED_GTFS_FILES:
        assert (GTFS_STATIC_DIR / name).exists(), f"{name} missing"


def test_test_stop_id_exists_with_valid_coordinates():
    stop_id = get_settings().test_stop_id
    stops_csv = GTFS_STATIC_DIR / "stops.txt"
    if not stop_id:
        pytest.skip("TEST_STOP_ID is not set in backend/.env")
    if not stops_csv.exists():
        pytest.skip("Run scripts/fetch_static_gtfs.py --download first")
    with stops_csv.open(newline="", encoding="utf-8-sig") as fh:
        rows = [row for row in csv.DictReader(fh) if row["stop_id"] == stop_id]
    assert rows, f"stop_id {stop_id!r} not found in stops.txt"
    float(rows[0]["stop_lat"])
    float(rows[0]["stop_lon"])
