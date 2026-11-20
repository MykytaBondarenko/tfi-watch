import zipfile
from pathlib import Path

import httpx
import pytest
import respx

from scripts.fetch_static_gtfs import (
    REQUIRED_GTFS_FILES,
    GtfsStaticError,
    download_gtfs_static,
    extract_and_validate_gtfs,
    search_stops,
)

pytestmark = pytest.mark.unit

STOPS_CSV = (
    "stop_id,stop_code,stop_name,stop_lat,stop_lon\n"
    "8250DB000001,1001,Maynooth Station,53.3813,-6.5918\n"
    "8250DB000002,1002,Main Street Maynooth,53.3810,-6.5920\n"
    "8220DB000003,2001,O'Connell Street,53.3498,-6.2603\n"
    "8220DB000004,2002,Parnell Square,53.3531,-6.2644\n"
)

FILE_CONTENTS = {
    "stops.txt": STOPS_CSV,
    "routes.txt": "route_id,route_short_name\nR1,C3\n",
    "trips.txt": "route_id,service_id,trip_id\nR1,S1,T1\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "T1,08:00:00,08:00:00,8250DB000001,1\n",
    "calendar.txt": "service_id,monday,start_date,end_date\nS1,1,20260101,20261231\n",
    "calendar_dates.txt": "service_id,date,exception_type\nS1,20261225,2\n",
}


def _make_zip(path: Path, omit: str | None = None) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in FILE_CONTENTS.items():
            if name != omit:
                archive.writestr(name, content)
    return path


@pytest.fixture
def stops_csv(tmp_path):
    path = tmp_path / "stops.txt"
    path.write_text(STOPS_CSV)
    return path


def test_extract_and_validate_succeeds_with_all_files(tmp_path):
    zip_path = _make_zip(tmp_path / "gtfs.zip")
    extract_dir = tmp_path / "out"
    found = extract_and_validate_gtfs(zip_path, extract_dir)
    assert set(found) == set(REQUIRED_GTFS_FILES)
    for name in REQUIRED_GTFS_FILES:
        assert (extract_dir / name).exists()


def test_extract_and_validate_reports_missing_file(tmp_path):
    zip_path = _make_zip(tmp_path / "gtfs.zip", omit="stops.txt")
    with pytest.raises(GtfsStaticError, match="stops.txt"):
        extract_and_validate_gtfs(zip_path, tmp_path / "out")
    assert not (tmp_path / "out" / "routes.txt").exists()


def test_extract_and_validate_rejects_non_zip(tmp_path):
    bad = tmp_path / "bad.zip"
    bad.write_text("definitely not a zip")
    with pytest.raises(GtfsStaticError):
        extract_and_validate_gtfs(bad, tmp_path / "out")


def test_search_by_name_is_case_insensitive(stops_csv):
    results = search_stops(stops_csv, query="mAyNoOtH")
    assert {r["stop_id"] for r in results} == {"8250DB000001", "8250DB000002"}
    assert results[0].keys() == {"stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"}


def test_search_by_exact_stop_code(stops_csv):
    results = search_stops(stops_csv, stop_code="2002")
    assert [r["stop_name"] for r in results] == ["Parnell Square"]
    assert search_stops(stops_csv, stop_code="200") == []


def test_search_no_match_returns_empty(stops_csv):
    assert search_stops(stops_csv, query="Nowhere At All") == []


def test_search_without_criteria_returns_empty(stops_csv):
    assert search_stops(stops_csv) == []


def test_search_enforces_limit(stops_csv):
    assert len(search_stops(stops_csv, query="street", limit=1)) == 1
    assert len(search_stops(stops_csv, query="a", limit=3)) == 3


@respx.mock
def test_download_streams_to_disk(tmp_path):
    payload = b"PK-fake-zip-bytes" * 1000
    respx.get("https://example.test/gtfs.zip").mock(
        return_value=httpx.Response(200, content=payload)
    )
    dest = tmp_path / "nested" / "gtfs.zip"
    result = download_gtfs_static("https://example.test/gtfs.zip", dest)
    assert result == dest
    assert dest.read_bytes() == payload
    assert not (tmp_path / "nested" / "gtfs.zip.part").exists()


@respx.mock
def test_download_failure_leaves_no_partial_file(tmp_path):
    respx.get("https://example.test/gtfs.zip").mock(return_value=httpx.Response(404))
    dest = tmp_path / "gtfs.zip"
    with pytest.raises(GtfsStaticError):
        download_gtfs_static("https://example.test/gtfs.zip", dest)
    assert not dest.exists()
    assert not (tmp_path / "gtfs.zip.part").exists()
