"""Download, validate and inspect the NTA GTFS Static archive."""

from __future__ import annotations

import argparse
import csv
import sys
import zipfile
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import GTFS_STATIC_DIR, GTFS_STATIC_ZIP, get_settings  # noqa: E402

REQUIRED_GTFS_FILES = (
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
)
DOWNLOAD_TIMEOUT = 120.0
CHUNK_SIZE = 64 * 1024


class GtfsStaticError(Exception):
    """The GTFS Static archive is missing, corrupt or incomplete."""


def download_gtfs_static(url: str, dest_zip: Path, client: httpx.Client | None = None) -> Path:
    """Stream the archive to disk without buffering it in memory."""
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    partial = dest_zip.with_name(dest_zip.name + ".part")
    owns_client = client is None
    http = client or httpx.Client(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True)
    try:
        with http.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()
            with partial.open("wb") as fh:
                for chunk in response.iter_bytes(CHUNK_SIZE):
                    fh.write(chunk)
    except httpx.HTTPError as exc:
        partial.unlink(missing_ok=True)
        raise GtfsStaticError(f"Download from {url} failed: {exc}") from exc
    finally:
        if owns_client:
            http.close()
    partial.replace(dest_zip)
    return dest_zip


def extract_and_validate_gtfs(zip_path: Path, extract_dir: Path) -> list[str]:
    """Validate the archive has all core files, extract it, and return the core file names."""
    try:
        with zipfile.ZipFile(zip_path) as archive:
            names = set(archive.namelist())
            missing = [name for name in REQUIRED_GTFS_FILES if name not in names]
            if missing:
                raise GtfsStaticError(f"GTFS archive is missing required files: {missing}")
            extract_dir.mkdir(parents=True, exist_ok=True)
            archive.extractall(extract_dir)
    except zipfile.BadZipFile as exc:
        raise GtfsStaticError(f"{zip_path} is not a valid zip archive") from exc
    return list(REQUIRED_GTFS_FILES)


def search_stops(
    stops_csv: Path,
    query: str | None = None,
    stop_code: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Stream stops.txt and return matches.

    ``query`` matches stop_name / stop_id (case-insensitive substring) or an exact stop_code.
    ``stop_code`` matches the exact stop_code. With neither argument, nothing is returned.
    """
    if not query and not stop_code:
        return []
    needle = query.lower() if query else None
    results: list[dict] = []
    with stops_csv.open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            row_code = (row.get("stop_code") or "").strip()
            matched = bool(stop_code) and row_code == stop_code
            if not matched and needle:
                matched = (
                    needle in (row.get("stop_name") or "").lower()
                    or needle in (row.get("stop_id") or "").lower()
                    or row_code.lower() == needle
                )
            if matched:
                results.append(
                    {
                        "stop_id": row.get("stop_id"),
                        "stop_code": row.get("stop_code"),
                        "stop_name": row.get("stop_name"),
                        "stop_lat": row.get("stop_lat"),
                        "stop_lon": row.get("stop_lon"),
                    }
                )
                if len(results) >= limit:
                    break
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true", help="Download and validate the zip")
    parser.add_argument("--search", help="Search stops by name / stop_id")
    parser.add_argument("--stop-code", help="Find a stop by its exact public stop code")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)

    settings = get_settings()
    try:
        if args.download:
            print(f"Downloading {settings.nta_gtfs_static_url} ...")
            download_gtfs_static(settings.nta_gtfs_static_url, GTFS_STATIC_ZIP)
            found = extract_and_validate_gtfs(GTFS_STATIC_ZIP, GTFS_STATIC_DIR)
            print(f"OK: extracted and validated {', '.join(found)} into {GTFS_STATIC_DIR}")
        if args.search or args.stop_code:
            stops_csv = GTFS_STATIC_DIR / "stops.txt"
            if not stops_csv.exists():
                print(f"{stops_csv} not found. Run with --download first.", file=sys.stderr)
                return 1
            matches = search_stops(stops_csv, args.search, args.stop_code, args.limit)
            if not matches:
                print("No matching stops.")
            for stop in matches:
                print(
                    f"stop_id={stop['stop_id']} code={stop['stop_code']} "
                    f"name={stop['stop_name']!r} lat={stop['stop_lat']} lon={stop['stop_lon']}"
                )
            if matches:
                print("\nSave your chosen stop_id as TEST_STOP_ID in backend/.env")
    except GtfsStaticError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
