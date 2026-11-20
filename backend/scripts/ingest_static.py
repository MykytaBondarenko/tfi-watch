"""Ingest the extracted GTFS Static CSVs into SQLite (optionally downloading them first)."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.fetch_static_gtfs import (  # noqa: E402
    GtfsStaticError,
    download_gtfs_static,
    extract_and_validate_gtfs,
)
from src.config import GTFS_STATIC_DIR, GTFS_STATIC_ZIP, get_settings  # noqa: E402
from src.ingestion.static_loader import ingest_gtfs_directory  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true", help="Download + extract the zip first")
    parser.add_argument("--gtfs-dir", type=Path, default=GTFS_STATIC_DIR)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args(argv)

    settings = get_settings()
    db_path = args.db_path or settings.sqlite_db_path
    try:
        if args.download:
            print(f"Downloading {settings.nta_gtfs_static_url} ...")
            download_gtfs_static(settings.nta_gtfs_static_url, GTFS_STATIC_ZIP)
            extract_and_validate_gtfs(GTFS_STATIC_ZIP, args.gtfs_dir)
        print(f"Ingesting {args.gtfs_dir} into {db_path} ...")
        summary = ingest_gtfs_directory(args.gtfs_dir, db_path)
    except (GtfsStaticError, FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1

    for table, count in summary["counts"].items():
        print(f"  {table:<15} {count:>12,}")
    size_mb = db_path.stat().st_size / 1_000_000
    print(f"OK in {summary['elapsed_seconds']}s -> {db_path} ({size_mb:,.0f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
