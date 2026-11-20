"""Probe the NTA GTFS-Realtime v2 endpoint and summarise the Protocol Buffer feed."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import get_settings  # noqa: E402

REQUEST_TIMEOUT = 15.0


class GtfsrError(Exception):
    """Base error for GTFS-R probe failures."""


class GtfsrAuthError(GtfsrError):
    """The API rejected the key (HTTP 401/403)."""


class GtfsrEmptyPayloadError(GtfsrError):
    """The API returned HTTP 200 with an empty body."""


class GtfsrParseError(GtfsrError):
    """The body was not a valid GTFS-R FeedMessage."""


def fetch_and_parse_gtfsr(url: str, api_key: str, client: httpx.Client | None = None) -> dict:
    owns_client = client is None
    http = client or httpx.Client(timeout=REQUEST_TIMEOUT)
    try:
        response = http.get(url, headers={"x-api-key": api_key}, timeout=REQUEST_TIMEOUT)
    except httpx.HTTPError as exc:
        raise GtfsrError(f"Request to {url} failed: {exc}") from exc
    finally:
        if owns_client:
            http.close()

    if response.status_code in (401, 403):
        raise GtfsrAuthError(
            f"HTTP {response.status_code}: the NTA API rejected the API key. "
            "Check NTA_API_KEY and your GTFS-Realtime subscription."
        )
    if response.status_code != 200:
        raise GtfsrError(f"Unexpected HTTP status {response.status_code} from {url}")
    if not response.content:
        raise GtfsrEmptyPayloadError("HTTP 200 but the response body was empty")

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except DecodeError as exc:
        raise GtfsrParseError(f"Response is not a valid GTFS-R FeedMessage: {exc}") from exc

    sample_trip_ids = [
        entity.trip_update.trip.trip_id
        for entity in feed.entity
        if entity.HasField("trip_update")
    ][:5]
    return {
        "gtfs_realtime_version": feed.header.gtfs_realtime_version,
        "timestamp": feed.header.timestamp,
        "entity_count": len(feed.entity),
        "sample_trip_ids": sample_trip_ids,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=None, help="Override NTA_GTFSR_URL")
    args = parser.parse_args(argv)

    settings = get_settings()
    if not settings.has_valid_nta_api_key():
        print("NTA_API_KEY is not set. Add your key to backend/.env first.", file=sys.stderr)
        return 1
    try:
        summary = fetch_and_parse_gtfsr(args.url or settings.nta_gtfsr_url, settings.nta_api_key)
    except GtfsrError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
