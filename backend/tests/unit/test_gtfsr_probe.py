import httpx
import pytest
import respx
from google.transit import gtfs_realtime_pb2

from scripts.check_gtfsr import (
    GtfsrAuthError,
    GtfsrEmptyPayloadError,
    GtfsrError,
    GtfsrParseError,
    fetch_and_parse_gtfsr,
)

pytestmark = pytest.mark.unit

URL = "https://example.test/gtfsr/v2/TripUpdates"


def _build_feed(trip_ids: list[str]) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    for index, trip_id in enumerate(trip_ids):
        entity = feed.entity.add()
        entity.id = str(index)
        entity.trip_update.trip.trip_id = trip_id
    return feed.SerializeToString()


@respx.mock
def test_happy_path_sends_api_key_and_summarises_feed():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=_build_feed(["T1", "T2"])))
    summary = fetch_and_parse_gtfsr(URL, "secret-key")
    assert route.calls.last.request.headers["x-api-key"] == "secret-key"
    assert summary == {
        "gtfs_realtime_version": "2.0",
        "timestamp": 1_700_000_000,
        "entity_count": 2,
        "sample_trip_ids": ["T1", "T2"],
    }


@respx.mock
def test_sample_trip_ids_capped_at_five():
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_build_feed([f"T{i}" for i in range(8)]))
    )
    summary = fetch_and_parse_gtfsr(URL, "k")
    assert summary["entity_count"] == 8
    assert len(summary["sample_trip_ids"]) == 5


@respx.mock
@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_raises_explicit_error(status):
    respx.get(URL).mock(return_value=httpx.Response(status))
    with pytest.raises(GtfsrAuthError, match=str(status)):
        fetch_and_parse_gtfsr(URL, "bad-key")


@respx.mock
def test_other_http_error_raises_generic_error():
    respx.get(URL).mock(return_value=httpx.Response(500))
    with pytest.raises(GtfsrError) as exc_info:
        fetch_and_parse_gtfsr(URL, "k")
    assert not isinstance(exc_info.value, GtfsrAuthError)


@respx.mock
def test_empty_payload_raises():
    respx.get(URL).mock(return_value=httpx.Response(200, content=b""))
    with pytest.raises(GtfsrEmptyPayloadError):
        fetch_and_parse_gtfsr(URL, "k")


@respx.mock
def test_corrupt_payload_raises_parse_error():
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"\xff\xff\xffnot protobuf"))
    with pytest.raises(GtfsrParseError):
        fetch_and_parse_gtfsr(URL, "k")


@respx.mock
def test_network_error_is_wrapped():
    respx.get(URL).mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(GtfsrError):
        fetch_and_parse_gtfsr(URL, "k")
