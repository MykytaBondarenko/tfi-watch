# Phase 2 Agent Implementation Specification: Backend Proxy Service

This specification defines everything a coding agent needs to implement and verify **Phase 2** (Steps 3–5, plus Step 6 local containerisation & daily GTFS refresh) of `tfi_live_wear_os_implementation_plan.md`, building directly on the completed Phase 1 codebase (`docs/phase1_agent_spec.md`).

---

## 0. Context, Assumptions & Locked Decisions

* **Phase 1 is complete:** `backend/` already contains `pyproject.toml`, `.env` / `.env.example`, `compose.yaml` (Redis 7), `Dockerfile`, `src/config.py`, `src/db.py`, `src/cache.py`, `scripts/check_gtfsr.py`, `scripts/fetch_static_gtfs.py`, `scripts/check_infra.py`, and 39 passing tests. `backend/data/gtfs_static/` contains real extracted NTA GTFS Static CSVs (note: `stop_times.txt` is ~437 MB).
* **Locked architectural decisions for Phase 2:**
  * **Backend stack:** Python 3.12+ (`FastAPI`, `uvicorn[standard]`, `httpx`, `gtfs-realtime-bindings`, `redis.asyncio`, `pydantic-settings`, `sqlite3`, stdlib `zoneinfo` with `Europe/Dublin`).
  * **Static GTFS storage:** Embedded **SQLite** (`backend/data/gtfs.db`) built via streaming bulk-insert into a staging file and atomically swapped (`os.replace`) with zero downtime.
  * **Live GTFS-R fetching strategy:** **On-demand fetch + Redis cache (cache-aside, default 60s TTL)** — no 24/7 background poller. Outbound NTA requests happen only when an API request finds the Redis cache expired, with single-flight lock protection so concurrent requests never stampede NTA.
  * **Stop lookup:** All `/stops/{stop_ref}` endpoints accept **both** GTFS `stop_id` (e.g., `8260B1034301`) **and** public TFI `stop_code` (e.g., `103431`).
  * **API authentication:** Optional shared API key via `APP_API_KEY` env var. When unset/empty, endpoints are open (MVP/local dev). When set, `/stops/*` endpoints require `X-API-Key: <APP_API_KEY>` (`/health` always stays unauthenticated).
  * **Step 6 scope:** Local containerisation (`Dockerfile` + 2-service `compose.yaml` with `api` + `redis`, compatible with both `podman compose` and `docker compose`) plus an in-app daily GTFS Static refresh scheduler. **Cloud hosting / HTTPS deployment is explicitly deferred to Step 6b** after local verification.
* **Observed NTA GTFS Static CSV columns (verified in Phase 1):**
  * `stops.txt`: `stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,...`
  * `routes.txt`: `route_id,agency_id,route_short_name,route_long_name,...`
  * `trips.txt`: `route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,...`
  * `stop_times.txt`: `trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,...`
  * `calendar.txt`: `service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date`
  * `calendar_dates.txt`: `service_id,date,exception_type` (`1` = service added, `2` = service removed)

---

## 1. Target Backend Structure (Additions & Updates)

Preserve existing Phase 1 modules/tests and add/update the following inside `backend/`:

```text
backend/
├── .env.example                           # Add GTFSR_TTL_SECONDS, APP_API_KEY, STATIC_REFRESH_*
├── compose.yaml                           # Update: add `api` service alongside `redis`
├── Dockerfile                             # Update: production uvicorn entrypoint + HEALTHCHECK
├── pyproject.toml                         # Add tzdata (optional safety) if needed; keep dev deps
├── scripts/
│   └── ingest_static.py                   # CLI to run GTFS static ingestion (with optional --download)
├── src/
│   ├── config.py                          # Extend Settings with Phase 2 config keys
│   ├── db.py                              # Add read-only connection helper & schema/metadata helpers
│   ├── main.py                            # FastAPI app, lifespan (Redis + daily static refresh), routers
│   ├── models/
│   │   └── schemas.py                     # Pydantic response models (StopInfo, Departure, DeparturesResponse, HealthResponse)
│   ├── ingestion/
│   │   ├── gtfs_time.py                   # GTFS HH:MM:SS (>24h) parsing + Europe/Dublin service-date calendar logic
│   │   ├── static_loader.py               # Streaming SQLite bulk loader + stop_routes materialisation + atomic swap
│   │   ├── gtfsr_cache.py                 # Cache-aside NTA GTFS-R fetcher, Protobuf extractor, Redis pipeline + lock
│   │   └── scheduler.py                   # Daily in-app GTFS Static refresh task (cancellable on shutdown)
│   ├── services/
│   │   ├── __init__.py
│   │   ├── stops.py                       # Stop resolution (stop_id or stop_code), metadata, and search
│   │   └── departures.py                  # Departure joining engine (static window + midnight rollover + Redis live merge)
│   └── api/
│       ├── auth.py                        # Optional X-API-Key dependency (constant-time compare)
│       ├── health.py                      # GET /health
│       └── stops.py                       # GET /stops/search, GET /stops/{stop_ref}, GET /stops/{stop_ref}/departures
└── tests/
    ├── unit/
    │   ├── test_gtfs_time_and_calendar.py
    │   ├── test_static_loader.py
    │   ├── test_gtfsr_cache.py
    │   ├── test_departure_engine.py
    │   └── test_api.py
    └── integration/
        ├── test_redis_gtfsr_cache_integration.py
        ├── test_api_integration.py
        └── test_live_phase2.py
```

---

## 2. Configuration Updates (`src/config.py` & `.env.example`)

Extend `Settings` in `backend/src/config.py` and document matching defaults in `backend/.env.example`:

| Env Var | Default | Purpose |
| :--- | :--- | :--- |
| `APP_API_KEY` | `""` | Optional shared secret for `X-API-Key` auth on `/stops/*`. Empty = auth disabled. |
| `GTFSR_TTL_SECONDS` | `60` | Redis freshness TTL for cached GTFS-R feed (`gtfsr:meta`). Trip keys use `TTL + 30s` grace. |
| `GTFSR_REQUEST_TIMEOUT_SECONDS` | `10.0` | Timeout for on-demand NTA GTFS-R HTTP fetch. |
| `DEFAULT_WINDOW_MINUTES` | `90` | Default lookahead window for `/departures`. |
| `DEFAULT_DEPARTURES_LIMIT` | `10` | Default max departures returned (clamped `1..30`). |
| `STATIC_REFRESH_ENABLED` | `False` | Enable in-app daily GTFS Static refresh loop (set `true` in `compose.yaml` for container runtime; kept `False` by default so local tests never download 145 MB unexpectedly). |
| `STATIC_REFRESH_HOUR_UTC` | `4` | UTC hour (0–23) for daily GTFS Static refresh. |
| `TIMEZONE` | `"Europe/Dublin"` | Local transit timezone for service-date & midnight-rollover calculations. |

---

## 3. Functionality to Implement

### Task 1 (Step 3): GTFS Time, Calendar & Static Ingestion Pipeline

#### 1A. GTFS Time & Calendar Utilities (`src/ingestion/gtfs_time.py`)
1. `parse_gtfs_time_to_seconds(value: str) -> int`:
   * Parses `HH:MM:SS` into integer seconds since service-day midnight.
   * **Must support hours `>= 24`** (e.g., `"24:15:00"` -> `87300`, `"25:30:00"` -> `91800`).
   * Reject malformed strings with `ValueError`.
2. `gtfs_service_datetime(service_date: date, seconds_since_midnight: int, tz: ZoneInfo) -> datetime`:
   * Per GTFS spec, compute service-day noon (`datetime(y, m, d, 12, 0, tzinfo=tz)`), subtract `timedelta(hours=12)` to get GTFS "midnight" (handles Irish DST spring-forward/fall-back accurately!), then add `timedelta(seconds=seconds_since_midnight)`.
3. `get_active_service_ids(conn: sqlite3.Connection, service_date: date) -> set[str]`:
   * Formats `date_str = service_date.strftime("%Y%m%d")` and weekday column (`monday`..`sunday`).
   * Queries `calendar` for `start_date <= date_str AND end_date >= date_str AND <weekday> = 1`.
   * Applies `calendar_dates` for `date = date_str`: adds rows with `exception_type = 1`, removes rows with `exception_type = 2`.

#### 1B. Streaming SQLite Bulk Loader (`src/ingestion/static_loader.py`)
`stop_times.txt` is ~437 MB (several million rows). The loader **must stream CSVs in batches** and complete in under ~60 seconds without blowing memory:
1. `ingest_gtfs_directory(gtfs_dir: Path, target_db_path: Path) -> dict`:
   * Verify all 6 required CSV files exist in `gtfs_dir` before touching anything.
   * Build into a sibling staging file: `tmp_db = target_db_path.with_suffix(".db.tmp")` (remove stale `.tmp` if present).
   * During bulk load on `tmp_db`, set `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-64000;`.
   * Create schema:
     * `stops(stop_id TEXT PRIMARY KEY, stop_code TEXT, stop_name TEXT NOT NULL, stop_lat REAL NOT NULL, stop_lon REAL NOT NULL)`
     * `routes(route_id TEXT PRIMARY KEY, route_short_name TEXT NOT NULL, route_long_name TEXT)`
     * `trips(trip_id TEXT PRIMARY KEY, route_id TEXT NOT NULL, service_id TEXT NOT NULL, trip_headsign TEXT, direction_id INTEGER)`
     * `stop_times(trip_id TEXT NOT NULL, stop_id TEXT NOT NULL, stop_sequence INTEGER NOT NULL, arrival_secs INTEGER NOT NULL, departure_secs INTEGER NOT NULL, stop_headsign TEXT, pickup_type INTEGER NOT NULL DEFAULT 0)`
     * `calendar(service_id TEXT PRIMARY KEY, monday INTEGER NOT NULL, tuesday INTEGER NOT NULL, wednesday INTEGER NOT NULL, thursday INTEGER NOT NULL, friday INTEGER NOT NULL, saturday INTEGER NOT NULL, sunday INTEGER NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL)`
     * `calendar_dates(service_id TEXT NOT NULL, date TEXT NOT NULL, exception_type INTEGER NOT NULL, PRIMARY KEY (service_id, date))`
     * `stop_routes(stop_id TEXT NOT NULL, route_short_name TEXT NOT NULL, PRIMARY KEY (stop_id, route_short_name))`
     * `metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)`
   * Stream each CSV with `csv.DictReader` (using `encoding="utf-8-sig"`) and insert via `executemany` in batches of `50_000` rows inside transactions.
   * Populate `stop_routes` via a single SQL query after bulk load:
     ```sql
     INSERT OR IGNORE INTO stop_routes (stop_id, route_short_name)
     SELECT DISTINCT st.stop_id, r.route_short_name
     FROM stop_times st
     JOIN trips t ON t.trip_id = st.trip_id
     JOIN routes r ON r.route_id = t.route_id
     WHERE st.pickup_type != 1 AND r.route_short_name != '';
     ```
   * Create indexes **after** all rows are inserted:
     * `idx_stops_code ON stops(stop_code)`
     * `idx_stops_name ON stops(stop_name COLLATE NOCASE)`
     * `idx_routes_short_name ON routes(route_short_name)`
     * `idx_trips_service_id ON trips(service_id)`
     * `idx_trips_route_id ON trips(route_id)`
     * `idx_stop_times_stop_dep ON stop_times(stop_id, departure_secs)`
     * `idx_stop_times_trip_id ON stop_times(trip_id)`
     * `idx_calendar_dates_date ON calendar_dates(date)`
     * `idx_stop_routes_stop_id ON stop_routes(stop_id)`
   * Record `ingested_at` (UTC ISO-8601) and table row counts in `metadata`.
   * Switch `tmp_db` to `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`, run `ANALYZE;`, and close cleanly.
   * Atomically swap into place: `os.replace(tmp_db, target_db_path)` (remove stale `-wal`/`-shm` sidecars if replacing cleanly, or use SQLite backup/replace carefully so readers never see half-written state).
   * On any exception, delete `tmp_db` and leave any existing `target_db_path` untouched.
   * Return a summary dict `{elapsed_seconds, counts: {...}, ingested_at}`.
2. `backend/scripts/ingest_static.py`:
   * CLI flags: `--download` (calls Phase 1's `download_gtfs_static` + `extract_and_validate_gtfs` first), `--gtfs-dir` (defaults to `GTFS_STATIC_DIR`), `--db-path` (defaults to `settings.sqlite_db_path`).
   * Prints row counts and elapsed seconds; exits `0` on success.

---

### Task 2 (Step 4): On-Demand GTFS-Realtime Fetcher & Redis Cache (`src/ingestion/gtfsr_cache.py`)

Implement a clean `GtfsrCacheService` class using `redis.asyncio.Redis` and `httpx.AsyncClient`:

1. **Redis Key Schema:**
   * `gtfsr:meta` -> JSON `{"fetched_at": <unix_float>, "feed_timestamp": <int>, "entity_count": <int>}` with `EX = ttl_seconds` (default `60`).
   * `gtfsr:trip:{trip_id}` -> compact JSON:
     ```json
     {
       "sr": "SCHEDULED",
       "updates": [
         {"seq": 12, "stop_id": "8260B1034301", "arr_delay": 120, "dep_delay": 120, "arr_time": null, "dep_time": null, "sr": "SCHEDULED"}
       ]
     }
     ```
     stored with `EX = ttl_seconds + 30` (grace buffer so trip keys do not expire slightly ahead of `gtfsr:meta`).
     *(Keep `updates` sorted by `seq` ascending for fast delay propagation.)*
2. **Cache-Aside & Single-Flight Behaviour (`ensure_fresh() -> GtfsrCacheStatus`):**
   * Check `await redis.get("gtfsr:meta")`. If present -> cache hit! Return status (`realtime_available=True`, metadata from Redis) with **zero** HTTP calls to NTA.
   * On cache miss -> acquire an `asyncio.Lock` (per-service single-flight) and re-check `gtfsr:meta` inside the lock (double-checked locking so concurrent requests trigger only **one** NTA fetch).
   * Fetch NTA GTFS-R Protobuf via `httpx.AsyncClient` (`x-api-key: <NTA_API_KEY>`, timeout `GTFSR_REQUEST_TIMEOUT_SECONDS`).
   * Parse `FeedMessage` and extract every `entity` that has `trip_update`:
     * Capture `trip.schedule_relationship` (`SCHEDULED`, `CANCELED`, `ADDED`, `UNSCHEDULED`).
     * Capture each `stop_time_update` (`stop_sequence`, `stop_id`, `arrival.delay`/`time`, `departure.delay`/`time`, `schedule_relationship`).
   * Write to Redis using **pipelined batches** (e.g., 500 keys per `redis.pipeline(transaction=False)` batch), then set `gtfsr:meta` with `EX = ttl_seconds`.
   * **Graceful degradation:** If `NTA_API_KEY` is missing, HTTP fails/times out, status != 200, or Protobuf fails to parse, log a structured warning and return `GtfsrCacheStatus(realtime_available=False, ...)` without raising an unhandled exception. Never crash API endpoints when NTA is down.
3. **Batch Trip Lookup (`get_trip_updates(trip_ids: list[str]) -> tuple[dict[str, TripLiveUpdate], bool]`):**
   * Calls `ensure_fresh()`.
   * If `trip_ids` is empty, returns `({}, status.realtime_available)`.
   * Fetches all `gtfsr:trip:{trip_id}` entries in **one** Redis `MGET` call and deserialises non-`None` values.

---

### Task 3 (Step 5): Stop Service, Departure Joining Engine & REST API

#### 3A. Stop Resolution & Search (`src/services/stops.py`)
1. `resolve_stop(conn, stop_ref: str) -> StopInfo | None`:
   * Looks up exact `stop_id = stop_ref` first; if no match, looks up exact `stop_code = stop_ref`.
   * Fetches serving routes from `stop_routes` sorted naturally (numeric part first, then alphanumeric, e.g., `66`, `115`, `C3`, `C4`).
2. `search_stops(conn, query: str, limit: int = 20) -> list[StopInfo]`:
   * Matches exact `stop_code`, exact `stop_id`, or case-insensitive substring on `stop_name` (ranking exact `stop_code` / prefix matches first).
   * Attaches serving `routes` from `stop_routes` for each returned stop.

#### 3B. Departure Joining Engine (`src/services/departures.py`)
1. **Candidate Static Window (handles delayed buses & midnight rollover):**
   * Convert `now` to `ZoneInfo(settings.timezone)` (`Europe/Dublin`).
   * Query up to 3 relevant service dates:
     * **Yesterday** (always check when `now` is before `04:00` local time): `departure_secs` in `[now_secs + 86400 - 1800, now_secs + 86400 + window_secs]`.
     * **Today**: `departure_secs` in `[now_secs - 1800, now_secs + window_secs]` (30-minute lookback catches buses that were scheduled up to 30 min ago but are running late!).
     * **Tomorrow** (when `now_secs + window_secs >= 86400`): `departure_secs` in `[0, (now_secs + window_secs) - 86400]`.
   * For each active service date, query `stop_times JOIN trips JOIN routes` for `stop_id = ? AND pickup_type != 1 AND trips.service_id IN (...) AND departure_secs BETWEEN ? AND ?`.
   * **Pre-slice route filter:** If `routes: list[str] | None` is provided and non-empty, normalise (`strip().upper()`) and filter `UPPER(routes.route_short_name) IN (...)` **in SQL before limiting**.
2. **Joining with Live GTFS-R Data:**
   * Fetch live updates for all candidate `trip_id`s via `gtfsr_cache.get_trip_updates(trip_ids)`.
   * For each candidate static trip:
     * Headsign fallback: `stop_headsign or trip_headsign or route_long_name or ""`.
     * Scheduled timestamp: `gtfs_service_datetime(service_date, departure_secs, tz)`.
     * If live update exists for `trip_id`:
       * If `trip.sr == "CANCELED"`: set `is_cancelled = True`, `is_live = True`, `predicted_time = scheduled_time`, `delay_seconds = 0`.
       * Else locate the applicable `stop_time_update`:
         1. Exact match on `stop_sequence == candidate.stop_sequence` (or `stop_id == candidate.stop_id` if sequence absent).
         2. Otherwise, **GTFS-R delay propagation**: pick the update with the largest `seq < candidate.stop_sequence` that carries a delay.
       * If matched update has `sr == "SKIPPED"`: set `is_cancelled = True`, `is_live = True`.
       * Else apply prediction:
         * If explicit `dep_time` (or `arr_time`) UNIX timestamp is present on exact stop match -> `predicted_time = datetime.fromtimestamp(ts, tz)`, `delay_seconds = int((predicted_time - scheduled_time).total_seconds())`, `is_live = True`.
         * Else if `dep_delay` (or `arr_delay`) is present -> `delay_seconds = delay`, `predicted_time = scheduled_time + timedelta(seconds=delay)`, `is_live = True`.
         * Else -> fall back to static (`is_live = False`).
     * Else (no live update) -> `predicted_time = scheduled_time`, `delay_seconds = 0`, `is_live = False`, `is_cancelled = False`.
   * **Past-departure filter:** Exclude entries where `predicted_time < now - timedelta(seconds=60)` (already departed more than 1 minute ago).
   * Compute `minutes_until = max(0, int((predicted_time - now).total_seconds() // 60))` (or rounded to nearest minute, clamped `>= 0`).
   * Sort by `(predicted_time, route, trip_id)` and slice to `limit`.

#### 3C. REST API (`src/main.py`, `src/api/auth.py`, `src/api/health.py`, `src/api/stops.py`, `src/models/schemas.py`)
* **Auth (`src/api/auth.py`):** FastAPI dependency `verify_api_key`. If `settings.app_api_key` is non-empty, require header `X-API-Key` matching via `hmac.compare_digest`, else raise `HTTPException(401, detail="Invalid or missing API key")`.
* **Endpoints:**
  1. `GET /health` *(no auth)* -> `200 OK`:
     ```json
     {
       "status": "ok",
       "sqlite_ok": true,
       "redis_ok": true,
       "gtfs_ingested_at": "2026-11-20T15:40:00+00:00",
       "gtfsr_last_fetched_at": 1795188630,
       "gtfsr_entity_count": 3389
     }
     ```
     *(Return `status: "degraded"` if SQLite is missing/unhealthy or Redis is unreachable, without crashing.)*
  2. `GET /stops/search?q=<str>&limit=20` *(auth if enabled)*:
     * `q` required (`min_length=1`), `limit` clamped `1..50`.
     * Returns `{"stops": [StopInfo, ...]}`.
  3. `GET /stops/{stop_ref}` *(auth if enabled)*:
     * Resolves by `stop_id` or `stop_code`.
     * Returns `StopInfo`: `{stop_id, stop_code, stop_name, lat, lon, routes}`.
     * Returns `404` JSON if stop does not exist.
  4. `GET /stops/{stop_ref}/departures?routes=C3,C4&limit=10&window_minutes=90` *(auth if enabled)*:
     * `routes`: optional comma-separated list (empty entries ignored, case-insensitive).
     * `limit`: `1..30` (default `10`).
     * `window_minutes`: `5..240` (default `90`).
     * Returns `DeparturesResponse`:
       ```json
       {
         "stop": {
           "stop_id": "8260B1034301",
           "stop_code": "103431",
           "stop_name": "Maynooth University",
           "lat": 53.382175,
           "lon": -6.598822
         },
         "generated_at": "2026-11-20T15:45:00+00:00",
         "realtime_available": true,
         "departures": [
           {
             "trip_id": "6066_67149",
             "route": "C3",
             "headsign": "Ringsend",
             "scheduled_time": "2026-11-20T15:50:00+00:00",
             "predicted_time": "2026-11-20T15:53:00+00:00",
             "minutes_until": 8,
             "delay_seconds": 180,
             "is_live": true,
             "is_cancelled": false
           }
         ]
       }
       ```
     * Returns `404` if stop does not exist; `503` with clear message if `gtfs.db` has not been ingested yet.

---

### Task 4 (Step 6 — Local Containerisation & Daily GTFS Refresh; Hosting Deferred)

1. **In-App Daily GTFS Static Refresh (`src/ingestion/scheduler.py`):**
   * Async background loop started in FastAPI `lifespan` when `settings.static_refresh_enabled` is `True` (and cleanly cancelled on shutdown).
   * Sleeps until the next `STATIC_REFRESH_HOUR_UTC` (default `04:00 UTC`), runs download + extract + `ingest_gtfs_directory` inside `asyncio.to_thread(...)` so the FastAPI event loop stays responsive during ingestion, logs timing/counts, and catches/logs any error without killing the loop.
2. **Containerisation (`backend/Dockerfile` & `backend/compose.yaml`):**
   * Update `backend/Dockerfile`:
     * Base `python:3.12-slim`, install package from `pyproject.toml`, copy `src/` and `scripts/`, expose `8000`.
     * Add `HEALTHCHECK` using Python stdlib `urllib.request.urlopen("http://localhost:8000/health")` (no `curl` needed in slim image).
     * `CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]`.
   * Update `backend/compose.yaml`:
     * Keep existing `redis` service (`redis:7-alpine`, healthcheck).
     * Add `api` service: builds `.`, `depends_on: redis (condition: service_healthy)`, `env_file: .env`, overrides `REDIS_URL=redis://redis:6379/0` and `STATIC_REFRESH_ENABLED=true`, mounts `./data:/app/data`, exposes `8000:8000`.
     * Ensure `compose.yaml` works cleanly with both `podman compose` and `docker compose`.
3. **Step 6b (Cloud HTTPS Deployment — Deferred):**
   * Do **not** commit cloud-specific manifests (`fly.toml`, `render.yaml`, Caddyfile) yet; keep all config env-driven (`PORT`, `REDIS_URL`, `SQLITE_DB_PATH`, `NTA_API_KEY`, `APP_API_KEY`) so any host can be plugged in later.

---

## 4. Testware to Implement

All tests live under `backend/tests/` and follow the existing markers (`unit`, `integration`, `live`).

### A. Unit Tests (`backend/tests/unit/`, `@pytest.mark.unit` — 100% offline, fast)
1. **`test_gtfs_time_and_calendar.py`**:
   * `parse_gtfs_time_to_seconds`: normal times (`"08:30:00"`), midnight (`"00:00:00"`), post-midnight (`"24:15:00"`, `"26:05:10"`), malformed inputs raise `ValueError`.
   * `gtfs_service_datetime`: verifies normal days AND Irish DST spring-forward/fall-back dates (`Europe/Dublin`).
   * `get_active_service_ids`: verifies weekday matching, `start_date`/`end_date` boundaries, `calendar_dates` addition (`exception_type=1`) and removal (`exception_type=2`).
2. **`test_static_loader.py`**:
   * Builds a rich mini-GTFS fixture directory in `tmp_path` (multiple stops, routes, trips, `pickup_type=0` and `pickup_type=1`, post-midnight times, calendar + exceptions).
   * Verifies `ingest_gtfs_directory()` creates all tables, populates `stop_routes` (excluding `pickup_type=1`), creates indexes, enables WAL mode, records `metadata.ingested_at`, and leaves no `.db.tmp` file behind.
   * Verifies atomic swap: re-ingesting with modified fixture data replaces the DB cleanly.
   * Verifies failure safety: ingesting a broken directory (missing `routes.txt`) raises an error and preserves the previously existing `gtfs.db` intact.
3. **`test_gtfsr_cache.py`** (using an in-memory fake async Redis + `respx` HTTP mocks):
   * **Cache miss -> populate -> cache hit:** First call hits mocked NTA endpoint once, populates `gtfsr:meta` and `gtfsr:trip:*` with expected TTLs; second call within TTL makes **0** HTTP calls.
   * **Single-flight deduplication:** 5 concurrent `asyncio.gather(cache.ensure_fresh(), ...)` calls on a cold cache result in exactly **1** outbound HTTP call to NTA.
   * **Protobuf extraction:** Verifies `CANCELED` trips, `SKIPPED` stops, `departure.delay`, `arrival.delay`, and explicit `departure.time` are parsed and stored accurately.
   * **Graceful degradation:** HTTP `500`, HTTP `401`, network timeout, or corrupt protobuf returns `realtime_available=False` without crashing.
4. **`test_departure_engine.py`**:
   * Live delay applied (bus delayed +5 min, early -2 min).
   * **Delayed bus whose scheduled time already passed:** e.g., scheduled 10 min ago, delayed +15 min -> still appears as arriving in 5 min!
   * **GTFS-R delay propagation:** update exists at `stop_sequence=3` (`+240s`), target stop is `stop_sequence=7` with no explicit update -> inherits `+240s` delay.
   * **Cancelled trip & skipped stop:** returned with `is_cancelled=True`, `is_live=True`.
   * **Drop-off-only terminus (`pickup_type=1`):** excluded from departures.
   * **Midnight rollover:**
     * Querying at `00:10` includes yesterday's active trip at `24:25:00` (`15 min`).
     * Querying at `23:30` includes tomorrow's active trip at `00:15:00` (`45 min`).
   * **Route filtering BEFORE slicing:** fixture has 6 departures of route `X` (sooner) followed by 3 departures of route `C3` (later); querying `routes=["C3"], limit=3` returns all 3 `C3` departures (not 0!).
   * **Unfiltered query:** returns all routes sorted chronologically.
   * **Already-departed filtering:** buses that departed >60s ago are excluded.
5. **`test_api.py`** (FastAPI `httpx.AsyncClient` with `ASGITransport` over fixture SQLite + mocked `GtfsrCacheService`):
   * `GET /health` returns `200` with `sqlite_ok`, `redis_ok`, `gtfs_ingested_at`.
   * `GET /stops/{stop_id}` AND `GET /stops/{stop_code}` both return `200` with coordinates and sorted `routes`.
   * `GET /stops/NONEXISTENT` returns `404`.
   * `GET /stops/search?q=Maynooth` and `?q=103431` return expected stops + routes.
   * `GET /stops/{stop_ref}/departures` (both unfiltered and `?routes=c3,C4` case-insensitive) returns valid minimal JSON schema; invalid `limit=0` or `limit=100` returns `422`.
   * **Optional API key auth:**
     * When `APP_API_KEY=""`: `/stops/*` returns `200` without header.
     * When `APP_API_KEY="secret"`: `/stops/*` returns `401` without header or with wrong header, `200` with `X-API-Key: secret`, and `/health` still returns `200` without header.

### B. Local Infrastructure Integration Tests (`backend/tests/integration/`, `@pytest.mark.integration`)
1. **`test_redis_gtfsr_cache_integration.py`**:
   * Uses the real running local Redis container + `respx` mocked NTA Protobuf response (no live internet call).
   * Verifies pipelined writes, real `MGET` retrieval, positive Redis TTLs on `gtfsr:meta` and `gtfsr:trip:*`, and clean key expiry/cleanup (use isolated test key prefix or clean up test keys).
2. **`test_api_integration.py`**:
   * Runs the full FastAPI app wired to a fixture SQLite DB + real local Redis + mocked NTA feed.
   * Verifies cold request populates Redis (`realtime_available=True`), warm request hits Redis without calling NTA again, and warm response completes in **<100 ms**.

### C. Live NTA + Real Dataset Verification Tests (`backend/tests/integration/test_live_phase2.py`, `@pytest.mark.live`)
* Guarded by skipping cleanly if `NTA_API_KEY` is invalid or `backend/data/gtfs.db` has not been ingested yet.
* Queries the real FastAPI app (backed by ingested `backend/data/gtfs.db` + real Redis + live NTA GTFS-R v2):
  1. `GET /stops/{TEST_STOP_ID}` returns `200` with valid coordinates and non-empty `routes`.
  2. Looking up the same stop by its `stop_code` returns the identical `stop_id`.
  3. `GET /stops/{TEST_STOP_ID}/departures` returns `200`, `realtime_available=True`, valid departure schema; warm second request completes in **<100 ms**.
  4. Filtered request `?routes=<first_route_at_stop>` returns only departures matching that route.

---

## 5. Agent Execution & Verification Checklist

Execute these steps in order and report all results (including ingestion timing, warm response latency, and sample live departures for `TEST_STOP_ID`):

1. **Lint & static check:** `ruff check src scripts tests` from `backend/` — 0 errors.
2. **Offline unit tests:** `pytest -m unit` from `backend/` — all existing Phase 1 + new Phase 2 unit tests pass.
3. **Ensure Redis is running:** `podman compose up -d redis --wait` (or `docker compose up -d redis --wait`).
4. **Run local integration tests:** `pytest -m integration` — all integration tests pass.
5. **Run real GTFS Static ingestion:** `python scripts/ingest_static.py` against `backend/data/gtfs_static/` — confirm completion time, row counts, and `backend/data/gtfs.db` size.
6. **Run live Phase 2 tests:** `pytest -m live` — confirm real `TEST_STOP_ID` (`8260B1034301`) metadata, filtered/unfiltered departures, and <100 ms warm latency pass.
7. **Containerised smoke test (Step 6 local verification):**
   * Build and start the full stack: `podman compose up -d --build --wait` (or `docker compose up -d --build --wait`).
   * Hit `http://localhost:8000/health`, `http://localhost:8000/stops/8260B1034301`, `http://localhost:8000/stops/103431`, and `http://localhost:8000/stops/8260B1034301/departures` and confirm valid responses.
8. **Verify Git cleanliness:** Run `git status` and confirm `gtfs.db`, `gtfs.db-wal`, `gtfs.db-shm`, `.env`, and `data/gtfs_static/` remain ignored.
