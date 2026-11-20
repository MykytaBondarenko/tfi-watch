# Phase 1 Agent Implementation Specification: Data Access & Environment Setup

This specification defines everything a coding agent needs to implement and verify **Phase 1** (Steps 1–2) of `tfi_live_wear_os_implementation_plan.md`, aligned with `tfi_live_wear_os_project_blueprint.md`.

---

## 0. Context & Stack Constraints

* **Repository root:** `tfi-watch/` (existing Git repo).
* **Locked stack for Phase 1:**
  * **Backend runtime:** Python 3.12+ (`pyproject.toml` with `uv` or `pip`/`venv`)
  * **Core libraries:** `httpx`, `gtfs-realtime-bindings`, `protobuf`, `redis` (`redis.asyncio`), `pydantic-settings`, `fastapi`, `uvicorn`
  * **Test & lint tools:** `pytest`, `pytest-asyncio`, `respx`, `ruff`
  * **Static GTFS DB:** Embedded **SQLite** (local file under `backend/data/`, WAL mode)
  * **Live cache:** **Redis 7** running in **Docker** via `backend/compose.yaml`
  * **Wear OS app (`wear-app/`):** Placeholder directory only in Phase 1 (Android Studio scaffolding happens in Phase 3, Step 7).
  * **Hosting:** Deferred until Phase 2 (Step 6).
* **Human-in-the-loop boundary:** A coding agent cannot register an account on the NTA Developer Portal (`developer.nationaltransport.ie`). Implement all configuration templates, fetch/inspection scripts, and live smoke tests so they work immediately once the human pastes their `NTA_API_KEY` into `backend/.env`, and ensure unit and local infrastructure tests pass without requiring a live API key.

---

## 1. Target Repository Structure

Create and populate the following structure without modifying existing planning documents unless explicitly requested:

```text
tfi-watch/
├── .gitignore
├── backend/
│   ├── pyproject.toml
│   ├── .env.example
│   ├── compose.yaml
│   ├── Dockerfile
│   ├── data/
│   │   └── .gitkeep
│   ├── scripts/
│   │   ├── check_gtfsr.py
│   │   ├── fetch_static_gtfs.py
│   │   └── check_infra.py
│   ├── src/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── cache.py
│   │   ├── ingestion/
│   │   │   └── __init__.py
│   │   ├── api/
│   │   │   └── __init__.py
│   │   └── models/
│   │       └── __init__.py
│   └── tests/
│       ├── conftest.py
│       ├── unit/
│       │   ├── test_config.py
│       │   ├── test_gitignore.py
│       │   ├── test_db.py
│       │   ├── test_gtfsr_probe.py
│       │   └── test_static_inspector.py
│       └── integration/
│           ├── test_infra_services.py
│           └── test_live_nta.py
└── wear-app/
    └── README.md
```

---

## 2. Functionality to Implement

### Task 1: Repository Hygiene & `.gitignore`
1. Create/update `.gitignore` at the repository root to exclude:
   * Environment secrets: `.env`, `**/.env`, `.env.*` (with `!**/.env.example` exception)
   * Python artifacts: `.venv/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`, `.ruff_cache/`
   * Downloaded GTFS & local SQLite files: `backend/data/*.zip`, `backend/data/gtfs_static/`, `backend/data/*.db`, `backend/data/*.db-wal`, `backend/data/*.db-shm`
   * Future Android/Wear OS artifacts: `wear-app/.gradle/`, `wear-app/build/`, `wear-app/local.properties`
   * OS clutter: `.DS_Store`
2. Add `backend/data/.gitkeep` and a brief `wear-app/README.md` noting that the Wear OS Compose app will be scaffolded in Phase 3 (Step 7).

### Task 2: Python Project & Configuration (`pyproject.toml`, `.env.example`, `src/config.py`)
1. Create `backend/pyproject.toml`:
   * Target Python `>=3.12`.
   * Runtime dependencies: `fastapi`, `uvicorn[standard]`, `httpx`, `gtfs-realtime-bindings`, `redis`, `pydantic-settings`.
   * Dev/test dependencies: `pytest`, `pytest-asyncio`, `respx`, `ruff`.
   * Configure `[tool.pytest.ini_options]` with `asyncio_mode = "auto"`, `pythonpath = ["."]`, and registered markers:
     * `unit`: fast, offline tests (no Docker, no network)
     * `integration`: requires local Docker Compose services (Redis + SQLite)
     * `live`: requires a real `NTA_API_KEY` and internet access
2. Create `backend/.env.example` with documented keys:
   * `NTA_API_KEY=your_nta_primary_api_key_here`
   * `NTA_GTFSR_URL=https://api.nationaltransport.ie/gtfsr/v2/TripUpdates` *(include a comment to confirm the exact GTFS-R v2 URL in the NTA Developer Portal)*
   * `NTA_GTFS_STATIC_URL=https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip` *(include a comment to confirm the latest combined GTFS Static `.zip` URL on `transportforireland.ie` / `data.gov.ie`)*
   * `REDIS_URL=redis://localhost:6379/0`
   * `SQLITE_DB_PATH=data/gtfs.db`
   * `TEST_STOP_ID=` *(populated after inspecting `stops.txt`)*
3. Implement `backend/src/config.py` using `pydantic-settings` (`BaseSettings`):
   * Loads from `backend/.env` (if present) and process environment variables.
   * Exposes a `Settings` class and cached `get_settings()` helper, plus a `has_valid_nta_api_key()` helper that returns `False` when `NTA_API_KEY` is empty or still set to the placeholder value.

### Task 3: Local Infrastructure (`compose.yaml`, `Dockerfile`, `src/db.py`, `src/cache.py`, `scripts/check_infra.py`)
1. Create `backend/compose.yaml`:
   * Define a `redis` service using `redis:7-alpine`.
   * Expose port `6379:6379`.
   * Configure Redis as an ephemeral cache (`redis-server --save "" --appendonly no --maxmemory 128mb --maxmemory-policy allkeys-lru`).
   * Add a Docker `healthcheck` running `["CMD", "redis-cli", "ping"]` (interval 5s, timeout 3s, retries 5).
2. Create a minimal `backend/Dockerfile` (`python:3.12-slim`) installing dependencies from `pyproject.toml` so container builds are wired up and ready for Phase 2.
3. Implement `backend/src/db.py`:
   * Function `get_sqlite_connection(db_path: Path) -> sqlite3.Connection`: ensures parent directory exists, opens SQLite connection, executes `PRAGMA journal_mode=WAL;` and `PRAGMA foreign_keys=ON;`.
   * Function `check_sqlite_health(db_path: Path) -> bool`: runs `SELECT 1` and confirms WAL mode is active.
4. Implement `backend/src/cache.py`:
   * Function `create_redis_client(redis_url: str) -> redis.asyncio.Redis`.
   * Async function `check_redis_health(redis_url: str) -> bool`: connects, executes `PING`, closes client cleanly, returns `True` on `PONG`.
5. Implement `backend/scripts/check_infra.py`:
   * CLI entrypoint that checks both Redis (`check_redis_health`) and SQLite (`check_sqlite_health`), prints clear pass/fail output, and exits with code `0` only if both succeed.

### Task 4: NTA GTFS-R Probe & GTFS Static Inspector (`scripts/check_gtfsr.py`, `scripts/fetch_static_gtfs.py`)
Implement clean, importable functions inside each script (so unit tests can test the logic directly without shelling out) plus a CLI `main()`:
1. `backend/scripts/check_gtfsr.py`:
   * `fetch_and_parse_gtfsr(url: str, api_key: str, client: httpx.Client | None = None) -> dict`:
     * Sends `GET` with header `{"x-api-key": api_key}` and a reasonable timeout (15s).
     * Raises a clear error on non-200 status (specifically calling out 401/403 API key issues) or empty response body.
     * Parses raw bytes into `google.transit.gtfs_realtime_pb2.FeedMessage()`.
     * Returns metadata summary: `gtfs_realtime_version`, `timestamp`, `entity_count`, `sample_trip_ids` (up to 5).
   * CLI prints the summary and exits `0` on success, `1` on failure.
2. `backend/scripts/fetch_static_gtfs.py`:
   * `download_gtfs_static(url: str, dest_zip: Path, client: httpx.Client | None = None) -> Path`: streams the `.zip` to disk (do not buffer the entire archive in memory).
   * `extract_and_validate_gtfs(zip_path: Path, extract_dir: Path) -> list[str]`: extracts the archive and verifies the required core files exist (`stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt`).
   * `search_stops(stops_csv: Path, query: str | None = None, stop_code: str | None = None, limit: int = 20) -> list[dict]`: streams `stops.txt` via `csv.DictReader` and matches case-insensitively on `stop_name`, `stop_id`, or exact `stop_code`, returning `{stop_id, stop_code, stop_name, stop_lat, stop_lon}`.
   * CLI flags: `--download` (downloads & validates zip), `--search <text>`, `--stop-code <code>` so the user can quickly locate a familiar local stop (e.g., Maynooth or Dublin) and save its `stop_id` as `TEST_STOP_ID`.

---

## 3. Testware to Implement

All tests live under `backend/tests/` and run with `pytest`.

### A. Unit Tests (`backend/tests/unit/`, marked `@pytest.mark.unit` — must run completely offline)
1. **`test_config.py`**:
   * Verifies defaults load cleanly when optional env vars are omitted.
   * Verifies env var overrides (`NTA_API_KEY`, `REDIS_URL`, `SQLITE_DB_PATH`).
   * Verifies `has_valid_nta_api_key()` returns `False` for empty string and `.env.example` placeholder, and `True` for a real-looking key.
2. **`test_gitignore.py`**:
   * Runs `git check-ignore` against synthetic paths `backend/.env`, `backend/data/gtfs_static.zip`, `backend/data/gtfs_static/stops.txt`, `backend/data/gtfs.db`, and `backend/data/gtfs.db-wal` and asserts all return exit code `0` (ignored).
   * Asserts `backend/.env.example` is **not** ignored (exit code `1`).
3. **`test_db.py`**:
   * Uses pytest's `tmp_path` to initialise a SQLite database in a nested non-existent directory.
   * Asserts the file is created, `PRAGMA journal_mode` returns `"wal"`, and `check_sqlite_health()` returns `True`.
4. **`test_gtfsr_probe.py`** (using `respx` to mock `httpx`):
   * **Happy path:** Builds a real `FeedMessage` protobuf in memory with 2 `TripUpdate` entities, serialises via `SerializeToString()`, mocks HTTP `200`, and asserts `fetch_and_parse_gtfsr()` sends the `x-api-key` header and returns `entity_count == 2` and expected `sample_trip_ids`.
   * **Auth failure:** Mocks HTTP `401` / `403` and asserts an explicit authentication error is raised.
   * **Empty payload:** Mocks HTTP `200` with `b""` and asserts a validation error is raised.
   * **Corrupt payload:** Mocks HTTP `200` with invalid protobuf bytes and asserts a parse error is raised.
5. **`test_static_inspector.py`**:
   * Builds a small fixture `.zip` in `tmp_path` containing valid headers + sample rows for `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, and `calendar_dates.txt`.
   * Tests `extract_and_validate_gtfs()` succeeds when all required files are present and raises a clear error if any required file (e.g., `stops.txt`) is missing.
   * Tests `search_stops()` for case-insensitive name match, exact `stop_code` match, non-matching query (returns `[]`), and `limit` enforcement.

### B. Local Infrastructure Integration Tests (`backend/tests/integration/test_infra_services.py`, marked `@pytest.mark.integration`)
1. Asserts `check_redis_health(settings.redis_url)` returns `True`.
2. Writes a temporary test key with a 5-second TTL (`set(..., ex=5)`), reads it back, verifies value & positive TTL, and deletes it in cleanup.
3. Asserts `check_sqlite_health(settings.sqlite_db_path)` returns `True` and can create/read/drop a temporary test table.

### C. Live NTA Smoke Tests (`backend/tests/integration/test_live_nta.py`, marked `@pytest.mark.live`)
1. Guarded by `@pytest.mark.skipif(not get_settings().has_valid_nta_api_key(), reason="Set NTA_API_KEY in backend/.env to run live NTA checks")`.
2. Calls `fetch_and_parse_gtfsr()` against the live NTA endpoint and asserts `entity_count > 0` and a recent feed `timestamp`.
3. If `backend/data/gtfs_static/stops.txt` exists locally, verifies all 6 core GTFS files exist; if `TEST_STOP_ID` is set in `.env`, verifies that `stop_id` is found in `stops.txt` with valid numeric `stop_lat` and `stop_lon`.

---

## 4. Agent Execution & Verification Checklist

Execute these steps in order and report all results:

1. **Install environment:** Create a virtual environment in `backend/.venv` and install the editable package + dev dependencies (`pip install -e ".[dev]"` or `uv sync`).
2. **Lint & static check:** Run `ruff check src scripts tests` from `backend/` — must pass with 0 errors.
3. **Run offline unit test suite:** Run `pytest -m unit` from `backend/` — all unit tests must pass.
4. **Start local infrastructure:** Run `docker compose up -d --wait` from `backend/` and verify the `redis` container reaches `healthy`.
5. **Run infrastructure verification & integration tests:**
   * Run `python scripts/check_infra.py` — must report both Redis and SQLite healthy (exit `0`).
   * Run `pytest -m integration` — all integration tests must pass.
6. **Verify `.gitignore` & cleanliness:** Run `git status` and confirm no `.env`, `.venv`, `.db`, or `.zip` files appear as untracked.
7. **Live NTA handoff (if `NTA_API_KEY` is already in `backend/.env`):**
   * Run `python scripts/check_gtfsr.py`
   * Run `python scripts/fetch_static_gtfs.py --download --search "Maynooth"` (or another familiar area) to record a `TEST_STOP_ID`
   * Run `pytest -m live`
   * *(If `NTA_API_KEY` is not yet configured, verify `pytest -m live` skips cleanly with the setup instruction and report the exact 3 commands for the user to run after adding their key to `backend/.env`.)*
