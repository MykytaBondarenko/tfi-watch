import pytest

from src.config import API_KEY_PLACEHOLDER, BACKEND_DIR, Settings

pytestmark = pytest.mark.unit

ENV_KEYS = ("NTA_API_KEY", "NTA_GTFSR_URL", "NTA_GTFS_STATIC_URL", "REDIS_URL", "SQLITE_DB_PATH")


@pytest.fixture
def clean_env(monkeypatch):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_defaults_load_without_env(clean_env):
    settings = Settings(_env_file=None)
    assert settings.nta_api_key == ""
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.sqlite_db_path == BACKEND_DIR / "data" / "gtfs.db"
    assert settings.nta_gtfsr_url.startswith("https://")
    assert settings.nta_gtfs_static_url.endswith(".zip")


def test_env_overrides(clean_env, tmp_path):
    db_path = tmp_path / "custom.db"
    clean_env.setenv("NTA_API_KEY", "abc123realkey")
    clean_env.setenv("REDIS_URL", "redis://example:6380/1")
    clean_env.setenv("SQLITE_DB_PATH", str(db_path))
    settings = Settings(_env_file=None)
    assert settings.nta_api_key == "abc123realkey"
    assert settings.redis_url == "redis://example:6380/1"
    assert settings.sqlite_db_path == db_path


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("", False),
        ("   ", False),
        (API_KEY_PLACEHOLDER, False),
        ("0123456789abcdef0123456789abcdef", True),
    ],
)
def test_has_valid_nta_api_key(clean_env, key, expected):
    assert Settings(_env_file=None, nta_api_key=key).has_valid_nta_api_key() is expected


def test_env_example_placeholder_matches_constant():
    example = (BACKEND_DIR / ".env.example").read_text()
    assert f"NTA_API_KEY={API_KEY_PLACEHOLDER}" in example
