import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]


def _check_ignore(path: str) -> int:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), "check-ignore", "-q", "--", path],
        capture_output=True,
    ).returncode


@pytest.mark.parametrize(
    "path",
    [
        "backend/.env",
        "backend/data/gtfs_static.zip",
        "backend/data/gtfs_static/stops.txt",
        "backend/data/gtfs.db",
        "backend/data/gtfs.db-wal",
    ],
)
def test_secrets_and_downloads_are_ignored(path):
    assert _check_ignore(path) == 0


def test_env_example_is_not_ignored():
    assert _check_ignore("backend/.env.example") == 1
