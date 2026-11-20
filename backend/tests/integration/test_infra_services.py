import uuid

import pytest

from src.cache import check_redis_health, create_redis_client
from src.config import get_settings
from src.db import check_sqlite_health, get_sqlite_connection

pytestmark = pytest.mark.integration


async def test_redis_is_healthy():
    assert await check_redis_health(get_settings().redis_url) is True


async def test_redis_set_get_with_ttl():
    client = create_redis_client(get_settings().redis_url)
    key = f"tfi-test:{uuid.uuid4()}"
    try:
        await client.set(key, "hello", ex=5)
        assert await client.get(key) == "hello"
        assert 0 < await client.ttl(key) <= 5
    finally:
        await client.delete(key)
        await client.aclose()


def test_sqlite_is_healthy_and_usable():
    db_path = get_settings().sqlite_db_path
    assert check_sqlite_health(db_path) is True
    conn = get_sqlite_connection(db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS tfi_test_tmp (id INTEGER PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO tfi_test_tmp (v) VALUES ('x')")
        assert conn.execute("SELECT v FROM tfi_test_tmp").fetchone() == ("x",)
    finally:
        conn.execute("DROP TABLE IF EXISTS tfi_test_tmp")
        conn.commit()
        conn.close()
