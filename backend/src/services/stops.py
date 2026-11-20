"""Stop lookup (by stop_id or public stop_code), metadata and search."""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict

from src.models.schemas import StopInfo

_CHUNK = 500


def route_sort_key(route: str) -> tuple:
    """Natural sort: numeric routes first (66, 115), then alphanumeric (C3, C4, X9, X10)."""
    chunks = tuple(
        (0, int(part), "") if part.isdigit() else (1, 0, part.lower())
        for part in re.split(r"(\d+)", route)
        if part
    )
    return (0 if route[:1].isdigit() else 1, chunks)


def _routes_by_stop(conn: sqlite3.Connection, stop_ids: list[str]) -> dict[str, list[str]]:
    routes: dict[str, list[str]] = defaultdict(list)
    for start in range(0, len(stop_ids), _CHUNK):
        chunk = stop_ids[start : start + _CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT stop_id, route_short_name FROM stop_routes "  # noqa: S608
            f"WHERE stop_id IN ({placeholders})",
            chunk,
        )
        for stop_id, route in rows:
            routes[stop_id].append(route)
    for route_list in routes.values():
        route_list.sort(key=route_sort_key)
    return routes


def _build_stops(conn: sqlite3.Connection, rows: list[tuple]) -> list[StopInfo]:
    routes = _routes_by_stop(conn, [row[0] for row in rows])
    return [
        StopInfo(
            stop_id=stop_id,
            stop_code=stop_code,
            stop_name=stop_name,
            lat=lat,
            lon=lon,
            routes=routes.get(stop_id, []),
        )
        for stop_id, stop_code, stop_name, lat, lon in rows
    ]


def resolve_stop(conn: sqlite3.Connection, stop_ref: str) -> StopInfo | None:
    """Look up a stop by exact stop_id first, then by exact stop_code."""
    ref = stop_ref.strip()
    columns = "stop_id, stop_code, stop_name, stop_lat, stop_lon"
    row = conn.execute(f"SELECT {columns} FROM stops WHERE stop_id = ?", (ref,)).fetchone()  # noqa: S608
    if row is None:
        row = conn.execute(
            f"SELECT {columns} FROM stops WHERE stop_code = ? ORDER BY stop_id LIMIT 1",  # noqa: S608
            (ref,),
        ).fetchone()
    if row is None:
        return None
    return _build_stops(conn, [row])[0]


def search_stops(conn: sqlite3.Connection, query: str, limit: int = 20) -> list[StopInfo]:
    """Match exact stop_code/stop_id or a case-insensitive substring of stop_name."""
    text = query.strip()
    if not text:
        return []
    escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    rows = conn.execute(
        """
        SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_code = :q OR stop_id = :q OR stop_name LIKE :contains ESCAPE '\\'
        ORDER BY
            CASE
                WHEN stop_code = :q OR stop_id = :q THEN 0
                WHEN stop_name LIKE :prefix ESCAPE '\\' THEN 1
                ELSE 2
            END,
            stop_name COLLATE NOCASE,
            stop_id
        LIMIT :limit
        """,
        {"q": text, "contains": f"%{escaped}%", "prefix": f"{escaped}%", "limit": limit},
    ).fetchall()
    return _build_stops(conn, rows)
