"""Small in-process caches for Copilot hot paths.

The cache is intentionally coarse and short-lived: Copilot answers should be
fresh, but repeated turns often rebuild the same orientation block and fetch the
same current-month summaries within seconds.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from typing import Any, Callable


DEFAULT_TTL_SECONDS = max(1, int(os.getenv("COPILOT_CACHE_TTL_SECONDS", "60")))
ENTITY_INDEX_TTL_SECONDS = max(1, int(os.getenv("COPILOT_ENTITY_INDEX_CACHE_TTL_SECONDS", "300")))
RESOLVER_TTL_SECONDS = max(1, int(os.getenv("COPILOT_RESOLVER_CACHE_TTL_SECONDS", "120")))
PROMPT_FRAGMENT_TTL_SECONDS = max(1, int(os.getenv("COPILOT_PROMPT_CACHE_TTL_SECONDS", "300")))

HOT_TOOL_NAMES = {
    "get_category_breakdown",
    "get_category_spend",
    "get_dashboard_bundle",
    "get_merchant_spend",
    "get_month_summary",
    "get_monthly_spending_trend",
    "get_net_worth_delta",
    "get_top_merchants",
    "get_top_categories",
    "get_transactions",
    "get_transactions_for_merchant",
    "get_summary",
    "get_recurring_summary",
    "get_net_worth_trend",
}

_LOCK = threading.Lock()
_CACHE: dict[tuple[str, str], tuple[float, Any]] = {}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=True)


def make_key(*parts: Any) -> str:
    return _stable_json(parts)


def _row_value(row: Any, key: str, idx: int, default: Any = None) -> Any:
    try:
        return row[key]
    except Exception:
        try:
            return row[idx]
        except Exception:
            return default


def _db_identity(conn) -> str:
    try:
        rows = conn.execute("PRAGMA database_list").fetchall()
        for row in rows:
            name = _row_value(row, "name", 1, "")
            path = _row_value(row, "file", 2, "")
            if name == "main":
                if path:
                    try:
                        stat = os.stat(path)
                        return make_key("file", path, stat.st_size, int(stat.st_mtime))
                    except OSError:
                        return make_key("file", path)
                return make_key("memory", id(conn))
    except Exception:
        pass
    return make_key("conn", id(conn))


def db_fingerprint(conn, profile: str | None = None) -> str:
    """Return a compact data fingerprint for cache invalidation.

    Mutating API endpoints still clear the cache explicitly. This fingerprint
    also protects direct/test calls and background DB changes by varying cache
    keys when visible finance data changes.
    """
    profile_clause = ""
    params: list[Any] = []
    if profile and profile != "household":
        profile_clause = " WHERE profile_id = ?"
        params.append(profile)

    tx_count = 0
    tx_max = ""
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS count, MAX(COALESCE(updated_at, date, '')) AS max_updated
            FROM transactions_visible{profile_clause}
            """,
            params,
        ).fetchone()
        tx_count = int(_row_value(row, "count", 0, 0) or 0)
        tx_max = str(_row_value(row, "max_updated", 1, "") or "")
    except Exception:
        pass

    category_count = 0
    category_max = ""
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS count, MAX(COALESCE(updated_at, name, '')) AS max_updated FROM categories"
        ).fetchone()
        category_count = int(_row_value(row, "count", 0, 0) or 0)
        category_max = str(_row_value(row, "max_updated", 1, "") or "")
    except Exception:
        try:
            row = conn.execute("SELECT COUNT(*) AS count, MAX(name) AS max_name FROM categories").fetchone()
            category_count = int(_row_value(row, "count", 0, 0) or 0)
            category_max = str(_row_value(row, "max_name", 1, "") or "")
        except Exception:
            pass

    return make_key(_db_identity(conn), profile or "household", tx_count, tx_max, category_count, category_max)


def get_or_set(namespace: str, key: str, factory: Callable[[], Any], ttl: int | None = None) -> Any:
    ttl = DEFAULT_TTL_SECONDS if ttl is None else ttl
    cache_key = (namespace, key)
    now = time.time()
    with _LOCK:
        entry = _CACHE.get(cache_key)
        if entry and now - entry[0] <= ttl:
            return entry[1]

    value = factory()
    with _LOCK:
        _CACHE[cache_key] = (time.time(), value)
    return value


def get_hot_tool_result(
    name: str,
    args: dict,
    profile: str | None,
    factory: Callable[[], Any],
    *,
    fingerprint: str | None = None,
) -> Any:
    if name not in HOT_TOOL_NAMES:
        return factory()
    return get_or_set("tool", make_key(name, args or {}, profile, fingerprint or ""), factory)


def get_entity_index(
    entity_type: str,
    profile: str | None,
    fingerprint: str,
    factory: Callable[[], Any],
) -> Any:
    return copy.deepcopy(
        get_or_set(
            "entity_index",
            make_key(entity_type, profile or "household", fingerprint),
            factory,
            ttl=ENTITY_INDEX_TTL_SECONDS,
        )
    )


def get_resolver_result(key: str, factory: Callable[[], Any]) -> Any:
    return copy.deepcopy(get_or_set("resolver", key, factory, ttl=RESOLVER_TTL_SECONDS))


def get_prompt_fragment(fragment: str, key: str, factory: Callable[[], Any], *, fingerprint: str | None = None) -> Any:
    return get_or_set(
        "prompt_fragment",
        make_key(fragment, key, fingerprint or ""),
        factory,
        ttl=PROMPT_FRAGMENT_TTL_SECONDS,
    )


def invalidate_all() -> None:
    with _LOCK:
        _CACHE.clear()


def stats() -> dict[str, Any]:
    with _LOCK:
        namespaces: dict[str, int] = {}
        for namespace, _ in _CACHE:
            namespaces[namespace] = namespaces.get(namespace, 0) + 1
        return {
            "entries": len(_CACHE),
            "namespaces": namespaces,
            "ttl_seconds": DEFAULT_TTL_SECONDS,
            "entity_index_ttl_seconds": ENTITY_INDEX_TTL_SECONDS,
            "resolver_ttl_seconds": RESOLVER_TTL_SECONDS,
            "prompt_fragment_ttl_seconds": PROMPT_FRAGMENT_TTL_SECONDS,
        }
