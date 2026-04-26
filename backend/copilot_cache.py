"""Small in-process caches for Copilot hot paths.

The cache is intentionally coarse and short-lived: Copilot answers should be
fresh, but repeated turns often rebuild the same orientation block and fetch the
same current-month summaries within seconds.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Callable


DEFAULT_TTL_SECONDS = max(1, int(os.getenv("COPILOT_CACHE_TTL_SECONDS", "60")))

HOT_TOOL_NAMES = {
    "get_top_merchants",
    "get_top_categories",
    "get_recurring_summary",
    "get_net_worth_trend",
}

_LOCK = threading.Lock()
_CACHE: dict[tuple[str, str], tuple[float, Any]] = {}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=True)


def make_key(*parts: Any) -> str:
    return _stable_json(parts)


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


def get_hot_tool_result(name: str, args: dict, profile: str | None, factory: Callable[[], Any]) -> Any:
    if name not in HOT_TOOL_NAMES:
        return factory()
    return get_or_set("tool", make_key(name, args or {}, profile), factory)


def invalidate_all() -> None:
    with _LOCK:
        _CACHE.clear()


def stats() -> dict[str, Any]:
    with _LOCK:
        return {"entries": len(_CACHE), "ttl_seconds": DEFAULT_TTL_SECONDS}
