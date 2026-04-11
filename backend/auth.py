"""
auth.py
Simple API key authentication and rate limiting for Folio.
"""

import os
import time
import hmac
import secrets
from collections import defaultdict
from fastapi import Request, HTTPException, Security
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# API KEY AUTH
# ══════════════════════════════════════════════════════════════════════════════

# Set this in .env: Folio_API_KEY=your-secret-key-here
# If not set, a session key is auto-generated (secure by default)
_env_key = os.getenv("Folio_API_KEY", "").strip()
if _env_key:
    API_KEY = _env_key
    _KEY_SOURCE = "env"
else:
    API_KEY = secrets.token_urlsafe(32)
    _KEY_SOURCE = "auto"
    # Print once at startup so the user can configure their frontend
    print(f"\n{'='*60}")
    print(f"  Folio_API_KEY not set — auto-generated session key:")
    print(f"  {API_KEY}")
    print(f"  Set Folio_API_KEY in .env to persist across restarts.")
    print(f"{'='*60}\n")

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str | None = Security(_api_key_header)):
    """
    FastAPI dependency that validates the API key.
    Auth is ALWAYS enforced. If no key is configured in .env,
    a session key is auto-generated at startup.
    """
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Include 'X-API-Key' header.",
        )

    # [FIX A2] Timing-safe comparison prevents character-by-character guessing
    if not hmac.compare_digest(api_key, API_KEY):
        raise HTTPException(
            status_code=403,
            detail="Invalid API key.",
        )

    return api_key


# ══════════════════════════════════════════════════════════════════════════════
# RATE LIMITING
# ══════════════════════════════════════════════════════════════════════════════

# Rate limit configuration (per-client IP)
# Format: {route_prefix: (max_requests, window_seconds)}
RATE_LIMITS = {
    "/api/copilot": (20, 60),     # 20 requests per minute
    "/api/sync": (5, 300),        # 5 syncs per 5 minutes
    "default": (120, 60),         # 120 requests per minute for everything else
}

# Storage: {client_key: [timestamp, timestamp, ...]}
_request_log: dict[str, list[float]] = defaultdict(list)
_last_global_cleanup: float = 0.0
_MAX_LOG_KEYS = 500  # Max unique client keys before forced global cleanup


def _get_client_key(request: Request, prefix: str) -> str:
    """Build a rate limit key from client IP + route prefix."""
    client_ip = request.client.host if request.client else "unknown"
    return f"{client_ip}:{prefix}"


def _cleanup_old_entries(entries: list[float], window: float) -> list[float]:
    """Remove entries older than the window."""
    cutoff = time.time() - window
    return [t for t in entries if t > cutoff]


def _global_cleanup():
    """Periodically purge stale keys to prevent unbounded memory growth."""
    global _last_global_cleanup
    now = time.time()
    # Run at most once per 5 minutes
    if now - _last_global_cleanup < 300:
        return
    _last_global_cleanup = now
    max_window = max(w for _, w in RATE_LIMITS.values())
    cutoff = now - max_window
    stale_keys = [k for k, v in _request_log.items() if not v or v[-1] < cutoff]
    for k in stale_keys:
        del _request_log[k]


async def rate_limit_middleware(request: Request, call_next):
    """
    FastAPI middleware for rate limiting.
    Checks request path against RATE_LIMITS config.
    """
    path = request.url.path

    # Skip rate limiting for health checks (used by Docker/load balancers)
    if path == "/health":
        return await call_next(request)

    # [FIX A3] Periodic memory cleanup to prevent unbounded growth
    if len(_request_log) > _MAX_LOG_KEYS:
        _global_cleanup()

    # Find matching rate limit config
    limit_config = RATE_LIMITS.get("default")
    for prefix, config in RATE_LIMITS.items():
        if prefix != "default" and path.startswith(prefix):
            limit_config = config
            break

    if limit_config:
        max_requests, window_seconds = limit_config
        prefix = next(
            (p for p in RATE_LIMITS if p != "default" and path.startswith(p)),
            "default",
        )
        client_key = _get_client_key(request, prefix)

        # Clean up old entries
        _request_log[client_key] = _cleanup_old_entries(
            _request_log[client_key], window_seconds
        )

        if len(_request_log[client_key]) >= max_requests:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Max {max_requests} requests per {window_seconds}s.",
            )

        _request_log[client_key].append(time.time())

    response = await call_next(request)
    return response