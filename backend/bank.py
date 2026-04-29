"""
bank.py
Teller API client with rate limiting, retry logic, pagination, and client caching.
Dynamically loads all Teller tokens from environment variables.
"""

import httpx
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

# ── Dynamic Token Loading ──

TELLER_TOKEN_PREFIX = os.getenv("TELLER_TOKEN_PREFIX", "_TOKEN")


def _load_tokens() -> list[str]:
    skip_vars = {
        "TELLER_TOKEN_PREFIX",
        "TOKEN_ENCRYPTION_KEY",
    }
    tokens = []
    suffix = TELLER_TOKEN_PREFIX

    for key, value in os.environ.items():
        if key in skip_vars:
            continue
        if key.endswith(suffix) and value and value.strip():
            tokens.append(value.strip())

    # Merge tokens from the persistent token store (Teller Connect enrollments)
    try:
        from token_store import load_all_tokens as _load_db_tokens
        db_tokens = _load_db_tokens()
        for profile_tokens in db_tokens.values():
            for tok in profile_tokens:
                if tok not in tokens:
                    tokens.append(tok)
    except Exception as e:
        logger.debug("Could not load tokens from token_store (first run?): %s", e)

    if not tokens:
        logger.warning("No Teller tokens found in environment or token store.")
        logger.warning("   Add tokens to .env with names ending in '%s'", suffix)
        logger.warning("   Example: BOFA%s=your_token_here", suffix)
        logger.warning("   Or enroll via Teller Connect in the dashboard.")

    return tokens


TOKENS = _load_tokens()


def _load_profiles() -> dict[str, list[str]]:
    skip_vars = {
        "TELLER_TOKEN_PREFIX",
        "TOKEN_ENCRYPTION_KEY",
    }
    suffix = TELLER_TOKEN_PREFIX
    profiles: dict[str, list[str]] = {}

    for key, value in os.environ.items():
        if key in skip_vars:
            continue
        if not (key.endswith(suffix) and value and value.strip()):
            continue
        prefix = key[: -len(suffix)]
        segments = prefix.split("_")
        if len(segments) >= 2:
            name = segments[0].lower()
            profiles.setdefault(name, []).append(value.strip())

    # Merge tokens from the persistent token store (Teller Connect enrollments)
    try:
        from token_store import load_all_tokens as _load_db_tokens
        db_tokens = _load_db_tokens()
        for profile_name, token_list in db_tokens.items():
            for tok in token_list:
                if tok not in profiles.get(profile_name, []):
                    profiles.setdefault(profile_name, []).append(tok)
    except Exception as e:
        logger.debug("Could not load profiles from token_store (first run?): %s", e)

    if not profiles and TOKENS:
        profiles["primary"] = list(TOKENS)

    return profiles


PROFILES: dict[str, list[str]] = _load_profiles()

def _resolve_cert_path(env_var: str) -> str | None:
    """Resolve certificate path — handles both absolute and relative paths."""
    path = os.getenv(env_var)
    if not path:
        return None
    # If absolute (e.g., /certs/teller-cert.pem in Docker), use as-is
    if os.path.isabs(path):
        return path
    # If relative, try relative to CWD first, then relative to this file
    if os.path.exists(path):
        return path
    alt = os.path.join(os.path.dirname(__file__), "..", path)
    if os.path.exists(alt):
        return os.path.abspath(alt)
    return path  # Return as-is, let validation catch it


CERT = (
    _resolve_cert_path("TELLER_CERT_PATH"),
    _resolve_cert_path("TELLER_KEY_PATH"),
)

TELLER_BASE = "https://api.teller.io"

# Rate limit: pause between API calls
RATE_LIMIT_DELAY = 1.0  # seconds

# Pagination safety limits (configurable via env)
TELLER_MAX_PAGES = int(os.getenv("TELLER_MAX_PAGES", "50"))
TELLER_MAX_TRANSACTIONS = int(os.getenv("TELLER_MAX_TRANSACTIONS", "5000"))
# Teller returns up to this many transactions per page
_TELLER_PAGE_SIZE = 100


# ══════════════════════════════════════════════════════════════════════════════
# CERTIFICATE VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_teller_config():
    """
    Validate Teller certificate configuration at startup.
    If Teller tokens exist but certificates are missing/unreadable, raises RuntimeError.
    If no Teller tokens exist, logs a warning and returns (allows SimpleFIN-only usage).
    Called from main.py startup event.
    """
    cert_path, key_path = CERT

    if not cert_path or not key_path:
        if not TOKENS:
            logger.warning(
                "Teller not configured (no certificates and no tokens). "
                "Teller sync will be unavailable. SimpleFIN or other providers can still be used."
            )
            return
        raise RuntimeError(
            "Teller certificate configuration incomplete. "
            "Set TELLER_CERT_PATH and TELLER_KEY_PATH environment variables."
        )

    cert_file = Path(cert_path)
    key_file = Path(key_path)

    if not cert_file.exists():
        if not TOKENS:
            logger.warning("Teller certificate file not found: %s — Teller sync will be unavailable.", cert_path)
            return
        raise RuntimeError(
            f"Teller certificate file not found: {cert_path}"
        )
    if not key_file.exists():
        if not TOKENS:
            logger.warning("Teller key file not found: %s — Teller sync will be unavailable.", key_path)
            return
        raise RuntimeError(
            f"Teller key file not found: {key_path}"
        )

    # Verify readable
    try:
        cert_file.read_bytes()
    except PermissionError:
        raise RuntimeError(
            f"Teller certificate file is not readable: {cert_path}"
        )
    try:
        key_file.read_bytes()
    except PermissionError:
        raise RuntimeError(
            f"Teller key file is not readable: {key_path}"
        )

    logger.info("Teller certificate validated: cert=%s, key=%s", cert_path, key_path)


# ══════════════════════════════════════════════════════════════════════════════
# CLIENT CACHING
# ══════════════════════════════════════════════════════════════════════════════

_client_cache: dict[str, httpx.Client] = {}


def _get_client(token: str) -> httpx.Client:
    """
    Get or create a cached httpx.Client for the given access token.
    Clients are reused across requests to avoid TLS handshake overhead.
    """
    if token not in _client_cache:
        _client_cache[token] = httpx.Client(
            base_url=TELLER_BASE,
            auth=(token, ""),
            cert=CERT,
            timeout=30.0,
        )
        logger.debug("Created new Teller client (total cached: %d)", len(_client_cache))
    return _client_cache[token]


def close_all_clients():
    """
    Close all cached httpx.Client instances.
    Called during FastAPI shutdown to clean up resources.
    """
    count = len(_client_cache)
    for token, client in list(_client_cache.items()):
        try:
            client.close()
        except Exception:
            logger.debug("Error closing Teller client", exc_info=True)
    _client_cache.clear()
    if count > 0:
        logger.info("Closed %d cached Teller client(s).", count)


def _request_with_retry(
    client: httpx.Client, method: str, url: str, max_retries: int = 3, params: dict | None = None,
):
    """Make request with retry on 429."""
    for attempt in range(max_retries):
        resp = client.request(method, url, params=params)

        if resp.status_code == 429:
            wait = min(2 ** attempt * 2, 30)
            logger.warning(
                "Rate limited on %s, waiting %ds (attempt %d/%d)...",
                url, wait, attempt + 1, max_retries,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    # Final attempt failed
    resp.raise_for_status()
    return resp.json()


def get_all_accounts() -> list[dict]:
    """Fetch accounts from all configured Teller tokens."""
    all_accounts = []
    for token in TOKENS:
        try:
            client = _get_client(token)
            accounts = _request_with_retry(client, "GET", "/accounts")
            for acc in accounts:
                acc["access_token"] = token
            all_accounts.extend(accounts)
            inst_names = set(
                acc.get("institution", {}).get("name", "unknown")
                for acc in accounts
            )
            logger.info("Loaded %d accounts from %s", len(accounts), ", ".join(inst_names))
        except Exception as e:
            logger.error("Failed to load accounts for a token: %s", e)
        time.sleep(RATE_LIMIT_DELAY)
    return all_accounts


def get_all_accounts_by_profile() -> list[dict]:
    """Fetch accounts from all configured tokens, tagged with profile name."""
    all_accounts = []
    for profile_name, tokens in PROFILES.items():
        for token in tokens:
            try:
                client = _get_client(token)
                accounts = _request_with_retry(client, "GET", "/accounts")
                for acc in accounts:
                    acc["access_token"] = token
                    acc["profile"] = profile_name
                all_accounts.extend(accounts)
                inst_names = set(
                    acc.get("institution", {}).get("name", "unknown")
                    for acc in accounts
                )
                logger.info(
                    "[%s] Loaded %d accounts from %s",
                    profile_name, len(accounts), ", ".join(inst_names),
                )
            except Exception as e:
                logger.error(
                    "Failed to load accounts for profile '%s': %s",
                    profile_name, e,
                )
            time.sleep(RATE_LIMIT_DELAY)
    return all_accounts


def get_transactions(account_id: str, token: str) -> list[dict]:
    """
    Fetch all transactions for an account using Teller's cursor-based pagination.

    Teller returns up to ~100 transactions per request. If a full page is
    returned, we use the last transaction's `id` as the `from_id` cursor
    to fetch the next page. Pagination continues until:
      - A page returns fewer than _TELLER_PAGE_SIZE results (last page), or
      - TELLER_MAX_PAGES is reached, or
      - TELLER_MAX_TRANSACTIONS total transactions are accumulated.
    """
    client = _get_client(token)
    all_transactions: list[dict] = []
    from_id: str | None = None

    for page in range(1, TELLER_MAX_PAGES + 1):
        params = {}
        if from_id:
            params["from_id"] = from_id

        page_results = _request_with_retry(
            client, "GET", f"/accounts/{account_id}/transactions", params=params or None,
        )

        if not page_results:
            break

        all_transactions.extend(page_results)

        logger.debug(
            "Teller page %d for account %s: %d transactions (total: %d)",
            page, account_id, len(page_results), len(all_transactions),
        )

        # Check if we've reached our safety limit
        if len(all_transactions) >= TELLER_MAX_TRANSACTIONS:
            logger.warning(
                "Reached max transaction limit (%d) for account %s after %d pages.",
                TELLER_MAX_TRANSACTIONS, account_id, page,
            )
            break

        # If this page returned fewer than the expected page size, it's the last page
        if len(page_results) < _TELLER_PAGE_SIZE:
            break

        # Use the last transaction's ID as cursor for the next page
        last_tx = page_results[-1]
        from_id = last_tx.get("id")
        if not from_id:
            logger.warning("Last transaction on page %d has no 'id' — stopping pagination.", page)
            break

        time.sleep(RATE_LIMIT_DELAY)

    if len(all_transactions) > 0:
        pages_fetched = min(
            (len(all_transactions) + _TELLER_PAGE_SIZE - 1) // _TELLER_PAGE_SIZE,
            TELLER_MAX_PAGES,
        )
        if pages_fetched > 1:
            logger.info(
                "Fetched %d transactions for account %s across %d page(s).",
                len(all_transactions), account_id, pages_fetched,
            )

    time.sleep(RATE_LIMIT_DELAY)
    return all_transactions


def get_balances(account_id: str, token: str) -> dict:
    client = _get_client(token)
    result = _request_with_retry(
        client, "GET", f"/accounts/{account_id}/balances"
    )
    time.sleep(RATE_LIMIT_DELAY)
    return result


def get_accounts_for_token(token: str) -> list[dict]:
    """Fetch accounts for a single Teller access token."""
    try:
        client = _get_client(token)
        accounts = _request_with_retry(client, "GET", "/accounts")
        time.sleep(RATE_LIMIT_DELAY)
        return accounts if isinstance(accounts, list) else []
    except Exception as e:
        logger.error("Failed to fetch accounts for token: %s", e)
        return []


def get_identity(token: str, account_id: str) -> dict:
    """
    Fetch the beneficial owner identity from Teller's Identity endpoint.
    
    Teller's identity API is a top-level endpoint (GET /identity) that returns
    all accounts with owner information. We find the matching account and
    extract the first owner's name.
    
    Returns {"first_name": ..., "last_name": ..., "full_name": ...}.
    Falls back gracefully if the endpoint is unavailable or the product
    wasn't requested during enrollment.
    """
    try:
        client = _get_client(token)
        # Teller's identity endpoint is GET /identity (top-level),
        # NOT /accounts/{id}/identity (which returns 404).
        result = _request_with_retry(client, "GET", "/identity")
        time.sleep(RATE_LIMIT_DELAY)

        # Response is a list of objects, each with "account" and "owners" keys.
        # Find the entry matching the requested account_id.
        if isinstance(result, list):
            for entry in result:
                account = entry.get("account", {})
                if account.get("id") == account_id:
                    owners = entry.get("owners", [])
                    if owners:
                        owner = owners[0]
                        # Owner names are in a "names" array
                        names = owner.get("names", [])
                        if names:
                            full_name = names[0].get("data", "")
                            # Attempt to split into first/last
                            parts = full_name.strip().split(None, 1)
                            first = parts[0] if len(parts) >= 1 else ""
                            last = parts[1] if len(parts) >= 2 else ""
                            return {
                                "first_name": first,
                                "last_name": last,
                                "full_name": full_name.strip(),
                            }
                    break

            # If account_id wasn't found but we got results,
            # fall back to the first entry's first owner
            if result:
                owners = result[0].get("owners", [])
                if owners:
                    names = owners[0].get("names", [])
                    if names:
                        full_name = names[0].get("data", "")
                        parts = full_name.strip().split(None, 1)
                        first = parts[0] if len(parts) >= 1 else ""
                        last = parts[1] if len(parts) >= 2 else ""
                        return {
                            "first_name": first,
                            "last_name": last,
                            "full_name": full_name.strip(),
                        }

    except Exception as e:
        logger.warning("Identity lookup failed: %s", e)

    return {"first_name": "", "last_name": "", "full_name": ""}


def reload_tokens_and_profiles():
    """
    Reload TOKENS and PROFILES from .env + token_store.
    Called after a new Teller Connect enrollment to pick up the new token
    without restarting the server.
    """
    global TOKENS, PROFILES

    # Close any cached httpx clients for tokens that may have been removed
    old_tokens = set(TOKENS)
    new_tokens_list = _load_tokens()
    new_profiles = _load_profiles()

    # Close clients for tokens no longer in the list
    removed = old_tokens - set(new_tokens_list)
    for tok in removed:
        client = _client_cache.pop(tok, None)
        if client:
            try:
                client.close()
            except Exception:
                pass

    TOKENS = new_tokens_list
    PROFILES = new_profiles

    logger.info(
        "Reloaded tokens: %d total, %d profiles (%s).",
        len(TOKENS), len(PROFILES), ", ".join(sorted(PROFILES.keys())),
    )
