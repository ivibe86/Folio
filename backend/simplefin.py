"""
simplefin.py
SimpleFIN Bridge API client.

Handles the setup-token → claim → access-url flow, fetches accounts
and transactions, and normalises the data into the same shape the
categoriser / inserter pipeline expects.

This module is fully independent of bank.py / token_store.py.
"""

import base64
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from database import get_db
from log_config import get_logger

logger = get_logger(__name__)

# Re-use the same Fernet cipher as token_store.py (same env key).
import os

_ENCRYPTION_KEY = os.getenv("TOKEN_ENCRYPTION_KEY")
_cipher = None

if _ENCRYPTION_KEY:
    try:
        from cryptography.fernet import Fernet
        _cipher = Fernet(
            _ENCRYPTION_KEY.encode() if isinstance(_ENCRYPTION_KEY, str) else _ENCRYPTION_KEY
        )
    except Exception:
        pass  # Falls back to plaintext — same behaviour as token_store.py


def _encrypt(plaintext: str) -> str:
    if _cipher:
        return _cipher.encrypt(plaintext.encode()).decode()
    return plaintext


def _decrypt(stored: str) -> str:
    if _cipher:
        return _cipher.decrypt(stored.encode()).decode()
    return stored


# ── Setup-Token → Access-URL exchange ────────────────────────────────────────

def claim_setup_token(setup_token: str) -> str:
    """
    Exchange a base64-encoded SimpleFIN Setup Token for a permanent Access URL.

    Flow:
        1. base64-decode the setup token → Claim URL
        2. POST to the Claim URL (no body) → response body is the Access URL
        3. Return the Access URL (https://user:pass@bridge.simplefin.org/…)

    Raises ValueError on any failure.
    """
    try:
        claim_url = base64.b64decode(setup_token.strip()).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"Invalid setup token — could not base64-decode: {exc}") from exc

    parsed = urlparse(claim_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Decoded setup token is not a valid URL.")

    try:
        resp = httpx.post(claim_url, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise ValueError(
            f"SimpleFIN claim failed (HTTP {exc.response.status_code}). "
            "The setup token may have already been claimed."
        ) from exc
    except httpx.RequestError as exc:
        raise ValueError(f"SimpleFIN claim request failed: {exc}") from exc

    access_url = resp.text.strip()
    if not access_url or "://" not in access_url:
        raise ValueError("SimpleFIN returned an invalid access URL.")

    return access_url


# ── Connection CRUD (simplefin_connections table) ────────────────────────────

def save_connection(profile: str, access_url: str, display_name: str = "") -> int:
    """
    Store a new SimpleFIN connection. Returns the new row id.
    Raises ValueError if a connection with the same access URL already exists
    for this profile.
    """
    normalized_profile = " ".join((profile or "").strip().split()).lower() or "primary"
    encrypted = _encrypt(access_url)
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        # Check for duplicates (same encrypted URL for this profile)
        existing = conn.execute(
            "SELECT id FROM simplefin_connections WHERE profile = ? AND access_url_encrypted = ? AND is_active = 1",
            (normalized_profile, encrypted),
        ).fetchone()
        if existing:
            raise ValueError("This SimpleFIN connection already exists for this profile.")

        cur = conn.execute(
            """INSERT INTO simplefin_connections
               (profile, display_name, access_url_encrypted, created_at)
               VALUES (?, ?, ?, ?)""",
            (normalized_profile, display_name, encrypted, now),
        )
        logger.info("Saved SimpleFIN connection for profile '%s' (id=%d).", normalized_profile, cur.lastrowid)
        return cur.lastrowid


def load_all_connections() -> list[dict]:
    """Return all active SimpleFIN connections (metadata only — no access URL)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, profile, display_name, last_synced_at, created_at
               FROM simplefin_connections WHERE is_active = 1
               ORDER BY profile, created_at"""
        ).fetchall()
    return [dict(r) for r in rows]


def _load_active_access_urls() -> list[dict]:
    """Load active connections with decrypted access URLs (internal use only)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, profile, display_name, access_url_encrypted, last_synced_at
               FROM simplefin_connections WHERE is_active = 1"""
        ).fetchall()

    result = []
    for r in rows:
        row = dict(r)
        row["access_url"] = _decrypt(row.pop("access_url_encrypted"))
        result.append(row)
    return result


def deactivate_connection(connection_id: int) -> bool:
    """Soft-delete a SimpleFIN connection."""
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE simplefin_connections SET is_active = 0 WHERE id = ? AND is_active = 1",
            (connection_id,),
        )
    if cur.rowcount > 0:
        logger.info("Deactivated SimpleFIN connection ID %d.", connection_id)
        return True
    return False


def update_last_synced(connection_id: int, synced_at: str):
    """Update last_synced_at timestamp after a successful sync."""
    with get_db() as conn:
        conn.execute(
            "UPDATE simplefin_connections SET last_synced_at = ? WHERE id = ?",
            (synced_at, connection_id),
        )


# ── Fetch accounts + transactions from SimpleFIN Bridge ──────────────────────

# SimpleFIN allows max 24 requests/day.  Skip connections synced within the
# last hour to stay well inside the budget.
_MIN_SYNC_INTERVAL_SECONDS = 3600


def _should_sync(last_synced_at: str | None) -> bool:
    """Return True if enough time has passed since the last sync."""
    if not last_synced_at:
        return True
    try:
        last = datetime.fromisoformat(last_synced_at)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return elapsed >= _MIN_SYNC_INTERVAL_SECONDS
    except Exception:
        return True


def fetch_data(access_url: str, start_date: str | None = None) -> dict:
    """
    GET /accounts?version=2 from SimpleFIN Bridge.

    Args:
        access_url: Full URL with embedded credentials (https://user:pass@host/…)
        start_date: Optional YYYY-MM-DD — only fetch transactions on or after this date.

    Returns dict with keys: "accounts" (list of account dicts with nested transactions).
    Raises ValueError on HTTP / parsing errors.
    """
    parsed = urlparse(access_url)
    base_url = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        base_url += f":{parsed.port}"
    base_url += parsed.path.rstrip("/")

    url = f"{base_url}/accounts"
    params = {"version": "2"}

    if start_date:
        try:
            dt = datetime.strptime(start_date, "%Y-%m-%d")
            params["start-date"] = str(int(dt.replace(tzinfo=timezone.utc).timestamp()))
        except ValueError:
            pass  # skip bad date, fetch all

    auth = (parsed.username or "", parsed.password or "")

    try:
        resp = httpx.get(url, params=params, auth=auth, timeout=60)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 403:
            raise ValueError(
                "SimpleFIN returned 403 Forbidden — the access URL may have been revoked. "
                "Please reconnect."
            ) from exc
        raise ValueError(f"SimpleFIN fetch failed (HTTP {status}).") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"SimpleFIN request failed: {exc}") from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise ValueError(f"SimpleFIN returned invalid JSON: {exc}") from exc

    return data


# ── Normalisation — SimpleFIN → Folio internal shapes ────────────────────────

_CREDIT_KEYWORDS = re.compile(
    r"credit\s*card|visa|mastercard|amex|discover|american\s*express|"
    r"sapphire|freedom|slate|ink\b|preferred|reserve|cash\s*back|rewards|"
    r"platinum|gold\s*card|blue\s*cash|citi\s*double|quicksilver|venture|"
    r"prime\s*rewards|signature|world\s*elite",
    re.IGNORECASE,
)


def _infer_account_type(account: dict) -> str:
    """
    SimpleFIN doesn't provide an explicit account type.

    Primary signal: a negative balance means you owe money → credit card.
    Depository accounts (checking/savings) never carry a negative balance
    under normal circumstances, so this is highly reliable.

    Fallback: keyword scan of the account/org name for known card product
    names — catches zero-balance cards that would otherwise look like
    depository accounts.
    """
    # Primary: negative balance → credit
    try:
        if float(account.get("balance", 0) or 0) < 0:
            return "credit"
    except (TypeError, ValueError):
        pass
    # Fallback: name keyword scan for zero-balance or paid-off cards
    name = account.get("name", "")
    org_name = (account.get("org") or {}).get("name", "")
    if _CREDIT_KEYWORDS.search(f"{name} {org_name}"):
        return "credit"
    return "depository"


def normalize_account(sf_account: dict, profile: str) -> dict:
    """
    Map a SimpleFIN account to the shape used by the accounts table upsert
    in data_manager.py.
    """
    org = sf_account.get("org") or {}
    sf_id = sf_account.get("id", "")
    account_type = _infer_account_type(sf_account)

    # SimpleFIN balance fields
    balance = sf_account.get("balance", "0")

    return {
        "id": f"sf_{sf_id}",
        "profile": profile,
        "institution_name": org.get("name", ""),
        "account_name": sf_account.get("name", ""),
        "account_type": account_type,
        "account_subtype": "",
        "current_balance": float(balance),
        "currency": sf_account.get("currency", "USD"),
        "provider": "simplefin",
    }


def normalize_transaction(sf_tx: dict, account_info: dict) -> dict:
    """
    Map a SimpleFIN transaction to the intermediate dict shape that
    categorize_transactions() and _insert_transaction() expect.

    SimpleFIN amounts already follow the correct sign convention:
      negative = money out (expense), positive = money in (income).
    No sign flip is needed (unlike Teller credit cards).
    """
    # Prefer transacted_at (authorization date) over posted (settlement date) so
    # transactions appear in the month they occurred, not when they cleared.
    timestamp = sf_tx.get("transacted_at") or sf_tx.get("posted") or 0
    try:
        date_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OSError, ValueError):
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    description = sf_tx.get("description", "").strip()
    amount = float(sf_tx.get("amount", "0"))
    sf_tx_id = sf_tx.get("id", "")

    return {
        "id": f"sf_{sf_tx_id}",
        "original_id": f"sf_{sf_tx_id}",
        "date": date_str,
        "description": description,
        "raw_description": description,
        "amount": str(amount),
        "type": "",
        "account_name": account_info.get("account_name", ""),
        "account_type": account_info.get("account_type", ""),
        "profile": account_info.get("profile", "primary"),
        "counterparty_name": sf_tx.get("payee", ""),
        "counterparty_type": "",
        "teller_category": "",
    }


def normalize_all(raw_data: dict, profile: str) -> tuple[list[dict], list[dict]]:
    """
    Given a raw SimpleFIN /accounts response and a profile name, return
    (normalised_accounts, normalised_transactions).
    """
    sf_accounts = raw_data.get("accounts", [])
    accounts = []
    transactions = []

    for sf_acct in sf_accounts:
        acct = normalize_account(sf_acct, profile)
        accounts.append(acct)

        for sf_tx in sf_acct.get("transactions", []):
            tx = normalize_transaction(sf_tx, acct)
            transactions.append(tx)

    return accounts, transactions
