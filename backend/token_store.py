"""
token_store.py
CRUD operations for dynamically enrolled Teller tokens.
Storage lives in the main Folio.db (enrolled_tokens table),
managed by database.py's schema system.
"""

import os
from datetime import datetime, timezone

from database import get_db, get_db_session
from log_config import get_logger

logger = get_logger(__name__)

# ── Encryption (optional but recommended) ──
# Set TOKEN_ENCRYPTION_KEY in .env to a Fernet key.
# Generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
_ENCRYPTION_KEY = os.getenv("TOKEN_ENCRYPTION_KEY")
_cipher = None

if _ENCRYPTION_KEY:
    try:
        from cryptography.fernet import Fernet
        _cipher = Fernet(
            _ENCRYPTION_KEY.encode() if isinstance(_ENCRYPTION_KEY, str) else _ENCRYPTION_KEY
        )
        logger.info("Token encryption enabled (Fernet).")
    except ImportError:
        logger.warning(
            "TOKEN_ENCRYPTION_KEY is set but 'cryptography' package is not installed. "
            "Tokens will be stored in plaintext. Install with: pip install cryptography"
        )
    except Exception as e:
        logger.warning("Failed to initialize Fernet cipher: %s — tokens stored in plaintext.", e)
else:
    logger.info("TOKEN_ENCRYPTION_KEY not set — dynamic tokens will be stored in plaintext.")


def _encrypt(plaintext: str) -> str:
    if _cipher:
        return _cipher.encrypt(plaintext.encode()).decode()
    return plaintext


def _decrypt(stored: str) -> str:
    if _cipher:
        return _cipher.decrypt(stored.encode()).decode()
    return stored


def save_token(
    profile: str,
    token: str,
    institution: str = "",
    owner_name: str = "",
    enrollment_id: str | None = None,
) -> bool:
    """
    Persist a new Teller access token into Folio.db's enrolled_tokens table.
    Returns True if inserted, False if duplicate (same profile + encrypted token).
    """
    encrypted = _encrypt(token)
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        try:
            conn.execute(
                """INSERT INTO enrolled_tokens
                   (profile, institution, token_encrypted, owner_name, enrollment_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (profile.lower(), institution, encrypted, owner_name, enrollment_id, now),
            )
            logger.info("Saved new token for profile '%s' (%s).", profile, institution)
            return True
        except Exception as e:
            # UNIQUE constraint violation = duplicate
            if "UNIQUE" in str(e).upper():
                logger.warning("Duplicate token for profile '%s' (%s) — skipped.", profile, institution)
                return False
            raise


def load_all_tokens() -> dict[str, list[str]]:
    """
    Load all active tokens grouped by profile.
    Returns: {"karthik": ["tok_abc", "tok_def"], "swati": ["tok_ghi"]}
    """
    with get_db() as conn:
        rows = conn.execute(
            "SELECT profile, token_encrypted FROM enrolled_tokens WHERE is_active = 1"
        ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        profile = row["profile"] if hasattr(row, "keys") else row[0]
        encrypted = row["token_encrypted"] if hasattr(row, "keys") else row[1]
        token = _decrypt(encrypted)
        result.setdefault(profile, []).append(token)
    return result


def load_all_enrollments() -> list[dict]:
    """Return all active enrollments with metadata (for the /api/enrollments endpoint)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, profile, institution, owner_name, enrollment_id, created_at
               FROM enrolled_tokens WHERE is_active = 1
               ORDER BY profile, created_at"""
        ).fetchall()
    return [dict(r) for r in rows]


def deactivate_token(token_id: int) -> bool:
    """Soft-delete an enrollment by ID."""
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE enrolled_tokens SET is_active = 0 WHERE id = ? AND is_active = 1",
            (token_id,),
        )
    if cur.rowcount > 0:
        logger.info("Deactivated token ID %d.", token_id)
        return True
    return False