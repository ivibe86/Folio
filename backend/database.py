"""
database.py
SQLite database initialization, connection management, and schema definition.
"""

import sqlite3
import os
import threading
import re
import time
from pathlib import Path
from contextlib import contextmanager
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback for lightweight scripts
    def load_dotenv():
        return False
from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

DB_FILE = os.getenv("DB_FILE", "Folio.db")
# If DB_FILE is an absolute path (e.g., /data/Folio.db from Docker),
# use it directly. Otherwise, place it relative to this file's directory.
_db_file_path = Path(DB_FILE)
if _db_file_path.is_absolute():
    DB_PATH = _db_file_path
else:
    DB_PATH = Path(__file__).parent / DB_FILE

_local = threading.local()
_wal_lock = threading.Lock()
_wal_initialized = False
_SQLITE_CONNECT_RETRIES = int(os.getenv("SQLITE_CONNECT_RETRIES", "3"))
_SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "10000"))


def _sqlite_regexp(pattern, value) -> int:
    """SQLite REGEXP implementation used by Copilot-generated queries."""
    if pattern is None or value is None:
        return 0
    try:
        return 1 if re.search(str(pattern), str(value), re.IGNORECASE) else 0
    except re.error:
        logger.debug("Invalid REGEXP pattern passed to SQLite helper: %s", pattern)
        return 0


def _is_transient_sqlite_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "disk i/o error" in message or "database is locked" in message or "database is busy" in message


def _open_connection() -> sqlite3.Connection:
    """Open SQLite with a short retry for transient Docker/macOS bind-mount I/O hiccups."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    delay = 0.05
    last_error: Exception | None = None
    for attempt in range(max(1, _SQLITE_CONNECT_RETRIES)):
        try:
            return sqlite3.connect(
                str(DB_PATH),
                check_same_thread=False,
                timeout=max(1.0, _SQLITE_BUSY_TIMEOUT_MS / 1000),
            )
        except sqlite3.OperationalError as exc:
            last_error = exc
            if not _is_transient_sqlite_error(exc) or attempt >= _SQLITE_CONNECT_RETRIES - 1:
                raise
            logger.warning("SQLite open failed transiently (%s); retrying in %.2fs", exc, delay)
            time.sleep(delay)
            delay *= 2
    raise last_error or sqlite3.OperationalError("failed to open SQLite database")


def _ensure_wal_mode() -> None:
    """Set WAL once per process instead of on every request-scoped connection."""
    global _wal_initialized
    if _wal_initialized:
        return
    with _wal_lock:
        if _wal_initialized:
            return
        delay = 0.05
        for attempt in range(max(1, _SQLITE_CONNECT_RETRIES)):
            conn = None
            try:
                conn = _open_connection()
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
                conn.close()
                _wal_initialized = True
                return
            except sqlite3.OperationalError as exc:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if not _is_transient_sqlite_error(exc) or attempt >= _SQLITE_CONNECT_RETRIES - 1:
                    raise
                logger.warning("SQLite WAL setup failed transiently (%s); retrying in %.2fs", exc, delay)
                time.sleep(delay)
                delay *= 2


def _configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
    conn.create_function("REGEXP", 2, _sqlite_regexp)
    return conn


def get_connection() -> sqlite3.Connection:
    """
    Get a thread-local SQLite connection.
    Returns the same connection for the same thread (reuse within request).

    DEPRECATED: Use get_db_session() with FastAPI Depends() for request-scoped
    connections that are properly closed. This function is retained only for
    background tasks (e.g., sync) and module-level code paths that cannot
    use FastAPI dependency injection.
    """
    _ensure_wal_mode()
    if not hasattr(_local, "connection") or _local.connection is None:
        conn = _open_connection()
        _local.connection = _configure_connection(conn)
    else:
        _configure_connection(_local.connection)
    return _local.connection


@contextmanager
def get_db():
    """Context manager for database operations with auto-commit/rollback."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise

def get_db_session():
    """
    FastAPI dependency that provides a request-scoped DB connection.
    Commits on success, rolls back on exception, and CLOSES the connection
    in the finally block — preventing connection leaks in long-running servers.

    Usage in route handlers:
        @app.get("/api/example")
        def example(db: sqlite3.Connection = Depends(get_db_session)):
            rows = db.execute("SELECT ...").fetchall()
            return rows
    """
    _ensure_wal_mode()
    conn = _open_connection()
    conn = _configure_connection(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def close_thread_local_connection():
    """
    Close and discard the thread-local connection if one exists.
    Called during shutdown to clean up any remaining connections.
    """
    if hasattr(_local, "connection") and _local.connection is not None:
        try:
            _local.connection.close()
        except Exception:
            pass
        _local.connection = None


def dict_from_row(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a plain dict."""
    if row is None:
        return None
    return dict(row)


def dicts_from_rows(rows: list[sqlite3.Row]) -> list[dict]:
    """Convert a list of sqlite3.Row to list of dicts."""
    return [dict(r) for r in rows]


def init_db():
    """
    Create all tables if they don't exist.
    Safe to call multiple times (idempotent).
    """
    with get_db() as conn:
        conn.executescript(SCHEMA_SQL)
        _migrate_expense_type(conn)
        _migrate_category_parent(conn)
        _migrate_merchants_table(conn)
        _migrate_merchant_aliases(conn)
        _migrate_transaction_expense_type(conn)
        _migrate_description_normalized(conn)
        _migrate_transaction_merchant_identity(conn)
        _migrate_canonical_merchant_metadata(conn)
        _migrate_category_pinned(conn)
        _migrate_accounts_provider(conn)
        _migrate_accounts_last_four(conn)
        _migrate_memory_entries_theme(conn)
        _seed_default_categories(conn)
        _seed_system_rules(conn)
        _seed_teller_category_map(conn)          


def _migrate_expense_type(conn: sqlite3.Connection):
    """
    Backfill expense_type and expense_type_source for existing databases
    that were created before these columns existed. Idempotent.
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(categories)").fetchall()]
    if "expense_type" not in cols:
        conn.execute("ALTER TABLE categories ADD COLUMN expense_type TEXT DEFAULT 'variable' CHECK(expense_type IN ('fixed', 'variable', 'non_expense'))")
    if "expense_type_source" not in cols:
        conn.execute("ALTER TABLE categories ADD COLUMN expense_type_source TEXT DEFAULT 'system' CHECK(expense_type_source IN ('system', 'user'))")

    # Backfill known defaults (only update rows still at 'variable' that should be fixed/non_expense)
    # Only backfill if expense_type_source is 'system' (don't override user choices)
    KNOWN_FIXED = ("Utilities", "Housing", "Subscriptions", "Taxes", "Insurance")
    KNOWN_NON_EXPENSE = ("Savings Transfer", "Credit Card Payment", "Income", "Personal Transfer")

    for name in KNOWN_FIXED:
        conn.execute(
            "UPDATE categories SET expense_type = 'fixed' WHERE name = ? AND expense_type = 'variable' AND (expense_type_source = 'system' OR expense_type_source IS NULL)",
            (name,),
        )
    for name in KNOWN_NON_EXPENSE:
        conn.execute(
            "UPDATE categories SET expense_type = 'non_expense' WHERE name = ? AND expense_type = 'variable' AND (expense_type_source = 'system' OR expense_type_source IS NULL)",
            (name,),
        )


def _migrate_category_parent(conn: sqlite3.Connection):
    """Ensure categories.parent_category exists for older databases."""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(categories)").fetchall()]
    if "parent_category" not in cols:
        conn.execute("ALTER TABLE categories ADD COLUMN parent_category TEXT DEFAULT NULL")

def _migrate_merchants_table(conn: sqlite3.Connection):
    """
    Ensure merchants table has all required columns for Enhancement 4
    (cancelled_at, cancelled_by_user). Idempotent.
    """
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(merchants)").fetchall()]
    except Exception:
        return  # Table doesn't exist yet; SCHEMA_SQL will create it

    if not cols:
        return

    if "cancelled_at" not in cols:
        conn.execute("ALTER TABLE merchants ADD COLUMN cancelled_at TEXT")
    if "cancelled_by_user" not in cols:
        conn.execute("ALTER TABLE merchants ADD COLUMN cancelled_by_user INTEGER DEFAULT 0")


def _migrate_merchant_aliases(conn: sqlite3.Connection):
    """Create merchant_aliases and backfill legacy user display overrides."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS merchant_aliases (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            merchant_key TEXT NOT NULL,
            profile_id  TEXT NOT NULL,
            display_name TEXT NOT NULL,
            source      TEXT NOT NULL DEFAULT 'user',
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(merchant_key, profile_id)
        );

        CREATE INDEX IF NOT EXISTS idx_merchant_aliases_profile
            ON merchant_aliases(profile_id);
        CREATE INDEX IF NOT EXISTS idx_merchant_aliases_key
            ON merchant_aliases(merchant_key);
        """
    )

    conn.execute(
        """
        INSERT INTO merchant_aliases (merchant_key, profile_id, display_name, source)
        SELECT UPPER(TRIM(merchant_key)),
               profile_id,
               TRIM(clean_name),
               'user'
          FROM merchants
         WHERE COALESCE(profile_id, '') != ''
           AND NULLIF(TRIM(clean_name), '') IS NOT NULL
           AND UPPER(TRIM(clean_name)) != UPPER(TRIM(COALESCE(merchant_key, '')))
           AND COALESCE(source, '') = 'user'
        ON CONFLICT(merchant_key, profile_id) DO NOTHING
        """
    )
        
def _migrate_transaction_expense_type(conn: sqlite3.Connection):
    """
    Ensure transactions table has an expense_type column for transfer
    sub-classification (transfer_internal, transfer_household, transfer_external).
    Idempotent — safe to call on every startup.
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "expense_type" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN expense_type TEXT DEFAULT NULL")
        logger.info("Added expense_type column to transactions table.")


def _migrate_description_normalized(conn: sqlite3.Connection):
    """
    Add description_normalized column and index to transactions, then backfill
    existing rows. Idempotent — safe to call on every startup.

    description_normalized stores the output of _extract_merchant_pattern(description),
    i.e. the canonical merchant token with store numbers, dates, and location noise
    stripped. Matching user-defined rules against this canonical form instead of the
    raw description fixes the mid-string noise bug (e.g. '#187' between merchant
    name tokens preventing substring containment).
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "description_normalized" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN description_normalized TEXT DEFAULT NULL")
        logger.info("Added description_normalized column to transactions table.")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_description_normalized "
        "ON transactions(description_normalized)"
    )

    # Backfill existing rows in batches of 500 to avoid a single huge transaction.
    # _extract_merchant_pattern is Python-only, so we fetch-loop-update.
    _BATCH = 500
    total = 0
    while True:
        rows = conn.execute(
            "SELECT id, description FROM transactions WHERE description_normalized IS NULL LIMIT ?",
            (_BATCH,),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            normalized = _extract_merchant_pattern(row[1])  # row[0]=id, row[1]=description
            conn.execute(
                "UPDATE transactions SET description_normalized = ? WHERE id = ?",
                (normalized, row[0]),
            )
        conn.commit()
        total += len(rows)
        if len(rows) < _BATCH:
            break
    if total:
        logger.info("Backfilled description_normalized for %d transactions.", total)


def _migrate_transaction_merchant_identity(conn: sqlite3.Connection):
    """
    Add explicit merchant identity columns.

    description_normalized remains as legacy rule-pattern data. This migration
    intentionally backfills merchant_key only from existing merchant_name, never
    from regex-derived description_normalized for ambiguous unenriched rows.
    """
    from merchant_identity import (
        MERCHANT_PURCHASE,
        canonicalize_merchant_key,
        infer_non_merchant_kind,
        normalize_merchant_kind,
    )

    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    additions = {
        "merchant_key": "TEXT DEFAULT ''",
        "merchant_source": "TEXT DEFAULT ''",
        "merchant_confidence": "TEXT DEFAULT ''",
        "merchant_kind": "TEXT DEFAULT ''",
    }
    for col, ddl in additions.items():
        if col not in cols:
            conn.execute(f"ALTER TABLE transactions ADD COLUMN {col} {ddl}")
            logger.info("Added %s column to transactions table.", col)

    updated = 0
    while True:
        rows = conn.execute(
            """SELECT id, description, raw_description, category, transaction_type,
                      expense_type, merchant_name, merchant_key, merchant_kind,
                      enriched, merchant_source, merchant_confidence
               FROM transactions
               WHERE NULLIF(TRIM(COALESCE(merchant_name, '')), '') IS NOT NULL
                 AND (
                    COALESCE(merchant_key, '') = ''
                    OR COALESCE(merchant_kind, '') = ''
                    OR COALESCE(merchant_source, '') = ''
                 )
               LIMIT 500"""
        ).fetchall()
        if not rows:
            break
        batch_updated = 0
        for row in rows:
            tx = dict(row)
            merchant_key = canonicalize_merchant_key(tx.get("merchant_key") or tx.get("merchant_name"))
            merchant_kind = normalize_merchant_kind(tx.get("merchant_kind"))
            merchant_source = (tx.get("merchant_source") or "").strip()
            merchant_confidence = (tx.get("merchant_confidence") or "").strip()

            if merchant_key and not merchant_kind:
                merchant_kind = MERCHANT_PURCHASE
            if merchant_key and merchant_kind == "unknown":
                merchant_kind = MERCHANT_PURCHASE
            if merchant_key and merchant_source in ("", "none"):
                merchant_source = "legacy"

            if merchant_key or merchant_kind or merchant_source or merchant_confidence:
                conn.execute(
                    """UPDATE transactions
                       SET merchant_key = COALESCE(NULLIF(?, ''), merchant_key, ''),
                           merchant_kind = COALESCE(NULLIF(?, ''), merchant_kind, ''),
                           merchant_source = COALESCE(NULLIF(?, ''), merchant_source, ''),
                           merchant_confidence = COALESCE(NULLIF(?, ''), merchant_confidence, '')
                       WHERE id = ?""",
                    (merchant_key, merchant_kind, merchant_source, merchant_confidence, tx["id"]),
                )
                updated += 1
                batch_updated += 1
        conn.commit()
        if batch_updated == 0:
            break

    # Mark obvious non-merchant rows without creating merchant keys from raw descriptions.
    # Keep ambiguous blank-merchant rows untouched so startup does not rewrite the whole ledger.
    non_merchant_updated = 0
    while True:
        rows = conn.execute(
            """SELECT id, description, raw_description, category, transaction_type,
                      expense_type, merchant_name, merchant_key, merchant_kind,
                      enriched, merchant_source, merchant_confidence
               FROM transactions
               WHERE COALESCE(merchant_key, '') = ''
                 AND NULLIF(TRIM(COALESCE(merchant_name, '')), '') IS NULL
                 AND COALESCE(merchant_kind, '') = ''
                 AND (
                    UPPER(COALESCE(description, '') || ' ' || COALESCE(raw_description, '')) LIKE '%ZELLE%'
                    OR UPPER(COALESCE(description, '') || ' ' || COALESCE(raw_description, '')) LIKE '%CREDIT CRD%'
                    OR UPPER(COALESCE(description, '') || ' ' || COALESCE(raw_description, '')) LIKE '%EPAY%'
                    OR UPPER(COALESCE(description, '') || ' ' || COALESCE(raw_description, '')) LIKE '%PAYROLL%'
                    OR UPPER(COALESCE(description, '') || ' ' || COALESCE(raw_description, '')) LIKE '%IRS%'
                    OR UPPER(COALESCE(category, '')) IN ('INCOME', 'PAYROLL', 'TAXES', 'FEES')
                 )
               LIMIT 500"""
        ).fetchall()
        if not rows:
            break
        batch_updated = 0
        for row in rows:
            tx = dict(row)
            merchant_kind = infer_non_merchant_kind(tx)
            if not merchant_kind:
                continue
            conn.execute(
                """UPDATE transactions
                   SET merchant_kind = ?,
                       merchant_source = COALESCE(NULLIF(merchant_source, ''), 'rule'),
                       merchant_confidence = COALESCE(NULLIF(merchant_confidence, ''), 'high')
                   WHERE id = ?""",
                (merchant_kind, tx["id"]),
            )
            updated += 1
            non_merchant_updated += 1
            batch_updated += 1
        conn.commit()
        if batch_updated == 0:
            break

    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_merchant_key ON transactions(merchant_key)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_profile_merchant_key "
        "ON transactions(profile_id, merchant_key)"
    )
    if updated:
        logger.info("Backfilled merchant identity for %d transactions.", updated)
    if non_merchant_updated:
        logger.info("Classified %d obvious non-merchant transactions.", non_merchant_updated)


def _migrate_canonical_merchant_metadata(conn: sqlite3.Connection):
    """Bridge legacy merchant metadata/aliases onto canonical merchant_key values."""
    from merchant_identity import canonicalize_merchant_key

    rows = conn.execute(
        """SELECT merchant_key, clean_name, logo_url, domain, category, industry,
                  source, is_subscription, subscription_frequency, subscription_amount,
                  subscription_status, cancelled_at, cancelled_by_user,
                  last_charge_date, next_expected_date, total_spent, charge_count,
                  profile_id
           FROM merchants
           WHERE COALESCE(profile_id, '') != ''
             AND NULLIF(TRIM(clean_name), '') IS NOT NULL"""
    ).fetchall()
    copied = 0
    for row in rows:
        item = dict(row)
        canonical_key = canonicalize_merchant_key(item.get("clean_name"))
        profile_id = item.get("profile_id")
        if not canonical_key or not profile_id:
            continue
        if canonical_key == canonicalize_merchant_key(item.get("merchant_key")):
            continue
        conn.execute(
            """INSERT INTO merchants
               (merchant_key, clean_name, logo_url, domain, category, industry,
                source, is_subscription, subscription_frequency, subscription_amount,
                subscription_status, cancelled_at, cancelled_by_user,
                last_charge_date, next_expected_date, total_spent, charge_count,
                profile_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(merchant_key, profile_id) DO UPDATE SET
                   clean_name = COALESCE(NULLIF(merchants.clean_name, ''), excluded.clean_name),
                   logo_url = COALESCE(NULLIF(merchants.logo_url, ''), excluded.logo_url),
                   domain = COALESCE(NULLIF(merchants.domain, ''), excluded.domain),
                   category = COALESCE(NULLIF(merchants.category, ''), excluded.category),
                   industry = COALESCE(NULLIF(merchants.industry, ''), excluded.industry),
                   is_subscription = MAX(merchants.is_subscription, excluded.is_subscription),
                   subscription_frequency = COALESCE(NULLIF(merchants.subscription_frequency, ''), excluded.subscription_frequency),
                   subscription_amount = COALESCE(merchants.subscription_amount, excluded.subscription_amount),
                   subscription_status = COALESCE(NULLIF(merchants.subscription_status, ''), excluded.subscription_status),
                   cancelled_at = COALESCE(NULLIF(merchants.cancelled_at, ''), excluded.cancelled_at),
                   cancelled_by_user = MAX(merchants.cancelled_by_user, excluded.cancelled_by_user),
                   last_charge_date = COALESCE(NULLIF(merchants.last_charge_date, ''), excluded.last_charge_date),
                   next_expected_date = COALESCE(NULLIF(merchants.next_expected_date, ''), excluded.next_expected_date),
                   updated_at = datetime('now')""",
            (
                canonical_key,
                item.get("clean_name"),
                item.get("logo_url"),
                item.get("domain"),
                item.get("category"),
                item.get("industry"),
                item.get("source") or "legacy",
                item.get("is_subscription") or 0,
                item.get("subscription_frequency"),
                item.get("subscription_amount"),
                item.get("subscription_status"),
                item.get("cancelled_at"),
                item.get("cancelled_by_user") or 0,
                item.get("last_charge_date"),
                item.get("next_expected_date"),
                item.get("total_spent") or 0,
                item.get("charge_count") or 0,
                profile_id,
            ),
        )
        conn.execute(
            """INSERT INTO merchant_aliases (merchant_key, profile_id, display_name, source)
               VALUES (?, ?, ?, 'user')
               ON CONFLICT(merchant_key, profile_id) DO NOTHING""",
            (canonical_key, profile_id, item.get("clean_name")),
        )
        copied += 1
    if copied:
        logger.info("Bridged %d merchant metadata rows to canonical merchant keys.", copied)


def _migrate_category_pinned(conn: sqlite3.Connection):
    """
    Add category_pinned column to transactions.
    When 1, the transaction was manually recategorized as a one-off — future rule
    applications and syncs must not overwrite its category.
    Idempotent — safe to call on every startup.
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "category_pinned" not in cols:
        conn.execute(
            "ALTER TABLE transactions ADD COLUMN category_pinned INTEGER NOT NULL DEFAULT 0"
        )
        logger.info("Added category_pinned column to transactions table.")


def _migrate_accounts_last_four(conn: sqlite3.Connection):
    """
    Add last_four column to accounts for cleaner cross-provider matching.
    Backfills by extracting the last 4-digit sequence from account_name or id.
    Idempotent.
    """
    import re as _re
    cols = [row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()]
    if "last_four" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN last_four TEXT DEFAULT NULL")
        rows = conn.execute("SELECT id, account_name FROM accounts").fetchall()
        for acct_id, acct_name in rows:
            for source in (acct_name or "", acct_id or ""):
                seqs = _re.findall(r'\d{4,}', source)
                if seqs:
                    conn.execute(
                        "UPDATE accounts SET last_four = ? WHERE id = ?",
                        (seqs[-1][-4:], acct_id),
                    )
                    break
        logger.info("Added last_four column to accounts table.")


def _migrate_accounts_provider(conn: sqlite3.Connection):
    """
    Add provider column to accounts table so we can distinguish
    Teller vs SimpleFIN (or future) account sources. Idempotent.
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()]
    if "provider" not in cols:
        conn.execute(
            "ALTER TABLE accounts ADD COLUMN provider TEXT NOT NULL DEFAULT 'teller'"
        )
        logger.info("Added provider column to accounts table.")


def _migrate_memory_entries_theme(conn: sqlite3.Connection):
    """Add theme column to memory_entries (used to link inferred entries to their observation theme)."""
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(memory_entries)").fetchall()]
    except Exception:
        return
    if cols and "theme" not in cols:
        conn.execute("ALTER TABLE memory_entries ADD COLUMN theme TEXT DEFAULT NULL")


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    id              TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    is_default      INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS accounts (
    id                  TEXT PRIMARY KEY,
    profile_id          TEXT NOT NULL REFERENCES profiles(id),
    institution_name    TEXT DEFAULT '',
    account_name        TEXT DEFAULT '',
    account_type        TEXT NOT NULL DEFAULT 'depository',
    account_subtype     TEXT DEFAULT '',
    current_balance     REAL DEFAULT 0.0,
    available_balance   REAL DEFAULT 0.0,
    currency            TEXT DEFAULT 'USD',
    last_synced_at      TEXT,
    is_active           INTEGER DEFAULT 1,
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

CREATE TABLE IF NOT EXISTS categories (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT UNIQUE NOT NULL,
    is_system           INTEGER DEFAULT 1,
    parent_category     TEXT DEFAULT NULL,
    expense_type        TEXT DEFAULT 'variable' CHECK(expense_type IN ('fixed', 'variable', 'non_expense')),
    expense_type_source TEXT DEFAULT 'system' CHECK(expense_type_source IN ('system', 'user')),
    is_active           INTEGER DEFAULT 1,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transactions (
    id                      TEXT PRIMARY KEY,
    account_id              TEXT REFERENCES accounts(id),
    profile_id              TEXT NOT NULL REFERENCES profiles(id),
    date                    TEXT NOT NULL,
    description             TEXT NOT NULL DEFAULT '',
    raw_description         TEXT DEFAULT '',
    amount                  REAL NOT NULL DEFAULT 0.0,
    category                TEXT REFERENCES categories(name),
    categorization_source   TEXT NOT NULL DEFAULT 'uncategorized',
    original_category       TEXT DEFAULT NULL,
    transaction_type        TEXT DEFAULT '',
    counterparty_name       TEXT DEFAULT '',
    counterparty_type       TEXT DEFAULT '',
    teller_category         TEXT DEFAULT '',
    account_name            TEXT DEFAULT '',
    account_type            TEXT DEFAULT '',
    merchant_name           TEXT DEFAULT '',
    merchant_domain         TEXT DEFAULT '',
    merchant_industry       TEXT DEFAULT '',
    merchant_city           TEXT DEFAULT '',
    merchant_state          TEXT DEFAULT '',
    merchant_key            TEXT DEFAULT '',
    merchant_source         TEXT DEFAULT '',
    merchant_confidence     TEXT DEFAULT '',
    merchant_kind           TEXT DEFAULT '',
    enriched                INTEGER DEFAULT 0,
    confidence              TEXT DEFAULT '',
    is_excluded             INTEGER DEFAULT 0,
    expense_type            TEXT DEFAULT NULL,
    description_normalized  TEXT DEFAULT NULL,
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_profile ON transactions(profile_id);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_transactions_profile_date ON transactions(profile_id, date);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
CREATE VIEW IF NOT EXISTS transactions_visible AS
SELECT *
FROM transactions
WHERE COALESCE(is_excluded, 0) = 0;
CREATE TABLE IF NOT EXISTS category_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern     TEXT NOT NULL,
    match_type  TEXT DEFAULT 'contains',
    category    TEXT NOT NULL REFERENCES categories(name),
    priority    INTEGER DEFAULT 100,
    source      TEXT NOT NULL DEFAULT 'user',
    profile_id  TEXT DEFAULT NULL,
    is_active   INTEGER DEFAULT 1,
    created_at  TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (category) REFERENCES categories(name),
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

CREATE INDEX IF NOT EXISTS idx_rules_source_priority ON category_rules(source, priority DESC);
CREATE INDEX IF NOT EXISTS idx_rules_active ON category_rules(is_active);

CREATE TABLE IF NOT EXISTS category_budgets (
    profile_id      TEXT NOT NULL,
    category        TEXT NOT NULL REFERENCES categories(name),
    amount          REAL NOT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (profile_id, category)
);

CREATE INDEX IF NOT EXISTS idx_category_budgets_profile ON category_budgets(profile_id);

CREATE TABLE IF NOT EXISTS net_worth_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    date        TEXT NOT NULL,
    profile_id  TEXT NOT NULL REFERENCES profiles(id),
    total_assets REAL DEFAULT 0.0,
    total_owed  REAL DEFAULT 0.0,
    net_worth   REAL DEFAULT 0.0,
    UNIQUE(date, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_nw_profile_date ON net_worth_history(profile_id, date);

CREATE TABLE IF NOT EXISTS copilot_conversations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id          TEXT REFERENCES profiles(id),
    user_message        TEXT NOT NULL,
    generated_sql       TEXT DEFAULT '',
    query_result        TEXT DEFAULT '',
    assistant_response  TEXT DEFAULT '',
    operation_type      TEXT DEFAULT 'read',
    rows_affected       INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS saved_insights (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id              TEXT REFERENCES profiles(id),
    question                TEXT NOT NULL,
    answer                  TEXT NOT NULL,
    kind                    TEXT NOT NULL DEFAULT 'insight' CHECK(kind IN ('insight', 'decision', 'policy_note')),
    pinned                  INTEGER NOT NULL DEFAULT 0,
    source_conversation_id  INTEGER REFERENCES copilot_conversations(id),
    created_at              TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_saved_insights_profile ON saved_insights(profile_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_saved_insights_pinned ON saved_insights(profile_id, pinned) WHERE pinned = 1;

CREATE TABLE IF NOT EXISTS memory_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      TEXT REFERENCES profiles(id),
    section         TEXT NOT NULL CHECK(section IN ('identity', 'preferences', 'goals', 'concerns', 'open_questions')),
    body            TEXT NOT NULL,
    confidence      TEXT NOT NULL DEFAULT 'stated' CHECK(confidence IN ('stated', 'saved', 'inferred')),
    evidence        TEXT DEFAULT '',
    theme           TEXT DEFAULT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    superseded_at   TEXT DEFAULT NULL,
    superseded_by   INTEGER REFERENCES memory_entries(id),
    expires_at      TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_entries_active ON memory_entries(profile_id, section) WHERE superseded_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_memory_entries_expiry ON memory_entries(expires_at) WHERE expires_at IS NOT NULL AND superseded_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_memory_entries_theme ON memory_entries(profile_id, theme) WHERE theme IS NOT NULL AND superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS memory_observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      TEXT REFERENCES profiles(id),
    theme           TEXT NOT NULL,
    note            TEXT NOT NULL,
    source_conversation_id INTEGER REFERENCES copilot_conversations(id),
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_obs_profile_theme ON memory_observations(profile_id, theme, created_at DESC);

CREATE TABLE IF NOT EXISTS memory_proposals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      TEXT REFERENCES profiles(id),
    section         TEXT NOT NULL CHECK(section IN ('identity', 'preferences', 'goals', 'concerns', 'open_questions')),
    body            TEXT NOT NULL,
    confidence      TEXT NOT NULL DEFAULT 'inferred' CHECK(confidence IN ('stated', 'saved', 'inferred')),
    evidence        TEXT DEFAULT '',
    theme           TEXT DEFAULT NULL,
    supersedes_id   INTEGER REFERENCES memory_entries(id),
    source          TEXT NOT NULL DEFAULT 'agent' CHECK(source IN ('agent', 'observation_threshold', 'consolidation', 'save_to_memory')),
    source_conversation_id INTEGER REFERENCES copilot_conversations(id),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'accepted', 'rejected')),
    created_at      TEXT DEFAULT (datetime('now')),
    resolved_at     TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_proposals_pending ON memory_proposals(profile_id, status, created_at DESC) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS subscription_seeds (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    pattern         TEXT NOT NULL,
    frequency_hint  TEXT DEFAULT 'monthly',
    category        TEXT DEFAULT 'Subscriptions',
    source          TEXT NOT NULL DEFAULT 'system' CHECK(source IN ('system', 'user')),
    created_by      TEXT DEFAULT NULL,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(pattern, source, created_by)
);

CREATE INDEX IF NOT EXISTS idx_sub_seeds_pattern ON subscription_seeds(pattern);
CREATE INDEX IF NOT EXISTS idx_sub_seeds_source ON subscription_seeds(source);
CREATE INDEX IF NOT EXISTS idx_sub_seeds_active ON subscription_seeds(is_active);

CREATE TABLE IF NOT EXISTS enrichment_cache (
    pattern_key         TEXT PRIMARY KEY,
    merchant_name       TEXT NOT NULL DEFAULT '',
    merchant_domain     TEXT DEFAULT '',
    merchant_industry   TEXT DEFAULT '',
    merchant_city       TEXT DEFAULT '',
    merchant_state      TEXT DEFAULT '',
    merchant_country    TEXT DEFAULT '',
    source              TEXT NOT NULL DEFAULT 'trove' CHECK(source IN ('trove', 'seed')),
    hit_count           INTEGER DEFAULT 1,
    first_seen          TEXT DEFAULT (datetime('now')),
    last_seen           TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_enrichment_cache_source ON enrichment_cache(source);

CREATE TABLE IF NOT EXISTS teller_category_map (
    teller_category     TEXT PRIMARY KEY,
    folio_category      TEXT DEFAULT NULL,
    confidence          TEXT NOT NULL DEFAULT 'rule-medium' CHECK(confidence IN ('rule-high', 'rule-medium')),
    source              TEXT NOT NULL DEFAULT 'system' CHECK(source IN ('system', 'user')),
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS enrolled_tokens (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile         TEXT    NOT NULL,
    institution     TEXT    NOT NULL DEFAULT '',
    token_encrypted TEXT    NOT NULL,
    owner_name      TEXT    NOT NULL DEFAULT '',
    enrollment_id   TEXT    DEFAULT NULL,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    is_active       INTEGER NOT NULL DEFAULT 1,
    UNIQUE(profile, token_encrypted)
);

CREATE INDEX IF NOT EXISTS idx_enrolled_tokens_profile ON enrolled_tokens(profile);
CREATE INDEX IF NOT EXISTS idx_enrolled_tokens_active ON enrolled_tokens(is_active);

CREATE TABLE IF NOT EXISTS merchants (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant_key        TEXT NOT NULL,
    clean_name          TEXT,
    logo_url            TEXT,
    domain              TEXT,
    category            TEXT,
    industry            TEXT,
    source              TEXT DEFAULT 'trove',
    is_subscription     INTEGER DEFAULT 0,
    subscription_frequency TEXT,
    subscription_amount REAL,
    subscription_status TEXT,
    cancelled_at        TEXT,
    cancelled_by_user   INTEGER DEFAULT 0,
    last_charge_date    TEXT,
    next_expected_date  TEXT,
    total_spent         REAL DEFAULT 0,
    charge_count        INTEGER DEFAULT 0,
    profile_id          TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(merchant_key, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_merchants_profile ON merchants(profile_id);
CREATE INDEX IF NOT EXISTS idx_merchants_subscription ON merchants(is_subscription, profile_id);
CREATE INDEX IF NOT EXISTS idx_merchants_key ON merchants(merchant_key);

CREATE TABLE IF NOT EXISTS user_declared_subscriptions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant_name       TEXT NOT NULL,
    amount              REAL NOT NULL,
    frequency           TEXT NOT NULL DEFAULT 'monthly',
    profile_id          TEXT NOT NULL,
    is_active           INTEGER DEFAULT 1,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(merchant_name, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_user_declared_subs_profile ON user_declared_subscriptions(profile_id);
CREATE INDEX IF NOT EXISTS idx_user_declared_subs_active ON user_declared_subscriptions(is_active, profile_id);

CREATE TABLE IF NOT EXISTS subscription_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type          TEXT NOT NULL,
    merchant_name       TEXT NOT NULL,
    profile_id          TEXT NOT NULL,
    detail              TEXT DEFAULT '{}',
    created_at          TEXT DEFAULT (datetime('now')),
    is_read             INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sub_events_profile ON subscription_events(profile_id);
CREATE INDEX IF NOT EXISTS idx_sub_events_unread ON subscription_events(is_read, profile_id);
CREATE INDEX IF NOT EXISTS idx_sub_events_type ON subscription_events(event_type);

CREATE TABLE IF NOT EXISTS dismissed_recurring (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant_name       TEXT NOT NULL,
    profile_id          TEXT NOT NULL,
    dismissed_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(merchant_name, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_dismissed_recurring_profile ON dismissed_recurring(profile_id);

CREATE TABLE IF NOT EXISTS simplefin_connections (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    profile             TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    access_url_encrypted TEXT NOT NULL,
    is_active           INTEGER NOT NULL DEFAULT 1,
    last_synced_at      TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sf_connections_profile ON simplefin_connections(profile);
CREATE INDEX IF NOT EXISTS idx_sf_connections_active  ON simplefin_connections(is_active);

CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# ══════════════════════════════════════════════════════════════════════════════
# SEEDING
# ══════════════════════════════════════════════════════════════════════════════

# Each entry is (name, expense_type).
# 'fixed'       = recurring / committed costs
# 'variable'    = discretionary spending
# 'non_expense' = transfers, income, credit card payments (excluded from F/V split)
DEFAULT_CATEGORIES = [
    ("Food & Dining",       "variable"),
    ("Groceries",           "variable"),
    ("Transportation",      "variable"),
    ("Entertainment",       "variable"),
    ("Shopping",            "variable"),
    ("Healthcare",          "variable"),
    ("Utilities",           "fixed"),
    ("Housing",             "fixed"),
    ("Savings Transfer",    "non_expense"),
    ("Credit Card Payment", "non_expense"),
    ("Income",              "non_expense"),
    ("Personal Transfer",   "non_expense"),
    ("Subscriptions",       "fixed"),
    ("Fees & Charges",      "variable"),
    ("Travel",              "variable"),
    ("Taxes",               "fixed"),
    ("Insurance",           "fixed"),
    ("Other",               "variable"),
]


def _seed_default_categories(conn: sqlite3.Connection):
    """Insert default categories if they don't exist."""
    for cat_name, exp_type in DEFAULT_CATEGORIES:
        conn.execute(
            "INSERT OR IGNORE INTO categories (name, is_system, expense_type, expense_type_source) VALUES (?, 1, ?, 'system')",
            (cat_name, exp_type),
        )

# System rules derived from categorizer.py's regex patterns
# These replace the hardcoded patterns — now queryable and editable
SYSTEM_RULES = [
    # Credit Card Payment patterns (high confidence)
    {"pattern": r"credit\s*c(?:a)?rd", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},
    {"pattern": r"credit\s*crd", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},
    {"pattern": r"\bautopay\b", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},
    {"pattern": r"applecard", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},
    {"pattern": r"gsbank.*payment", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},
    {"pattern": r"card\s*payment", "match_type": "regex", "category": "Credit Card Payment", "priority": 900, "source": "system"},

    # Savings Transfer patterns (high confidence)
    {"pattern": r"transfer\s+to\s+sav", "match_type": "regex", "category": "Savings Transfer", "priority": 900, "source": "system"},
    {"pattern": r"transfer\s+from\s+chk", "match_type": "regex", "category": "Savings Transfer", "priority": 900, "source": "system"},
    {"pattern": r"transfer\s+from\s+sav", "match_type": "regex", "category": "Savings Transfer", "priority": 900, "source": "system"},
    {"pattern": r"transfer\s+to\s+chk", "match_type": "regex", "category": "Savings Transfer", "priority": 900, "source": "system"},
    {"pattern": r"savings\s+transfer", "match_type": "regex", "category": "Savings Transfer", "priority": 900, "source": "system"},
    {"pattern": r"online\s+(?:scheduled\s+)?transfer", "match_type": "regex", "category": "Savings Transfer", "priority": 850, "source": "system"},
    {"pattern": r"internal\s+transfer", "match_type": "regex", "category": "Savings Transfer", "priority": 850, "source": "system"},
    {"pattern": r"account\s+transfer", "match_type": "regex", "category": "Savings Transfer", "priority": 850, "source": "system"},
    {"pattern": r"xfer\s+(?:to|from)", "match_type": "regex", "category": "Savings Transfer", "priority": 850, "source": "system"},
    {"pattern": r"mobile\s+transfer", "match_type": "regex", "category": "Savings Transfer", "priority": 850, "source": "system"},

    # Tax patterns (medium confidence)
    {"pattern": r"\birs\b", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"tax\s*(?:payment|pymt|pmt|refund)", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"\bus\s*treasury\b", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"state\s*tax", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"franchise\s*tax", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"tax\s*board", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},
    {"pattern": r"usataxpymt", "match_type": "regex", "category": "Taxes", "priority": 700, "source": "system"},

    # P2P patterns (medium confidence)
    {"pattern": r"\bzelle\b", "match_type": "regex", "category": "Personal Transfer", "priority": 600, "source": "system"},
    {"pattern": r"\bvenmo\b", "match_type": "regex", "category": "Personal Transfer", "priority": 600, "source": "system"},
    {"pattern": r"\bcashapp\b", "match_type": "regex", "category": "Personal Transfer", "priority": 600, "source": "system"},
    {"pattern": r"cash\s*app", "match_type": "regex", "category": "Personal Transfer", "priority": 600, "source": "system"},
    {"pattern": r"paypal.*(?:send|p2p|instant)", "match_type": "regex", "category": "Personal Transfer", "priority": 600, "source": "system"},
]


def _seed_system_rules(conn: sqlite3.Connection):
    """Insert system categorization rules if the table is empty."""
    count = conn.execute(
        "SELECT COUNT(*) FROM category_rules WHERE source = 'system'"
    ).fetchone()[0]

    if count > 0:
        return  # Already seeded

    for rule in SYSTEM_RULES:
        conn.execute(
            """INSERT INTO category_rules (pattern, match_type, category, priority, source)
               VALUES (?, ?, ?, ?, ?)""",
            (rule["pattern"], rule["match_type"], rule["category"], rule["priority"], rule["source"]),
        )

# Teller category → Folio category mapping defaults.
# Source: Teller API docs (28 documented values as of 2025).
# NULL folio_category means "no useful signal — skip, let other rules or LLM handle it."
# Users can override any mapping via source='user' rows in the DB.
TELLER_CATEGORY_DEFAULTS = [
    # (teller_category, folio_category, confidence)
    ("accommodation", "Travel",          "rule-medium"),
    ("advertising",   None,              "rule-medium"),
    ("bar",           "Food & Dining",   "rule-medium"),
    ("charity",       None,              "rule-medium"),
    ("clothing",      "Shopping",        "rule-medium"),
    ("dining",        "Food & Dining",   "rule-medium"),
    ("education",     None,              "rule-medium"),
    ("electronics",   "Shopping",        "rule-medium"),
    ("entertainment", "Entertainment",   "rule-medium"),
    ("fuel",          "Transportation",  "rule-medium"),
    ("general",       None,              "rule-medium"),
    ("groceries",     "Groceries",       "rule-medium"),
    ("health",        "Healthcare",      "rule-medium"),
    ("home",          "Housing",         "rule-medium"),
    ("income",        "Income",          "rule-medium"),
    ("insurance",     "Insurance",       "rule-medium"),
    ("investment",    None,              "rule-medium"),
    ("loan",          None,              "rule-medium"),
    ("office",        "Shopping",        "rule-medium"),
    ("phone",         "Utilities",       "rule-medium"),
    ("service",       None,              "rule-medium"),
    ("shopping",      "Shopping",        "rule-medium"),
    ("software",      "Subscriptions",   "rule-medium"),
    ("sport",         "Entertainment",   "rule-medium"),
    ("tax",           "Taxes",           "rule-medium"),
    ("transport",     "Transportation",  "rule-medium"),
    ("transportation","Transportation",  "rule-medium"),
    ("utilities",     "Utilities",       "rule-medium"),
]

def _seed_teller_category_map(conn: sqlite3.Connection):
    """
    Seed the teller_category_map table with system defaults.
    Uses INSERT OR IGNORE so user overrides (source='user') are never clobbered.
    Re-run safe — only inserts rows that don't already exist.
    """
    for teller_cat, folio_cat, confidence in TELLER_CATEGORY_DEFAULTS:
        conn.execute(
            """INSERT OR IGNORE INTO teller_category_map
               (teller_category, folio_category, confidence, source)
               VALUES (?, ?, ?, 'system')""",
            (teller_cat, folio_cat, confidence),
        )


def _extract_merchant_pattern(description: str) -> str:
    """
    Extract a reusable merchant pattern from a transaction description.
    Strips store numbers, locations, dates, phone numbers, and transaction-specific noise.
    """
    import re

    if not description:
        return ""

    text = description.upper().strip()

    # Remove common noise tokens BEFORE splitting
    text = re.sub(r"#\s*\d+", "", text)                    # #0742, # 123
    text = re.sub(r"\b\d{5,}\b", "", text)                 # Long numbers (zip, ID)
    text = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", "", text)  # Dates
    text = re.sub(r"\b\d{3}[-.]\d{3}[-.]\d{4}\b", "", text)  # Phone numbers (800-275-2273)
    text = re.sub(r"\b(?:CA|NY|TX|FL|WA|OR|AZ|NV|IL|GA|MA|PA|OH|NJ|NC|VA|CO|MD|CT|MN|WI|IN|TN|MO|SC|AL|LA|KY|OK|IA|MS|AR|KS|UT|NE|NM|WV|ID|HI|ME|NH|RI|MT|DE|SD|ND|AK|VT|WY|DC)\b", "", text)  # State codes
    text = re.sub(r"\b(?:CUPERTINO|SEATTLE|NEW YORK|SAN JOSE|AUSTIN|PORTLAND)\b", "", text)  # Common HQ cities
    text = re.sub(r"\bID:\*{3}\b", "", text)               # Sanitized IDs
    text = re.sub(r"\bCOM/BILL\b", "", text)               # APPLE.COM/BILL → APPLE.
    text = re.sub(r"\s+", " ", text).strip()

    # Take the first meaningful chunk (usually the merchant name)
    # Split on common delimiters
    parts = re.split(r"[\-\*]", text)
    if parts:
        text = parts[0].strip()

    # Remove trailing dots and whitespace
    text = text.rstrip(". ")

    # Final cleanup
    text = re.sub(r"\s+", " ", text).strip()

    # Don't create patterns from very short or very generic descriptions
    if len(text) < 3 or text in ("ACH", "DEBIT", "CREDIT", "PAYMENT", "TRANSFER", "CHECK", "UNKNOWN"):
        return ""

    return text


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

def sync_subscription_seeds():
    """
    Load subscription_seeds.json into the subscription_seeds table.
    System seeds are upserted on every startup.
    User-contributed seeds (source='user') are never touched.
    """
    import json as json_module
    from pathlib import Path as FilePath

    seed_path = FilePath(__file__).parent / "subscription_seeds.json"
    if not seed_path.exists():
        logger.info("subscription_seeds.json not found — skipping seed sync.")
        return

    with open(seed_path, "r", encoding="utf-8") as f:
        data = json_module.load(f)

    # Accept both top-level list and {"subscriptions": [...]} wrapper
    if isinstance(data, list):
        seeds = data
    elif isinstance(data, dict):
        seeds = data.get("subscriptions", data.get("merchants", []))
    else:
        seeds = []

    if not seeds:
        return

    with get_db() as conn:
        # Clear all system seeds and re-insert (ensures deletions in JSON are reflected)
        conn.execute("DELETE FROM subscription_seeds WHERE source = 'system'")

        for seed in seeds:
            name = seed.get("name", "")
            frequency_hint = seed.get("frequency_hint", "monthly")
            category = seed.get("category", "Subscriptions")

            for pattern in seed.get("patterns", []):
                conn.execute(
                    """INSERT OR IGNORE INTO subscription_seeds
                       (name, pattern, frequency_hint, category, source)
                       VALUES (?, ?, ?, ?, 'system')""",
                    (name, pattern.upper(), frequency_hint, category),
                )

    count = 0
    with get_db() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM subscription_seeds WHERE source = 'system'"
        ).fetchone()[0]
    logger.info("Subscription seeds synced: %d system patterns loaded.", count)


def sync_enrichment_cache_from_seeds():
    """
    Populate enrichment_cache with merchant identities derived from
    subscription_seeds. Runs on startup after seed sync.

    Only inserts seed-sourced entries — never overwrites Trove-sourced entries
    (which have richer metadata like domain, city, industry).

    Uses the same dedup key normalization as enricher.py so that runtime
    lookups match correctly.
    """
    import re as _re

    def _normalize_pattern_key(pattern: str) -> str:
        """Match enricher.py's _dedup_key normalization."""
        normalized = _re.sub(r"\d{6,}", "XXXXX", pattern)
        return normalized.upper().strip()

    with get_db() as conn:
        # Load all active subscription seeds
        rows = conn.execute(
            """SELECT DISTINCT name, pattern, category
               FROM subscription_seeds
               WHERE is_active = 1
               ORDER BY source DESC, length(pattern) DESC"""
        ).fetchall()

        if not rows:
            return

        inserted = 0
        for row in rows:
            name = row[0]
            pattern = row[1]
            category = row[2] or "Subscriptions"

            pattern_key = _normalize_pattern_key(pattern)

            if len(pattern_key) < 3:
                continue

            # INSERT OR IGNORE — never overwrite existing entries
            # (Trove entries are richer and should take precedence)
            result = conn.execute(
                """INSERT OR IGNORE INTO enrichment_cache
                   (pattern_key, merchant_name, merchant_industry, source)
                   VALUES (?, ?, ?, 'seed')""",
                (pattern_key, name, category),
            )
            if result.rowcount > 0:
                inserted += 1

    logger.info(
        "Enrichment cache seeded: %d patterns from subscription seeds.", inserted
    )

def upsert_merchant_from_enrichment(
    conn: sqlite3.Connection,
    merchant_key: str,
    enrichment: dict,
    profile_id: str,
    source: str = "trove",
):
    """
    Upsert the merchants table with enrichment data.
    Never overwrites fields where existing source = 'user'.
    Trove data can update trove-sourced fields and fill empty fields.
    """
    if not merchant_key or not profile_id:
        return

    clean_name = (enrichment.get("name") or enrichment.get("merchant_name") or "").strip()
    domain = (enrichment.get("domain") or enrichment.get("merchant_domain") or "").strip()
    category = (enrichment.get("category") or "").strip()
    industry = (enrichment.get("industry") or enrichment.get("merchant_industry") or "").strip()
    logo_url = (enrichment.get("logo_url") or "").strip()

    if not clean_name and not domain:
        return

    try:
        conn.execute(
            """INSERT INTO merchants
               (merchant_key, clean_name, logo_url, domain, category, industry, source, profile_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(merchant_key, profile_id) DO UPDATE SET
                   clean_name = CASE
                       WHEN merchants.source = 'user' AND merchants.clean_name != '' THEN merchants.clean_name
                       WHEN excluded.clean_name != '' THEN excluded.clean_name
                       ELSE merchants.clean_name END,
                   logo_url = CASE
                       WHEN excluded.logo_url != '' THEN excluded.logo_url
                       ELSE merchants.logo_url END,
                   domain = CASE
                       WHEN merchants.source = 'user' AND merchants.domain != '' THEN merchants.domain
                       WHEN excluded.domain != '' THEN excluded.domain
                       ELSE merchants.domain END,
                   category = CASE
                       WHEN merchants.source = 'user' AND merchants.category != '' THEN merchants.category
                       WHEN excluded.category != '' THEN excluded.category
                       ELSE merchants.category END,
                   industry = CASE
                       WHEN merchants.source = 'user' AND merchants.industry != '' THEN merchants.industry
                       WHEN excluded.industry != '' THEN excluded.industry
                       ELSE merchants.industry END,
                   source = CASE
                       WHEN merchants.source = 'user' THEN 'user'
                       ELSE excluded.source END,
                   updated_at = datetime('now')""",
            (merchant_key, clean_name, logo_url, domain, category, industry, source, profile_id),
        )
    except Exception as e:
        logger.debug("Merchant upsert failed for %s: %s", merchant_key, e)
# ══════════════════════════════════════════════════════════════════════════════
# NOTE: Database initialization is handled by main.py's startup event.
# Do NOT auto-initialize here — it causes double-init and can trigger
# DB operations during Docker build or test imports.
# ══════════════════════════════════════════════════════════════════════════════
