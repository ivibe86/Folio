"""
database.py
SQLite database initialization, connection management, and schema definition.
"""

import sqlite3
import os
import threading
from pathlib import Path
from contextlib import contextmanager
from dotenv import load_dotenv
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


def get_connection() -> sqlite3.Connection:
    """
    Get a thread-local SQLite connection.
    Returns the same connection for the same thread (reuse within request).

    DEPRECATED: Use get_db_session() with FastAPI Depends() for request-scoped
    connections that are properly closed. This function is retained only for
    background tasks (e.g., sync) and module-level code paths that cannot
    use FastAPI dependency injection.
    """
    if not hasattr(_local, "connection") or _local.connection is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        _local.connection = conn
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
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
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
        _migrate_merchants_table(conn)
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
    enriched                INTEGER DEFAULT 0,
    confidence              TEXT DEFAULT '',
    is_excluded             INTEGER DEFAULT 0,
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_profile ON transactions(profile_id);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_transactions_profile_date ON transactions(profile_id, date);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);

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
    category = (enrichment.get("category") or enrichment.get("industry") or enrichment.get("merchant_industry") or "").strip()
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