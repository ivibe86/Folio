"""
copilot.py
NLP-to-SQL copilot powered by Claude.
Supports read (SELECT) and write (UPDATE/INSERT) operations.
All PII is anonymized before sending to the LLM.
"""

import json
import re
import os
import httpx
import certifi
from datetime import date as dt_date
from dotenv import load_dotenv
from database import get_db, dicts_from_rows, _extract_merchant_pattern
from log_config import get_logger
from privacy import sanitize_rows_for_llm
import secrets as _secrets
import time as _time
import threading as _threading

load_dotenv()

logger = get_logger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

# Maximum rows a single copilot write operation can affect
COPILOT_MAX_WRITE_ROWS = int(os.getenv("COPILOT_MAX_WRITE_ROWS", "5000"))

# ══════════════════════════════════════════════════════════════════════════════
# [FIX C3] SERVER-SIDE PENDING SQL STORE
# Prevents clients from submitting arbitrary SQL at the confirm step.
# SQL is stored server-side with a short-lived nonce; client only gets the ID.
# ══════════════════════════════════════════════════════════════════════════════


_PENDING_SQL_TTL = 300  # 5 minutes
_pending_sql_lock = _threading.Lock()
_pending_sql_store: dict[str, dict] = {}  # {nonce: {"sql": ..., "profile": ..., "expires": ...}}


def store_pending_sql(sql: str, profile: str | None) -> str:
    """Store validated SQL server-side and return a nonce for the client."""
    nonce = _secrets.token_urlsafe(24)
    with _pending_sql_lock:
        # Cleanup expired entries
        now = _time.time()
        expired = [k for k, v in _pending_sql_store.items() if v["expires"] < now]
        for k in expired:
            del _pending_sql_store[k]
        # Store new entry
        _pending_sql_store[nonce] = {
            "sql": sql,
            "profile": profile,
            "expires": now + _PENDING_SQL_TTL,
        }
    return nonce


def retrieve_pending_sql(nonce: str) -> dict | None:
    """Retrieve and consume stored SQL by nonce. Returns None if expired/missing."""
    with _pending_sql_lock:
        entry = _pending_sql_store.pop(nonce, None)
    if entry is None:
        return None
    if _time.time() > entry["expires"]:
        return None
    return entry
# ══════════════════════════════════════════════════════════════════════════════
# SQL SAFETY VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

_ALLOWED_READ_TABLES = {"transactions", "accounts", "categories", "category_rules", "net_worth_history"}
_ALLOWED_WRITE_CONFIGS = {
    ("transactions", "UPDATE"): {"category", "categorization_source", "is_excluded", "updated_at", "original_category", "confidence"},
    ("category_rules", "INSERT"): None,
    ("category_rules", "UPDATE"): {"category", "priority", "is_active"},
    ("categories", "INSERT"): None,
}

_FORBIDDEN_KEYWORDS = {
    "DROP", "ALTER", "TRUNCATE", "VACUUM", "ATTACH", "DETACH",
    "PRAGMA", "REINDEX", "ANALYZE", "EXPLAIN",
    "CREATE TABLE", "CREATE INDEX", "DROP TABLE", "DROP INDEX",
}


def _validate_read_sql(sql: str) -> tuple[bool, str]:
    cleaned = sql.strip()
    upper = cleaned.upper()

    statements = _split_sql_statements(cleaned)
    if len(statements) > 1:
        return False, "Multiple statements are not allowed in read queries."

    if not upper.startswith("SELECT"):
        return False, "Read queries must be SELECT statements."

    for keyword in _FORBIDDEN_KEYWORDS:
        if keyword in upper:
            return False, f"Forbidden SQL keyword: {keyword}"

    for write_op in ("INSERT", "UPDATE", "DELETE", "REPLACE"):
        if _keyword_outside_strings(upper, write_op):
            return False, f"Write operation '{write_op}' not allowed in read queries."

    return True, ""


def _validate_write_sql(sql: str) -> tuple[bool, str]:
    statements = _split_sql_statements(sql)

    for stmt in statements:
        upper = stmt.upper().strip()

        for keyword in _FORBIDDEN_KEYWORDS:
            if keyword in upper:
                return False, f"Forbidden SQL keyword: {keyword}"

        if upper.startswith("UPDATE"):
            table = _extract_table_name(stmt, "UPDATE")
            if not table:
                return False, "Could not determine target table for UPDATE."
            allowed_columns = _ALLOWED_WRITE_CONFIGS.get((table.lower(), "UPDATE"))
            if allowed_columns is None and (table.lower(), "UPDATE") not in _ALLOWED_WRITE_CONFIGS:
                return False, f"UPDATE on table '{table}' is not allowed."

            # [FIX C1] Enforce column allowlist — parse SET clause columns
            if allowed_columns is not None:
                set_columns = _extract_update_columns(stmt)
                if not set_columns:
                    return False, "Could not parse SET clause columns in UPDATE."
                disallowed = set_columns - allowed_columns
                if disallowed:
                    return False, (
                        f"UPDATE modifies disallowed column(s): {', '.join(sorted(disallowed))}. "
                        f"Allowed: {', '.join(sorted(allowed_columns))}"
                    )

            # [FIX C2] Reject subqueries in SET values — prevents data exfiltration
            set_clause = _extract_set_clause(stmt)
            if set_clause and _keyword_outside_strings(set_clause.upper(), "SELECT"):
                return False, "Subqueries (SELECT) are not allowed in UPDATE SET values."

        elif upper.startswith("INSERT"):
            table = _extract_table_name(stmt, "INSERT")
            if not table:
                return False, "Could not determine target table for INSERT."
            if (table.lower(), "INSERT") not in _ALLOWED_WRITE_CONFIGS:
                return False, f"INSERT into table '{table}' is not allowed."

            # [FIX C2] Reject subqueries in INSERT VALUES too
            if _keyword_outside_strings(upper, "SELECT"):
                return False, "Subqueries (SELECT) are not allowed in INSERT statements."

        elif upper.startswith("DELETE"):
            return False, "DELETE operations are not allowed through the copilot."

        elif upper.startswith("SELECT"):
            continue

        else:
            return False, f"Unsupported SQL operation: {stmt[:30]}..."

    return True, ""


def _extract_update_columns(stmt: str) -> set[str] | None:
    """
    [FIX C1] Parse column names from an UPDATE ... SET col1=val1, col2=val2 ... WHERE ...
    Returns a set of lowercase column names, or None if parsing fails.
    """
    # Match everything between SET and WHERE (or end of statement)
    set_clause = _extract_set_clause(stmt)
    if not set_clause:
        return None

    columns = set()
    # Split on commas that are outside parentheses and quotes
    depth = 0
    in_sq = False
    in_dq = False
    current = []
    for ch in set_clause:
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                part = "".join(current).strip()
                col = part.split("=")[0].strip().lower() if "=" in part else None
                if col:
                    columns.add(col)
                current = []
                continue
        current.append(ch)

    # Last assignment
    part = "".join(current).strip()
    if "=" in part:
        col = part.split("=")[0].strip().lower()
        if col:
            columns.add(col)

    return columns if columns else None


def _extract_set_clause(stmt: str) -> str | None:
    """Extract the text between SET and WHERE (or end) in an UPDATE statement."""
    upper = stmt.upper()
    set_match = re.search(r"\bSET\b", upper)
    if not set_match:
        return None
    start = set_match.end()

    # Find WHERE outside of quotes
    where_pos = None
    in_sq = False
    in_dq = False
    i = start
    while i < len(upper):
        ch = upper[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if upper[i:i+5] == "WHERE" and (i == 0 or not upper[i-1].isalnum()):
                where_pos = i
                break
        i += 1

    if where_pos:
        return stmt[start:where_pos].strip()
    return stmt[start:].strip()


def _split_sql_statements(sql: str) -> list[str]:
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif char == ";" and not in_single_quote and not in_double_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue
        current.append(char)
        i += 1
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


def _keyword_outside_strings(upper_sql: str, keyword: str) -> bool:
    """Check if a keyword appears outside of string literals in SQL."""
    in_single_quote = False
    in_double_quote = False
    kw_len = len(keyword)
    for i in range(len(upper_sql)):
        if upper_sql[i] == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif upper_sql[i] == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif not in_single_quote and not in_double_quote:
            if upper_sql[i:i + kw_len] == keyword:
                before_ok = (i == 0 or not upper_sql[i - 1].isalnum())
                after_ok = (i + kw_len >= len(upper_sql) or not upper_sql[i + kw_len].isalnum())
                if before_ok and after_ok:
                    return True
    return False


def _extract_table_name(stmt: str, operation: str) -> str | None:
    upper = stmt.upper().strip()
    if operation == "UPDATE":
        match = re.match(r"UPDATE\s+(\w+)", upper)
        return match.group(1).lower() if match else None
    elif operation == "INSERT":
        match = re.match(r"INSERT\s+(?:OR\s+\w+\s+)?INTO\s+(\w+)", upper)
        return match.group(1).lower() if match else None
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PII ANONYMIZATION
# ══════════════════════════════════════════════════════════════════════════════

def _build_profile_map() -> tuple[dict[str, str], dict[str, str]]:
    with get_db() as conn:
        rows = conn.execute("SELECT id, display_name FROM profiles ORDER BY id").fetchall()

    real_to_alias = {}
    alias_to_real = {}

    for i, row in enumerate(rows):
        real_id = row[0]
        alias = f"profile_{i + 1}"
        real_to_alias[real_id] = alias
        alias_to_real[alias] = real_id

    real_to_alias["household"] = "all_profiles"
    alias_to_real["all_profiles"] = "household"

    return real_to_alias, alias_to_real


def _anonymize_sql_result(rows: list[dict], real_to_alias: dict) -> list[dict]:
    anonymized = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if k == "profile_id" and isinstance(v, str) and v in real_to_alias:
                new_row[k] = real_to_alias[v]
            else:
                new_row[k] = v
        anonymized.append(new_row)
    return anonymized


def _deanoymize_sql(sql: str, alias_to_real: dict) -> str:
    result = sql
    for alias, real_id in alias_to_real.items():
        result = result.replace(f"'{alias}'", f"'{real_id}'")
        result = result.replace(f'"{alias}"', f'"{real_id}"')
    return result


def _sanitize_profile_filter(sql: str) -> str:
    patterns = [
        r"\s*AND\s+profile_id\s+IN\s*\(\s*'household'\s*\)",
        r"\s*AND\s+profile_id\s*=\s*'household'",
        r"\s*AND\s+profile_id\s+IN\s*\(\s*'all_profiles'\s*\)",
        r"\s*AND\s+profile_id\s*=\s*'all_profiles'",
        r"\s*AND\s+\(?\s*profile_id\s*=\s*'household'\s*\)?",
        r"\s*AND\s+\(?\s*profile_id\s*=\s*'all_profiles'\s*\)?",
    ]
    result = sql
    for pat in patterns:
        result = re.sub(pat, "", result, flags=re.IGNORECASE)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA CONTEXT FOR CLAUDE (anonymized)
# ══════════════════════════════════════════════════════════════════════════════

def _build_schema_context(real_to_alias: dict) -> str:
    with get_db() as conn:
        cats = conn.execute(
            "SELECT name FROM categories WHERE is_active = 1 ORDER BY name"
        ).fetchall()

    cat_list = ", ".join(f'"{r[0]}"' for r in cats)

    profile_lines = "\n".join(
        f"  - '{alias}'" for alias in sorted(real_to_alias.values())
        if alias != "all_profiles"
    )

    return f"""SQLite database schema:

TABLE: transactions
  - id (TEXT, PK) — transaction ID
  - account_id (TEXT, FK → accounts.id)
  - profile_id (TEXT) — owner profile
  - date (TEXT, format 'YYYY-MM-DD')
  - description (TEXT) — merchant/payee description
  - amount (REAL) — negative = expense, positive = income
  - category (TEXT) — one of: {cat_list}
  - categorization_source (TEXT) — 'rule-high', 'llm', 'user', 'user-rule', 'fallback'
  - account_name (TEXT)
  - account_type (TEXT) — 'checking', 'savings', 'credit_card', 'credit'
  - merchant_name (TEXT)
  - enriched (INTEGER, 0 or 1)
  - is_excluded (INTEGER, 0 or 1)

TABLE: accounts
  - id (TEXT, PK)
  - profile_id (TEXT)
  - account_name (TEXT)
  - account_type (TEXT) — 'depository' or 'credit'
  - account_subtype (TEXT) — 'checking', 'savings', 'credit_card'
  - current_balance (REAL)

TABLE: categories
  - name (TEXT, UNIQUE)
  - is_system (INTEGER)
  - is_active (INTEGER)

TABLE: category_rules
  - id (INTEGER, PK)
  - pattern (TEXT) — substring or regex to match against description
  - match_type (TEXT) — 'contains' or 'regex'
  - category (TEXT) — target category
  - priority (INTEGER) — higher = checked first
  - source (TEXT) — 'system' or 'user'
  - is_active (INTEGER)

TABLE: net_worth_history
  - date (TEXT)
  - profile_id (TEXT)
  - total_assets (REAL)
  - total_owed (REAL)
  - net_worth (REAL)

Available profiles:
{profile_lines}
  - 'all_profiles' (means no profile filter — all data)

Today's date: {dt_date.today().isoformat()}

IMPORTANT RULES:
- Expenses have amount < 0
- Income has amount > 0 AND category = 'Income'
- When calculating spending totals, use ABS(amount) for readability
- Categories that are NOT real spending: 'Savings Transfer', 'Personal Transfer', 'Credit Card Payment', 'Income'
- Refunds are amount > 0 AND category NOT IN ('Income', 'Savings Transfer', 'Personal Transfer', 'Credit Card Payment')
- Always filter is_excluded = 0 unless explicitly asked about excluded transactions
- For write operations, return the SQL wrapped in WRITE: prefix
- For read operations, return plain SQL

CRITICAL — profile_id FILTERING:
- 'all_profiles' is a VIRTUAL profile. No transaction row has profile_id = 'all_profiles' or profile_id = 'household'.
- Every transaction is stored with a REAL profile_id (e.g. 'profile_1', 'profile_2').
- When the active profile is 'all_profiles', do NOT add any profile_id filter at all — just omit it.
- When the active profile is a specific profile like 'profile_1', filter by profile_id = 'profile_1'.
- NEVER use profile_id = 'all_profiles' or profile_id IN ('all_profiles') in any query.

DESCRIPTION MATCHING:
- Always use UPPER(description) LIKE UPPER('%pattern%') for case-insensitive matching.
- When the user mentions a merchant name partially (e.g. "beverages"), use a broad LIKE pattern: UPPER(description) LIKE UPPER('%beverages%')
- Do NOT guess full merchant descriptions. Use only the keywords the user provides.
"""


# ══════════════════════════════════════════════════════════════════════════════
# CORE: Ask the Copilot
# ══════════════════════════════════════════════════════════════════════════════

def ask_copilot(
    question: str,
    profile: str | None = None,
    confirm_write: bool = False,
    pending_sql: str | None = None,
) -> dict:
    if not ANTHROPIC_API_KEY:
        return {
            "answer": "Copilot is not configured. Please set ANTHROPIC_API_KEY.",
            "sql": "",
            "data": None,
            "operation": "error",
            "rows_affected": 0,
            "needs_confirmation": False,
        }

    # ── Handle write confirmation ──
    if confirm_write and pending_sql:
        return _execute_write(pending_sql, profile, question)

    # ── Build anonymized context ──
    real_to_alias, alias_to_real = _build_profile_map()

    profile_alias = "all_profiles"
    if profile and profile != "household":
        profile_alias = real_to_alias.get(profile, "profile_1")

    schema_context = _build_schema_context(real_to_alias)

    prompt = f"""{schema_context}

The user's currently active profile is: '{profile_alias}'
If the question doesn't specify a profile, filter by the active profile.
If the active profile is 'all_profiles', do NOT add any profile_id WHERE clause at all — return results across all profiles.
REMINDER: No rows exist with profile_id = 'all_profiles'. Omit the filter entirely.

User question: {question}

Generate a single SQLite query to answer this question.
- For SELECT queries, return just the SQL
- For UPDATE/INSERT/DELETE operations, prefix with "WRITE:" 
  For WRITE operations that recategorize:
    1. Generate the UPDATE for transactions table
    2. Also include an INSERT OR REPLACE into category_rules if the user is setting a pattern
- Do not use markdown. Return only the raw SQL.
- If the question cannot be answered with SQL, respond with: CANNOT: <explanation>
"""

    # ── Call Claude ──
    try:
        resp = httpx.post(
            ANTHROPIC_URL,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-3-haiku-20240307",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30.0,
            verify=certifi.where(),
        )

        result = resp.json()
        if "content" not in result:
            return _error_response(f"API error: {result}")

        raw_sql = result["content"][0]["text"].strip()

    except Exception as e:
        logger.exception("Failed to reach Claude for copilot query")
        return _error_response(f"Failed to reach Claude: {e}")

    # ── Handle CANNOT responses ──
    if raw_sql.startswith("CANNOT:"):
        return {
            "answer": raw_sql.replace("CANNOT:", "").strip(),
            "sql": "",
            "data": None,
            "operation": "error",
            "rows_affected": 0,
            "needs_confirmation": False,
        }

    # ── Handle WRITE operations (preview only — needs confirmation) ──
    if raw_sql.startswith("WRITE:"):
        sql_body = raw_sql.replace("WRITE:", "").strip()
        sql_body = re.sub(r"^```\w*\n?", "", sql_body)
        sql_body = re.sub(r"\n?```$", "", sql_body)

        real_sql = _deanoymize_sql(sql_body, alias_to_real)
        real_sql = _sanitize_profile_filter(real_sql)

        is_valid, validation_error = _validate_write_sql(real_sql)
        if not is_valid:
            return _error_response(f"Write validation failed: {validation_error}")

        preview = _preview_write(real_sql, profile)

        # [FIX C3] Store SQL server-side — client only gets a confirmation_id
        confirmation_id = store_pending_sql(real_sql, profile)

        _log_conversation(profile, question, real_sql, str(preview), "", "write_preview")

        return {
            "answer": f"This will affect {preview['count']} transaction(s). Confirm to proceed.",
            "sql": real_sql,
            "confirmation_id": confirmation_id,
            "data": preview.get("sample", []),
            "operation": "write_preview",
            "rows_affected": preview["count"],
            "needs_confirmation": True,
        }
    # ── Handle READ operations ──
    sql = raw_sql.strip()
    sql = re.sub(r"^```\w*\n?", "", sql)
    sql = re.sub(r"\n?```$", "", sql)

    real_sql = _deanoymize_sql(sql, alias_to_real)
    if not profile or profile == "household":
        real_sql = _sanitize_profile_filter(real_sql)

    is_valid, validation_error = _validate_read_sql(real_sql)
    if not is_valid:
        return _error_response(f"SQL validation failed: {validation_error}")

    try:
        with get_db() as conn:
            conn.execute("PRAGMA query_only = ON")
            try:
                rows = dicts_from_rows(conn.execute(real_sql).fetchall())
            finally:
                conn.execute("PRAGMA query_only = OFF")
    except Exception as e:
        logger.error("Copilot SQL execution error: %s | SQL: %s", e, real_sql)
        return _error_response(f"SQL execution error: {e}")

    # ── Send results back to Claude for natural language answer ──
    anon_rows = _anonymize_sql_result(rows, real_to_alias)
    display_rows = anon_rows[:50]
    # Mask amounts, counterparty names, and other PII before sending to LLM
    privacy_safe_rows = sanitize_rows_for_llm(display_rows)

    answer = _generate_natural_answer(question, privacy_safe_rows)

    _log_conversation(profile, question, real_sql, json.dumps(display_rows), answer, "read")

    return {
        "answer": answer,
        "sql": real_sql,
        "data": rows[:100],
        "operation": "read",
        "rows_affected": len(rows),
        "needs_confirmation": False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_categories_exist(conn, statements: list[str]):
    for stmt in statements:
        upper = stmt.upper()

        set_match = re.search(
            r"SET\s+category\s*=\s*'([^']+)'",
            stmt, re.IGNORECASE
        )
        if set_match:
            cat = set_match.group(1)
            conn.execute(
                """INSERT OR IGNORE INTO categories (name, is_system, is_active)
                   VALUES (?, 0, 1)""",
                (cat,),
            )

        if "CATEGORY_RULES" in upper and "INSERT" in upper:
            vals_match = re.search(
                r"VALUES\s*\(([^)]+)\)",
                stmt, re.IGNORECASE
            )
            if vals_match:
                vals = vals_match.group(1)
                parts = [v.strip().strip("'\"") for v in vals.split(",")]
                if len(parts) >= 3:
                    cat = parts[2]
                    conn.execute(
                        """INSERT OR IGNORE INTO categories (name, is_system, is_active)
                           VALUES (?, 0, 1)""",
                        (cat,),
                    )


def _generate_natural_answer(question: str, rows: list[dict]) -> str:
    if not rows:
        return "No results found for your query."

    result_str = json.dumps(rows[:25], indent=2, default=str)

    prompt = f"""The user asked: "{question}"

The database returned these results (dollar amounts and personal names have been anonymized for privacy):
{result_str}

{"(Showing first 25 of " + str(len(rows)) + " total results)" if len(rows) > 25 else ""}

Provide a clear, concise natural language answer.
IMPORTANT: The dollar amounts in the results are masked as "$XXX" for privacy. Do NOT invent specific dollar figures.
When discussing amounts, use general terms like "your spending" or "the total" — do not fabricate numbers.
If the user asked for a specific number, say that the exact figures are available in the data view.
If there are multiple rows, summarize the key findings (counts, categories, patterns).
Do not mention SQL, databases, or queries in your answer.
Do not reveal any personal names — use generic terms like "your account" instead."""

    try:
        resp = httpx.post(
            ANTHROPIC_URL,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-3-haiku-20240307",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30.0,
            verify=certifi.where(),
        )

        result = resp.json()
        if "content" in result:
            return result["content"][0]["text"].strip()
    except Exception:
        logger.exception("Failed to generate natural language answer from Claude")

    if len(rows) == 1 and len(rows[0]) == 1:
        val = list(rows[0].values())[0]
        if isinstance(val, (int, float)):
            return f"${val:,.2f}"
        return str(val)

    return f"Found {len(rows)} results."


def _preview_write(sql: str, profile: str | None) -> dict:
    """
    [FIX C4] Preview uses PRAGMA query_only to prevent any writes,
    and validates the constructed SQL before execution.
    """
    try:
        # Only preview the first UPDATE statement
        statements = _split_sql_statements(sql)
        update_stmt = None
        for stmt in statements:
            if stmt.upper().strip().startswith("UPDATE"):
                update_stmt = stmt
                break

        if not update_stmt:
            return {"count": 0, "sample": []}

        # Extract WHERE clause safely using quote-aware parsing
        where_clause = _extract_where_clause(update_stmt)
        if not where_clause:
            return {"count": 0, "sample": []}

        # Build and validate preview queries
        preview_sql = f"SELECT id, description, amount, category, date FROM transactions {where_clause} LIMIT 20"
        count_sql = f"SELECT COUNT(*) as cnt FROM transactions {where_clause}"

        # Validate both as read queries
        is_valid, _ = _validate_read_sql(preview_sql)
        if not is_valid:
            return {"count": 0, "sample": []}
        is_valid, _ = _validate_read_sql(count_sql)
        if not is_valid:
            return {"count": 0, "sample": []}

        with get_db() as conn:
            conn.execute("PRAGMA query_only = ON")
            try:
                rows = dicts_from_rows(conn.execute(preview_sql).fetchall())
                count = conn.execute(count_sql).fetchone()[0]
            finally:
                conn.execute("PRAGMA query_only = OFF")

        return {"count": count, "sample": rows}
    except Exception:
        logger.debug("Write preview failed, returning empty preview", exc_info=True)

    return {"count": 0, "sample": []}


def _extract_where_clause(stmt: str) -> str | None:
    """
    [FIX C4] Quote-aware extraction of WHERE clause from a SQL statement.
    Returns 'WHERE ...' portion or None if not found.
    """
    upper = stmt.upper()
    in_sq = False
    in_dq = False
    i = 0
    while i < len(upper):
        ch = upper[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if upper[i:i+5] == "WHERE" and (i == 0 or not upper[i-1].isalnum()):
                if i + 5 >= len(upper) or not upper[i+5].isalnum():
                    # Return from WHERE to end, trimming trailing semicolons
                    clause = stmt[i:].rstrip().rstrip(";").strip()
                    return clause
        i += 1
    return None


def _execute_write(sql: str, profile: str | None, original_question: str) -> dict:
    """
    [FIX C5] Uses safe WHERE clause extraction and validates count queries.
    """
    try:
        is_valid, validation_error = _validate_write_sql(sql)
        if not is_valid:
            return _error_response(f"Write validation failed: {validation_error}")

        statements = _split_sql_statements(sql)

        total_affected = 0
        with get_db() as conn:
            _ensure_categories_exist(conn, statements)
            for stmt in statements:
                upper = stmt.upper().strip()
                if not any(upper.startswith(op) for op in ("UPDATE", "INSERT")):
                    continue

                if upper.startswith("UPDATE"):
                    # [FIX C5] Safe WHERE extraction + validated count query
                    where_clause = _extract_where_clause(stmt)
                    table = _extract_table_name(stmt, "UPDATE")
                    if where_clause and table:
                        # Only count against allowed tables
                        if table.lower() in ("transactions", "category_rules"):
                            count_sql = f"SELECT COUNT(*) FROM {table} {where_clause}"
                            valid, _ = _validate_read_sql(count_sql)
                            if valid:
                                try:
                                    count = conn.execute(count_sql).fetchone()[0]
                                    if count > COPILOT_MAX_WRITE_ROWS:
                                        return _error_response(
                                            f"This operation would affect {count} rows, "
                                            f"exceeding the safety limit of {COPILOT_MAX_WRITE_ROWS}. "
                                            f"Please narrow your query."
                                        )
                                except Exception:
                                    logger.debug("Row count pre-check failed for copilot write", exc_info=True)
                    elif not where_clause and table:
                        # UPDATE without WHERE — count all rows as safety check
                        try:
                            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                            if count > COPILOT_MAX_WRITE_ROWS:
                                return _error_response(
                                    f"UPDATE without WHERE would affect {count} rows, "
                                    f"exceeding the safety limit of {COPILOT_MAX_WRITE_ROWS}."
                                )
                        except Exception:
                            logger.debug("Row count pre-check failed for copilot write", exc_info=True)

                cursor = conn.execute(stmt)
                total_affected += cursor.rowcount

                if upper.startswith("UPDATE") and "CATEGORY" in upper and "TRANSACTIONS" in upper:
                    _auto_create_rules_from_update(conn, stmt)

        _log_conversation(profile, original_question, sql, "", f"Affected {total_affected} rows", "write_executed", total_affected)

        return {
            "answer": f"Done! Updated {total_affected} transaction(s).",
            "sql": sql,
            "data": None,
            "operation": "write_executed",
            "rows_affected": total_affected,
            "needs_confirmation": False,
        }

    except Exception as e:
        logger.exception("Copilot write execution failed")
        return _error_response(f"Write failed: {e}")


def _auto_create_rules_from_update(conn, update_sql: str):
    """[FIX C5] Uses safe WHERE extraction and read-validated query."""
    try:
        set_match = re.search(r"SET\s+category\s*=\s*'([^']+)'", update_sql, re.IGNORECASE)
        if not set_match:
            return
        new_category = set_match.group(1)

        where_clause = _extract_where_clause(update_sql)
        if not where_clause:
            return

        select_sql = f"SELECT DISTINCT description FROM transactions {where_clause}"
        is_valid, _ = _validate_read_sql(select_sql)
        if not is_valid:
            return
        rows = conn.execute(select_sql).fetchall()

        for row in rows:
            pattern = _extract_merchant_pattern(row[0])
            if pattern and len(pattern) >= 3:
                conn.execute(
                    """INSERT OR REPLACE INTO category_rules
                       (pattern, match_type, category, priority, source, is_active)
                       VALUES (?, 'contains', ?, 1000, 'user', 1)""",
                    (pattern, new_category),
                )

    except Exception:
        logger.debug("Auto-create rules from copilot update failed", exc_info=True)


def _log_conversation(
    profile: str | None,
    question: str,
    sql: str,
    result: str,
    answer: str,
    operation: str,
    rows_affected: int = 0,
):
    try:
        with get_db() as conn:
            conn.execute(
                """INSERT INTO copilot_conversations
                   (profile_id, user_message, generated_sql, query_result,
                    assistant_response, operation_type, rows_affected)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (profile, question, sql, result[:5000], answer, operation, rows_affected),
            )
    except Exception:
        logger.debug("Failed to log copilot conversation", exc_info=True)


def _error_response(message: str) -> dict:
    return {
        "answer": message,
        "sql": "",
        "data": None,
        "operation": "error",
        "rows_affected": 0,
        "needs_confirmation": False,
    }