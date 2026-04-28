"""
copilot.py
NLP-to-SQL copilot powered by Claude.
Supports read (SELECT) and write (UPDATE/INSERT) operations.
All PII is anonymized before sending to the LLM.
"""

import json
import re
import os
from datetime import date as dt_date
from dotenv import load_dotenv
from database import get_db, dicts_from_rows, _extract_merchant_pattern
from log_config import get_logger
from privacy import sanitize_rows_for_llm
from copilot_context import build_copilot_context
import llm_client
import secrets as _secrets
import time as _time
import threading as _threading

load_dotenv()

logger = get_logger(__name__)

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
READ_TRANSACTIONS_TABLE = "transactions_visible"


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

_ALLOWED_READ_TABLES = {"transactions", "transactions_visible", "accounts", "categories", "category_rules", "net_worth_history"}
_ALLOWED_WRITE_CONFIGS = {
    ("transactions", "UPDATE"): {"category", "categorization_source", "is_excluded", "updated_at", "original_category", "confidence", "merchant_name"},
    ("category_rules", "INSERT"): None,
    ("category_rules", "UPDATE"): {"category", "priority", "is_active"},
    ("categories", "INSERT"): None,
    ("merchants", "UPDATE"): {"clean_name", "updated_at"},
}

_FORBIDDEN_KEYWORDS = {
    "DROP", "ALTER", "TRUNCATE", "VACUUM", "ATTACH", "DETACH",
    "PRAGMA", "REINDEX", "ANALYZE", "EXPLAIN",
    "CREATE TABLE", "CREATE INDEX", "DROP TABLE", "DROP INDEX",
}
_REPAIRABLE_SQLITE_ERROR_MARKERS = (
    "no such function",
    "syntax error",
    "no such column",
    "ambiguous column name",
    "misuse of aggregate",
    "wrong number of arguments",
)


def _validate_read_sql(sql: str) -> tuple[bool, str]:
    cleaned = sql.strip()
    upper = cleaned.upper()

    statements = _split_sql_statements(cleaned)
    if len(statements) > 1:
        return False, "Multiple statements are not allowed in read queries."

    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return False, "Read queries must start with SELECT or a read-only WITH clause."

    for keyword in _FORBIDDEN_KEYWORDS:
        if keyword in upper:
            return False, f"Forbidden SQL keyword: {keyword}"

    for write_op in ("INSERT", "UPDATE", "DELETE", "REPLACE"):
        if _keyword_outside_strings(upper, write_op):
            return False, f"Write operation '{write_op}' not allowed in read queries."

    return True, ""


def _rewrite_transaction_read_sources(sql: str) -> str:
    """Route read-only transaction queries through the canonical visible view."""
    return re.sub(
        r"(?i)\b(FROM|JOIN)\s+transactions\b",
        lambda match: f"{match.group(1)} {READ_TRANSACTIONS_TABLE}",
        sql,
    )


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
    for part in _split_assignments(set_clause):
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


def _split_assignments(set_clause: str) -> list[str]:
    """Split a SQL SET clause into individual assignments."""
    parts = []
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
                if part:
                    parts.append(part)
                current = []
                continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


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


def _anonymize_sql_text(sql: str, real_to_alias: dict[str, str]) -> str:
    result = sql
    for real_id, alias in real_to_alias.items():
        result = result.replace(f"'{real_id}'", f"'{alias}'")
        result = result.replace(f'"{real_id}"', f'"{alias}"')
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


def _should_mask_copilot_results_for_llm() -> bool:
    """Mask exact financial values only when results leave the local machine."""
    try:
        return llm_client.get_provider() != "ollama"
    except Exception:
        return True


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

TABLE: transactions_visible
  - read-only filtered view over transactions
  - contains only rows where is_excluded = 0
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
  - merchant_name (TEXT) — human display label
  - merchant_key (TEXT) — stable merchant grouping key
  - merchant_kind (TEXT) — merchant_purchase, personal_transfer, credit_card_payment, income, tax, bank_fee, unknown
  - enriched (INTEGER, 0 or 1)
  - is_excluded (INTEGER, 0 or 1)

TABLE: transactions
  - base table backing transactions_visible
  - use this only for write operations

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

MERCHANT REPORTING:
- merchant_key and merchant_name can be NULL or blank for transactions that have not been enriched yet.
- For merchant rankings, "biggest merchant", top merchants, or merchant grouping, never GROUP BY merchant_name alone.
- Use COALESCE(NULLIF(TRIM(merchant_key), ''), NULLIF(TRIM(merchant_name), ''), description) for grouping.
- Use COALESCE(NULLIF(TRIM(merchant_name), ''), NULLIF(TRIM(merchant_key), ''), description) for the displayed merchant label.
- Do NOT use COALESCE(merchant_name, description); it does not handle blank merchant_name values.
- This prevents all unenriched transactions from being combined into a fake blank merchant.
- For spending reports, exclude non-spending categories: 'Savings Transfer', 'Personal Transfer', 'Credit Card Payment', and 'Income'.

SQLITE COMPATIBILITY:
- Use SQLite-compatible SQL only.
- Prefer LIKE, CASE WHEN, COALESCE, ABS, and strftime for date/time work.
- SQLite does NOT support date('now', 'end of month'). For current month ranges, use either date LIKE 'YYYY-MM-%' or date >= date('now', 'start of month') AND date < date('now', 'start of month', '+1 month').
- Do NOT use database-specific functions that SQLite doesn't support.
- This app supports the infix REGEXP operator, but prefer LIKE unless regex matching is truly required.
"""


# ══════════════════════════════════════════════════════════════════════════════
# CORE: Ask the Copilot
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# INTENT EXTRACTION + DETERMINISTIC DISPATCH
# For known structured operations (recategorize, rename, explain, rule creation),
# we classify intent from the user's text, then call the deterministic Python tool.
# This avoids LLM SQL generation for known operations — more reliable, faster.
# A regex keyword pre-filter skips the LLM call entirely for analytics questions.
# ══════════════════════════════════════════════════════════════════════════════

_INTENT_KEYWORDS = re.compile(
    r"\b(why\s+is|how\s+is|why\s+was|how\s+was"
    r"|incorrectly\s+categorized|wrongly\s+categorized|miscategorized"
    r"|missing\s+categor|uncategorized\s+merchant"
    r"|move\s+all|recategorize|reclassify|rename|create\s+(a\s+)?rule)\b",
    re.IGNORECASE,
)


def _extract_intent(question: str) -> dict | None:
    """
    Two-phase intent extraction:
    1. Regex keyword pre-filter — skips LLM for pure analytics questions (zero latency).
    2. Lightweight LLM call (max 80 tokens) to classify intent + extract params.
    Returns a dict {intent, params} or None (fall through to SQL path).
    """
    if not _INTENT_KEYWORDS.search(question):
        return None  # clearly analytics — no added latency

    if not llm_client.is_available():
        return None

    prompt = (
        'Classify this message into one intent. Return JSON only, no explanation.\n\n'
        'Intents:\n'
        '- explain_category: why/how is a merchant categorized. params: {"merchant": "name"}\n'
        '- find_missing_categories: find merchants/transactions with no category. params: {}\n'
        '- bulk_recategorize: move/recategorize/change transactions for a merchant to a new category. '
        'params: {"merchant": "name", "category": "name"}\n'
        '- create_rule: create a categorization rule for a merchant/pattern. '
        'params: {"pattern": "name", "category": "name"}\n'
        '- rename_merchant: rename/clean up a merchant display name. '
        'params: {"old_name": "current", "new_name": "desired"}\n'
        '- free_form: analytics, spending reports, balance queries, or anything else. params: {}\n\n'
        'Example: {"intent": "bulk_recategorize", "params": {"merchant": "Netflix", "category": "Entertainment"}}\n\n'
        f'Message: "{question}"\n\nJSON:'
    )

    try:
        raw = llm_client.complete(prompt, max_tokens=80, purpose="copilot")
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
        result = json.loads(raw)
        if isinstance(result, dict) and result.get("intent"):
            return result
    except Exception:
        pass
    return None


def _dispatch_intent(intent_result: dict, profile: str | None, question: str) -> dict | None:
    """
    Route a classified intent to the appropriate deterministic data_manager tool.
    Returns a copilot response dict, or None to fall through to LLM SQL.
    Any exception also falls through — intent routing is best-effort.
    """
    from data_manager import (
        explain_category_assignment, find_merchants_missing_category,
        bulk_recategorize_preview, preview_rule_creation, rename_merchant_variants,
    )

    intent = intent_result.get("intent")
    params = intent_result.get("params") or {}

    _SOURCE_LABELS = {
        "user": "a manual override",
        "user-rule": "a user-defined rule",
        "llm": "AI categorization",
        "rule": "a built-in rule",
        "fallback": "the fallback default",
        "teller": "the bank's own category",
        "enricher": "merchant enrichment",
        "merchant-memory": "merchant memory",
    }

    try:
        if intent == "explain_category":
            merchant = (params.get("merchant") or "").strip()
            if not merchant:
                return None
            with get_db() as conn:
                data = explain_category_assignment(merchant, profile, conn)
            count = data["transaction_count"]
            dominant_cat = data["dominant_category"] or "an unknown category"
            dominant_src = data["dominant_source"] or "unknown"
            rule = data["rule"]
            source_label = _SOURCE_LABELS.get(dominant_src, dominant_src)
            if count == 0:
                answer = f'No transactions found matching "{merchant}" (pattern: {data["normalized_pattern"]}).'
            else:
                rule_detail = ""
                if rule:
                    rule_detail = (
                        f" A {'user' if rule['source'] == 'user' else 'built-in'} rule exists "
                        f"for pattern **{rule['pattern']}** (priority {rule['priority']})."
                    )
                answer = (
                    f'**{merchant}** is categorized as **{dominant_cat}** '
                    f"across {count} transaction{'s' if count != 1 else ''}. "
                    f"Assigned by {source_label}.{rule_detail}"
                )
            _log_conversation(profile, question, "", answer, answer, "read", 0)
            return {
                "answer": answer, "sql": "", "operation": "read",
                "data": data["samples"], "rows_affected": count, "needs_confirmation": False,
            }

        elif intent == "find_missing_categories":
            with get_db() as conn:
                items = find_merchants_missing_category(profile, conn)
            total_tx = sum(item["transaction_count"] for item in items)
            if not items:
                answer = "No uncategorized transactions found. Your data looks clean!"
            else:
                top = ", ".join(item["pattern"] for item in items[:5])
                more = f" and {len(items) - 5} more" if len(items) > 5 else ""
                answer = (
                    f"Found **{len(items)}** merchant pattern{'s' if len(items) != 1 else ''} "
                    f"with {total_tx} uncategorized transaction{'s' if total_tx != 1 else ''}: "
                    f"{top}{more}."
                )
            _log_conversation(profile, question, "", answer, answer, "read", 0)
            return {
                "answer": answer, "sql": "", "operation": "read",
                "data": items, "rows_affected": len(items), "needs_confirmation": False,
            }

        elif intent == "bulk_recategorize":
            merchant = (params.get("merchant") or "").strip()
            category = (params.get("category") or "").strip()
            if not merchant or not category:
                return None
            with get_db() as conn:
                data = bulk_recategorize_preview(merchant, category, profile, conn)
            count = data["count"]
            if count == 0:
                answer = (
                    f'No transactions found for "{merchant}" that aren\'t already '
                    f'categorized as **{category}**.'
                )
                _log_conversation(profile, question, "", answer, answer, "read", 0)
                return {
                    "answer": answer, "sql": "", "operation": "read",
                    "data": None, "rows_affected": 0, "needs_confirmation": False,
                }
            confirmation_id = store_pending_sql(data["update_sql"], profile)
            answer = (
                f"Found **{count}** {merchant} transaction{'s' if count != 1 else ''} "
                f"to move to **{category}**. Confirm to apply."
            )
            _log_conversation(profile, question, data["update_sql"], answer, answer, "write", 0)
            return {
                "answer": answer, "sql": data["update_sql"], "operation": "write_preview",
                "data": data["samples"],
                "preview_changes": [{"column": "category", "raw_value": category, "new_value": category}],
                "confirmation_id": confirmation_id,
                "needs_confirmation": True, "rows_affected": count,
            }

        elif intent == "create_rule":
            pattern = (params.get("pattern") or "").strip()
            category = (params.get("category") or "").strip()
            if not pattern or not category:
                return None
            with get_db() as conn:
                data = preview_rule_creation(pattern, category, profile, conn)
            count = data["count"]
            existing = data["existing_rule"]
            confirmation_id = store_pending_sql(data["insert_sql"], profile)
            existing_note = ""
            if existing:
                existing_note = (
                    f" Note: a rule for **{data['pattern']}** already exists "
                    f"(→ {existing['category']}) — this will replace it."
                )
            answer = (
                f"Creating rule **{data['pattern']}** → **{category}** will apply to "
                f"**{count}** existing transaction{'s' if count != 1 else ''} "
                f"and all future matches.{existing_note} Confirm to create."
            )
            _log_conversation(profile, question, data["insert_sql"], answer, answer, "write", 0)
            return {
                "answer": answer, "sql": data["insert_sql"], "operation": "write_preview",
                "data": data["samples"],
                "preview_changes": [{"column": "rule", "raw_value": f"{data['pattern']} → {category}", "new_value": category}],
                "confirmation_id": confirmation_id,
                "needs_confirmation": True, "rows_affected": count,
            }

        elif intent == "rename_merchant":
            old_name = (params.get("old_name") or "").strip()
            new_name = (params.get("new_name") or "").strip()
            if not old_name or not new_name:
                return None
            with get_db() as conn:
                data = rename_merchant_variants(old_name, new_name, profile, conn)
            count = data["count"]
            if count == 0:
                answer = f'No transactions found matching "{old_name}".'
                _log_conversation(profile, question, "", answer, answer, "read", 0)
                return {
                    "answer": answer, "sql": "", "operation": "read",
                    "data": None, "rows_affected": 0, "needs_confirmation": False,
                }
            confirmation_id = store_pending_sql(data["update_sql"], profile)
            answer = (
                f"Found **{count}** transaction{'s' if count != 1 else ''} for "
                f"**{old_name}** to rename to **{new_name}**. Confirm to apply."
            )
            _log_conversation(profile, question, data["update_sql"], answer, answer, "write", 0)
            return {
                "answer": answer, "sql": data["update_sql"], "operation": "write_preview",
                "data": data["samples"],
                "preview_changes": [{"column": "merchant_name", "raw_value": new_name, "new_value": new_name}],
                "confirmation_id": confirmation_id,
                "needs_confirmation": True, "rows_affected": count,
            }

    except Exception as e:
        logger.warning("Intent dispatch failed for intent '%s': %s", intent, e)
        return None  # fall through to LLM SQL

    return None


def _validate_read_semantics(question: str, sql: str) -> tuple[bool, str]:
    """
    Validate app-level finance semantics that raw SQL syntax cannot capture.
    These checks catch plausible-looking queries that would answer the wrong
    business question.
    """
    lower_question = question.lower()
    upper_sql = sql.upper()
    compact_sql = re.sub(r"\s+", " ", upper_sql)

    invalid_sqlite_date_modifiers = (
        "END OF MONTH",
        "START OF WEEK",
        "END OF WEEK",
        "START OF QUARTER",
        "END OF QUARTER",
        "START OF YEAR",
        "END OF YEAR",
    )
    for modifier in invalid_sqlite_date_modifiers:
        if f"'{modifier}'" in upper_sql or f'"{modifier}"' in upper_sql:
            return (
                False,
                f"SQLite does not support the date modifier '{modifier.lower()}'. "
                "Use supported SQLite date modifiers, for example date('now', 'start of month', '+1 month') "
                "as an exclusive upper bound for this month.",
            )

    asks_for_spending = any(
        token in lower_question
        for token in ("spending", "spent", "expense", "expenses", "merchant", "merchants")
    )
    asks_for_transfer = any(
        token in lower_question
        for token in ("transfer", "transfers", "credit card payment", "cc payment", "savings transfer")
    )

    if asks_for_spending and not asks_for_transfer and "TRANSACTIONS_VISIBLE" in upper_sql:
        excluded_categories = (
            "SAVINGS TRANSFER",
            "PERSONAL TRANSFER",
            "CREDIT CARD PAYMENT",
            "INCOME",
        )
        missing = [
            category
            for category in excluded_categories
            if category not in upper_sql
        ]
        if "AMOUNT < 0" in compact_sql and missing:
            return (
                False,
                "Spending queries must exclude non-spending categories: "
                "'Savings Transfer', 'Personal Transfer', 'Credit Card Payment', and 'Income'.",
            )

    merchant_report = (
        "merchant" in lower_question
        and "MERCHANT_NAME" in upper_sql
        and "GROUP BY" in upper_sql
    )
    if merchant_report:
        qualified_merchant = r"(?:[A-Z_][A-Z0-9_]*\.)?MERCHANT_NAME"
        qualified_merchant_key = r"(?:[A-Z_][A-Z0-9_]*\.)?MERCHANT_KEY"
        qualified_description = r"(?:[A-Z_][A-Z0-9_]*\.)?DESCRIPTION"
        merchant_fallback_pattern = (
            rf"COALESCE\(\s*NULLIF\(\s*TRIM\(\s*{qualified_merchant_key}\s*\)\s*,\s*''\s*\)\s*,\s*NULLIF\(\s*TRIM\(\s*{qualified_merchant}\s*\)\s*,\s*''\s*\)\s*,\s*{qualified_description}\s*\)"
        )
        weak_merchant_fallback_pattern = (
            rf"COALESCE\(\s*{qualified_merchant}\s*,\s*{qualified_description}\s*\)"
        )
        if re.search(weak_merchant_fallback_pattern, compact_sql):
            return (
                False,
                "Merchant fallback must treat blank merchant names as missing and prefer merchant_key. Use "
                "COALESCE(NULLIF(TRIM(merchant_key), ''), NULLIF(TRIM(merchant_name), ''), description), not COALESCE(merchant_name, description).",
            )
        if re.search(merchant_fallback_pattern, compact_sql) and re.search(r"\bGROUP BY\s+MERCHANT_NAME\b", compact_sql):
            return (
                False,
                "Merchant grouping must GROUP BY the full COALESCE(NULLIF(TRIM(merchant_key), ''), NULLIF(TRIM(merchant_name), ''), description) "
                "expression, not GROUP BY merchant_name, because SQLite may bind that to the raw nullable column.",
            )
        if re.search(r"\bGROUP BY\s+MERCHANT_NAME\b", compact_sql):
            return (
                False,
                "Merchant grouping must handle blank merchant_name values with "
                "COALESCE(NULLIF(TRIM(merchant_key), ''), NULLIF(TRIM(merchant_name), ''), description).",
            )
        if not re.search(merchant_fallback_pattern, compact_sql):
            return (
                False,
                "Merchant reports must show the transaction description when merchant_name is missing or blank. "
                "Use COALESCE(NULLIF(TRIM(merchant_key), ''), NULLIF(TRIM(merchant_name), ''), description).",
            )

    return True, ""


def ask_copilot(
    question: str,
    profile: str | None = None,
    confirm_write: bool = False,
    pending_sql: str | None = None,
    history: list[dict] | None = None,
) -> dict:
    if not llm_client.is_available():
        return {
            "answer": "Copilot is not configured. Set ANTHROPIC_API_KEY (Anthropic) or OLLAMA_BASE_URL (Ollama).",
            "sql": "",
            "data": None,
            "operation": "error",
            "rows_affected": 0,
            "needs_confirmation": False,
        }

    # ── Handle write confirmation ──
    if confirm_write and pending_sql:
        return _execute_write(pending_sql, profile, question)

    # ── Route natural-language questions through the tool-using agent ──
    # The dispatcher now performs LLM-first intent routing for both read and
    # write-preview requests. Write confirmation still uses the nonce flow above.
    try:
        from copilot_agent import run_agent
        agent_result = run_agent(question=question, profile=profile, history=history)
        _log_conversation(
            profile,
            question,
            "",
            json.dumps({"tool_trace": agent_result.get("tool_trace", [])}, default=str),
            agent_result.get("answer") or "",
            "read",
            0,
        )
        agent_data = agent_result.get("data")
        pending_write = agent_result.get("pending_write") or {}
        if pending_write:
            return {
                "answer": agent_result.get("answer") or "",
                "sql": pending_write.get("sql") or "",
                "confirmation_id": pending_write.get("confirmation_id"),
                "data": pending_write.get("samples") or agent_data,
                "preview_changes": pending_write.get("preview_changes", []),
                "operation": "write_preview",
                "rows_affected": pending_write.get("rows_affected") or 0,
                "needs_confirmation": True,
                "tool_trace": agent_result.get("tool_trace", []),
                "data_source": agent_result.get("data_source"),
                "iterations": agent_result.get("iterations", 0),
                "memory_proposals": agent_result.get("memory_proposals", []),
                "memory_observations": agent_result.get("memory_observations", []),
            }
        return {
            "answer": agent_result.get("answer") or "",
            "sql": "",
            "data": agent_data,
            "operation": "error" if agent_result.get("error") else "read",
            "rows_affected": len(agent_data) if isinstance(agent_data, list) else 0,
            "needs_confirmation": False,
            "tool_trace": agent_result.get("tool_trace", []),
            "data_source": agent_result.get("data_source"),
            "iterations": agent_result.get("iterations", 0),
            "memory_proposals": agent_result.get("memory_proposals", []),
            "memory_observations": agent_result.get("memory_observations", []),
        }
    except Exception as e:
        logger.exception("Agent loop failed; falling back to single-shot SQL path")

    # ── Build anonymized context ──
    real_to_alias, alias_to_real = _build_profile_map()

    profile_alias = "all_profiles"
    if profile and profile != "household":
        profile_alias = real_to_alias.get(profile, "profile_1")

    schema_context = _build_schema_context(real_to_alias)

    try:
        with get_db() as _ctx_conn:
            live_context = build_copilot_context(profile, _ctx_conn)
    except Exception:
        logger.exception("Failed to build copilot live context")
        live_context = ""

    context_block = f"\n{live_context}\n" if live_context else ""

    prompt = f"""{schema_context}
{context_block}
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

    # ── Call LLM ──
    try:
        raw_sql = llm_client.complete(prompt, max_tokens=1024, purpose="copilot")
    except Exception as e:
        logger.exception("Failed to reach LLM for copilot query")
        return _error_response(f"Failed to reach LLM: {e}")

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
            logger.warning("Copilot write validation failed | SQL: %s | Error: %s", real_sql, validation_error)
            return _error_response(f"Write validation failed: {validation_error}", sql=real_sql)

        preview = _preview_write(real_sql, profile)

        # [FIX C3] Store SQL server-side — client only gets a confirmation_id
        confirmation_id = store_pending_sql(real_sql, profile)

        _log_conversation(profile, question, real_sql, str(preview), "", "write_preview")

        return {
            "answer": f"This will affect {preview['count']} transaction(s). Confirm to proceed.",
            "sql": real_sql,
            "confirmation_id": confirmation_id,
            "data": preview.get("sample", []),
            "preview_changes": preview.get("changes", []),
            "operation": "write_preview",
            "rows_affected": preview["count"],
            "needs_confirmation": True,
        }
    # ── Handle READ operations ──
    sql = _clean_sql_response(raw_sql)

    real_sql = _deanoymize_sql(sql, alias_to_real)
    if not profile or profile == "household":
        real_sql = _sanitize_profile_filter(real_sql)
    real_sql = _rewrite_transaction_read_sources(real_sql)

    is_valid, validation_error = _validate_read_sql(real_sql)
    if not is_valid:
        logger.warning("Copilot read validation failed | SQL: %s | Error: %s", real_sql, validation_error)
        return _error_response(f"SQL validation failed: {validation_error}", sql=real_sql)

    is_valid, validation_error = _validate_read_semantics(question, real_sql)
    if not is_valid:
        logger.warning("Copilot read semantic validation failed | SQL: %s | Error: %s", real_sql, validation_error)
        anonymized_failed_sql = _anonymize_sql_text(real_sql, real_to_alias)
        repaired_alias_sql = _repair_read_sql(
            question=question,
            failed_sql=anonymized_failed_sql,
            error_message=f"Semantic validation error: {validation_error}",
            schema_context=schema_context,
            profile_alias=profile_alias,
        )
        if not repaired_alias_sql:
            return _error_response(f"SQL semantic validation failed: {validation_error}", sql=real_sql)

        candidate_sql = _deanoymize_sql(repaired_alias_sql, alias_to_real)
        if not profile or profile == "household":
            candidate_sql = _sanitize_profile_filter(candidate_sql)
        candidate_sql = _rewrite_transaction_read_sources(candidate_sql)

        is_valid, validation_error = _validate_read_sql(candidate_sql)
        if is_valid:
            is_valid, validation_error = _validate_read_semantics(question, candidate_sql)
        if not is_valid:
            logger.warning(
                "Copilot semantic repair produced invalid SQL | SQL: %s | Error: %s",
                candidate_sql,
                validation_error,
            )
            return _error_response(f"SQL semantic validation failed: {validation_error}", sql=real_sql)

        logger.info(
            "Copilot repaired semantically invalid read SQL | Original: %s | Repaired: %s",
            real_sql,
            candidate_sql,
        )
        real_sql = candidate_sql

    try:
        rows = _run_read_query(real_sql)
    except Exception as e:
        logger.error("Copilot SQL execution error: %s | SQL: %s", e, real_sql)

        repaired_sql = None
        if _is_repairable_sqlite_error(e):
            anonymized_failed_sql = _anonymize_sql_text(real_sql, real_to_alias)
            repaired_alias_sql = _repair_read_sql(
                question=question,
                failed_sql=anonymized_failed_sql,
                error_message=str(e),
                schema_context=schema_context,
                profile_alias=profile_alias,
            )
            if repaired_alias_sql:
                candidate_sql = _deanoymize_sql(repaired_alias_sql, alias_to_real)
                if not profile or profile == "household":
                    candidate_sql = _sanitize_profile_filter(candidate_sql)
                candidate_sql = _rewrite_transaction_read_sources(candidate_sql)

                is_valid, validation_error = _validate_read_sql(candidate_sql)
                if is_valid:
                    try:
                        rows = _run_read_query(candidate_sql)
                        repaired_sql = candidate_sql
                        logger.info(
                            "Copilot repaired failing read SQL | Error: %s | Original: %s | Repaired: %s",
                            e,
                            real_sql,
                            candidate_sql,
                        )
                    except Exception as repair_error:
                        logger.error(
                            "Copilot SQL repair attempt failed: %s | Repaired SQL: %s",
                            repair_error,
                            candidate_sql,
                        )
                else:
                    logger.warning(
                        "Copilot SQL repair produced invalid SQL | SQL: %s | Error: %s",
                        candidate_sql,
                        validation_error,
                    )

        if repaired_sql is None:
            return _error_response(f"SQL execution error: {e}", sql=real_sql)

        real_sql = repaired_sql

    # ── Send results back to the configured LLM for natural language answer ──
    anon_rows = _anonymize_sql_result(rows, real_to_alias)
    display_rows = anon_rows[:50]
    mask_for_llm = _should_mask_copilot_results_for_llm()
    rows_for_answer = sanitize_rows_for_llm(display_rows) if mask_for_llm else display_rows

    answer = _generate_natural_answer(
        question,
        rows_for_answer,
        live_context=live_context,
        amounts_masked=mask_for_llm,
    )

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


def _generate_natural_answer(
    question: str,
    rows: list[dict],
    live_context: str = "",
    amounts_masked: bool = True,
) -> str:
    if not rows:
        return "No results found for your query."

    result_str = json.dumps(rows[:25], indent=2, default=str)
    context_block = f"\n{live_context}\n" if live_context else ""
    result_count_note = (
        f"\n\n(Showing first 25 of {len(rows)} total results)"
        if len(rows) > 25
        else ""
    )

    amount_guidance = (
        f"""The database returned these results (dollar amounts and personal names have been anonymized for privacy):
{result_str}{result_count_note}
Provide a clear, concise natural language answer.
IMPORTANT: The dollar amounts in the results are masked as "$XXX" for privacy. Do NOT invent specific dollar figures.
When discussing amounts, use general terms like "your spending" or "the total" — do not fabricate numbers.
If the user asked for a specific number, say that the exact figures are available in the data view."""
        if amounts_masked
        else f"""The database returned these local-only results:
{result_str}{result_count_note}
Provide a clear, concise natural language answer.
You are running locally, so exact dollar amounts in the results may be used in the answer.
If the user asked for a specific number, include the exact number from the results."""
    )

    prompt = f"""The user asked: "{question}"
{context_block}
{amount_guidance}
If there are multiple rows, summarize the key findings (counts, categories, patterns).
Do not mention SQL, databases, or queries in your answer.
Do not reveal any personal names — use generic terms like "your account" instead."""

    try:
        return llm_client.complete(prompt, max_tokens=1024, purpose="copilot")
    except Exception:
        logger.exception("Failed to generate natural language answer from LLM")

    if len(rows) == 1 and len(rows[0]) == 1:
        val = list(rows[0].values())[0]
        if isinstance(val, (int, float)):
            return f"${val:,.2f}"
        return str(val)

    return f"Found {len(rows)} results."


def _clean_sql_response(raw_sql: str) -> str:
    sql = raw_sql.strip()
    sql = re.sub(r"^```\w*\n?", "", sql)
    sql = re.sub(r"\n?```$", "", sql)
    return sql.strip()


def _run_read_query(sql: str, params: list | tuple = ()) -> list[dict]:
    sql = _rewrite_transaction_read_sources(sql)
    with get_db() as conn:
        conn.execute("PRAGMA query_only = ON")
        try:
            return dicts_from_rows(conn.execute(sql, params).fetchall())
        finally:
            conn.execute("PRAGMA query_only = OFF")


def _is_repairable_sqlite_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _REPAIRABLE_SQLITE_ERROR_MARKERS)


def _repair_read_sql(
    question: str,
    failed_sql: str,
    error_message: str,
    schema_context: str,
    profile_alias: str,
) -> str | None:
    prompt = f"""{schema_context}

The user's currently active profile is: '{profile_alias}'
Original user question: {question}

The previous SQLite query failed.

SQLite error:
{error_message}

Failed SQL:
{failed_sql}

Return one corrected SQLite read query that answers the original question.

Rules:
- Return only raw SQL. No markdown.
- Return only a SELECT query or a read-only WITH query.
- Use SQLite-compatible SQL only.
- Prefer LIKE for text matching unless regex is truly necessary.
- This app supports the infix REGEXP operator.
- If the active profile is 'all_profiles', do not add any profile_id filter.
"""

    try:
        repaired_sql = llm_client.complete(prompt, max_tokens=1024, purpose="copilot")
    except Exception:
        logger.exception("Failed to repair copilot SQL after execution error")
        return None

    cleaned = _clean_sql_response(repaired_sql)
    if not cleaned or cleaned.startswith("CANNOT:") or cleaned.startswith("WRITE:"):
        return None
    return cleaned


def _parse_sql_literal(value: str):
    """Parse simple SQL literal values for preview rendering."""
    raw = value.strip().rstrip(";")
    upper = raw.upper()

    if upper == "NULL":
        return None
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        return raw[1:-1].replace("''", "'")
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        return raw[1:-1]
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?\d+\.\d+", raw):
        return float(raw)
    return raw


def _parse_update_assignments(stmt: str) -> list[dict]:
    """Parse UPDATE assignments into preview-friendly change objects."""
    set_clause = _extract_set_clause(stmt)
    if not set_clause:
        return []

    changes = []
    for part in _split_assignments(set_clause):
        if "=" not in part:
            continue
        left, right = part.split("=", 1)
        column = left.strip().lower()
        if "." in column:
            column = column.split(".")[-1]
        changes.append({
            "column": column,
            "raw_value": right.strip(),
            "new_value": _parse_sql_literal(right),
        })
    return changes


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
            return {"count": 0, "sample": [], "changes": []}

        # Extract WHERE clause safely using quote-aware parsing
        where_clause = _extract_where_clause(update_stmt)
        if not where_clause:
            return {"count": 0, "sample": [], "changes": []}

        # Build and validate preview queries
        preview_sql = (
            "SELECT id, description, amount, category, date, is_excluded, categorization_source "
            f"FROM {READ_TRANSACTIONS_TABLE} {where_clause} LIMIT 20"
        )
        count_sql = f"SELECT COUNT(*) as cnt FROM {READ_TRANSACTIONS_TABLE} {where_clause}"

        # Validate both as read queries
        is_valid, _ = _validate_read_sql(preview_sql)
        if not is_valid:
            return {"count": 0, "sample": [], "changes": []}
        is_valid, _ = _validate_read_sql(count_sql)
        if not is_valid:
            return {"count": 0, "sample": [], "changes": []}

        with get_db() as conn:
            conn.execute("PRAGMA query_only = ON")
            try:
                rows = dicts_from_rows(conn.execute(preview_sql).fetchall())
                count = conn.execute(count_sql).fetchone()[0]
            finally:
                conn.execute("PRAGMA query_only = OFF")

        changes = _parse_update_assignments(update_stmt)
        preview_rows = []
        for row in rows:
            preview_row = dict(row)
            for change in changes:
                preview_row[f"current_{change['column']}"] = row.get(change["column"])
                preview_row[f"new_{change['column']}"] = change["new_value"]
            preview_rows.append(preview_row)

        return {"count": count, "sample": preview_rows, "changes": changes}
    except Exception:
        logger.debug("Write preview failed, returning empty preview", exc_info=True)

    return {"count": 0, "sample": [], "changes": []}


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
            logger.warning("Copilot write execution validation failed | SQL: %s | Error: %s", sql, validation_error)
            return _error_response(f"Write validation failed: {validation_error}", sql=sql)

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
                                            f"Please narrow your query.",
                                            sql=sql,
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
                                    f"exceeding the safety limit of {COPILOT_MAX_WRITE_ROWS}.",
                                    sql=sql,
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
        return _error_response(f"Write failed: {e}", sql=sql)


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

        select_sql = f"SELECT DISTINCT description FROM {READ_TRANSACTIONS_TABLE} {where_clause}"
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
        from data_manager import log_copilot_conversation, prepare_copilot_history_record, prune_copilot_conversations

        record = prepare_copilot_history_record(
            profile=profile,
            question=question,
            generated_sql=sql,
            result=result,
            answer=answer,
            operation=operation,
            rows_affected=rows_affected,
        )
        log_copilot_conversation(**record)
        prune_copilot_conversations(profile=profile)
    except Exception:
        logger.debug("Failed to log copilot conversation", exc_info=True)


def _error_response(message: str, sql: str = "") -> dict:
    return {
        "answer": message,
        "sql": sql,
        "data": None,
        "operation": "error",
        "rows_affected": 0,
        "needs_confirmation": False,
    }
