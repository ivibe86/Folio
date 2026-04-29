"""
copilot.py
Compatibility wrapper for Mira's dispatcher-based agent.
"""

import json
import re
from dotenv import load_dotenv
from log_config import get_logger
import llm_client

load_dotenv()

logger = get_logger(__name__)

READ_TRANSACTIONS_TABLE = "transactions_visible"
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
# CORE: Ask the Copilot
# ══════════════════════════════════════════════════════════════════════════════

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
    history: list[dict] | None = None,
) -> dict:
    if not llm_client.is_available():
        return {
            "answer": "Mira needs a reachable local Ollama setup before she can answer.",
            "sql": "",
            "data": None,
            "operation": "error",
            "rows_affected": 0,
            "needs_confirmation": False,
        }

    # Route natural-language questions through Mira's dispatcher. Write
    # confirmation is handled by structured pending operations in /copilot/confirm.
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
                "sql": "",
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
        logger.exception("Mira dispatcher failed")
        return _error_response(f"Mira hit an error: {e}")

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
