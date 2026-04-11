"""
privacy.py
Shared privacy utilities for sanitizing data before sending to external APIs.
Used by categorizer.py (LLM), copilot.py (LLM), and enricher.py (Trove).
"""


def mask_amount(amount, placeholder: str = "$XXX") -> str:
    """
    Replace exact dollar amounts with a placeholder for LLM contexts.
    Preserves the sign indicator (expense vs income) which has
    categorization value, but removes the precise figure.

    Args:
        amount: The transaction amount (str or float)
        placeholder: What to show instead of the real amount

    Returns:
        String like "-$XXX" (expense) or "+$XXX" (income/refund)
    """
    try:
        val = float(amount)
    except (ValueError, TypeError):
        return placeholder

    if val < 0:
        return f"-{placeholder}"
    elif val > 0:
        return f"+{placeholder}"
    return placeholder


def mask_counterparty(name: str, counterparty_type: str) -> str:
    """
    Replace real counterparty names with type-based placeholders.
    For P2P transfers (Zelle, Venmo), the counterparty is a person's
    real name — pure PII with zero categorization value.

    Args:
        name: The counterparty name from Teller
        counterparty_type: 'person', 'organization', or ''

    Returns:
        Masked placeholder or empty string if no name provided
    """
    if not name or not name.strip():
        return ""

    if counterparty_type == "person":
        return "[person]"
    elif counterparty_type == "organization":
        return "[organization]"
    else:
        # Unknown type — still mask it since it could be a person's name
        return "[counterparty]"


def sanitize_row_for_llm(row: dict) -> dict:
    """
    Sanitize a single database result row before sending to an LLM
    for natural language answer generation.

    Masks amounts, counterparty names, and strips fields that have
    no value for answer formatting but expose private data.

    Args:
        row: A dict from a SQL query result

    Returns:
        New dict with sensitive fields masked
    """
    sanitized = dict(row)

    # Mask dollar amounts
    if "amount" in sanitized:
        sanitized["amount"] = mask_amount(sanitized["amount"])

    # Mask balance fields
    for balance_key in ("current_balance", "balance", "available_balance",
                        "total_assets", "total_owed", "net_worth"):
        if balance_key in sanitized:
            sanitized[balance_key] = "$XXX"

    # Mask counterparty
    if "counterparty_name" in sanitized:
        cp_type = sanitized.get("counterparty_type", "")
        sanitized["counterparty_name"] = mask_counterparty(
            sanitized["counterparty_name"], cp_type
        )

    # Strip raw description (contains unsanitized merchant + location data)
    if "raw_description" in sanitized:
        del sanitized["raw_description"]

    return sanitized


def sanitize_rows_for_llm(rows: list[dict]) -> list[dict]:
    """
    Sanitize a list of database result rows before sending to an LLM.
    Convenience wrapper around sanitize_row_for_llm.
    """
    return [sanitize_row_for_llm(row) for row in rows]