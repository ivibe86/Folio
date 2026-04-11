"""
sanitizer.py
Light sanitization — strips sensitive IDs but preserves merchant names,
counterparty names, and description context needed for categorization.
"""

import re


def sanitize_transaction(tx: dict) -> dict | None:
    """
    Sanitize a single transaction for categorization.
    Returns None for transactions that should be skipped entirely.
    """
    description = tx.get("description", "")
    tx_type = tx.get("type", "")
    raw_description = description  # preserve for enrichment
    amount = float(tx.get("amount", 0))
    account_type = tx.get("account_type", "")

    # ── Skip card-side payment transactions (already counted on bank side) ──
    if tx_type == "payment":
        return None

    # ── Normalize credit card amount signs ──
    is_credit = account_type in ("credit_card", "credit")

    if is_credit:
        # Teller returns CC purchases as positive — flip to negative (expense)
        if tx_type in ("card_payment", "fee") and amount > 0:
            amount = -amount
        # CC "transaction" type with negative amount = refund/credit → flip to positive
        if tx_type == "transaction" and amount < 0:
            amount = -amount

    # ── Light sanitization — remove sensitive IDs only ──

    # Strip account IDs (e.g., "ID:XXXXX48210")
    description = re.sub(r"ID:\s*\S+", "ID:***", description, flags=re.IGNORECASE)

    # Strip confirmation numbers
    description = re.sub(r"conf(?:irmation)?#?\s*\S+", "", description, flags=re.IGNORECASE)

    # Strip INDN (individual name in ACH — often duplicates counterparty)
    description = re.sub(r"INDN:\S+(\s\S+)?", "", description)

    # Strip CO ID references
    description = re.sub(r"CO\s*ID:\s*\S+", "", description)

    # Clean extra whitespace
    description = re.sub(r"\s+", " ", description).strip()

    # ── Extract counterparty info ──
    details = tx.get("details") or {}
    counterparty = details.get("counterparty") or {}
    cp_name = counterparty.get("name", "")
    cp_type = counterparty.get("type", "")

    sanitized = {
        "description": description,
        "raw_description": raw_description,
        "amount": str(amount),
        "type": tx_type,
        "date": tx.get("date"),
        "original_id": tx.get("id"),
        "counterparty_type": cp_type,
        "counterparty_name": cp_name,
        "teller_category": details.get("category", ""),
        "account_name": tx.get("account_name", ""),
        "account_type": account_type,
    }
    # Preserve profile tag if present (household multi-profile support)
    if "profile" in tx:
        sanitized["profile"] = tx["profile"]
    return sanitized

def sanitize_transactions(transactions: list[dict]) -> list[dict]:
    """Sanitize a list of transactions, filtering out skipped ones."""
    results = [sanitize_transaction(tx) for tx in transactions]
    return [r for r in results if r is not None]