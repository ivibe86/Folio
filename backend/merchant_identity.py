"""
Merchant identity helpers.

The app keeps merchant_name as a human-facing label, but uses merchant_key as
the stable grouping identity. Regex here is limited to canonical formatting,
non-merchant guardrails, and light evidence checks; it should not invent
merchant names from ambiguous bank text.
"""

from __future__ import annotations

import re
from typing import Any


NON_MERCHANT_KINDS = {
    "personal_transfer",
    "credit_card_payment",
    "credit_refund",
    "income",
    "tax",
    "bank_fee",
}

MERCHANT_PURCHASE = "merchant_purchase"
UNKNOWN_KIND = "unknown"

_TRANSFER_CATEGORIES = {"Savings Transfer", "Personal Transfer", "Credit Card Payment", "Income", "Credits & Refunds", "Taxes", "Fees & Charges"}


def canonicalize_merchant_key(value: str | None) -> str:
    """Convert a display merchant name into a stable uppercase key."""
    raw = (value or "").strip()
    if not raw:
        return ""

    text = raw.upper()
    text = re.sub(r"#\s*\d+\b", " ", text)
    text = re.sub(r"['’]", "", text)
    text = re.sub(r"&", " AND ", text)
    text = re.sub(r"\b(?:INC|INCORPORATED|LLC|LTD|CORP|CORPORATION|CO)\b\.?", "", text)
    text = re.sub(r"[^A-Z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def display_from_key(merchant_key: str | None) -> str:
    key = (merchant_key or "").strip()
    if not key:
        return ""
    return " ".join(part.capitalize() for part in key.replace("_", " ").split())


def infer_non_merchant_kind(tx: dict[str, Any]) -> str:
    """Return a non-merchant kind when the row is clearly not merchant spend."""
    description = f"{tx.get('raw_description') or ''} {tx.get('description') or ''}".lower()
    category = (tx.get("category") or "").strip()
    tx_type = (tx.get("type") or tx.get("transaction_type") or "").strip().lower()
    expense_type = (tx.get("expense_type") or "").strip().lower()

    if category == "Personal Transfer" or any(token in description for token in ("zelle", "venmo", "cash app", "cashapp", "apple cash")):
        return "personal_transfer"
    if category == "Credit Card Payment" or expense_type == "transfer_cc_payment":
        return "credit_card_payment"
    if category == "Credits & Refunds":
        return "credit_refund"
    if category == "Income":
        return "income"
    if category == "Taxes" or "tax" in description or "irs" in description:
        return "tax"
    if category == "Fees & Charges" or tx_type in {"fee", "interest"}:
        return "bank_fee"
    if category in _TRANSFER_CATEGORIES or tx_type in {"transfer", "payment", "deposit", "ach"} or expense_type.startswith("transfer_"):
        return "personal_transfer"
    return ""


def normalize_merchant_kind(value: str | None, tx: dict[str, Any] | None = None) -> str:
    raw = (value or "").strip().lower()
    raw = raw.replace("-", "_").replace(" ", "_")
    aliases = {
        "merchant": MERCHANT_PURCHASE,
        "purchase": MERCHANT_PURCHASE,
        "transfer": "personal_transfer",
        "p2p": "personal_transfer",
        "card_payment": "credit_card_payment",
        "cc_payment": "credit_card_payment",
        "fee": "bank_fee",
        "taxes": "tax",
    }
    raw = aliases.get(raw, raw)
    allowed = NON_MERCHANT_KINDS | {MERCHANT_PURCHASE, UNKNOWN_KIND}
    if raw in allowed:
        return raw
    if tx:
        inferred = infer_non_merchant_kind(tx)
        if inferred:
            return inferred
    return UNKNOWN_KIND


def merchant_name_supported(description: str, merchant_name: str, evidence_tokens: list[str] | None = None) -> bool:
    """Guard against hallucinated names by requiring lexical evidence."""
    desc = (description or "").lower()
    name = (merchant_name or "").lower()
    if not desc or not name:
        return False

    desc_tokens = {t for t in re.findall(r"[a-z0-9]+", desc) if len(t) >= 3}
    name_tokens = {t for t in re.findall(r"[a-z0-9]+", name) if len(t) >= 3}
    if desc_tokens and name_tokens and not desc_tokens.isdisjoint(name_tokens):
        return True

    alias_map = {
        "amazon": {"amzn"},
        "amazon marketplace": {"amzn", "mktpl", "mktplace"},
        "amazon digital services": {"amzn", "digital"},
        "door dash": {"dd", "doordash"},
        "doordash": {"dd"},
        "openai": {"chatgpt"},
        "costco": {"whse"},
        "beverages and more": {"beverages", "more"},
    }
    aliases = alias_map.get(name.replace("&", "and"), set())
    if aliases and not desc_tokens.isdisjoint(aliases):
        return True

    compact_desc = re.sub(r"[^a-z0-9]", "", desc)
    compact_name = re.sub(r"[^a-z0-9]", "", name)
    if len(compact_name) >= 5 and compact_name in compact_desc:
        return True

    for token in evidence_tokens or []:
        if (token or "").strip().lower() in desc:
            return True
    return False


def build_merchant_identity(tx: dict[str, Any]) -> dict[str, str]:
    """
    Return the canonical identity for a transaction-like dict.

    Existing merchant_key wins. merchant_name is the only source used to create
    a new merchant key; raw description fallback remains a compatibility concern
    in SQL reads, not a source of new identity.
    """
    merchant_key = canonicalize_merchant_key(tx.get("merchant_key"))
    merchant_name = (tx.get("merchant_name") or "").strip()
    kind = normalize_merchant_kind(tx.get("merchant_kind"), tx)

    if not merchant_key and merchant_name and kind not in NON_MERCHANT_KINDS:
        merchant_key = canonicalize_merchant_key(merchant_name)
        if kind == UNKNOWN_KIND:
            kind = MERCHANT_PURCHASE

    if not merchant_key and kind == UNKNOWN_KIND:
        inferred = infer_non_merchant_kind(tx)
        if inferred:
            kind = inferred

    display_name = merchant_name or display_from_key(merchant_key)
    source = (tx.get("merchant_source") or ("legacy" if merchant_name else "none")).strip()
    confidence = (tx.get("merchant_confidence") or tx.get("enrichment_confidence") or "").strip()

    return {
        "merchant_key": merchant_key,
        "display_name": display_name,
        "source": source,
        "confidence": confidence,
        "kind": kind,
    }
