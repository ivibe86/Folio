"""
Category-first cashflow classifier.

This module chooses user-facing categories for special cashflow rows. It does
not mutate the database and does not introduce a visible cashflow type.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any


CREDITS_REFUNDS_CATEGORY = "Credits & Refunds"
TRANSFER_CATEGORIES = {
    "Savings Transfer",
    "Personal Transfer",
    "Credit Card Payment",
    "Cash Withdrawal",
    "Cash Deposit",
    "Investment Transfer",
}
USER_AUTHORITY_SOURCES = {"user", "user-rule"}

_CARD_PAYMENT_PATTERNS = (
    r"credit\s*c(?:a)?rd",
    r"credit\s*crd",
    r"\bepay\b",
    r"\bautopay\b",
    r"apple\s*card",
    r"applecard",
    r"gsbank.*payment",
    r"card\s*payment",
    r"\bpayment\b",
)

_CREDIT_REFUND_PATTERNS = (
    r"\brefund\b",
    r"\breversal\b",
    r"\breversed\b",
    r"statement\s+credit",
    r"travel\s+credit",
    r"annual\s+fee\s+credit",
    r"\brewards?\b",
    r"cash\s*back",
    r"cashback",
    r"promo\s+credit",
    r"dispute\s+credit",
    r"merchant\s+credit",
)

_PAYROLL_PATTERNS = (
    r"\bpayroll\b",
    r"direct\s+dep(?:osit)?",
    r"\bsalary\b",
    r"\bwages?\b",
    r"\bpaycheck\b",
    r"\bemployer\b",
    r"\badp\b",
    r"\bgusto\b",
    r"\bpaychex\b",
)

_TAX_REFUND_PATTERNS = (
    r"\birs\b",
    r"\bus\s*treasury\b",
    r"\btreas\b",
    r"tax\s+refund",
    r"state\s+tax",
    r"franchise\s+tax\s+board",
)

_P2P_PATTERNS = (
    r"\bzelle\b",
    r"\bvenmo\b",
    r"\bcash\s*app\b",
    r"\bcashapp\b",
    r"paypal.*(?:send|p2p|instant)",
    r"\bxoom\b",
)

_SAVINGS_TRANSFER_PATTERNS = (
    r"transfer\s+to\s+sav",
    r"transfer\s+from\s+chk",
    r"transfer\s+from\s+sav",
    r"transfer\s+to\s+chk",
    r"transfer\s+from\s+acct",
    r"transfer\s+to\s+acct",
    r"savings\s+transfer",
    r"\bdes:transfer\b",
    r"online\s+(?:scheduled\s+)?transfer",
    r"internal\s+transfer",
    r"account\s+transfer",
    r"xfer\s+(?:to|from)",
    r"mobile\s+transfer",
)

_CASH_WITHDRAWAL_PATTERNS = (
    r"\batm\b.*\bwithdraw",
    r"\bwithdraw(?:al)?\b.*\batm\b",
    r"\bcash\s+withdraw(?:al)?\b",
    r"\bcash\s+advance\b",
)

_CASH_DEPOSIT_PATTERNS = (
    r"\batm\b.*\bdeposit\b",
    r"\bcash\s+deposit\b",
    r"\bdeposit\b.*\bcash\b",
    r"\bbranch\s+deposit\b",
)

_INVESTMENT_TRANSFER_PATTERNS = (
    r"\bcoinbase\b",
    r"\bgemini\b",
    r"\brobinhood\b",
    r"\bfidelity\b",
    r"\bschwab\b",
    r"\bvanguard\b",
    r"\be[-*]?trade\b",
    r"\betrade\b",
    r"\bwealthfront\b",
    r"\bbetterment\b",
    r"\bkraken\b",
    r"\bcrypto\.?com\b",
    r"\bbrokerage\b",
)


def _text(tx: dict[str, Any]) -> str:
    return " ".join(
        str(tx.get(key) or "")
        for key in (
            "description",
            "raw_description",
            "counterparty_name",
            "merchant_name",
            "account_name",
        )
    ).lower()


def _matches(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def _amount(tx: dict[str, Any]) -> float:
    try:
        return float(tx.get("amount") or 0)
    except (TypeError, ValueError):
        return 0.0


def _account_type(tx: dict[str, Any]) -> str:
    return str(tx.get("account_type") or tx.get("account_subtype") or "").strip().lower()


def _tx_type(tx: dict[str, Any]) -> str:
    return str(tx.get("type") or tx.get("transaction_type") or "").strip().lower()


def _is_credit_card_account(tx: dict[str, Any]) -> bool:
    account_type = _account_type(tx)
    account_subtype = str(tx.get("account_subtype") or "").strip().lower()
    account_name = str(tx.get("account_name") or "").strip().lower()
    return (
        account_type in {"credit", "credit_card"}
        or account_subtype in {"credit", "credit_card"}
        or "credit card" in account_name
    )


def _is_depository_account(tx: dict[str, Any]) -> bool:
    account_type = _account_type(tx)
    account_subtype = str(tx.get("account_subtype") or "").strip().lower()
    return account_type in {"depository", "checking", "savings", "bank"} or account_subtype in {"checking", "savings"}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    raw = str(value)[:10]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _category_allowed(category: str, active_categories: list[str] | None) -> str:
    if active_categories is None or category in active_categories:
        return category
    return "Other"


def _locked_user_category(tx: dict[str, Any]) -> bool:
    source = str(tx.get("categorization_source") or "").strip()
    pinned = tx.get("category_pinned")
    return source in USER_AUTHORITY_SOURCES or pinned in {1, True, "1", "true", "True"}


def _profile_id(tx: dict[str, Any]) -> str:
    return str(tx.get("profile") or tx.get("profile_id") or "").strip()


def _normalized_account_id(tx: dict[str, Any]) -> str:
    return str(tx.get("account_id") or "").strip().lower()


def _normalized_account_name(tx: dict[str, Any]) -> str:
    return re.sub(r"\s+", " ", str(tx.get("account_name") or "").strip().lower())


def _different_known_accounts(tx: dict[str, Any], other: dict[str, Any]) -> bool:
    tx_account_id = _normalized_account_id(tx)
    other_account_id = _normalized_account_id(other)
    if tx_account_id and other_account_id:
        return tx_account_id != other_account_id

    tx_account_name = _normalized_account_name(tx)
    other_account_name = _normalized_account_name(other)
    if tx_account_name and other_account_name:
        return tx_account_name != other_account_name

    return False


def _looks_like_account_movement(tx: dict[str, Any], other: dict[str, Any]) -> bool:
    movement_patterns = _SAVINGS_TRANSFER_PATTERNS + _P2P_PATTERNS
    return _matches(_text(tx), movement_patterns) or _matches(_text(other), movement_patterns)


def _paired_account_evidence(tx: dict[str, Any], other: dict[str, Any]) -> dict[str, Any] | None:
    amount = _amount(tx)
    other_amount = _amount(other)
    if abs(abs(other_amount) - abs(amount)) >= 0.01 or other_amount * amount >= 0:
        return None

    tx_is_credit = _is_credit_card_account(tx)
    other_is_credit = _is_credit_card_account(other)
    if tx_is_credit != other_is_credit and (
        (_is_depository_account(tx) and amount < 0 and other_is_credit and other_amount > 0)
        or (tx_is_credit and amount > 0 and _is_depository_account(other) and other_amount < 0)
    ):
        return {
            "kind": "credit_card_payment",
            "paired_transaction_id": other.get("id") or other.get("original_id"),
            "signals": ["same_amount_opposite_sign", "credit_card_account_pair"],
        }

    if not (_is_depository_account(tx) and _is_depository_account(other)):
        return None
    if not _different_known_accounts(tx, other):
        return None
    if not _looks_like_account_movement(tx, other):
        return None

    tx_profile = _profile_id(tx)
    other_profile = _profile_id(other)
    if not tx_profile or not other_profile:
        return None

    same_profile = tx_profile == other_profile
    return {
        "kind": "linked_account_transfer",
        "paired_transaction_id": other.get("id") or other.get("original_id"),
        "category": "Savings Transfer" if same_profile else "Personal Transfer",
        "expense_type": "transfer_internal" if same_profile else "transfer_household",
        "signals": [
            "same_amount_opposite_sign",
            "linked_depository_account_pair",
            "same_profile" if same_profile else "cross_profile",
        ],
    }


def find_account_pair_evidence(
    tx: dict[str, Any],
    *,
    conn,
    days: int = 5,
) -> dict[str, Any] | None:
    """Find same-amount opposite-side account movement in existing rows."""
    if conn is None:
        return None
    amount = _amount(tx)
    tx_date = _parse_date(tx.get("date"))
    tx_id = tx.get("id") or tx.get("original_id")
    if abs(amount) <= 0 or tx_date is None:
        return None

    start = (tx_date - timedelta(days=days)).isoformat()
    end = (tx_date + timedelta(days=days)).isoformat()
    rows = conn.execute(
        """
        SELECT id, date, amount, account_type, account_name, account_id, profile_id
          FROM transactions
         WHERE ABS(ABS(amount) - ?) < 0.01
           AND amount * ? < 0
           AND date BETWEEN ? AND ?
           AND COALESCE(id, '') != COALESCE(?, '')
           AND COALESCE(is_excluded, 0) = 0
        """,
        (abs(amount), amount, start, end, tx_id or ""),
    ).fetchall()

    for row in rows:
        other = dict(row)
        pair = _paired_account_evidence(tx, other)
        if pair:
            return pair
    return None


def build_batch_pair_evidence(transactions: list[dict[str, Any]], *, days: int = 5) -> dict[int, dict[str, Any]]:
    """Pair same-batch transactions without touching the database."""
    evidence: dict[int, dict[str, Any]] = {}
    for i, tx in enumerate(transactions):
        if i in evidence:
            continue
        amount = _amount(tx)
        tx_date = _parse_date(tx.get("date"))
        if abs(amount) <= 0 or tx_date is None:
            continue
        for j, other in enumerate(transactions[i + 1 :], start=i + 1):
            other_date = _parse_date(other.get("date"))
            if other_date is None or abs((other_date - tx_date).days) > days:
                continue
            if abs(abs(_amount(other)) - abs(amount)) >= 0.01 or _amount(other) * amount >= 0:
                continue
            pair = _paired_account_evidence(tx, other)
            if pair:
                reverse_pair = _paired_account_evidence(other, tx)
                evidence[i] = pair
                if reverse_pair:
                    evidence[j] = reverse_pair
                break
    return evidence


def classify_cashflow_category(
    tx: dict[str, Any],
    *,
    conn=None,
    account_pair_evidence: dict[str, Any] | None = None,
    active_categories: list[str] | None = None,
) -> dict[str, Any]:
    amount = _amount(tx)
    text = _text(tx)
    tx_type = _tx_type(tx)
    is_credit = _is_credit_card_account(tx)
    is_depository = _is_depository_account(tx)
    source_category = (tx.get("category") or "").strip()

    if _locked_user_category(tx) and source_category:
        return {
            "category": _category_allowed(source_category, active_categories),
            "confidence": "high",
            "source": "user-rule" if tx.get("categorization_source") == "user-rule" else "user",
            "evidence": {"signals": ["user_authority"], "paired_transaction_id": None},
        }

    pair = account_pair_evidence or find_account_pair_evidence(tx, conn=conn)
    if pair and pair.get("kind") == "credit_card_payment":
        return {
            "category": _category_allowed("Credit Card Payment", active_categories),
            "confidence": "high",
            "source": "pairing",
            "evidence": pair,
        }
    if pair and pair.get("kind") == "linked_account_transfer":
        return {
            "category": _category_allowed(pair.get("category") or "Savings Transfer", active_categories),
            "confidence": "high",
            "source": "pairing",
            "evidence": pair,
        }

    if _matches(text, _P2P_PATTERNS):
        return {
            "category": _category_allowed("Personal Transfer", active_categories),
            "confidence": "medium",
            "source": "provider",
            "evidence": {"signals": ["p2p_transfer_text"], "paired_transaction_id": None},
        }

    if is_depository and _matches(text, _INVESTMENT_TRANSFER_PATTERNS):
        return {
            "category": _category_allowed("Investment Transfer", active_categories),
            "confidence": "high" if amount != 0 else "medium",
            "source": "provider",
            "evidence": {"signals": ["investment_platform_text"], "paired_transaction_id": None},
        }

    if amount < 0 and is_depository and (
        tx_type == "withdrawal" or _matches(text, _CASH_WITHDRAWAL_PATTERNS)
    ):
        return {
            "category": _category_allowed("Cash Withdrawal", active_categories),
            "confidence": "high",
            "source": "provider",
            "evidence": {"signals": ["cash_withdrawal_text"], "paired_transaction_id": None},
        }

    if amount > 0 and is_depository and _matches(text, _CASH_DEPOSIT_PATTERNS):
        return {
            "category": _category_allowed("Cash Deposit", active_categories),
            "confidence": "high",
            "source": "provider",
            "evidence": {"signals": ["cash_deposit_text"], "paired_transaction_id": None},
        }

    if amount > 0 and is_credit and _matches(text, _CREDIT_REFUND_PATTERNS):
        return {
            "category": _category_allowed(CREDITS_REFUNDS_CATEGORY, active_categories),
            "confidence": "high",
            "source": "provider",
            "evidence": {"signals": ["positive_credit_card_unpaired", "credit_refund_text"], "paired_transaction_id": None},
        }

    if amount > 0 and is_credit:
        return {
            "category": _category_allowed(CREDITS_REFUNDS_CATEGORY, active_categories),
            "confidence": "medium",
            "source": "provider",
            "evidence": {"signals": ["positive_credit_card_unpaired"], "paired_transaction_id": None},
        }

    if amount < 0 and is_depository and tx_type in {"ach", "payment", "transfer", "debit", ""}:
        if _matches(text, _CARD_PAYMENT_PATTERNS):
            return {
                "category": _category_allowed("Credit Card Payment", active_categories),
                "confidence": "high",
                "source": "provider",
                "evidence": {
                    "signals": ["negative_depository_outflow", "payment_rail", "card_payment_text"],
                    "paired_transaction_id": None,
                },
            }

    if amount != 0 and is_depository and _matches(text, _SAVINGS_TRANSFER_PATTERNS):
        return {
            "category": _category_allowed("Savings Transfer", active_categories),
            "confidence": "medium",
            "source": "provider",
            "evidence": {"signals": ["transfer_rail", "savings_transfer_text"], "paired_transaction_id": None},
        }

    if amount > 0 and is_depository and _matches(text, _TAX_REFUND_PATTERNS):
        return {
            "category": _category_allowed(CREDITS_REFUNDS_CATEGORY, active_categories),
            "confidence": "high",
            "source": "provider",
            "evidence": {"signals": ["positive_depository_inflow", "tax_refund_text"], "paired_transaction_id": None},
        }

    if amount > 0 and is_depository:
        if tx_type == "interest":
            return {
                "category": _category_allowed("Income", active_categories),
                "confidence": "high",
                "source": "provider",
                "evidence": {"signals": ["interest_income"], "paired_transaction_id": None},
            }
        if _matches(text, _PAYROLL_PATTERNS):
            return {
                "category": _category_allowed("Income", active_categories),
                "confidence": "high",
                "source": "provider",
                "evidence": {"signals": ["positive_depository_inflow", "payroll_text"], "paired_transaction_id": None},
            }
        if _matches(text, _CREDIT_REFUND_PATTERNS):
            return {
                "category": _category_allowed(CREDITS_REFUNDS_CATEGORY, active_categories),
                "confidence": "medium",
                "source": "provider",
                "evidence": {"signals": ["positive_depository_inflow", "credit_refund_text"], "paired_transaction_id": None},
            }

    return {
        "category": "Other",
        "confidence": "low",
        "source": "fallback",
        "evidence": {"signals": ["no_strong_cashflow_evidence"], "paired_transaction_id": None},
    }
