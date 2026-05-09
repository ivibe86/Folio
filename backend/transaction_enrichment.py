from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from merchant_identity import canonicalize_merchant_key


TAXONOMY_VERSION = "folio_taxonomy_v1"
RULE_VERSION = "transaction_enrichment_rules_v1"
LOW_CONFIDENCE_DEFAULT = 0.7

TOP_LEVEL_CATEGORIES = (
    "Income",
    "Transfers",
    "Housing",
    "Utilities",
    "Groceries",
    "Dining",
    "Transportation",
    "Healthcare",
    "Insurance",
    "Debt & Payments",
    "Subscriptions",
    "Entertainment",
    "Shopping",
    "Travel",
    "Taxes",
    "Fees & Financial",
    "Personal Care",
    "Other",
)

FOLIO_CATEGORY_MAP: dict[str, tuple[str, str, str, str]] = {
    "food & dining": ("Dining", "Restaurants", "Meals", "discretionary"),
    "dining": ("Dining", "Restaurants", "Meals", "discretionary"),
    "restaurants": ("Dining", "Restaurants", "Meals", "discretionary"),
    "groceries": ("Groceries", "Groceries", "Household food", "essential"),
    "transportation": ("Transportation", "Transportation", "Mobility", "essential"),
    "gas & fuel": ("Transportation", "Gas & Fuel", "Vehicle fuel", "essential"),
    "parking & tolls": ("Transportation", "Parking & Tolls", "Vehicle access", "essential"),
    "auto maintenance": ("Transportation", "Auto Maintenance", "Vehicle upkeep", "essential"),
    "vehicle registration": ("Transportation", "Vehicle Registration", "Vehicle licensing", "essential"),
    "entertainment": ("Entertainment", "Entertainment", "Leisure", "discretionary"),
    "shopping": ("Shopping", "General Shopping", "Retail purchases", "discretionary"),
    "household supplies": ("Shopping", "Household Supplies", "Household essentials", "essential"),
    "personal care": ("Personal Care", "Personal Care", "Personal services", "discretionary"),
    "healthcare": ("Healthcare", "Healthcare", "Health", "essential"),
    "utilities": ("Utilities", "Utilities", "Home services", "essential"),
    "internet & phone": ("Utilities", "Internet & Phone", "Telecom", "essential"),
    "housing": ("Housing", "Housing", "Shelter", "essential"),
    "rent & mortgage": ("Housing", "Rent & Mortgage", "Shelter payment", "essential"),
    "home maintenance": ("Housing", "Home Maintenance", "Home upkeep", "essential"),
    "education": ("Other", "Education", "Education", "discretionary"),
    "childcare": ("Other", "Childcare", "Caregiving", "essential"),
    "pets": ("Other", "Pets", "Pet care", "discretionary"),
    "gifts & donations": ("Other", "Gifts & Donations", "Giving", "discretionary"),
    "auto payment": ("Debt & Payments", "Auto Payment", "Vehicle debt payment", "essential"),
    "debt & loan payment": ("Debt & Payments", "Debt & Loan Payment", "Debt payment", "essential"),
    "savings transfer": ("Transfers", "Savings Transfer", "Internal transfer", "non_expense"),
    "personal transfer": ("Transfers", "Personal Transfer", "Personal transfer", "non_expense"),
    "cash withdrawal": ("Transfers", "Cash Withdrawal", "Cash movement", "non_expense"),
    "cash deposit": ("Transfers", "Cash Deposit", "Cash movement", "non_expense"),
    "investment transfer": ("Transfers", "Investment Transfer", "Investment movement", "non_expense"),
    "credit card payment": ("Debt & Payments", "Credit Card Payment", "Debt payment", "non_expense"),
    "income": ("Income", "Income", "Income", "non_expense"),
    "subscriptions": ("Subscriptions", "Subscriptions", "Recurring services", "essential"),
    "fees & charges": ("Fees & Financial", "Bank Fees", "Financial fees", "essential"),
    "fees": ("Fees & Financial", "Bank Fees", "Financial fees", "essential"),
    "travel": ("Travel", "Travel", "Travel", "discretionary"),
    "taxes": ("Taxes", "Taxes", "Taxes", "essential"),
    "insurance": ("Insurance", "Insurance", "Risk protection", "essential"),
    "other": ("Other", "Other", "Unclassified spending", "unknown"),
}

PROVIDER_CATEGORY_MAP: dict[str, str] = {
    "bar": "Food & Dining",
    "dining": "Food & Dining",
    "groceries": "Groceries",
    "education": "Education",
    "fuel": "Gas & Fuel",
    "transport": "Transportation",
    "transportation": "Transportation",
    "health": "Healthcare",
    "home": "Housing",
    "income": "Income",
    "insurance": "Insurance",
    "investment": "Investment Transfer",
    "loan": "Debt & Loan Payment",
    "phone": "Internet & Phone",
    "software": "Subscriptions",
    "tax": "Taxes",
    "utilities": "Utilities",
}

INDUSTRY_CATEGORY_MAP: dict[str, str] = {
    "grocery": "Groceries",
    "restaurant": "Food & Dining",
    "coffee shop": "Food & Dining",
    "fast food": "Food & Dining",
    "bar / nightlife": "Food & Dining",
    "gas station": "Gas & Fuel",
    "pharmacy": "Healthcare",
    "healthcare provider": "Healthcare",
    "insurance": "Insurance",
    "utilities": "Utilities",
    "internet / telecom": "Internet & Phone",
    "streaming / media": "Subscriptions",
    "software / saas": "Subscriptions",
    "e-commerce marketplace": "Shopping",
    "electronics retail": "Shopping",
    "general retail": "Shopping",
    "home improvement": "Home Maintenance",
    "transportation / rideshare": "Transportation",
    "travel / airline": "Travel",
    "travel / hotel": "Travel",
    "subscription service": "Subscriptions",
    "bank / financial service": "Fees & Charges",
    "government / tax": "Taxes",
    "education": "Education",
    "fitness": "Personal Care",
    "personal care": "Personal Care",
}

SEMANTIC_NON_EXPENSE_CATEGORIES = {
    "income": "income",
    "savings transfer": "transfer",
    "personal transfer": "transfer",
    "cash withdrawal": "transfer",
    "cash deposit": "transfer",
    "investment transfer": "transfer",
    "credit card payment": "payment",
}

CORRECTABLE_FIELDS = {
    "canonical_counterparty",
    "display_counterparty",
    "top_level_category",
    "leaf_category",
    "purpose_category",
    "essentiality",
    "recurrence",
    "semantic_type",
}


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent local schema helper for scripts/tests that do not call init_db()."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS transaction_enrichment (
            transaction_id              TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
            profile_id                  TEXT NOT NULL,
            canonical_counterparty      TEXT DEFAULT '',
            display_counterparty        TEXT DEFAULT '',
            top_level_category          TEXT DEFAULT '',
            leaf_category               TEXT DEFAULT '',
            purpose_category            TEXT DEFAULT '',
            essentiality                TEXT DEFAULT 'unknown',
            recurrence                  TEXT DEFAULT 'unknown',
            semantic_type               TEXT DEFAULT 'spending',
            confidence_json             TEXT NOT NULL DEFAULT '{}',
            evidence_summary            TEXT DEFAULT '',
            evidence_json               TEXT NOT NULL DEFAULT '{}',
            source                      TEXT NOT NULL DEFAULT 'rules',
            method                      TEXT NOT NULL DEFAULT 'deterministic',
            model_version               TEXT NOT NULL DEFAULT 'transaction_enrichment_rules_v1',
            taxonomy_version            TEXT NOT NULL DEFAULT 'folio_taxonomy_v1',
            user_reviewed               INTEGER NOT NULL DEFAULT 0,
            created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (transaction_id, profile_id)
        );
        CREATE TABLE IF NOT EXISTS transaction_enrichment_corrections (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id      TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
            profile_id          TEXT NOT NULL,
            corrected_field     TEXT NOT NULL,
            old_value           TEXT DEFAULT '',
            new_value           TEXT NOT NULL,
            source              TEXT NOT NULL DEFAULT 'user/manual',
            created_at          TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_transaction_enrichment_profile
            ON transaction_enrichment(profile_id, updated_at);
        CREATE INDEX IF NOT EXISTS idx_transaction_enrichment_category
            ON transaction_enrichment(profile_id, top_level_category, leaf_category);
        CREATE INDEX IF NOT EXISTS idx_transaction_enrichment_counterparty
            ON transaction_enrichment(profile_id, canonical_counterparty);
        CREATE INDEX IF NOT EXISTS idx_transaction_enrichment_review
            ON transaction_enrichment(profile_id, user_reviewed);
        CREATE INDEX IF NOT EXISTS idx_tx_enrichment_corrections_tx
            ON transaction_enrichment_corrections(profile_id, transaction_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_tx_enrichment_corrections_field
            ON transaction_enrichment_corrections(profile_id, corrected_field, created_at);
        """
    )


def taxonomy_snapshot() -> dict[str, Any]:
    return {
        "version": TAXONOMY_VERSION,
        "top_level_categories": list(TOP_LEVEL_CATEGORIES),
        "folio_category_map": {
            name: {
                "top_level_category": values[0],
                "leaf_category": values[1],
                "purpose_category": values[2],
                "essentiality": values[3],
            }
            for name, values in sorted(FOLIO_CATEGORY_MAP.items())
        },
    }


def enrich_transaction_dict(
    tx: dict[str, Any],
    conn: sqlite3.Connection | None = None,
    *,
    apply_user_corrections: bool = True,
) -> dict[str, Any]:
    profile_id = str(tx.get("profile_id") or tx.get("profile") or "household")
    transaction_id = str(tx.get("id") or tx.get("original_id") or tx.get("transaction_id") or "")
    confidence: dict[str, float] = {}
    evidence: dict[str, Any] = {"layers": []}

    counterparty = _counterparty(tx, conn, profile_id, confidence, evidence)
    category = _category(tx, confidence, evidence)
    semantic_type, semantic_conf = _semantic_type(tx, category)
    confidence["semantic_type"] = semantic_conf
    recurrence, recurrence_conf, recurrence_evidence = _recurrence(tx, conn, profile_id, counterparty["canonical_counterparty"])
    confidence["recurrence"] = recurrence_conf
    if recurrence_evidence:
        evidence["recurring"] = recurrence_evidence
        evidence["layers"].append("recurring_obligation")

    if bool(tx.get("is_excluded")):
        semantic_type = "excluded"
        confidence["semantic_type"] = 1.0
        evidence["layers"].append("explicit_excluded_flag")

    result = {
        "transaction_id": transaction_id,
        "profile_id": profile_id,
        "canonical_counterparty": counterparty["canonical_counterparty"],
        "display_counterparty": counterparty["display_counterparty"],
        "top_level_category": category["top_level_category"],
        "leaf_category": category["leaf_category"],
        "purpose_category": category["purpose_category"],
        "essentiality": category["essentiality"],
        "recurrence": recurrence,
        "semantic_type": semantic_type,
        "confidence": _round_confidence(confidence),
        "evidence_summary": _evidence_summary(tx, counterparty, category, semantic_type, recurrence, evidence),
        "evidence": evidence,
        "source": "rules",
        "method": "deterministic",
        "model_version": RULE_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
        "user_reviewed": 0,
    }

    if conn is not None and apply_user_corrections and transaction_id:
        _apply_latest_corrections(conn, result)
    return result


def enrich_transaction_by_id(
    conn: sqlite3.Connection,
    transaction_id: str,
    profile_id: str | None = None,
    *,
    persist: bool = False,
) -> dict[str, Any] | None:
    tx = _load_transaction(conn, transaction_id, profile_id)
    if tx is None:
        return None
    enrichment = enrich_transaction_dict(tx, conn)
    if persist:
        upsert_enrichment(conn, enrichment)
        stored = get_stored_enrichment(conn, enrichment["transaction_id"], enrichment["profile_id"])
        return stored or enrichment
    return enrichment


def get_stored_enrichment(conn: sqlite3.Connection, transaction_id: str, profile_id: str | None = None) -> dict[str, Any] | None:
    ensure_schema(conn)
    params: list[Any] = [transaction_id]
    where = "transaction_id = ?"
    if profile_id and profile_id != "household":
        where += " AND profile_id = ?"
        params.append(profile_id)
    row = conn.execute(f"SELECT * FROM transaction_enrichment WHERE {where} LIMIT 1", params).fetchone()
    if row is None:
        return None
    return _row_to_enrichment(row, persisted=True)


def upsert_enrichment(conn: sqlite3.Connection, enrichment: dict[str, Any]) -> None:
    ensure_schema(conn)
    transaction_id = str(enrichment.get("transaction_id") or "")
    profile_id = str(enrichment.get("profile_id") or "household")
    corrected_fields = _corrected_fields(conn, transaction_id, profile_id)
    stored = get_stored_enrichment(conn, transaction_id, profile_id) if corrected_fields else None
    storage = dict(enrichment)
    if stored:
        for field in corrected_fields:
            if field in CORRECTABLE_FIELDS:
                storage[field] = stored.get(field, storage.get(field, ""))
    storage["user_reviewed"] = int(bool(corrected_fields or storage.get("user_reviewed")))

    confidence = storage.get("confidence") if isinstance(storage.get("confidence"), dict) else {}
    evidence = storage.get("evidence") if isinstance(storage.get("evidence"), dict) else {}
    conn.execute(
        """
        INSERT INTO transaction_enrichment (
            transaction_id, profile_id, canonical_counterparty, display_counterparty,
            top_level_category, leaf_category, purpose_category, essentiality,
            recurrence, semantic_type, confidence_json, evidence_summary,
            evidence_json, source, method, model_version, taxonomy_version, user_reviewed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transaction_id, profile_id) DO UPDATE SET
            canonical_counterparty = excluded.canonical_counterparty,
            display_counterparty = excluded.display_counterparty,
            top_level_category = excluded.top_level_category,
            leaf_category = excluded.leaf_category,
            purpose_category = excluded.purpose_category,
            essentiality = excluded.essentiality,
            recurrence = excluded.recurrence,
            semantic_type = excluded.semantic_type,
            confidence_json = excluded.confidence_json,
            evidence_summary = excluded.evidence_summary,
            evidence_json = excluded.evidence_json,
            source = excluded.source,
            method = excluded.method,
            model_version = excluded.model_version,
            taxonomy_version = excluded.taxonomy_version,
            user_reviewed = excluded.user_reviewed,
            updated_at = datetime('now')
        """,
        (
            transaction_id,
            profile_id,
            storage.get("canonical_counterparty", ""),
            storage.get("display_counterparty", ""),
            storage.get("top_level_category", ""),
            storage.get("leaf_category", ""),
            storage.get("purpose_category", ""),
            storage.get("essentiality", "unknown"),
            storage.get("recurrence", "unknown"),
            storage.get("semantic_type", "spending"),
            json.dumps(confidence, sort_keys=True),
            storage.get("evidence_summary", ""),
            json.dumps(evidence, sort_keys=True),
            storage.get("source", "rules"),
            storage.get("method", "deterministic"),
            storage.get("model_version", RULE_VERSION),
            storage.get("taxonomy_version", TAXONOMY_VERSION),
            int(bool(storage.get("user_reviewed"))),
        ),
    )


def record_correction(
    conn: sqlite3.Connection,
    *,
    transaction_id: str,
    profile_id: str,
    corrected_field: str,
    new_value: str,
    source: str = "user/manual",
) -> dict[str, Any]:
    ensure_schema(conn)
    if corrected_field not in CORRECTABLE_FIELDS:
        raise ValueError(f"Unsupported correction field: {corrected_field}")
    current = get_stored_enrichment(conn, transaction_id, profile_id)
    if current is None:
        current = enrich_transaction_by_id(conn, transaction_id, profile_id, persist=True)
    if current is None:
        raise ValueError("transaction not found")
    old_value = str(current.get(corrected_field) or "")
    conn.execute(
        """
        INSERT INTO transaction_enrichment_corrections
            (transaction_id, profile_id, corrected_field, old_value, new_value, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (transaction_id, profile_id, corrected_field, old_value, str(new_value), source),
    )
    conn.execute(
        f"""
        UPDATE transaction_enrichment
           SET {corrected_field} = ?,
               user_reviewed = 1,
               updated_at = datetime('now')
         WHERE transaction_id = ? AND profile_id = ?
        """,
        (str(new_value), transaction_id, profile_id),
    )
    updated = get_stored_enrichment(conn, transaction_id, profile_id) or {}
    return {
        "transaction_id": transaction_id,
        "profile_id": profile_id,
        "corrected_field": corrected_field,
        "old_value": old_value,
        "new_value": str(new_value),
        "source": source,
        "enrichment": updated,
    }


def explain_transaction(conn: sqlite3.Connection, transaction_id: str, profile_id: str | None = None) -> dict[str, Any]:
    ensure_schema(conn)
    tx = _load_transaction(conn, transaction_id, profile_id)
    if tx is None:
        return {"error": "transaction not found", "transaction_id": transaction_id}
    stored = get_stored_enrichment(conn, transaction_id, tx.get("profile_id"))
    enrichment = stored or enrich_transaction_dict(tx, conn)
    corrections = _correction_rows(conn, transaction_id, tx.get("profile_id"))
    return {
        "transaction_id": transaction_id,
        "profile_id": tx.get("profile_id"),
        "persisted": bool(stored),
        "transaction": _public_transaction(tx),
        "enrichment": enrichment,
        "confidence": enrichment.get("confidence", {}),
        "evidence_summary": enrichment.get("evidence_summary", ""),
        "corrections": corrections,
        "provenance": {
            "tool": "explain_transaction_enrichment",
            "transaction_id": transaction_id,
            "profile_id": tx.get("profile_id"),
            "source": enrichment.get("source"),
            "method": enrichment.get("method"),
            "model_version": enrichment.get("model_version"),
            "taxonomy_version": enrichment.get("taxonomy_version"),
        },
    }


def find_low_confidence(
    conn: sqlite3.Connection,
    profile_id: str | None = None,
    *,
    threshold: float = LOW_CONFIDENCE_DEFAULT,
    limit: int = 25,
) -> dict[str, Any]:
    ensure_schema(conn)
    threshold = max(0.0, min(float(threshold), 1.0))
    limit = max(1, min(int(limit or 25), 100))
    rows = _candidate_transactions(conn, profile_id, max(limit * 4, 50))
    matches: list[dict[str, Any]] = []
    for tx in rows:
        stored = get_stored_enrichment(conn, tx["id"], tx["profile_id"])
        enrichment = stored or enrich_transaction_dict(tx, conn)
        min_conf = min((enrichment.get("confidence") or {"overall": 0}).values() or [0])
        if min_conf < threshold or not stored:
            matches.append(
                {
                    "transaction_id": tx["id"],
                    "profile_id": tx["profile_id"],
                    "date": tx.get("date"),
                    "description": tx.get("description"),
                    "amount": tx.get("amount"),
                    "category": tx.get("category"),
                    "persisted": bool(stored),
                    "minimum_confidence": round(float(min_conf), 3),
                    "enrichment": _compact_enrichment(enrichment),
                    "evidence_summary": enrichment.get("evidence_summary", ""),
                }
            )
        if len(matches) >= limit:
            break
    return {
        "threshold": threshold,
        "count": len(matches),
        "transactions": matches,
        "provenance": {
            "tool": "find_low_confidence_transactions",
            "profile_id": profile_id or "household",
            "threshold": threshold,
            "limit": limit,
        },
    }


def quality_summary(conn: sqlite3.Connection, profile_id: str | None = None) -> dict[str, Any]:
    ensure_schema(conn)
    pwhere, pparams = _profile_where(profile_id, "t")
    total = conn.execute(f"SELECT COUNT(*) FROM transactions t WHERE 1=1{pwhere}", pparams).fetchone()[0]
    ewhere, eparams = _profile_where(profile_id, "e")
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS persisted,
               SUM(CASE WHEN user_reviewed = 1 THEN 1 ELSE 0 END) AS reviewed
          FROM transaction_enrichment e
         WHERE 1=1{ewhere}
        """,
        eparams,
    ).fetchone()
    persisted = int(row["persisted"] or 0)
    reviewed = int(row["reviewed"] or 0)
    low = conn.execute(
        f"SELECT confidence_json FROM transaction_enrichment e WHERE 1=1{ewhere}",
        eparams,
    ).fetchall()
    low_count = 0
    for item in low:
        conf = _json_dict(item["confidence_json"])
        if conf and min(conf.values()) < LOW_CONFIDENCE_DEFAULT:
            low_count += 1
    top_rows = conn.execute(
        f"""
        SELECT top_level_category, COUNT(*) AS count
          FROM transaction_enrichment e
         WHERE 1=1{ewhere}
         GROUP BY top_level_category
         ORDER BY count DESC, top_level_category
        """,
        eparams,
    ).fetchall()
    sem_rows = conn.execute(
        f"""
        SELECT semantic_type, COUNT(*) AS count
          FROM transaction_enrichment e
         WHERE 1=1{ewhere}
         GROUP BY semantic_type
         ORDER BY count DESC, semantic_type
        """,
        eparams,
    ).fetchall()
    return {
        "profile_id": profile_id or "household",
        "transaction_count": int(total or 0),
        "persisted_enrichment_count": persisted,
        "coverage_ratio": round(persisted / total, 4) if total else 0.0,
        "user_reviewed_count": reviewed,
        "low_confidence_count": low_count,
        "low_confidence_threshold": LOW_CONFIDENCE_DEFAULT,
        "taxonomy_version": TAXONOMY_VERSION,
        "model_version": RULE_VERSION,
        "top_level_distribution": [dict(row) for row in top_rows],
        "semantic_type_distribution": [dict(row) for row in sem_rows],
        "provenance": {
            "tool": "get_enrichment_quality_summary",
            "profile_id": profile_id or "household",
            "source": "transaction_enrichment",
        },
    }


def classify_ambiguous_with_local_model(tx: dict[str, Any]) -> None:
    """Reserved Layer 1 hook; current production slice stays deterministic."""
    return None


def _counterparty(
    tx: dict[str, Any],
    conn: sqlite3.Connection | None,
    profile_id: str,
    confidence: dict[str, float],
    evidence: dict[str, Any],
) -> dict[str, str]:
    kind = str(tx.get("merchant_kind") or "").strip()
    merchant_key = str(tx.get("merchant_key") or "").strip()
    merchant_name = str(tx.get("merchant_name") or "").strip()
    counterparty = str(tx.get("counterparty_name") or "").strip()
    description = str(tx.get("description") or tx.get("raw_description") or "").strip()

    display = merchant_name or counterparty or description
    canonical = merchant_key or canonicalize_merchant_key(display) or display.upper()
    source = "merchant_identity" if merchant_key or merchant_name else "description"
    conf = _merchant_confidence(tx.get("merchant_confidence"), source)
    if kind and kind != "merchant_purchase" and not merchant_name:
        canonical = kind
        display = counterparty or _title_from_description(description) or kind.replace("_", " ").title()
        conf = 0.86
        source = "non_merchant_kind"

    alias = _merchant_alias(conn, canonical, profile_id) if conn is not None and canonical else ""
    if alias:
        display = alias
        evidence["layers"].append("merchant_alias")

    confidence["canonical_counterparty"] = conf
    confidence["display_counterparty"] = conf if display else 0.35
    evidence["counterparty_source"] = source
    if merchant_key:
        evidence["merchant_key"] = merchant_key
    return {"canonical_counterparty": canonical, "display_counterparty": display}


def _category(tx: dict[str, Any], confidence: dict[str, float], evidence: dict[str, Any]) -> dict[str, str]:
    raw_category = str(tx.get("category") or "").strip()
    category_source = "existing_category"
    category_conf = 0.82 if raw_category and raw_category.lower() not in {"other", "uncategorized"} else 0.45

    if not raw_category:
        provider = str(tx.get("teller_category") or "").strip().lower()
        raw_category = PROVIDER_CATEGORY_MAP.get(provider, "")
        if raw_category:
            category_source = "provider_category"
            category_conf = 0.66

    if not raw_category:
        industry = str(tx.get("merchant_industry") or "").strip().lower()
        raw_category = INDUSTRY_CATEGORY_MAP.get(industry, "")
        if raw_category:
            category_source = "merchant_industry"
            category_conf = 0.62

    mapped = _map_category(raw_category)
    if mapped.get("custom"):
        category_conf = min(category_conf, 0.58)
        category_source = "custom_category_alias"

    confidence["top_level_category"] = category_conf
    confidence["leaf_category"] = category_conf
    confidence["purpose_category"] = max(0.5, category_conf - 0.05)
    confidence["essentiality"] = _essentiality_confidence(mapped["essentiality"], category_source)
    evidence["category_source"] = category_source
    evidence["original_category"] = raw_category
    evidence["layers"].append(category_source)
    return mapped


def _map_category(category: str) -> dict[str, Any]:
    key = str(category or "").strip().lower()
    if key in FOLIO_CATEGORY_MAP:
        top, leaf, purpose, essentiality = FOLIO_CATEGORY_MAP[key]
        return {
            "top_level_category": top,
            "leaf_category": leaf,
            "purpose_category": purpose,
            "essentiality": essentiality,
            "custom": False,
        }
    if not key:
        return {
            "top_level_category": "Other",
            "leaf_category": "Uncategorized",
            "purpose_category": "Needs review",
            "essentiality": "unknown",
            "custom": True,
        }
    inferred = _infer_custom_top_level(key)
    return {
        "top_level_category": inferred,
        "leaf_category": category.strip(),
        "purpose_category": category.strip(),
        "essentiality": _essentiality_for_top(inferred),
        "custom": True,
    }


def _semantic_type(tx: dict[str, Any], category: dict[str, str]) -> tuple[str, float]:
    cat = str(tx.get("category") or category.get("leaf_category") or "").strip().lower()
    if cat in SEMANTIC_NON_EXPENSE_CATEGORIES:
        return SEMANTIC_NON_EXPENSE_CATEGORIES[cat], 0.96
    expense_type = str(tx.get("expense_type") or "").strip().lower()
    if expense_type.startswith("transfer"):
        return "transfer", 0.95
    try:
        amount = float(tx.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0.0
    merchant_kind = str(tx.get("merchant_kind") or "").strip().lower()
    description = f"{tx.get('description') or ''} {tx.get('raw_description') or ''}".lower()
    tax_refund_text = (
        "tax ref" in description
        or "tax refund" in description
        or "treas tax ref" in description
        or ("irs treas" in description and "ref" in description)
    )
    if amount > 0 and (merchant_kind == "tax" or cat == "taxes") and tax_refund_text:
        return "refund", 0.9
    if merchant_kind in {"personal_transfer", "credit_card_payment", "income", "tax", "bank_fee"}:
        mapped = {"personal_transfer": "transfer", "credit_card_payment": "payment", "income": "income", "tax": "spending", "bank_fee": "fee"}
        return mapped[merchant_kind], 0.9
    if amount > 0 and cat not in {"income"}:
        return "refund", 0.62
    return "spending", 0.82 if cat else 0.55


def _recurrence(
    tx: dict[str, Any],
    conn: sqlite3.Connection | None,
    profile_id: str,
    canonical_counterparty: str,
) -> tuple[str, float, dict[str, Any]]:
    key = canonical_counterparty or str(tx.get("merchant_key") or "")
    if conn is not None and key:
        try:
            row = conn.execute(
                """
                SELECT display_name, state, confidence_score, confidence_label, frequency
                  FROM recurring_obligations
                 WHERE profile_id = ?
                   AND merchant_key = ?
                   AND state IN ('active', 'confirmed', 'candidate')
                 ORDER BY CASE state WHEN 'confirmed' THEN 0 WHEN 'active' THEN 1 ELSE 2 END,
                          confidence_score DESC
                 LIMIT 1
                """,
                (profile_id, key),
            ).fetchone()
            if row:
                score = max(0.0, min(float(row["confidence_score"] or 0) / 100.0, 1.0))
                state = str(row["state"] or "")
                recurrence = "recurring" if state in {"active", "confirmed"} else "likely_recurring"
                return recurrence, max(score, 0.75), {
                    "display_name": row["display_name"],
                    "state": state,
                    "confidence_label": row["confidence_label"],
                    "frequency": row["frequency"],
                }
        except Exception:
            pass
    category = str(tx.get("category") or "").strip().lower()
    if category == "subscriptions":
        return "likely_recurring", 0.76, {"category": "Subscriptions"}
    return "one_off", 0.68, {}


def _apply_latest_corrections(conn: sqlite3.Connection, enrichment: dict[str, Any]) -> None:
    rows = _correction_rows(conn, enrichment["transaction_id"], enrichment["profile_id"])
    if not rows:
        return
    for row in rows:
        field = row.get("corrected_field")
        if field in CORRECTABLE_FIELDS:
            enrichment[field] = row.get("new_value") or ""
    enrichment["user_reviewed"] = 1
    evidence = enrichment.get("evidence")
    if isinstance(evidence, dict):
        evidence["user_corrections"] = rows
    enrichment["evidence_summary"] = (enrichment.get("evidence_summary") or "") + " User-reviewed correction applied."


def _correction_rows(conn: sqlite3.Connection, transaction_id: str, profile_id: str | None) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT corrected_field, old_value, new_value, source, created_at
          FROM transaction_enrichment_corrections
         WHERE transaction_id = ? AND profile_id = ?
         ORDER BY created_at DESC, id DESC
        """,
        (transaction_id, profile_id),
    ).fetchall()
    latest: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        field = str(row["corrected_field"] or "")
        if field in seen:
            continue
        seen.add(field)
        latest.append(dict(row))
    return latest


def _corrected_fields(conn: sqlite3.Connection, transaction_id: str, profile_id: str | None) -> set[str]:
    if not transaction_id:
        return set()
    return {
        str(row.get("corrected_field") or "")
        for row in _correction_rows(conn, transaction_id, profile_id)
        if row.get("corrected_field") in CORRECTABLE_FIELDS
    }


def _load_transaction(conn: sqlite3.Connection, transaction_id: str, profile_id: str | None = None) -> dict[str, Any] | None:
    params: list[Any] = [transaction_id]
    where = "id = ?"
    if profile_id and profile_id != "household":
        where += " AND profile_id = ?"
        params.append(profile_id)
    row = conn.execute(f"SELECT * FROM transactions WHERE {where} LIMIT 1", params).fetchone()
    return dict(row) if row else None


def _candidate_transactions(conn: sqlite3.Connection, profile_id: str | None, limit: int) -> list[dict[str, Any]]:
    pwhere, params = _profile_where(profile_id, "t")
    rows = conn.execute(
        f"""
        SELECT t.*
          FROM transactions t
          LEFT JOIN transaction_enrichment e
            ON e.transaction_id = t.id AND e.profile_id = t.profile_id
         WHERE 1=1{pwhere}
         ORDER BY CASE WHEN e.transaction_id IS NULL THEN 0 ELSE 1 END,
                  t.date DESC, t.id DESC
         LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return [dict(row) for row in rows]


def _row_to_enrichment(row: sqlite3.Row, *, persisted: bool) -> dict[str, Any]:
    data = dict(row)
    data["confidence"] = _json_dict(data.pop("confidence_json", "{}"))
    data["evidence"] = _json_dict(data.pop("evidence_json", "{}"))
    data["user_reviewed"] = bool(data.get("user_reviewed"))
    data["persisted"] = persisted
    return data


def _compact_enrichment(enrichment: dict[str, Any]) -> dict[str, Any]:
    return {
        key: enrichment.get(key)
        for key in (
            "canonical_counterparty",
            "display_counterparty",
            "top_level_category",
            "leaf_category",
            "purpose_category",
            "essentiality",
            "recurrence",
            "semantic_type",
            "user_reviewed",
        )
    } | {"confidence": enrichment.get("confidence", {})}


def _public_transaction(tx: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": tx.get("id"),
        "date": tx.get("date"),
        "description": tx.get("description"),
        "amount": tx.get("amount"),
        "category": tx.get("category"),
        "merchant_name": tx.get("merchant_name"),
        "merchant_key": tx.get("merchant_key"),
        "merchant_kind": tx.get("merchant_kind"),
        "is_excluded": bool(tx.get("is_excluded")),
    }


def _merchant_alias(conn: sqlite3.Connection | None, merchant_key: str, profile_id: str) -> str:
    if conn is None or not merchant_key:
        return ""
    try:
        row = conn.execute(
            "SELECT display_name FROM merchant_aliases WHERE merchant_key = ? AND profile_id = ?",
            (merchant_key, profile_id),
        ).fetchone()
    except Exception:
        return ""
    return str(row["display_name"] or "") if row else ""


def _merchant_confidence(raw: Any, source: str) -> float:
    value = str(raw or "").strip().lower()
    if value == "high":
        return 0.9
    if value == "medium":
        return 0.72
    if value == "low":
        return 0.45
    return 0.78 if source == "merchant_identity" else 0.48


def _essentiality_confidence(essentiality: str, source: str) -> float:
    if essentiality == "unknown":
        return 0.45
    return 0.84 if source in {"existing_category", "custom_category_alias"} else 0.68


def _infer_custom_top_level(key: str) -> str:
    keyword_map = (
        (("rent", "mortgage", "home"), "Housing"),
        (("electric", "water", "internet", "phone"), "Utilities"),
        (("grocery", "market", "costco"), "Groceries"),
        (("restaurant", "coffee", "bar", "dining", "food"), "Dining"),
        (("gas", "uber", "lyft", "parking", "transit"), "Transportation"),
        (("doctor", "pharmacy", "medical", "health"), "Healthcare"),
        (("insurance",), "Insurance"),
        (("subscription", "streaming", "software"), "Subscriptions"),
        (("movie", "music", "game"), "Entertainment"),
        (("flight", "hotel", "travel"), "Travel"),
        (("tax", "irs"), "Taxes"),
        (("fee", "bank"), "Fees & Financial"),
        (("salary", "payroll", "income"), "Income"),
        (("transfer", "zelle", "venmo"), "Transfers"),
    )
    for needles, top in keyword_map:
        if any(needle in key for needle in needles):
            return top
    return "Other"


def _essentiality_for_top(top: str) -> str:
    if top in {"Income", "Transfers", "Debt & Payments"}:
        return "non_expense"
    if top in {"Housing", "Utilities", "Groceries", "Transportation", "Healthcare", "Insurance", "Taxes", "Fees & Financial"}:
        return "essential"
    if top in {"Dining", "Entertainment", "Shopping", "Travel", "Personal Care"}:
        return "discretionary"
    return "unknown"


def _evidence_summary(
    tx: dict[str, Any],
    counterparty: dict[str, str],
    category: dict[str, Any],
    semantic_type: str,
    recurrence: str,
    evidence: dict[str, Any],
) -> str:
    parts = [
        f"counterparty={counterparty.get('display_counterparty') or 'unknown'}",
        f"category={category.get('top_level_category')}/{category.get('leaf_category')}",
        f"semantic_type={semantic_type}",
        f"recurrence={recurrence}",
    ]
    if tx.get("category"):
        parts.append(f"existing_category={tx.get('category')}")
    if tx.get("teller_category"):
        parts.append(f"provider_category={tx.get('teller_category')}")
    layers = ", ".join(evidence.get("layers") or [])
    if layers:
        parts.append(f"layers={layers}")
    return "; ".join(parts)


def _title_from_description(value: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", value or "")
    return " ".join(words[:5]).title()


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _round_confidence(confidence: dict[str, float]) -> dict[str, float]:
    return {key: round(max(0.0, min(float(value), 1.0)), 3) for key, value in confidence.items()}


def _profile_where(profile_id: str | None, alias: str) -> tuple[str, list[Any]]:
    if profile_id and profile_id != "household":
        return f" AND {alias}.profile_id = ?", [profile_id]
    return "", []
