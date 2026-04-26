"""
categorizer.py
Two-phase categorization:
  Phase 1 — Deterministic rules using Teller API fields (works for any bank)
  Phase 2 — LLM categorization for uncategorized + validation of rule-based
"""

import json
import re
import time
import os
from dotenv import load_dotenv
from sanitizer import sanitize_transactions
from enricher import enrich_transactions
from log_config import get_logger
from privacy import mask_amount, mask_counterparty
from database import _extract_merchant_pattern
from merchant_identity import canonicalize_merchant_key
import llm_client

load_dotenv()

logger = get_logger(__name__)

# Feature toggle: set ENABLE_LLM_CATEGORIZATION=false in .env to skip LLM categorization
ENABLE_LLM = os.getenv("ENABLE_LLM_CATEGORIZATION", "true").lower() in ("true", "1", "yes")

# Default categories — used as fallback if DB is unavailable
_DEFAULT_CATEGORIES = [
    "Food & Dining",
    "Groceries",
    "Transportation",
    "Entertainment",
    "Shopping",
    "Healthcare",
    "Utilities",
    "Housing",
    "Savings Transfer",
    "Credit Card Payment",
    "Income",
    "Personal Transfer",
    "Subscriptions",
    "Fees & Charges",
    "Travel",
    "Taxes",
    "Insurance",
    "Other",
]


def get_active_categories() -> list[str]:
    """Fetch active categories from DB, with fallback to defaults."""
    try:
        from database import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT name FROM categories WHERE is_active = 1 ORDER BY name"
            ).fetchall()
            if rows:
                return [row[0] for row in rows]
    except Exception:
        pass
    return list(_DEFAULT_CATEGORIES)


# For backward compatibility — modules that import CATEGORIES get the live list
@property
def CATEGORIES():
    return get_active_categories()

# Keep a simple reference for imports that expect a list
CATEGORIES = _DEFAULT_CATEGORIES  # Will be replaced dynamically in functions that use it

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Deterministic Rule-Based Categorization
# ══════════════════════════════════════════════════════════════════════════════

# Confidence levels for rules:
#   "rule-high"   → Do NOT send to LLM (clear-cut, no ambiguity)
#   "rule-medium" → Send to LLM with suggestion for validation

# Generic patterns — work across US banks
CC_PAYMENT_PATTERNS = [
    r"credit\s*c(?:a)?rd",
    r"credit\s*crd",
    r"\bepay\b",
    r"\bautopay\b",
    r"applecard",
    r"gsbank.*payment",
    r"card\s*payment",
]

SAVINGS_TRANSFER_PATTERNS = [
    r"transfer\s+to\s+sav",
    r"transfer\s+from\s+chk",
    r"transfer\s+from\s+sav",
    r"transfer\s+to\s+chk",
    r"savings\s+transfer",
    r"\bdes:transfer\b",
]

INTERNAL_TRANSFER_PATTERNS = [
    r"online\s+(?:scheduled\s+)?transfer",
    r"internal\s+transfer",
    r"account\s+transfer",
    r"xfer\s+(?:to|from)",
    r"mobile\s+transfer",
]

P2P_PATTERNS = [
    r"\bzelle\b",
    r"\bvenmo\b",
    r"\bcashapp\b",
    r"cash\s*app",
    r"paypal.*(?:send|p2p|instant)",
    r"\bxoom\b",
]

TAX_PATTERNS = [
    r"\birs\b",
    r"tax\s*(?:payment|pymt|pmt|refund)",
    r"\bus\s*treasury\b",
    r"state\s*tax",
    r"franchise\s*tax",
    r"tax\s*board",
    r"usataxpymt",
]

def _matches_any(text: str, patterns: list[str]) -> bool:
    """Check if text matches any regex pattern (case-insensitive)."""
    text_lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, text_lower):
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# TELLER CATEGORY MAPPING (DB-backed, cached in memory)
# ══════════════════════════════════════════════════════════════════════════════

_teller_map_cache: dict[str, tuple[str | None, str]] | None = None


def _get_teller_category_map() -> dict[str, tuple[str | None, str]]:
    """
    Load the Teller category → Folio category mapping from the database.
    Cached in memory after first load. Returns a dict of:
        {teller_category: (folio_category_or_None, confidence)}

    User overrides (source='user') take precedence over system defaults —
    ORDER BY source DESC puts 'user' before 'system', and the dict keeps
    the last value written per key (i.e., user wins).
    """
    global _teller_map_cache
    if _teller_map_cache is not None:
        return _teller_map_cache

    mapping = {}
    try:
        from database import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT teller_category, folio_category, confidence
                   FROM teller_category_map
                   ORDER BY source ASC"""
            ).fetchall()
            for row in rows:
                mapping[row[0]] = (row[1], row[2])
    except Exception:
        logger.debug("Failed to load teller_category_map from DB — Teller hint mapping unavailable")

    _teller_map_cache = mapping
    return _teller_map_cache

def _rule_based_categorize(tx: dict) -> tuple[str | None, str]:
    """
    Apply deterministic rules to categorize a transaction.

    Returns:
        (category, confidence) where confidence is:
        - "rule-high": certain, skip LLM
        - "rule-medium": likely correct, but send to LLM for validation
        - (None, ""): no rule matched, needs full LLM categorization
    """
    tx_type = (tx.get("type") or "")
    amount = float(tx.get("amount", 0))
    account_type = (tx.get("account_type") or "")
    cp_type = (tx.get("counterparty_type") or "")
    teller_cat = (tx.get("teller_category") or "").lower()
    description = (tx.get("description") or "")
    is_credit = account_type in ("credit_card", "credit")

    # ── HIGH confidence rules (skip LLM) ──

    # Rule: Interest earned → Income
    if tx_type == "interest" and amount > 0:
        return "Income", "rule-high"

    # Rule: CC payments from bank side
    if not is_credit and tx_type == "ach" and amount < 0:
        if _matches_any(description, CC_PAYMENT_PATTERNS):
            return "Credit Card Payment", "rule-high"

    # Rule: Card-side payments (should be skipped by sanitizer, but safety net)
    if is_credit and tx_type == "payment":
        return None, ""  # sanitizer handles this

    # Rule: Internal savings transfers (description clearly says "transfer to SAV" etc.)
    if tx_type in ("transfer", "ach") and amount < 0 and _matches_any(description, SAVINGS_TRANSFER_PATTERNS):
        return "Savings Transfer", "rule-high"

    # Rule: Internal transfers between own accounts (not P2P)
    if tx_type in ("transfer", "ach") and amount < 0 and _matches_any(description, INTERNAL_TRANSFER_PATTERNS):
        if not _matches_any(description, P2P_PATTERNS):
            return "Savings Transfer", "rule-high"

    # Rule: Transfer/remittance services are not merchant spending.
    if tx_type in ("transfer", "ach") and amount < 0 and _matches_any(description, [r"\bxoom\b"]):
        return "Personal Transfer", "rule-high"

    # Rule: Fees on credit cards
    if tx_type == "fee" and is_credit:
        return "Fees & Charges", "rule-high"

    # Rule: Bank fees / adjustments
    if tx_type in ("fee", "adjustment") and not is_credit:
        return "Fees & Charges", "rule-high"

    # ── MEDIUM confidence rules (send to LLM with suggestion) ──

    # Rule: Deposits into bank accounts → probably Income
    if tx_type == "deposit" and amount > 0 and not is_credit:
        return "Income", "rule-medium"

    # Rule: Tax payments
    if _matches_any(description, TAX_PATTERNS) or teller_cat == "tax":
        return "Taxes", "rule-medium"

    # Rule: P2P transfers to a person
    if _matches_any(description, P2P_PATTERNS) and cp_type == "person":
        return "Personal Transfer", "rule-medium"

    # Rule: P2P transfers to organization (could be payment for a service)
    if _matches_any(description, P2P_PATTERNS) and cp_type == "organization":
        return None, ""  # Let LLM decide

    # Rule: ATM withdrawals
    if tx_type == "withdrawal":
        return "Other", "rule-medium"

    # Rule: Teller's own category hints (DB-backed mapping)
    if teller_cat:
        teller_map = _get_teller_category_map()
        if teller_cat in teller_map:
            folio_cat, confidence = teller_map[teller_cat]
            if folio_cat:  # NULL folio_category means "skip — no useful signal"
                return folio_cat, confidence
        else:
            # Unknown Teller category — log for future mapping additions
            logger.debug(
                "Unmapped Teller category '%s' for: %s",
                teller_cat, description[:80],
            )

    # ── No rule matched ──
    return None, ""


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: LLM Categorization + Validation
# ══════════════════════════════════════════════════════════════════════════════

def _build_llm_line(idx: int, tx: dict, rule_suggestion: str | None = None) -> str:
    """Build a single line for the LLM prompt. Amounts and counterparty names are masked."""
    masked_amount = mask_amount(tx.get("amount", 0))
    masked_cp = mask_counterparty(
        tx.get("counterparty_name", ""),
        tx.get("counterparty_type", ""),
    )
    line = (
        f"{idx}. Description: {tx['description']} | "
        f"Amount: {masked_amount} | Type: {tx['type']} | "
        f"Counterparty: {masked_cp} | "
        f"Account: {tx.get('account_type', '')}"
    )
    merchant_name = tx.get("merchant_name", "")
    merchant_domain = tx.get("merchant_domain", "")    
    merchant_industry = tx.get("merchant_industry", "")
    if merchant_name:
        line += f" | Merchant: {merchant_name}"
    if merchant_domain:
        line += f" | Domain: {merchant_domain}"        
    if merchant_industry:
        line += f" | Industry: {merchant_industry}"
    if rule_suggestion:
        line += f" | Pre-categorized: {rule_suggestion}"
    return line


def _categorize_batch_llm(batch: list[dict], start_index: int) -> list[dict]:
    """Send a batch of transactions to Claude for categorization / validation."""
    lines = ""
    for i, entry in enumerate(batch):
        idx = start_index + i
        tx = entry["tx"]
        suggestion = entry.get("suggestion")
        lines += _build_llm_line(idx, tx, suggestion) + "\n"

    active_categories = get_active_categories()
    prompt = f"""You are a personal finance categorizer. For each transaction, assign exactly one category from this list:
{', '.join(active_categories)}

Important rules:
- Amounts are anonymized: -$XXX = money going out (expenses/payments), +$XXX = money coming in (income/refunds)
- Counterparty names are anonymized: [person] or [organization] indicates the counterparty type
- Categorize based on the description, merchant name, industry, and transaction type and  if none are available merchant domain — not the amount
- Some transactions have a "Pre-categorized" suggestion from our rule engine
  - If the suggestion looks correct, use it
  - If the suggestion looks wrong based on the description, override it with the correct category
  - Pay special attention: deposits labeled "Income" are usually correct, but check if it might be a refund
- "Food & Dining" = restaurants, food delivery, cafes, bars
- "Groceries" = supermarkets, grocery stores, ethnic/specialty food stores
- "Shopping" = retail, Amazon, online shopping, general merchandise
- "Subscriptions" = recurring services (Netflix, Spotify, cloud services, premium features)
- "Transportation" = gas stations, parking, rideshare (Uber/Lyft), public transit, tolls
- "Travel" = airlines, hotels, car rental, travel bookings, in-flight purchases
- "Healthcare" = doctors, pharmacy, dental, vision, medical
- "Insurance" = auto insurance, health insurance, home/renters insurance
- "Utilities" = electric, water, gas, internet, phone bills
- "Housing" = rent, mortgage, home maintenance, home improvement
- "Entertainment" = movies, games, events, concerts, streaming (if not subscription)
- "Fees & Charges" = bank fees, interest charges, annual fees, late fees, ATM fees
- "Taxes" = IRS, state tax, property tax, tax payments
- "Personal Transfer" = person-to-person payments (Zelle, Venmo to individuals)
- Be specific — prefer the most accurate category over "Other"

Transactions:
{lines}

Respond ONLY with a JSON array, no markdown, no explanation:
[{{"index": 0, "category": "Category Name", "confidence": "high/medium/low"}}, ...]"""

    raw = llm_client.complete(prompt, max_tokens=4096, purpose="categorize")

    # Strip markdown fences
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

    return json.loads(raw.strip())


def _load_merchant_category_memory(conn, transactions: list[dict]) -> dict[tuple[str, str], dict]:
    """
    Build a lightweight merchant -> category memory from historical transactions.
    This is not model training; it is operational reuse of prior categorizations.

    We only trust:
    - user / user-rule history strongly
    - otherwise, a dominant non-fallback historical category for the same merchant
    """
    merchant_keys = {
        (tx.get("profile", ""), canonicalize_merchant_key(tx.get("merchant_key") or tx.get("merchant_name") or ""))
        for tx in transactions
        if (tx.get("merchant_key") or tx.get("merchant_name") or "").strip()
    }
    if not merchant_keys:
        return {}

    merchant_names = sorted({merchant for _, merchant in merchant_keys if merchant})
    placeholders = ",".join("?" for _ in merchant_names)
    rows = conn.execute(
        f"""
        SELECT profile_id,
               UPPER(TRIM(COALESCE(NULLIF(merchant_key, ''), merchant_name))) AS merchant_key,
               category,
               COUNT(*) AS tx_count,
               SUM(CASE WHEN categorization_source IN ('user', 'user-rule')
                         OR confidence = 'manual'
                        THEN 1 ELSE 0 END) AS trusted_count,
               SUM(CASE WHEN categorization_source = 'fallback'
                         OR confidence = 'fallback'
                        THEN 1 ELSE 0 END) AS fallback_count
        FROM transactions
        WHERE COALESCE(NULLIF(merchant_key, ''), merchant_name) != ''
          AND category IS NOT NULL
          AND category != ''
          AND category != 'Other'
          AND UPPER(TRIM(COALESCE(NULLIF(merchant_key, ''), merchant_name))) IN ({placeholders})
        GROUP BY profile_id, merchant_key, category
        """,
        merchant_names,
    ).fetchall()

    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (row[0] or "", row[1] or "")
        grouped.setdefault(key, []).append(
            {
                "category": row[2],
                "count": int(row[3] or 0),
                "trusted_count": int(row[4] or 0),
                "fallback_count": int(row[5] or 0),
            }
        )

    memory: dict[tuple[str, str], dict] = {}
    for key, options in grouped.items():
        total = sum(item["count"] for item in options)
        best = max(options, key=lambda item: (item["trusted_count"], item["count"]))
        if total <= 0:
            continue
        dominance = best["count"] / total

        if best["trusted_count"] > 0 and dominance >= 0.60:
            memory[key] = {"category": best["category"], "strength": "high"}
        elif best["fallback_count"] == 0 and best["count"] >= 2 and dominance >= 0.80:
            memory[key] = {"category": best["category"], "strength": "medium"}

    return memory


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def categorize_transactions(
    transactions: list[dict], batch_size: int | None = None
) -> list[dict]:
    """
    Two-phase categorization:
    1. Apply deterministic rules to all transactions
    2. Send to LLM:
       - rule-high: skip LLM entirely (certain categories)
       - rule-medium: send with suggestion for LLM to validate/override
       - no rule: send for full LLM categorization
    """
    inter_batch_delay_s = 1.0
    if batch_size is None:
        try:
            policy = llm_client.local_llm.get_categorization_policy()
            batch_size = policy.get("batch_size") or 25
            inter_batch_delay_s = max((policy.get("inter_batch_delay_ms") or 600) / 1000.0, 0.0)
        except Exception:
            batch_size = 25
            inter_batch_delay_s = 1.0

    # Phase 1: Sanitize (normalize signs, light cleanup)
    sanitized = sanitize_transactions(transactions)

    # Phase 1.5: Enrich via Trove (adds merchant_name, industry, etc.)
    sanitized = enrich_transactions(sanitized)

    # Phase 1.6: Apply DB-backed rules (user overrides first, then editable system defaults)
    db_rule_count = 0
    user_rule_count = 0
    try:
        from database import get_db
        import re as _re
        with get_db() as conn:
            db_rules = conn.execute(
                """SELECT pattern, match_type, category, source, profile_id
                   FROM category_rules
                   WHERE is_active = 1
                   ORDER BY CASE source WHEN 'user' THEN 0 ELSE 1 END,
                            CASE WHEN COALESCE(profile_id, '') = '' THEN 1 ELSE 0 END,
                            priority DESC,
                            id ASC"""
            ).fetchall()
            merchant_memory = _load_merchant_category_memory(conn, sanitized)
    except Exception:
        db_rules = []
        merchant_memory = {}

    for tx in sanitized:
        raw_desc = tx.get("description") or ""
        desc = raw_desc.upper()  # for 'exact' match_type
        # Canonical form for 'contains' matching. description_normalized is not
        # in the tx dict yet (it's computed and stored only at DB insert time),
        # so we derive it here on the fly using the same function.
        desc_normalized = _extract_merchant_pattern(raw_desc)
        merchant_patterns = {
            canonicalize_merchant_key(tx.get("merchant_key") or tx.get("merchant_name") or ""),
            _extract_merchant_pattern(tx.get("merchant_name") or ""),
        }
        merchant_patterns.discard("")
        matched = False
        tx_profile = tx.get("profile") or tx.get("profile_id") or ""
        for rule in db_rules:
            pattern, match_type, category, source, rule_profile_id = rule[0], rule[1], rule[2], rule[3], rule[4]
            if rule_profile_id and rule_profile_id != tx_profile:
                continue
            if match_type == "contains":
                if (
                    (desc_normalized and pattern == desc_normalized)
                    or pattern in merchant_patterns
                ):
                    matched = True
            elif match_type == "regex" and _re.search(pattern, raw_desc, _re.IGNORECASE):
                matched = True
            elif match_type == "exact" and pattern.upper() == desc:
                matched = True

            if matched:
                tx["category"] = category
                tx["confidence"] = "rule"
                tx["categorization_source"] = "user-rule" if source == "user" else "system-rule"
                db_rule_count += 1
                if source == "user":
                    user_rule_count += 1
                break

    logger.info("    DB rules matched:        %d", db_rule_count)

    # Phase 1b: Apply rule-based categorization (skip those already matched by user rules)
    high_confidence = []     # indices — skip LLM
    needs_llm = []           # list of {"tx": tx, "suggestion": str|None, "original_idx": int}
    rule_high_count = 0
    rule_medium_count = 0
    no_rule_count = 0
    memory_high_count = 0
    memory_medium_count = 0

    for i, tx in enumerate(sanitized):
        # Skip if an explicit DB rule already assigned the category
        if (tx.get("categorization_source") or "").endswith("-rule"):
            continue

        category, confidence = _rule_based_categorize(tx)
        merchant_key = (tx.get("profile", ""), canonicalize_merchant_key(tx.get("merchant_key") or tx.get("merchant_name") or ""))
        memory = merchant_memory.get(merchant_key)

        if confidence == "rule-high":
            # Certain — apply directly, no LLM needed
            tx["category"] = category
            tx["confidence"] = "rule"
            rule_high_count += 1

        elif confidence == "rule-medium":
            # Likely correct, but let LLM validate.
            tx["category"] = category
            tx["confidence"] = "rule"
            needs_llm.append({
                "tx": tx,
                "suggestion": category,
                "original_idx": i,
            })
            rule_medium_count += 1

        elif memory and memory["strength"] == "high":
            # Strong historical consensus for this merchant.
            tx["category"] = memory["category"]
            tx["confidence"] = "rule"
            tx["categorization_source"] = "merchant-memory"
            memory_high_count += 1

        elif memory:
            # Use merchant memory as a suggestion for the LLM to validate.
            needs_llm.append({
                "tx": tx,
                "suggestion": memory["category"],
                "original_idx": i,
            })
            memory_medium_count += 1

        else:
            # No rule — full LLM categorization
            needs_llm.append({
                "tx": tx,
                "suggestion": None,
                "original_idx": i,
            })
            no_rule_count += 1

    logger.info("    User rules (skip all):   %d", user_rule_count)
    logger.info("    Rule-high (skip LLM):    %d", rule_high_count)
    logger.info("    Rule-medium (validate):  %d", rule_medium_count)
    logger.info("    Merchant memory (skip):  %d", memory_high_count)
    logger.info("    Merchant memory (hint):  %d", memory_medium_count)
    logger.info("    No rule (full LLM):      %d", no_rule_count)
    logger.info("    Total for LLM:           %d", len(needs_llm))

    # Phase 2: LLM categorization + validation
    if needs_llm and ENABLE_LLM and llm_client.is_available():
        total_batches = -(-len(needs_llm) // batch_size)

        all_llm_results = []
        for batch_start in range(0, len(needs_llm), batch_size):
            batch = needs_llm[batch_start : batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            logger.info(
                "    LLM batch %d/%d (%d transactions)...",
                batch_num, total_batches, len(batch),
            )

            try:
                batch_cats = _categorize_batch_llm(batch, batch_start)
                all_llm_results.extend(batch_cats)
            except Exception as e:
                logger.warning("    LLM batch %d failed: %s", batch_num, e)
                # For failed batches: keep rule suggestion if available, else "Other"
                for j in range(len(batch)):
                    entry = batch[j]
                    fallback_cat = entry.get("suggestion") or "Other"
                    all_llm_results.append({
                        "index": batch_start + j,
                        "category": fallback_cat,
                        "confidence": "fallback",
                    })

            if batch_num < total_batches and inter_batch_delay_s > 0:
                time.sleep(inter_batch_delay_s)

        # Apply LLM results back
        for cat_result in all_llm_results:
            llm_idx = cat_result["index"]
            if llm_idx < len(needs_llm):
                original_idx = needs_llm[llm_idx]["original_idx"]
                suggestion = needs_llm[llm_idx].get("suggestion")
                llm_category = cat_result["category"]
                llm_confidence = cat_result.get("confidence", "medium")

                # Validate: if LLM returned a category not in our list, fall back
                active_categories = get_active_categories()
                if llm_category not in active_categories:
                    llm_category = suggestion or "Other"
                    llm_confidence = "fallback"

                sanitized[original_idx]["category"] = llm_category
                sanitized[original_idx]["confidence"] = llm_confidence

                # Track if LLM overrode the rule suggestion
                if suggestion and llm_category != suggestion:
                    sanitized[original_idx]["rule_override"] = {
                        "original": suggestion,
                        "llm_corrected_to": llm_category,
                    }

    elif needs_llm:
        # LLM disabled via toggle or no API key — fall back to rule suggestions + Teller hints
        if not ENABLE_LLM:
            logger.info(
                "    LLM categorization disabled (ENABLE_LLM_CATEGORIZATION=false) — "
                "using rule/Teller suggestions for %d transactions, 'Other' for unmatched",
                len(needs_llm),
            )
        else:
            logger.warning(
                "    LLM not available — using rule suggestions where available, 'Other' for rest"
            )
        for entry in needs_llm:
            idx = entry["original_idx"]
            suggestion = entry.get("suggestion")
            if suggestion:
                sanitized[idx]["category"] = suggestion
                sanitized[idx]["confidence"] = "rule-medium"
                sanitized[idx]["categorization_source"] = "rule-medium"
            else:
                sanitized[idx]["category"] = "Other"
                sanitized[idx]["confidence"] = "fallback"
                sanitized[idx]["categorization_source"] = "fallback"

    # Final validation: ensure every transaction has a category
    # Build source profile map for defensive backfill
    _source_profiles: dict[str, str] = {}
    for src_tx in transactions:
        src_id = src_tx.get("id") or src_tx.get("original_id")
        if src_id and "profile" in src_tx:
            _source_profiles[src_id] = src_tx["profile"]

    # Final validation: ensure every transaction has a category and profile
    for tx in sanitized:
        if "category" not in tx or not tx["category"]:
            tx["category"] = "Other"
            tx["confidence"] = "fallback"

        # Defensive: ensure profile tag survives through the pipeline
        if "profile" not in tx:
            orig_id = tx.get("original_id", "")
            if orig_id in _source_profiles:
                tx["profile"] = _source_profiles[orig_id]

        # Guard: credit card accounts never receive income — any positive inflow
        # categorized as Income is a CC bill payment, not real income.
        # Merchant refunds land in their original expense category (e.g. Shopping),
        # not Income, so they are unaffected by this override.
        acct_type = (tx.get("account_type") or "")
        if acct_type in ("credit_card", "credit") and tx.get("category") == "Income":
            tx["category"] = "Credit Card Payment"
            tx["confidence"] = "rule"
            tx["categorization_source"] = "rule-high"

    return sanitized
