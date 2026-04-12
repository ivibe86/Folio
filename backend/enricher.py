"""
enricher.py
Trove transaction enrichment.
Sits between sanitizer and categorizer in the pipeline.
Enriches merchant data (name, domain, industry) without exposing PII.
"""

import hashlib
import httpx
import os
import re
import time
import json
import threading
from collections import OrderedDict
from dotenv import load_dotenv
from log_config import get_logger
from privacy import mask_amount

load_dotenv()

logger = get_logger(__name__)

TROVE_API_KEY = os.getenv("TROVE_API_KEY")
TROVE_ENRICH_URL = "https://trove.headline.com/api/v1/transactions/enrich"
TROVE_BULK_URL = "https://trove.headline.com/api/v1/transactions/bulk"

# Feature toggle: set ENABLE_TROVE=false in .env to skip all Trove enrichment
ENABLE_TROVE = os.getenv("ENABLE_TROVE", "true").lower() in ("true", "1", "yes")

# Transactions with these categories (from rule-high) don't need enrichment
SKIP_ENRICHMENT_CATEGORIES = {
    "Savings Transfer",
    "Credit Card Payment",
    "Personal Transfer",
    "Income",
    "Fees & Charges",
}

# Rate limiting
SINGLE_REQUEST_DELAY = 0.3  # seconds between single API calls

# Cache configuration
TROVE_CACHE_MAX_SIZE = int(os.getenv("TROVE_CACHE_MAX_SIZE", "1000"))

# Strategy threshold: use bulk API only when deduplicated count exceeds this.
# Set to 0 via env var to force bulk for everything, or very high to force single.
BULK_THRESHOLD = int(os.getenv("TROVE_BULK_THRESHOLD", "0"))

BULK_BATCH_SIZE = int(os.getenv("TROVE_BULK_BATCH_SIZE", "100"))
# ══════════════════════════════════════════════════════════════════════════════
# PERSISTENT ENRICHMENT CACHE (DB-backed)
# ══════════════════════════════════════════════════════════════════════════════

def _get_db_conn():
    """Lazy import to avoid circular dependency at module load time."""
    from database import get_db
    return get_db


def _lookup_persistent_cache(pattern_key: str) -> dict | None:
    """
    Look up a normalized description key in the persistent enrichment_cache table.
    Returns an enrichment dict compatible with _apply_enrichment(), or None on miss.
    """
    try:
        get_db = _get_db_conn()
        with get_db() as conn:
            row = conn.execute(
                """SELECT merchant_name, merchant_domain, merchant_industry,
                          merchant_city, merchant_state, merchant_country, source
                   FROM enrichment_cache WHERE pattern_key = ?""",
                (pattern_key,),
            ).fetchone()

            if row is None:
                return None

            # Update hit tracking (fire-and-forget, don't block on failure)
            conn.execute(
                """UPDATE enrichment_cache
                   SET hit_count = hit_count + 1, last_seen = datetime('now')
                   WHERE pattern_key = ?""",
                (pattern_key,),
            )

            return {
                "name": row[0] or "",
                "domain": row[1] or "",
                "industry": row[2] or "",
                "hq_city": row[3] or "",
                "hq_state_code": row[4] or "",
                "hq_country_code": row[5] or "",
                "_cache_source": row[6] or "trove",
            }
    except Exception as e:
        logger.debug("Persistent cache lookup failed: %s", e)
        return None


def _persist_enrichment(pattern_key: str, enrichment: dict, source: str = "trove"):
    """
    Store a Trove enrichment result in the persistent cache.
    Uses INSERT OR REPLACE so Trove results upgrade seed entries
    (Trove data is richer — has domain, city, industry).
    """
    try:
        name = (enrichment.get("name") or "").strip()
        domain = (enrichment.get("domain") or "").strip()

        # Only persist if we got meaningful data back
        if not name and not domain:
            return

        get_db = _get_db_conn()
        with get_db() as conn:
            conn.execute(
                """INSERT INTO enrichment_cache
                   (pattern_key, merchant_name, merchant_domain, merchant_industry,
                    merchant_city, merchant_state, merchant_country, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(pattern_key) DO UPDATE SET
                       merchant_name = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_name = ''
                           THEN excluded.merchant_name ELSE enrichment_cache.merchant_name END,
                       merchant_domain = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_domain = ''
                           THEN excluded.merchant_domain ELSE enrichment_cache.merchant_domain END,
                       merchant_industry = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_industry = ''
                           THEN excluded.merchant_industry ELSE enrichment_cache.merchant_industry END,
                       merchant_city = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_city = ''
                           THEN excluded.merchant_city ELSE enrichment_cache.merchant_city END,
                       merchant_state = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_state = ''
                           THEN excluded.merchant_state ELSE enrichment_cache.merchant_state END,
                       merchant_country = CASE
                           WHEN excluded.source = 'trove' OR enrichment_cache.merchant_country = ''
                           THEN excluded.merchant_country ELSE enrichment_cache.merchant_country END,
                       source = CASE
                           WHEN excluded.source = 'trove' THEN 'trove'
                           ELSE enrichment_cache.source END,
                       hit_count = enrichment_cache.hit_count + 1,
                       last_seen = datetime('now')""",
                (
                    pattern_key,
                    name,
                    domain,
                    (enrichment.get("industry") or "").strip(),
                    (enrichment.get("hq_city") or enrichment.get("city") or "").strip(),
                    (enrichment.get("hq_state_code") or enrichment.get("state_code") or "").strip(),
                    (enrichment.get("hq_country_code") or enrichment.get("country_code") or "").strip(),
                    source,
                ),
            )
    except Exception as e:
        logger.debug("Persistent cache write failed: %s", e)

def _upsert_merchant_from_tx(tx: dict, enrichment: dict):
    """
    After enriching a transaction, upsert the merchants table.
    Requires profile_id on the transaction dict (set during categorize_transactions).
    If profile_id is not available, silently skips — enrichment still works,
    merchants table just won't be populated until sync writes it.
    """
    profile_id = tx.get("profile", "")
    if not profile_id:
        return

    merchant_key = _dedup_key(tx)
    if not merchant_key or len(merchant_key) < 3:
        return

    try:
        get_db = _get_db_conn()
        with get_db() as conn:
            from database import upsert_merchant_from_enrichment
            upsert_merchant_from_enrichment(
                conn=conn,
                merchant_key=merchant_key,
                enrichment=enrichment,
                profile_id=profile_id,
                source="trove",
            )
    except Exception as e:
        logger.debug("Merchant upsert from enrichment failed: %s", e)        
# ══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT CACHE
# ══════════════════════════════════════════════════════════════════════════════

class _EnrichmentCache:
    """
    Thread-safe LRU cache for Trove enrichment results.
    Keyed on description_upper only — amount is no longer sent to Trove
    (privacy: we send amount=0), and the same merchant description yields
    identical enrichment regardless of amount or date.
    """

    def __init__(self, max_size: int = 1000):
        self._max_size = max_size
        self._cache: OrderedDict[str, dict] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, description: str) -> dict | None:
        """Return cached enrichment result, or None on miss."""
        key = description.upper().strip()
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            return None

    def put(self, description: str, enrichment: dict):
        """Store an enrichment result in the cache."""
        key = description.upper().strip()
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._cache[key] = enrichment
            else:
                self._cache[key] = enrichment
                if len(self._cache) > self._max_size:
                    self._cache.popitem(last=False)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._cache)


# Module-level cache instance
_enrichment_cache = _EnrichmentCache(max_size=TROVE_CACHE_MAX_SIZE)


def _scrub_for_trove(description: str) -> str:
    """
    Minimal sanitization for Trove — only remove actual PII.
    Trove WANTS the raw messy merchant string with store numbers,
    asterisks, location codes, etc. That's how it matches.
    """
    # Remove INDN fields (individual names in ACH transactions)
    desc = re.sub(r"INDN:\S+(?:\s*,\s*\S+)?", "", description)
    # Remove CO ID fields
    desc = re.sub(r"CO\s*ID:\S+", "", desc)
    # Remove email addresses
    desc = re.sub(r"\S+@\S+\.\S+", "", desc)
    # Clean up extra whitespace
    desc = re.sub(r"\s+", " ", desc).strip()
    return desc


def _get_anonymous_user_id() -> str:
    """
    Generate a stable, non-PII user identifier for Trove.
    Trove requires user_id but explicitly says not to send real PII.
    """
    seed = os.getenv("TROVE_USER_SEED", "Folio-default-seed")
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def _should_enrich(tx: dict) -> bool:
    """
    Determine if a transaction should be sent to Trove.
    Skip internal transfers, payments, income, fees, ACH deposits —
    these have no meaningful merchant for Trove to identify.
    """
    if tx.get("enriched"):
        return False

    cat = tx.get("category", "")
    if cat in SKIP_ENRICHMENT_CATEGORIES:
        return False

    desc = tx.get("description", "").strip()
    if not desc or len(desc) < 3:
        return False

    # Skip transaction types that aren't merchant purchases
    tx_type = tx.get("type", "")
    if tx_type in ("transfer", "payment", "fee", "adjustment", "interest",
                    "deposit", "ach"):
        return False

    # Skip descriptions that are clearly not merchants
    desc_lower = desc.lower()
    skip_patterns = [
        "payroll", "direct dep", "des:payroll",
        "tax refund", "tax rfd", "casttaxrfd",
        "mobile deposit", "atm deposit",
    ]
    if any(p in desc_lower for p in skip_patterns):
        return False

    return True


def _apply_enrichment(tx: dict, enrichment: dict) -> dict:
    """
    Apply Trove enrichment data to a transaction.
    Trove returns two tiers of matches:
      - Full match: name, domain, industry, location all populated
      - Partial match: only domain (and sometimes categories) populated
    Both are useful — domain gives us merchant identity even without full details.
    """
    domain = (enrichment.get("domain") or "").strip()
    name = (enrichment.get("name") or "").strip()
    industry = (enrichment.get("industry") or "").strip()
    categories = enrichment.get("categories") or []

    # Consider it a match if we got at least a domain
    if domain:
        tx["merchant_domain"] = domain
        tx["merchant_name"] = name if name else _domain_to_name(domain)
        tx["merchant_industry"] = industry
        tx["merchant_categories"] = categories
        tx["merchant_city"] = (
            enrichment.get("hq_city") or enrichment.get("city") or ""
        )
        tx["merchant_state"] = (
            enrichment.get("hq_state_code") or enrichment.get("state_code") or ""
        )
        tx["merchant_country"] = (
            enrichment.get("hq_country_code") or enrichment.get("country_code") or ""
        )
        tx["enriched"] = True
        tx["enrichment_tier"] = "full" if name else "partial"
    else:
        tx["enriched"] = False

    return tx


def _domain_to_name(domain: str) -> str:
    """
    Derive a readable merchant name from a domain when Trove
    only returns a partial match.
    """
    name = domain.split(".")[0] if domain else ""
    for prefix in ("www", "shop", "store", "pay", "my"):
        if name.lower().startswith(prefix) and len(name) > len(prefix):
            name = name[len(prefix):]
    return name.capitalize() if name else ""


def _build_trove_payload(tx: dict, anonymous_id: str) -> dict | None:
    """
    Build a single Trove request payload from a transaction.
    Uses raw_description (pre-sanitization) for best Trove matching.
    Amount is sent as a fixed dummy value (1.00) to satisfy Trove's non-zero
    requirement without exposing exact spending to third parties —
    Trove's matching is description-based and does not require real amounts.
    Returns None if the transaction doesn't have valid data for Trove.
    """
    # Prefer raw description — Trove wants the original messy string
    description = tx.get("raw_description") or tx.get("description", "")
    description = _scrub_for_trove(description)
    date = tx.get("date", "")

    if not description or len(description.strip()) < 2:
        logger.debug(
            "Skipping Trove payload: description too short (%r) for tx %s",
            description, tx.get("original_id", tx.get("id", "?")),
        )
        return None

    if not date or len(date) < 10:
        logger.debug(
            "Skipping Trove payload: invalid date (%r) for tx %s",
            date, tx.get("original_id", tx.get("id", "?")),
        )
        return None
    date = date[:10]
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        logger.debug(
            "Skipping Trove payload: date format mismatch (%r) for tx %s",
            date, tx.get("original_id", tx.get("id", "?")),
        )
        return None

    # Validate that the original transaction has a non-zero amount
    # (zero-amount transactions are not real purchases worth enriching)
    try:
        original_amount = round(abs(float(tx.get("amount", 0))), 2)
    except (ValueError, TypeError):
        logger.debug(
            "Skipping Trove payload: invalid amount for tx %s",
            tx.get("original_id", tx.get("id", "?")),
        )
        return None

    if original_amount == 0:
        logger.debug(
            "Skipping Trove payload: zero amount for tx %s",
            tx.get("original_id", tx.get("id", "?")),
        )
        return None

    return {
        "description": description,
        "amount": 1.00,
        "date": date,
        "user_id": anonymous_id,
    }


# ══════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION — send unique descriptions to Trove, fan out results
# ══════════════════════════════════════════════════════════════════════════════

def _dedup_key(tx: dict) -> str:
    """
    Build a normalized key for deduplication.
    Uses raw_description (what Trove actually sees) with long numeric
    sequences masked so that e.g. "CHECK #001234" and "CHECK #005678"
    collapse to the same key — Trove returns the same merchant for both.
    """
    desc = (tx.get("raw_description") or tx.get("description") or "").strip()
    normalized = re.sub(r"\d{6,}", "XXXXX", desc)
    return normalized.upper()


def _deduplicate_for_trove(
    transactions: list[dict], indices: list[int]
) -> tuple[list[int], dict[str, list[int]]]:
    """
    Given a list of transactions and enrichable indices, pick one
    representative index per unique merchant description and build a
    mapping so results can be fanned out to all duplicates.

    Returns:
        representative_indices: list of indices to actually send to Trove
        fanout_map: {dedup_key: [all indices sharing that key]}
    """
    groups: dict[str, list[int]] = {}
    for idx in indices:
        key = _dedup_key(transactions[idx])
        groups.setdefault(key, []).append(idx)

    representative_indices = []
    for key, idx_list in groups.items():
        representative_indices.append(idx_list[0])

    return representative_indices, groups


def _fanout_enrichment(
    transactions: list[dict],
    fanout_map: dict[str, list[int]],
    enriched_index: int,
) -> int:
    """
    After enriching a representative transaction, copy its enrichment
    data to all other transactions that share the same dedup key.

    Returns:
        Number of sibling transactions that received enrichment via fanout.
    """
    source_tx = transactions[enriched_index]
    key = _dedup_key(source_tx)
    siblings = fanout_map.get(key, [])
    fanout_count = 0

    if not source_tx.get("enriched"):
        for idx in siblings:
            if idx != enriched_index:
                transactions[idx]["enriched"] = False
        return 0

    enrichment_fields = [
        "merchant_domain", "merchant_name", "merchant_industry",
        "merchant_categories", "merchant_city", "merchant_state",
        "merchant_country", "enriched", "enrichment_tier",
    ]

    for idx in siblings:
        if idx == enriched_index:
            continue
        for field in enrichment_fields:
            if field in source_tx:
                transactions[idx][field] = source_tx[field]
        fanout_count += 1

    return fanout_count


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def enrich_single(tx: dict) -> dict:
    """Enrich a single transaction via Trove's single-enrich endpoint."""
    if not ENABLE_TROVE:
        return tx
    if not TROVE_API_KEY:
        return tx
    if not _should_enrich(tx):
        return tx

    # Check cache first
    desc = tx.get("raw_description") or tx.get("description", "")
    cached = _enrichment_cache.get(desc)
    if cached is not None:
        return _apply_enrichment(tx, cached)

    anonymous_id = _get_anonymous_user_id()

    try:
        payload = _build_trove_payload(tx, anonymous_id)
        if payload is None:
            tx["enriched"] = False
            return tx

        resp = httpx.post(
            TROVE_ENRICH_URL,
            headers={
                "X-API-KEY": TROVE_API_KEY,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=10.0,
        )

        if resp.status_code == 200:
            data = resp.json()
            _enrichment_cache.put(desc, data)
            tx = _apply_enrichment(tx, data)
        elif resp.status_code == 429:
            logger.warning("Trove rate limit hit on single enrich")
            tx["enriched"] = False
        else:
            tx["enriched"] = False

    except Exception as e:
        logger.error("Trove single enrichment failed: %s", e)
        tx["enriched"] = False

    return tx


def enrich_transactions(transactions: list[dict]) -> list[dict]:
    """
    Enrich a list of transactions using the best available strategy:
    1. Filter to enrichable transactions
    2. Resolve cache hits
    3. Deduplicate remaining by merchant description
    4. Send deduplicated set to Trove:
       - If deduplicated count <= BULK_THRESHOLD: single-enrich (higher match rate)
       - If deduplicated count > BULK_THRESHOLD: bulk API (lower latency for huge sets)
    5. Fan out enrichment results to all duplicate transactions
    6. Log accurate enrichment summary

    This is the main entry point called by categorizer.py.
    """
    if not ENABLE_TROVE:
        logger.info("    Trove enrichment disabled (ENABLE_TROVE=false) — skipping")
        return transactions

    if not TROVE_API_KEY:
        logger.warning("No TROVE_API_KEY set — skipping enrichment")
        return transactions

    # ── Step 1: Identify enrichable transactions ──
    enrichable_indices = []
    for i, tx in enumerate(transactions):
        if _should_enrich(tx):
            enrichable_indices.append(i)

    if not enrichable_indices:
        logger.info("    No transactions need enrichment")
        return transactions

    total_enrichable = len(enrichable_indices)
    logger.info(
        "    Enriching %d of %d transactions via Trove...",
        total_enrichable, len(transactions),
    )

    # ── Step 2: Resolve cache hits ──
    cache_hit_count = 0
    cache_hit_enriched = 0
    remaining_indices = []
    for idx in enrichable_indices:
        tx = transactions[idx]
        desc = tx.get("raw_description") or tx.get("description", "")
        cached = _enrichment_cache.get(desc)
        if cached is not None:
            transactions[idx] = _apply_enrichment(tx, cached)
            cache_hit_count += 1
            if transactions[idx].get("enriched"):
                cache_hit_enriched += 1
        else:
            remaining_indices.append(idx)

    if cache_hit_count > 0:
        logger.info(
            "    Cache hits: %d transactions resolved from cache (%d enriched)",
            cache_hit_count, cache_hit_enriched,
        )

    if not remaining_indices:
        _log_enrichment_summary(
            total_enrichable=total_enrichable,
            from_cache=cache_hit_enriched,
            from_api=0,
            from_fanout=0,
            from_persistent_cache=0,
        )
        return transactions

    # ── Step 2b: Check persistent DB cache (enrichment_cache table) ──
    persistent_hit_count = 0
    persistent_hit_enriched = 0
    still_remaining_indices = []

    for idx in remaining_indices:
        tx = transactions[idx]
        desc = tx.get("raw_description") or tx.get("description", "")
        pattern_key = _dedup_key(tx)

        cached = _lookup_persistent_cache(pattern_key)
        if cached is not None:
            # Also populate the in-memory cache so duplicates resolve at Step 2
            _enrichment_cache.put(desc, cached)
            transactions[idx] = _apply_enrichment(tx, cached)
            persistent_hit_count += 1
            if transactions[idx].get("enriched"):
                persistent_hit_enriched += 1
                # Track that this came from persistent cache
                transactions[idx]["enrichment_tier"] = (
                    transactions[idx].get("enrichment_tier", "full") + "+db_cache"
                )
        else:
            still_remaining_indices.append(idx)

    if persistent_hit_count > 0:
        logger.info(
            "    Persistent cache hits: %d transactions resolved from DB (%d enriched)",
            persistent_hit_count, persistent_hit_enriched,
        )

    remaining_indices = still_remaining_indices

    if not remaining_indices:
        _log_enrichment_summary(
            total_enrichable=total_enrichable,
            from_cache=cache_hit_enriched,
            from_api=0,
            from_fanout=0,
            from_persistent_cache=persistent_hit_enriched,
        )
        return transactions

    # ── Step 3: Deduplicate by merchant description ──
    representative_indices, fanout_map = _deduplicate_for_trove(
        transactions, remaining_indices,
    )

    duplicate_count = len(remaining_indices) - len(representative_indices)
    if duplicate_count > 0:
        logger.info(
            "    Deduplicated: %d unique descriptions from %d transactions (%d duplicates will be fanned out)",
            len(representative_indices), len(remaining_indices), duplicate_count,
        )

    # ── Step 4: Choose strategy based on deduplicated volume ──
    if len(representative_indices) <= BULK_THRESHOLD:
        est_seconds = len(representative_indices) * SINGLE_REQUEST_DELAY
        logger.info(
            "    Using single-enrich for %d unique descriptions (~%.0fs estimated)",
            len(representative_indices), est_seconds,
        )
        api_enriched, fanout_enriched = _enrich_via_single(
            transactions, representative_indices, fanout_map,
        )
    else:
        logger.info(
            "    Using bulk API for %d unique descriptions (above threshold of %d)",
            len(representative_indices), BULK_THRESHOLD,
        )
        api_enriched, fanout_enriched = _enrich_via_bulk(
            transactions, representative_indices, fanout_map,
        )

    # ── Step 5: Log accurate summary ──
    _log_enrichment_summary(
        total_enrichable=total_enrichable,
        from_cache=cache_hit_enriched,
        from_api=api_enriched,
        from_fanout=fanout_enriched,
        from_persistent_cache=persistent_hit_enriched,
    )

    return transactions


def _log_enrichment_summary(
    total_enrichable: int,
    from_cache: int,
    from_api: int,
    from_fanout: int,
    from_persistent_cache: int = 0,
):
    """Log a clear breakdown of how transactions were enriched."""
    total_enriched = from_cache + from_api + from_fanout + from_persistent_cache
    not_enriched = total_enrichable - total_enriched

    parts = []
    if from_cache > 0:
        parts.append(f"{from_cache} from memory cache")
    if from_persistent_cache > 0:
        parts.append(f"{from_persistent_cache} from DB cache")
    if from_api > 0:
        parts.append(f"{from_api} from Trove API")
    if from_fanout > 0:
        parts.append(f"{from_fanout} via dedup fanout")

    breakdown = ", ".join(parts) if parts else "none"

    if total_enrichable > 0:
        pct = round(total_enriched / total_enrichable * 100, 1)
    else:
        pct = 0.0

    logger.info(
        "    Enrichment complete: %d/%d transactions enriched (%.1f%%) — %s",
        total_enriched, total_enrichable, pct, breakdown,
    )
    if not_enriched > 0:
        logger.info(
            "    Not enriched: %d transactions (no Trove match)",
            not_enriched,
        )


def _enrich_via_single(
    transactions: list[dict],
    indices: list[int],
    fanout_map: dict[str, list[int]],
) -> tuple[int, int]:
    """
    Enrich transactions one-by-one via single-enrich endpoint, then fan out.

    Returns:
        (api_enriched_count, fanout_enriched_count)
    """
    anonymous_id = _get_anonymous_user_id()
    api_enriched = 0
    fanout_enriched = 0
    skipped_count = 0
    total_valid = 0

    for i, idx in enumerate(indices):
        tx = transactions[idx]
        payload = _build_trove_payload(tx, anonymous_id)

        if payload is None:
            skipped_count += 1
            tx["enriched"] = False
            _fanout_enrichment(transactions, fanout_map, idx)
            continue

        total_valid += 1

        try:
            resp = httpx.post(
                TROVE_ENRICH_URL,
                headers={
                    "X-API-KEY": TROVE_API_KEY,
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=10.0,
            )

            if resp.status_code == 200:
                data = resp.json()
                desc = tx.get("raw_description") or tx.get("description", "")
                _enrichment_cache.put(desc, data)

                # Persist to DB cache for future runs
                _persist_enrichment(_dedup_key(tx), data, source="trove")

                # Upsert merchants table (Enhancement 5)
                _upsert_merchant_from_tx(tx, data)

                transactions[idx] = _apply_enrichment(tx, data)
                if transactions[idx].get("enriched"):
                    api_enriched += 1
            elif resp.status_code == 429:
                logger.warning(
                    "Trove rate limit at %d/%d — pausing 30s then retrying",
                    i + 1, len(indices),
                )
                tx["enriched"] = False
                time.sleep(30)
                # Retry this one after the pause
                try:
                    retry_resp = httpx.post(
                        TROVE_ENRICH_URL,
                        headers={
                            "X-API-KEY": TROVE_API_KEY,
                            "Content-Type": "application/json",
                        },
                        json=payload,
                        timeout=10.0,
                    )
                    if retry_resp.status_code == 200:
                        data = retry_resp.json()
                        desc = tx.get("raw_description") or tx.get("description", "")
                        _enrichment_cache.put(desc, data)
                        _persist_enrichment(_dedup_key(tx), data, source="trove")
                        transactions[idx] = _apply_enrichment(tx, data)
                        if transactions[idx].get("enriched"):
                            api_enriched += 1
                    elif retry_resp.status_code == 429:
                        logger.warning("Trove rate limit persists after retry — stopping enrichment")
                        fanout_enriched += _fanout_enrichment(transactions, fanout_map, idx)
                        break
                    else:
                        tx["enriched"] = False
                except Exception as e:
                    logger.error("Trove retry failed: %s", e)
                    tx["enriched"] = False

                fanout_enriched += _fanout_enrichment(transactions, fanout_map, idx)
                time.sleep(SINGLE_REQUEST_DELAY)
                continue
            else:
                tx["enriched"] = False

        except Exception as e:
            logger.error("Trove enrich failed for index %d: %s", idx, e)
            tx["enriched"] = False

        fanout_enriched += _fanout_enrichment(transactions, fanout_map, idx)

        time.sleep(SINGLE_REQUEST_DELAY)

    if skipped_count > 0:
        logger.info("    Skipped %d transactions with invalid data", skipped_count)
    logger.info(
        "    Trove single-enrich: %d/%d matched, %d fanned out to duplicates",
        api_enriched, total_valid, fanout_enriched,
    )
    return api_enriched, fanout_enriched


def _enrich_via_bulk(
    transactions: list[dict],
    indices: list[int],
    fanout_map: dict[str, list[int]],
) -> tuple[int, int]:
    """
    Enrich many transactions via Trove's bulk API in batches, then fan out.
    Splits into chunks of BULK_BATCH_SIZE to avoid Trove polling timeouts
    on large monolithic requests. Failed batches fall back to single-enrich
    for only the affected chunk.

    Returns:
        (api_enriched_count, fanout_enriched_count)
    """
    anonymous_id = _get_anonymous_user_id()

    # ── Build payloads and filter invalid ──
    bulk_payload = []
    valid_indices = []

    for idx in indices:
        payload = _build_trove_payload(transactions[idx], anonymous_id)
        if payload is not None:
            bulk_payload.append(payload)
            valid_indices.append(idx)

    if not bulk_payload:
        logger.warning("No valid transactions for Trove after validation")
        return 0, 0

    skipped = len(indices) - len(valid_indices)
    if skipped > 0:
        logger.info("    Skipped %d transactions with invalid data for Trove", skipped)

    # ── Split into batches ──
    total_batches = -(-len(bulk_payload) // BULK_BATCH_SIZE)
    logger.info(
        "    Sending %d transactions to Trove bulk API in %d batches of ≤%d...",
        len(bulk_payload), total_batches, BULK_BATCH_SIZE,
    )

    total_api_enriched = 0
    total_fanout_enriched = 0

    for batch_num in range(total_batches):
        batch_start = batch_num * BULK_BATCH_SIZE
        batch_end = min(batch_start + BULK_BATCH_SIZE, len(bulk_payload))

        batch_payloads = bulk_payload[batch_start:batch_end]
        batch_indices = valid_indices[batch_start:batch_end]

        logger.info(
            "    Bulk batch %d/%d (%d transactions)...",
            batch_num + 1, total_batches, len(batch_payloads),
        )

        batch_api, batch_fanout = _submit_and_poll_bulk_batch(
            transactions, batch_payloads, batch_indices, fanout_map, anonymous_id,
        )
        total_api_enriched += batch_api
        total_fanout_enriched += batch_fanout

    return total_api_enriched, total_fanout_enriched


def _submit_and_poll_bulk_batch(
    transactions: list[dict],
    batch_payloads: list[dict],
    batch_indices: list[int],
    fanout_map: dict[str, list[int]],
    anonymous_id: str,
) -> tuple[int, int]:
    """
    Submit and poll a single bulk batch. Falls back to single-enrich
    for only this batch on failure.

    Returns:
        (api_enriched_count, fanout_enriched_count)
    """
    try:
        submit_resp = httpx.post(
            TROVE_BULK_URL,
            headers={
                "X-API-KEY": TROVE_API_KEY,
                "Content-Type": "application/json",
            },
            json={"transactions": batch_payloads},
            timeout=30.0,
        )

        if submit_resp.status_code == 400:
            logger.info("    Retrying Trove bulk batch with raw array format...")
            submit_resp = httpx.post(
                TROVE_BULK_URL,
                headers={
                    "X-API-KEY": TROVE_API_KEY,
                    "Content-Type": "application/json",
                },
                json=batch_payloads,
                timeout=30.0,
            )

        if submit_resp.status_code == 429:
            logger.warning(
                "Trove bulk: rate limited, waiting 30s then falling back to single for this batch (%d txns)",
                len(batch_indices),
            )
            time.sleep(30)
            return _enrich_via_single(transactions, batch_indices, fanout_map)

        if submit_resp.status_code not in (200, 201):
            logger.warning(
                "Trove bulk batch submit failed: %d — %s, falling back to single",
                submit_resp.status_code, submit_resp.text[:200],
            )
            return _enrich_via_single(transactions, batch_indices, fanout_map)

        request_id = submit_resp.json().get("requestId")
        if not request_id:
            logger.warning(
                "Trove bulk batch: no requestId in response: %s, falling back to single",
                submit_resp.text[:200],
            )
            return _enrich_via_single(transactions, batch_indices, fanout_map)

        logger.info(
            "    Trove bulk batch submitted: %s, polling for results...", request_id
        )

        # Scale first poll to batch size (~0.5s per tx, minimum 10s)
        # Avoids wasted early polls — 100 items takes Trove ~50-70s
        first_wait = max(10, len(batch_payloads) // 2)
        poll_intervals = [first_wait, 20, 20, 30, 30, 60, 120]
        for attempt, wait in enumerate(poll_intervals):
            time.sleep(wait)

            poll_resp = httpx.get(
                f"{TROVE_BULK_URL}/{request_id}",
                headers={"X-API-KEY": TROVE_API_KEY},
                timeout=30.0,
            )

            if poll_resp.status_code == 200:
                raw_text = poll_resp.text.strip()

                results = None
                try:
                    parsed = poll_resp.json()
                    if isinstance(parsed, list):
                        results = parsed
                    elif isinstance(parsed, dict):
                        if "status" in parsed and parsed["status"] == "pending":
                            logger.info(
                                "    Trove bulk batch still processing (poll %d/%d)...",
                                attempt + 1, len(poll_intervals),
                            )
                            continue
                        results = [parsed]
                except Exception:
                    pass

                if results is None:
                    try:
                        results = []
                        for line in raw_text.splitlines():
                            line = line.strip()
                            if line:
                                results.append(json.loads(line))
                    except Exception as e:
                        logger.error(
                            "Trove bulk batch: could not parse response: %s", e
                        )
                        logger.debug(
                            "    First 500 chars: %s", raw_text[:500]
                        )
                        return _enrich_via_single(transactions, batch_indices, fanout_map)

                if results:
                    return _apply_bulk_batch_results(
                        transactions, results, batch_indices, fanout_map, anonymous_id,
                    )
                else:
                    logger.warning("Trove bulk batch returned empty results, falling back to single")
                    return _enrich_via_single(transactions, batch_indices, fanout_map)

            elif poll_resp.status_code == 202:
                logger.info(
                    "    Trove bulk batch still processing (poll %d/%d)...",
                    attempt + 1, len(poll_intervals),
                )
                continue

            elif poll_resp.status_code == 500:
                logger.error(
                    "Trove bulk batch processing error (500) — falling back to single"
                )
                return _enrich_via_single(transactions, batch_indices, fanout_map)

            else:
                logger.warning(
                    "Trove bulk batch poll unexpected status: %d, falling back to single",
                    poll_resp.status_code,
                )
                return _enrich_via_single(transactions, batch_indices, fanout_map)

        # Polling timed out for this batch only
        logger.warning(
            "Trove bulk batch: polling timed out (%d txns), falling back to single-enrich",
            len(batch_indices),
        )
        return _enrich_via_single(transactions, batch_indices, fanout_map)

    except httpx.TimeoutException:
        logger.error("Trove bulk batch: request timed out, falling back to single")
        return _enrich_via_single(transactions, batch_indices, fanout_map)
    except Exception as e:
        logger.exception("Trove bulk batch failed: %s", e)
        return _enrich_via_single(transactions, batch_indices, fanout_map)


def _apply_bulk_batch_results(
    transactions: list[dict],
    results: list[dict],
    batch_indices: list[int],
    fanout_map: dict[str, list[int]],
    anonymous_id: str,
) -> tuple[int, int]:
    """
    Match Trove bulk results back to transactions, persist, cache, and fan out.

    Returns:
        (api_enriched_count, fanout_enriched_count)
    """
    enrichment_lookup: dict[tuple, list[dict]] = {}
    for enrichment in results:
        query = enrichment.get("query", {})
        key = (
            query.get("description", ""),
            query.get("date", ""),
        )
        enrichment_lookup.setdefault(key, []).append(enrichment)

    api_enriched = 0
    fanout_enriched = 0
    matched_count = 0

    for idx in batch_indices:
        tx = transactions[idx]
        payload = _build_trove_payload(tx, anonymous_id)
        if payload is None:
            continue

        date_with_tz = payload["date"] + "T00:00:00.000Z"
        key_exact = (payload["description"], date_with_tz)
        key_plain = (payload["description"], payload["date"])

        enrichment = None
        for try_key in (key_exact, key_plain):
            candidates = enrichment_lookup.get(try_key)
            if candidates:
                enrichment = candidates.pop(0)
                if not candidates:
                    del enrichment_lookup[try_key]
                break

        if enrichment:
            matched_count += 1
            desc = tx.get("raw_description") or tx.get("description", "")
            _enrichment_cache.put(desc, enrichment)
            _persist_enrichment(_dedup_key(tx), enrichment, source="trove")

            # Upsert merchants table (Enhancement 5)
            _upsert_merchant_from_tx(tx, enrichment)

            transactions[idx] = _apply_enrichment(tx, enrichment)
            if transactions[idx].get("enriched"):
                api_enriched += 1

        fanout_enriched += _fanout_enrichment(transactions, fanout_map, idx)

    logger.info(
        "    Trove bulk batch: %d/%d results matched, %d enriched, %d fanned out",
        matched_count, len(results), api_enriched, fanout_enriched,
    )

    return api_enriched, fanout_enriched
