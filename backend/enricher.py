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
import llm_client
from merchant_identity import (
    MERCHANT_PURCHASE,
    build_merchant_identity,
    canonicalize_merchant_key,
    infer_non_merchant_kind,
    merchant_name_supported,
    normalize_merchant_kind,
)

load_dotenv()

logger = get_logger(__name__)

TROVE_API_KEY = os.getenv("TROVE_API_KEY")
TROVE_ENRICH_URL = "https://trove.headline.com/api/v1/transactions/enrich"
TROVE_BULK_URL = "https://trove.headline.com/api/v1/transactions/bulk"

# Feature toggle: set ENABLE_TROVE=false in .env to skip all Trove enrichment
ENABLE_TROVE = os.getenv("ENABLE_TROVE", "true").lower() in ("true", "1", "yes")
ENABLE_LOCAL_ENRICHMENT = os.getenv("ENABLE_LOCAL_ENRICHMENT", "true").lower() in ("true", "1", "yes")

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
LOCAL_ENRICHMENT_BATCH_SIZE = int(os.getenv("LOCAL_ENRICHMENT_BATCH_SIZE", "20"))
LOCAL_ENRICHMENT_MIN_CONFIDENCE = os.getenv("LOCAL_ENRICHMENT_MIN_CONFIDENCE", "medium").strip().lower()

MERCHANT_INDUSTRIES = [
    "Grocery",
    "Restaurant",
    "Coffee Shop",
    "Fast Food",
    "Bar / Nightlife",
    "Gas Station",
    "Pharmacy",
    "Healthcare Provider",
    "Insurance",
    "Utilities",
    "Internet / Telecom",
    "Streaming / Media",
    "Software / SaaS",
    "E-commerce Marketplace",
    "Electronics Retail",
    "General Retail",
    "Home Improvement",
    "Transportation / Rideshare",
    "Travel / Airline",
    "Travel / Hotel",
    "Subscription Service",
    "Bank / Financial Service",
    "Government / Tax",
    "Education",
    "Fitness",
    "Personal Care",
    "Unknown",
]

_CONFIDENCE_RANK = {
    "low": 0,
    "medium": 1,
    "high": 2,
}

PLATFORM_NORMALIZE_MERCHANTS = {
    "doordash",
    "uber eats",
    "instacart",
    "grubhub",
}
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

def _upsert_merchant_from_tx(tx: dict, enrichment: dict, source: str = "trove"):
    """
    After enriching a transaction, upsert the merchants table.
    Requires profile_id on the transaction dict (set during categorize_transactions).
    If profile_id is not available, silently skips — enrichment still works,
    merchants table just won't be populated until sync writes it.
    """
    profile_id = tx.get("profile", "")
    if not profile_id:
        return

    merchant_name = (enrichment.get("name") or enrichment.get("merchant_name") or "").strip()
    merchant_key = canonicalize_merchant_key(merchant_name)
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
                source=source,
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


def _resolve_enrichment_mode() -> str:
    """
    Pick the active enrichment backend.

    Preference order:
    1. Local LLM enrichment, if enabled and an LLM is available
    2. Trove fallback, if enabled and configured
    3. None
    """
    if ENABLE_LOCAL_ENRICHMENT and llm_client.get_provider() == "ollama" and llm_client.is_available():
        return "local_llm"
    if ENABLE_TROVE and TROVE_API_KEY:
        return "trove"
    return "none"


def _normalize_industry(raw_industry: str) -> str:
    """Map model output to the closest allowed merchant industry label."""
    if not raw_industry:
        return "Unknown"

    raw = raw_industry.strip().lower()
    allowed = {item.lower(): item for item in MERCHANT_INDUSTRIES}
    if raw in allowed:
        return allowed[raw]

    synonyms = {
        "groceries": "Grocery",
        "supermarket": "Grocery",
        "cafe": "Coffee Shop",
        "cafes": "Coffee Shop",
        "coffee": "Coffee Shop",
        "dining": "Restaurant",
        "food": "Restaurant",
        "rideshare": "Transportation / Rideshare",
        "ride share": "Transportation / Rideshare",
        "airline": "Travel / Airline",
        "hotel": "Travel / Hotel",
        "telecom": "Internet / Telecom",
        "internet": "Internet / Telecom",
        "streaming": "Streaming / Media",
        "media": "Streaming / Media",
        "software": "Software / SaaS",
        "saas": "Software / SaaS",
        "electronics": "Electronics Retail",
        "retail": "General Retail",
        "shopping": "General Retail",
        "subscription": "Subscription Service",
        "subscriptions": "Subscription Service",
        "bank": "Bank / Financial Service",
        "financial": "Bank / Financial Service",
        "government": "Government / Tax",
        "tax": "Government / Tax",
        "medical": "Healthcare Provider",
        "healthcare": "Healthcare Provider",
        "fitness center": "Fitness",
    }
    if raw in synonyms:
        return synonyms[raw]

    for token, canonical in synonyms.items():
        if token in raw:
            return canonical

    return "Unknown"


def _confidence_meets_threshold(confidence: str) -> bool:
    minimum = LOCAL_ENRICHMENT_MIN_CONFIDENCE
    if minimum not in _CONFIDENCE_RANK:
        minimum = "medium"
    return _CONFIDENCE_RANK.get(confidence, 0) >= _CONFIDENCE_RANK[minimum]


def _match_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", (text or "").lower())
        if len(token) >= 4
    }


def _compact_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _merchant_alias_tokens(merchant_name: str) -> set[str]:
    """
    Generic aliases for common abbreviations and descriptor variants that
    often appear in bank strings. Keep this intentionally small and universal.
    """
    normalized = merchant_name.strip().lower()
    aliases: set[str] = set()

    alias_map = {
        "amazon": {"amzn"},
        "amazon marketplace": {"amzn", "mktpl", "mktplace"},
        "amazon grocery": {"amzn", "groce"},
        "southwest": {"southwes"},
        "lufthansa": {"lufthan"},
        "air new zealand": {"air", "nz"},
        "mcdonald's": {"mcdonalds"},
        "total wine": {"totalwine"},
        "openai": {"chatgpt"},
        "costco": {"whse"},
    }

    for canonical, extras in alias_map.items():
        if normalized == canonical:
            aliases.update(extras)

    return aliases


def _merchant_name_supported(description: str, merchant_name: str) -> bool:
    """
    Generic guardrail against cross-row hallucinations: the chosen merchant
    should have some lexical support in the transaction text.
    """
    return merchant_name_supported(description, merchant_name)


def _validate_local_enrichment_with_reason(tx: dict, enrichment: dict | None) -> tuple[dict | None, str]:
    """Reject weak local enrichments and return the rejection reason."""
    if enrichment is None:
        return None, "parse_failed"

    merchant_name = (enrichment.get("name") or "").strip()
    merchant_kind = normalize_merchant_kind(enrichment.get("merchant_kind"), tx)
    merchant_industry = (enrichment.get("industry") or "").strip()
    description = (tx.get("raw_description") or tx.get("description") or "").strip()

    if not merchant_name and merchant_kind in {"personal_transfer", "credit_card_payment", "income", "tax", "bank_fee"}:
        enrichment["merchant_kind"] = merchant_kind
        return enrichment, "accepted_non_merchant"

    if not merchant_name:
        return None, "empty_merchant_name"

    if not merchant_industry or merchant_industry == "Unknown":
        return None, "unknown_industry"

    evidence_tokens = enrichment.get("evidence_tokens") or []
    if not merchant_name_supported(description, merchant_name, evidence_tokens):
        return None, "unsupported_merchant_name"

    enrichment["merchant_kind"] = MERCHANT_PURCHASE if merchant_kind == "unknown" else merchant_kind
    return enrichment, "accepted"


def _validate_local_enrichment(tx: dict, enrichment: dict | None) -> dict | None:
    validated, _ = _validate_local_enrichment_with_reason(tx, enrichment)
    return validated


def _persist_local_result(tx: dict, enrichment: dict):
    """Store a validated local enrichment in caches/tables for reuse."""
    desc = tx.get("raw_description") or tx.get("description", "")
    _enrichment_cache.put(desc, enrichment)
    _persist_enrichment(_dedup_key(tx), enrichment, source="seed")
    _upsert_merchant_from_tx(tx, enrichment, source="llm")


def _local_rule_enrichment(tx: dict) -> dict | None:
    """
    Fast path for obvious merchants where the raw description is already a
    stronger signal than the LLM. This both improves speed and avoids batch
    cross-contamination on repeated high-volume merchants.
    """
    desc = (tx.get("raw_description") or tx.get("description") or "").strip()
    if not desc:
        return None

    upper = desc.upper()

    rules: list[tuple[re.Pattern[str], str, str]] = [
        (
            re.compile(
                r"\bAMZ\*PRIME SHIPPING CLUB\b|\bAMAZON PRIME\b|\bAMZ[N ]?\*?PRIME\b",
                re.I,
            ),
            "Amazon Prime",
            "Subscription Service",
        ),
        (
            re.compile(
                r"\bAMAZON DIGITAL SVCS\b|\bAMZN DIGITAL\b|\bVIDEO ON DEMAND\b",
                re.I,
            ),
            "Amazon Digital Services",
            "Streaming / Media",
        ),
        (
            re.compile(r"\bAMAZONFRESH\b|\bAMZN\.COM/FRESH\b|\bAMAZON FRESH\b", re.I),
            "Amazon Fresh",
            "Grocery",
        ),
        (
            re.compile(
                r"\bAMAZON\.COM\*PMT SVC\b|\bAMZN PMTS\b|\bAMZN\.COM/PMTS\b|\bAMAZON PAY\b",
                re.I,
            ),
            "Amazon Pay",
            "E-commerce Marketplace",
        ),
        (
            re.compile(r"\bAMAZON RETAIL LLC\b|\bAMAZON BOOKSTORE\b", re.I),
            "Amazon Retail",
            "General Retail",
        ),
        (re.compile(r"\bAMAZON\s+GROCE\b|\bAMZN\s+GROCE\b", re.I), "Amazon Grocery", "Grocery"),
        (re.compile(r"\bAMAZON\s+MKTPL\b|\bAMAZON\s+MKTPLACE\b|\bAMZN\s+MKTP\b", re.I), "Amazon Marketplace", "E-commerce Marketplace"),
        (re.compile(r"\bAMAZON\.COM\b|\bAMZN\.COM/BILL\b|\bPOS AMAZON\b|\bAMAZON MERCHANDISE\b", re.I), "Amazon", "E-commerce Marketplace"),
        (re.compile(r"\bNETFLIX\b", re.I), "Netflix", "Streaming / Media"),
        (re.compile(r"\bSPOTIFY\b", re.I), "Spotify", "Streaming / Media"),
        (re.compile(r"\bHULU\b", re.I), "Hulu", "Streaming / Media"),
        (re.compile(r"\bAPPLE\.COM/BILL\b|\bAPPLE ITUNES\b", re.I), "Apple", "Subscription Service"),
        (re.compile(r"\bGOOGLE \*GOOGLE ONE\b|\bGOOGLE ONE\b", re.I), "Google", "Software / SaaS"),
        (re.compile(r"\bEXPRESSVPN\b", re.I), "ExpressVPN", "Software / SaaS"),
        (re.compile(r"\bGEICO\b", re.I), "GEICO", "Insurance"),
        (re.compile(r"\bDOORDASH\b|\bDD \*DOORDASH\b", re.I), "DoorDash", "Restaurant"),
        (re.compile(r"\bUBER\* TRIP\b|\bUBER TRIP\b", re.I), "Uber", "Transportation / Rideshare"),
        (re.compile(r"\bUBER EATS\b", re.I), "Uber Eats", "Restaurant"),
        (re.compile(r"\bLYFT\b", re.I), "Lyft", "Transportation / Rideshare"),
        (re.compile(r"\bWHOLEFDS\b|\bWHOLE FOODS\b", re.I), "Whole Foods", "Grocery"),
        (re.compile(r"\bCOSTCO\b", re.I), "Costco", "Grocery"),
        (re.compile(r"\bSAFEWAY\b", re.I), "Safeway", "Grocery"),
        (re.compile(r"\bTRADER JOE'?S\b", re.I), "Trader Joe's", "Grocery"),
        (re.compile(r"\bCVS\b", re.I), "CVS Pharmacy", "Pharmacy"),
        (re.compile(r"\bWALGREENS\b", re.I), "Walgreens", "Pharmacy"),
        (re.compile(r"\bCHEVRON\b", re.I), "Chevron", "Gas Station"),
        (re.compile(r"\bSHELL\b", re.I), "Shell", "Gas Station"),
        (re.compile(r"\bEXXON\b|\bEXXONMOBIL\b", re.I), "Exxon", "Gas Station"),
        (re.compile(r"\bCIRCLE K\b", re.I), "Circle K", "Gas Station"),
        (re.compile(r"\bVALERO\b", re.I), "Valero", "Gas Station"),
        (re.compile(r"\bSTARBUCKS\b", re.I), "Starbucks", "Coffee Shop"),
    ]

    for pattern, name, industry in rules:
        if pattern.search(upper):
            return {
                "name": name,
                "merchant_kind": MERCHANT_PURCHASE,
                "industry": industry,
                "confidence": "high",
                "evidence_tokens": [name],
                "_cache_source": "local_rule",
            }

    return None


def _local_enrichment_line(tx: dict, index: int) -> str:
    """Build one transaction line for local merchant-normalization prompts."""
    description = (tx.get("raw_description") or tx.get("description") or "").strip()
    clean_description = (tx.get("description") or "").strip()
    tx_type = (tx.get("type") or "").strip()
    account_type = (tx.get("account_type") or "").strip()
    teller_category = (tx.get("teller_category") or "").strip()
    return (
        f'{index}. Raw description: "{description}" | '
        f'Clean description: "{clean_description}" | '
        f'Type: "{tx_type}" | '
        f'Account type: "{account_type}" | '
        f'Teller category: "{teller_category}"'
    )


LOCAL_ENRICHMENT_EXAMPLES = """
Examples:
- Raw description: "ALASKA AIR 0272123773667 SEATTLE"
  Output: {"merchant_name":"Alaska Air","merchant_kind":"merchant_purchase","merchant_industry":"Travel / Airline","confidence":"high","evidence_tokens":["ALASKA AIR"]}
- Raw description: "LUFTHAN 2204081455193 NEW YORK"
  Output: {"merchant_name":"Lufthansa","merchant_kind":"merchant_purchase","merchant_industry":"Travel / Airline","confidence":"high","evidence_tokens":["LUFTHAN"]}
- Raw description: "DOORDASH*11/16-2 ORDER 855-431-0459"
  Output: {"merchant_name":"DoorDash","merchant_kind":"merchant_purchase","merchant_industry":"Restaurant","confidence":"high","evidence_tokens":["DOORDASH"]}
- Raw description: "DD *DOORDASH SAFEWAY 855-973-1040"
  Output: {"merchant_name":"DoorDash","merchant_kind":"merchant_purchase","merchant_industry":"Restaurant","confidence":"medium","evidence_tokens":["DD","DOORDASH"]}
- Raw description: "AMAZON DIGITAL SVCS AMZN.COM/BILL"
  Output: {"merchant_name":"Amazon Digital Services","merchant_kind":"merchant_purchase","merchant_industry":"Streaming / Media","confidence":"high","evidence_tokens":["AMAZON DIGITAL"]}
- Raw description: "AMZN Mktp US *A1B2C3D4E"
  Output: {"merchant_name":"Amazon Marketplace","merchant_kind":"merchant_purchase","merchant_industry":"E-commerce Marketplace","confidence":"high","evidence_tokens":["AMZN","Mktp"]}
- Raw description: "AMAZONFRESH AMZN.COM/FRESH"
  Output: {"merchant_name":"Amazon Fresh","merchant_kind":"merchant_purchase","merchant_industry":"Grocery","confidence":"high","evidence_tokens":["AMAZONFRESH"]}
- Raw description: "CHATGPT SUBSCRIPTION HTTPSOPENAI.C"
  Output: {"merchant_name":"OpenAI","merchant_kind":"merchant_purchase","merchant_industry":"Software / SaaS","confidence":"high","evidence_tokens":["CHATGPT"]}
- Raw description: "PAYPAL *TRANSFER JOHN D"
  Output: {"merchant_name":"","merchant_kind":"personal_transfer","merchant_industry":"","confidence":"high","evidence_tokens":["PAYPAL TRANSFER"]}
""".strip()

LOCAL_ENRICHMENT_PLATFORM_POLICY = """
Platform policy:
- Normalize to the platform name for delivery/marketplace platforms when the
  underlying seller may be missing or inconsistent: DoorDash, Uber Eats,
  Instacart, Grubhub, Amazon Marketplace, Amazon Pay.
- For processor-style wrappers like Square, Toast, Stripe, Clover, and PayPal,
  prefer the underlying merchant only when it is clearly supported by the row's
  text; otherwise keep the processor name or abstain.
""".strip()


def _build_local_enrichment_prompt(tx: dict) -> str:
    """
    Ask the local LLM to normalize merchant identity without inventing
    domains or location data.
    """
    return f"""You normalize merchant purchase descriptions for a personal finance app.

Return ONLY valid JSON with this exact schema:
{{"merchant_name":"...", "merchant_kind":"merchant_purchase|personal_transfer|credit_card_payment|income|tax|bank_fee|unknown", "merchant_industry":"...", "confidence":"high|medium|low", "evidence_tokens":["..."]}}

Rules:
- Use ONLY this merchant_industry list:
  {", ".join(MERCHANT_INDUSTRIES)}
- Do not generate website domains, cities, states, or countries
- Normalize the merchant_name to a clean human-readable merchant label
- Set merchant_kind even when merchant_name is empty
- If the transaction is too ambiguous or not clearly a merchant purchase, return merchant_name="" and merchant_industry=""
- If the merchant appears to be a payment processor wrapper (like PayPal, Square, Toast), prefer the underlying merchant when it is clearly present; otherwise keep the processor name
- If confidence would be low, leave both merchant_name and merchant_industry empty
- Never infer a merchant that is not directly supported by this transaction's text
- Do not let one transaction influence another; each row must be judged independently
- Only use Amazon/Amazon Marketplace when the row itself contains Amazon/AMZN text
- Do not include explanations or extra keys

{LOCAL_ENRICHMENT_PLATFORM_POLICY}

{LOCAL_ENRICHMENT_EXAMPLES}

Transaction:
- {_local_enrichment_line(tx, 0)}
"""


def _build_local_enrichment_batch_prompt(batch: list[dict]) -> str:
    """Ask the local LLM to normalize multiple merchant descriptions in one call."""
    lines = "\n".join(_local_enrichment_line(tx, i) for i, tx in enumerate(batch))

    return f"""You normalize merchant purchase descriptions for a personal finance app.

Return ONLY valid JSON as an array with this exact schema:
[{{"index":0,"merchant_name":"...","merchant_kind":"merchant_purchase|personal_transfer|credit_card_payment|income|tax|bank_fee|unknown","merchant_industry":"...","confidence":"high|medium|low","evidence_tokens":["..."]}}]

Rules:
- Use ONLY this merchant_industry list:
  {", ".join(MERCHANT_INDUSTRIES)}
- Do not generate website domains, cities, states, or countries
- Normalize the merchant_name to a clean human-readable merchant label
- Set merchant_kind even when merchant_name is empty
- If the transaction is too ambiguous or not clearly a merchant purchase, return merchant_name="" and merchant_industry=""
- If the merchant appears to be a payment processor wrapper (like PayPal, Square, Toast), prefer the underlying merchant when it is clearly present; otherwise keep the processor name
- If confidence would be low, leave both merchant_name and merchant_industry empty
- Never infer a merchant that is not directly supported by that transaction's text
- Do not let one transaction influence another; each row must be judged independently
- Only use Amazon/Amazon Marketplace when that row itself contains Amazon/AMZN text
- Do not include explanations or extra keys
- Return one object for every index, even if the merchant is unknown

{LOCAL_ENRICHMENT_PLATFORM_POLICY}

{LOCAL_ENRICHMENT_EXAMPLES}

Transactions:
{lines}
"""


def _extract_json_blob(raw: str) -> str | None:
    """Best-effort extraction of a JSON object or array from model output."""
    if not raw:
        return None

    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)

    if text.startswith("{") or text.startswith("["):
        return text

    array_match = re.search(r"\[.*\]", text, re.DOTALL)
    if array_match:
        return array_match.group(0)

    obj_match = re.search(r"\{.*\}", text, re.DOTALL)
    if obj_match:
        return obj_match.group(0)

    return None


def _parse_local_enrichment_response(raw: str) -> dict | None:
    """Parse and validate the local LLM JSON response."""
    text = _extract_json_blob(raw)
    if text is None:
        return None

    try:
        parsed = json.loads(text)
    except Exception:
        return None

    if not isinstance(parsed, dict):
        return None

    merchant_name = (parsed.get("merchant_name") or "").strip()
    merchant_kind = normalize_merchant_kind(parsed.get("merchant_kind"))
    merchant_industry = _normalize_industry(parsed.get("merchant_industry") or "")
    confidence = (parsed.get("confidence") or "medium").strip().lower()
    evidence_tokens = parsed.get("evidence_tokens") or []
    if not isinstance(evidence_tokens, list):
        evidence_tokens = []
    if confidence not in {"high", "medium", "low"}:
        confidence = "medium"

    if not merchant_name and merchant_kind not in {"personal_transfer", "credit_card_payment", "income", "tax", "bank_fee"}:
        return None

    if merchant_name and merchant_industry == "Unknown":
        return None

    if merchant_name and not _confidence_meets_threshold(confidence):
        return None

    return {
        "name": merchant_name,
        "merchant_kind": merchant_kind,
        "industry": merchant_industry,
        "confidence": confidence,
        "evidence_tokens": [str(token)[:80] for token in evidence_tokens[:6]],
    }


def _parse_local_enrichment_batch_response(raw: str) -> dict[int, dict]:
    """Parse a batch merchant-normalization response keyed by batch index."""
    text = _extract_json_blob(raw)
    if text is None:
        return {}

    try:
        parsed = json.loads(text)
    except Exception:
        return {}

    if not isinstance(parsed, list):
        return {}

    results: dict[int, dict] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except Exception:
            continue

        normalized = _parse_local_enrichment_response(json.dumps(item))
        if normalized is not None:
            results[idx] = normalized

    return results


def _detect_batch_contamination(batch_txs: list[dict], parsed_results: dict[int, dict]) -> str | None:
    """
    Detect suspicious merchant-name overconcentration in a batch response.
    This catches a common small-model failure mode where one merchant identity
    bleeds across many rows in the same prompt.
    """
    if len(parsed_results) < 4:
        return None

    counts: dict[str, list[int]] = {}
    for batch_pos, parsed in parsed_results.items():
        merchant_name = (parsed.get("name") or "").strip()
        if merchant_name:
            counts.setdefault(merchant_name, []).append(batch_pos)

    if not counts:
        return None

    threshold = max(4, int(len(batch_txs) * 0.4 + 0.999))
    for merchant_name, positions in counts.items():
        if len(positions) < threshold:
            continue
        supported = 0
        for pos in positions:
            tx = batch_txs[pos]
            description = (tx.get("raw_description") or tx.get("description") or "").strip()
            if _merchant_name_supported(description, merchant_name):
                supported += 1
        if supported / len(positions) < 0.6:
            return merchant_name

    return None


def _enrich_single_local(tx: dict) -> dict:
    """Enrich a single transaction via the configured LLM provider."""
    try:
        raw = llm_client.complete(
            _build_local_enrichment_prompt(tx),
            max_tokens=220,
            purpose="categorize",
        )
        parsed = _parse_local_enrichment_response(raw)
        parsed, reason = _validate_local_enrichment_with_reason(tx, parsed)
        if parsed is None:
            tx["enriched"] = False
            tx["enrichment_reject_reason"] = reason
            return tx
        parsed["_cache_source"] = "local_llm"
        tx["enrichment_confidence"] = parsed.get("confidence", "")
        tx["enrichment_reject_reason"] = ""
        return _apply_enrichment(tx, parsed)
    except Exception as e:
        logger.error("Local LLM enrichment failed: %s", e)
        tx["enriched"] = False
        tx["enrichment_reject_reason"] = "single_call_error"
        return tx


def _should_enrich(tx: dict) -> bool:
    """
    Determine if a transaction should be sent to Trove.
    Skip internal transfers, payments, income, fees, ACH deposits —
    these have no meaningful merchant for Trove to identify.
    """
    if tx.get("enriched"):
        return False

    non_merchant_kind = infer_non_merchant_kind(tx)
    if non_merchant_kind:
        tx["merchant_kind"] = non_merchant_kind
        tx["merchant_source"] = tx.get("merchant_source") or "rule"
        tx["merchant_confidence"] = tx.get("merchant_confidence") or "high"
        return False

    cat = tx.get("category", "")
    if cat in SKIP_ENRICHMENT_CATEGORIES:
        return False

    desc = (tx.get("raw_description") or tx.get("description") or "").strip()
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
        "atm withdrawal", "withdrwl", "withdrawal",
        "venmo", "zelle", "cash app", "cashapp",
        "apple cash", "paypal transfer", "paypal *transfer",
        "bkofamerica atm", "bank of america atm",
        "pai iso", "payment sent", "transfer to",
        "payment received", "ach credit", "ach debit",
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
    source = enrichment.get("_cache_source") or enrichment.get("source") or "trove"
    confidence = (enrichment.get("confidence") or enrichment.get("merchant_confidence") or "").strip()
    merchant_kind = normalize_merchant_kind(enrichment.get("merchant_kind"), tx)

    merchant_name = name if name else (_domain_to_name(domain) if domain else "")
    merchant_city = enrichment.get("hq_city") or enrichment.get("city") or ""
    merchant_state = enrichment.get("hq_state_code") or enrichment.get("state_code") or ""
    merchant_country = enrichment.get("hq_country_code") or enrichment.get("country_code") or ""

    # For local LLM enrichment we may intentionally omit literal website domains.
    # Count only actionable merchant enrichment as success; unknown industry
    # should not inflate enrichment stats.
    has_actionable_industry = bool(industry and industry != "Unknown")
    if domain or (merchant_name and has_actionable_industry):
        tx["merchant_domain"] = domain
        tx["merchant_name"] = merchant_name
        tx["merchant_key"] = canonicalize_merchant_key(merchant_name)
        tx["merchant_source"] = "llm" if source in ("local_llm", "llm") else source
        tx["merchant_confidence"] = confidence or ("high" if domain else "medium")
        tx["merchant_kind"] = merchant_kind if merchant_kind != "unknown" else MERCHANT_PURCHASE
        tx["merchant_industry"] = industry
        tx["merchant_categories"] = categories
        tx["merchant_city"] = merchant_city
        tx["merchant_state"] = merchant_state
        tx["merchant_country"] = merchant_country
        tx["enriched"] = True
        tx["enrichment_tier"] = "full" if (domain and merchant_name) else "normalized"
    else:
        inferred_kind = merchant_kind if merchant_kind != "unknown" else infer_non_merchant_kind(tx)
        if inferred_kind:
            tx["merchant_kind"] = inferred_kind
            tx["merchant_source"] = "llm" if source in ("local_llm", "llm") else source
            tx["merchant_confidence"] = confidence or "high"
        tx["merchant_key"] = tx.get("merchant_key", "")
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
        "merchant_country", "merchant_key", "merchant_source",
        "merchant_confidence", "merchant_kind", "enriched", "enrichment_tier",
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
    """Enrich a single transaction via the active enrichment backend."""
    mode = _resolve_enrichment_mode()
    if mode == "none":
        return tx
    if not _should_enrich(tx):
        return tx

    rule_hit = _local_rule_enrichment(tx) if mode == "local_llm" else None
    if rule_hit is not None:
        enriched = _apply_enrichment(tx, rule_hit)
        if enriched.get("enriched"):
            _persist_local_result(tx, rule_hit)
        return enriched

    # Check cache first
    desc = tx.get("raw_description") or tx.get("description", "")
    cached = _enrichment_cache.get(desc)
    if cached is not None:
        return _apply_enrichment(tx, cached)

    if mode == "local_llm":
        enriched = _enrich_single_local(tx)
        if enriched.get("enriched"):
            cache_payload = {
                "name": enriched.get("merchant_name", ""),
                "industry": enriched.get("merchant_industry", ""),
                "_cache_source": "local_llm",
            }
            _persist_local_result(tx, cache_payload)
        return enriched

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
    mode = _resolve_enrichment_mode()
    if mode == "none":
        logger.info("    No enrichment backend available — skipping")
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
    backend_label = "Trove" if mode == "trove" else "local LLM"
    logger.info("    Enriching %d of %d transactions via %s...", total_enrichable, len(transactions), backend_label)

    # ── Step 2: Resolve cache hits ──
    cache_hit_count = 0
    cache_hit_enriched = 0
    remaining_indices = []
    for idx in enrichable_indices:
        tx = transactions[idx]
        if mode == "local_llm":
            rule_hit = _local_rule_enrichment(tx)
            if rule_hit is not None:
                transactions[idx] = _apply_enrichment(tx, rule_hit)
                _persist_local_result(tx, rule_hit)
                cache_hit_count += 1
                if transactions[idx].get("enriched"):
                    cache_hit_enriched += 1
                continue

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
            api_label="local LLM" if mode == "local_llm" else "Trove API",
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
            api_label="local LLM" if mode == "local_llm" else "Trove API",
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

    # ── Step 4: Choose strategy based on active backend ──
    if mode == "local_llm":
        logger.info(
            "    Using local LLM enrichment for %d unique descriptions",
            len(representative_indices),
        )
        api_enriched, fanout_enriched = _enrich_via_local_llm(
            transactions, representative_indices, fanout_map,
        )
    elif len(representative_indices) <= BULK_THRESHOLD:
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
        api_label="local LLM" if mode == "local_llm" else "Trove API",
    )

    return transactions


def _log_enrichment_summary(
    total_enrichable: int,
    from_cache: int,
    from_api: int,
    from_fanout: int,
    from_persistent_cache: int = 0,
    api_label: str = "Trove API",
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
        parts.append(f"{from_api} from {api_label}")
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
            "    Not enriched: %d transactions (no enrichment match)",
            not_enriched,
        )


def _enrich_via_local_llm(
    transactions: list[dict],
    indices: list[int],
    fanout_map: dict[str, list[int]],
) -> tuple[int, int]:
    """
    Enrich transactions via the configured LLM provider, then fan out to
    duplicate descriptions using the same dedup flow as Trove.
    """
    api_enriched = 0
    fanout_enriched = 0
    reject_counts: dict[str, int] = {}

    for start in range(0, len(indices), LOCAL_ENRICHMENT_BATCH_SIZE):
        batch_indices = indices[start:start + LOCAL_ENRICHMENT_BATCH_SIZE]
        batch_txs = [transactions[idx] for idx in batch_indices]
        max_tokens = min(4096, max(512, 140 * len(batch_indices)))

        try:
            raw = llm_client.complete(
                _build_local_enrichment_batch_prompt(batch_txs),
                max_tokens=max_tokens,
                purpose="categorize",
            )
            parsed_results = _parse_local_enrichment_batch_response(raw)
            contaminated_merchant = _detect_batch_contamination(batch_txs, parsed_results)
            if contaminated_merchant:
                logger.warning(
                    "    Local LLM batch %d-%d looked contaminated by merchant '%s' — retrying rows individually",
                    start,
                    start + len(batch_indices) - 1,
                    contaminated_merchant,
                )
                parsed_results = {}
        except Exception as e:
            logger.error(
                "Local LLM batch enrich failed for batch %d-%d: %s",
                start,
                start + len(batch_indices) - 1,
                e,
            )
            parsed_results = {}

        for batch_pos, idx in enumerate(batch_indices):
            tx = transactions[idx]
            parsed, reason = _validate_local_enrichment_with_reason(tx, parsed_results.get(batch_pos))
            if parsed is None:
                reject_counts[reason] = reject_counts.get(reason, 0) + 1
                if len(batch_indices) > 1:
                    fallback_tx = _enrich_single_local(tx)
                    transactions[idx] = fallback_tx
                    if fallback_tx.get("enriched"):
                        payload = {
                            "name": fallback_tx.get("merchant_name", ""),
                            "industry": fallback_tx.get("merchant_industry", ""),
                            "confidence": fallback_tx.get("enrichment_confidence", "medium"),
                            "_cache_source": "local_llm",
                        }
                        _persist_local_result(fallback_tx, payload)
                        api_enriched += 1
                    else:
                        tx["enriched"] = False
                else:
                    tx["enriched"] = False
            else:
                parsed["_cache_source"] = "local_llm"
                tx["enrichment_confidence"] = parsed.get("confidence", "")
                _persist_local_result(tx, parsed)
                transactions[idx] = _apply_enrichment(tx, parsed)
                if transactions[idx].get("enriched"):
                    api_enriched += 1

            fanout_enriched += _fanout_enrichment(transactions, fanout_map, idx)

    logger.info(
        "    Local LLM enrich: %d/%d normalized, %d fanned out to duplicates",
        api_enriched, len(indices), fanout_enriched,
    )
    if reject_counts:
        logger.info(
            "    Local LLM rejects: %s",
            ", ".join(f"{reason}={count}" for reason, count in sorted(reject_counts.items())),
        )
    return api_enriched, fanout_enriched


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
