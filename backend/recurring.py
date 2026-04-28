"""
recurring.py
Subscription and recurring charge detection service.

Three-layer detection:
  Layer 1 — Seed matching  (known subscription patterns from DB)
  Layer 2 — Algorithmic    (frequency + amount pattern detection)
  Layer 3 — Category-based (transactions already tagged "Subscriptions")

Key design principles:
  - Algorithm is king; seeds assist with naming and hints
  - Price-change segmentation: evaluate the latest stable price segment
  - Date consistency (±2 days) is the primary recurrence signal
  - Category exclusions are enforced (except for whitelisted seed categories)
  - Merchant grouping splits on description patterns, seeds merge related groups
"""

import re
import time
from datetime import datetime, timedelta
from statistics import mean, stdev, median
from collections import defaultdict
from database import _extract_merchant_pattern
from merchant_identity import canonicalize_merchant_key
from recurring_obligations import (
    advance_recurring_date,
    canonical_key as recurring_canonical_key,
    event_period_bucket,
    merchant_match_keys,
    recurring_event_key,
    sync_detection_results,
)
from log_config import get_logger

logger = get_logger(__name__)


# ── Frequency definitions (single source of truth) ───────────────
# (nominal_days, grace_days, range_low, range_high)
FREQUENCY_DEFS = {
    "monthly":     (30,  15,  25,  38),
    "quarterly":   (91,  30,  80, 105),
    "semi_annual": (182, 45, 160, 210),
    "annual":      (365, 45, 340, 400),
}

FREQ_RANGES = {k: (v[2], v[3]) for k, v in FREQUENCY_DEFS.items()}

TRANSFER_CATEGORIES = {"Savings Transfer", "Personal Transfer", "Credit Card Payment", "Internal Transfer"}
NON_SPENDING_CATEGORIES = TRANSFER_CATEGORIES | {"Income", "Tax Refund", "Refund"}

# Seed categories that are ALLOWED through the category exclusion filter.
# If a seed's own category is in this set, it bypasses ALGO_EXCLUDED_CATEGORIES.
# This lets "Amazon Prime" (category: "Membership") survive even if the
# transaction was LLM-categorized as "Shopping".
_SEED_WHITELISTED_CATEGORIES = {
    "Subscriptions", "Membership", "Cloud Storage", "Streaming",
    "Music", "Software", "Gaming", "News", "Productivity",
    "VPN", "Security", "Education Platform", "Fitness",
    "Cloud Services", "Developer Tools", "AI Services",
    "Design Tools", "Communication", "Social Media",
    "Email", "Domains & Hosting", "Storage",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_dates(date_strings: list[str]) -> list:
    """Parse date strings to date objects, skipping unparseable values."""
    out = []
    for ds in date_strings:
        try:
            out.append(datetime.strptime(ds[:10], "%Y-%m-%d").date())
        except (ValueError, TypeError):
            continue
    return out


def _normalise_seed_frequency(freq_hint: str | None) -> str:
    """Normalize frequency hint string to canonical form."""
    if not freq_hint:
        return "monthly"
    f = freq_hint.lower().replace("-", "_").strip()
    if f in ("monthly", "quarterly", "semi_annual", "annual"):
        return f
    if f == "monthly_or_annual":
        return "monthly"          # Will be overridden by interval detection
    if f in ("semiannual", "semi-annual", "biannual"):
        return "semi_annual"
    if f == "yearly":
        return "annual"
    return "monthly"


def _profile_scope_key(merchant: str | None, profile_id: str | None = None) -> str:
    key = recurring_canonical_key(merchant or "")
    return f"{profile_id or 'household'}::{key}" if key else ""


def _profile_from_group(group_txns: list[dict], fallback: str | None = None) -> str:
    for tx in group_txns or []:
        profile_id = tx.get("profile_id") or tx.get("profile")
        if profile_id:
            return profile_id
    return fallback or "household"


def _is_dismissed_match(dismissed: set[str], merchant: str | None, profile_id: str | None = None) -> bool:
    keys = merchant_match_keys(merchant)
    if any(key in dismissed for key in keys):
        return True
    if profile_id:
        return any(f"{profile_id}::{key}" in dismissed for key in keys)
    return False


def _detect_frequency(dates: list, seed_freq_hint: str | None = None) -> str | None:
    """
    Detect billing frequency from a sorted list of date objects.

    For N=2: directly checks the single interval against all frequency ranges
    (most specific first: monthly → quarterly → semi_annual → annual).

    For N≥3: uses median interval with 0.25 relative tolerance and 70% pass rate.

    If a seed_freq_hint is provided, it gets priority when its range matches.
    """
    if len(dates) < 2:
        return _normalise_seed_frequency(seed_freq_hint) if seed_freq_hint else None

    intervals = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    if not intervals:
        return None

    # ── N=2 special case: single interval, direct range check ──
    if len(intervals) == 1:
        iv = intervals[0]

        # If seed hint is provided and matches, prefer it
        if seed_freq_hint:
            canonical = _normalise_seed_frequency(seed_freq_hint)
            lo, hi = FREQ_RANGES.get(canonical, (25, 38))
            if lo <= iv <= hi:
                return canonical

            # For "monthly_or_annual" hint: also try annual if monthly didn't match
            if seed_freq_hint.lower().replace("-", "_") == "monthly_or_annual":
                lo_a, hi_a = FREQ_RANGES["annual"]
                if lo_a <= iv <= hi_a:
                    return "annual"

        # Try all frequencies (most specific first)
        for freq_name in ("monthly", "quarterly", "semi_annual", "annual"):
            lo, hi = FREQ_RANGES[freq_name]
            if lo <= iv <= hi:
                return freq_name

        return None

    # ── N≥3: median-based detection ──
    med_iv = median(intervals)

    # Seed hint gets first try
    if seed_freq_hint:
        canonical = _normalise_seed_frequency(seed_freq_hint)
        lo, hi = FREQ_RANGES.get(canonical, (25, 38))
        if lo <= med_iv <= hi:
            nominal = (lo + hi) / 2
            ok = sum(
                1 for iv in intervals
                if abs(iv - nominal) / nominal <= 0.25
            )
            if ok / len(intervals) >= 0.70:
                return canonical

        # For "monthly_or_annual": try annual too
        if seed_freq_hint.lower().replace("-", "_") == "monthly_or_annual":
            lo_a, hi_a = FREQ_RANGES["annual"]
            if lo_a <= med_iv <= hi_a:
                nominal_a = (lo_a + hi_a) / 2
                ok_a = sum(
                    1 for iv in intervals
                    if abs(iv - nominal_a) / nominal_a <= 0.25
                )
                if ok_a / len(intervals) >= 0.70:
                    return "annual"

    # General detection across all frequencies
    for freq_name, (lo, hi) in FREQ_RANGES.items():
        if lo <= med_iv <= hi:
            nominal = (lo + hi) / 2
            ok = sum(
                1 for iv in intervals
                if abs(iv - nominal) / nominal <= 0.25
            )
            if ok / len(intervals) >= 0.70:
                return freq_name

    return None


def _check_date_consistency(dates_sorted: list, frequency: str, tolerance_days: int = 2) -> bool:
    """
    Check whether charges land on a consistent day-of-month (±tolerance_days).

    For monthly/quarterly/semi_annual: checks day-of-month consistency.
    For annual: checks month + day-of-month consistency, AND validates
                that intervals are approximately 365 days.
    For N=2: uses direct interval validation instead of statistical thresholds.

    Returns True if dates are consistent, False otherwise.
    With fewer than 2 dates, returns True (nothing to validate against).
    """
    if len(dates_sorted) < 2:
        return True

    # ── N=2 special case: direct interval check ──
    if len(dates_sorted) == 2:
        interval = (dates_sorted[1] - dates_sorted[0]).days
        day_diff = abs(dates_sorted[0].day - dates_sorted[1].day)
        # Handle month-end wrapping for day comparison
        day_ok = (day_diff <= tolerance_days or
                  (dates_sorted[0].day >= 28 and dates_sorted[1].day <= tolerance_days) or
                  (dates_sorted[1].day >= 28 and dates_sorted[0].day <= tolerance_days))

        if frequency == "annual":
            return 350 <= interval <= 380 and day_ok
        elif frequency == "semi_annual":
            return 170 <= interval <= 195 and day_ok
        elif frequency == "quarterly":
            return 80 <= interval <= 100 and day_ok
        elif frequency == "monthly":
            return 25 <= interval <= 38 and day_ok
        # Unknown frequency — accept if days match
        return day_ok

    # ── N≥3: statistical consistency check ──
    if frequency in ("monthly", "quarterly", "semi_annual"):
        days_of_month = [d.day for d in dates_sorted]
        expected_day = int(median(days_of_month))

        consistent = 0
        for day in days_of_month:
            diff = abs(day - expected_day)
            if diff <= tolerance_days:
                consistent += 1
            elif expected_day >= 28 and day <= tolerance_days:
                consistent += 1
            elif day >= 28 and expected_day <= tolerance_days:
                consistent += 1

        return (consistent / len(days_of_month)) >= 0.70

    elif frequency == "annual":
        if len(dates_sorted) < 2:
            return True

        # Check month + day consistency
        month_days = [(d.month, d.day) for d in dates_sorted]
        expected_month = int(median([md[0] for md in month_days]))
        expected_day = int(median([md[1] for md in month_days]))

        consistent = 0
        for m, d in month_days:
            month_ok = m == expected_month
            day_diff = abs(d - expected_day) <= tolerance_days
            if month_ok and day_diff:
                consistent += 1

        if (consistent / len(month_days)) < 0.70:
            return False

        # Also validate intervals are ~365 days
        intervals = [(dates_sorted[i + 1] - dates_sorted[i]).days
                     for i in range(len(dates_sorted) - 1)]
        interval_ok = sum(1 for iv in intervals if 340 <= iv <= 400)
        return (interval_ok / len(intervals)) >= 0.70

    return True


def _amount_confidence(amounts: list[float]) -> float:
    """Return a 0-1 score for amount consistency (1 = perfectly consistent)."""
    if len(amounts) < 2:
        return 1.0
    avg = sum(amounts) / len(amounts)
    if avg == 0:
        return 0.0
    sd = (sum((a - avg) ** 2 for a in amounts) / len(amounts)) ** 0.5
    return round(max(0.0, 1.0 - (sd / avg)), 2)


def _compute_confidence(
    occurrences: int,
    amounts: list[float],
    dates_sorted: list,
    frequency: str | None,
    matched_by: str,
    date_tolerance: int = 2,
) -> str:
    """
    Compute a holistic confidence level: 'high', 'medium', or 'low'.

    NOTE: This function should receive ALREADY-SEGMENTED amounts and dates
    (i.e., the latest stable price segment). The caller is responsible for
    segmentation. This function does NOT segment internally.

    Rules:
    - 1 occurrence:  always 'low'
    - 2 occurrences: starts at 'medium', can be downgraded
    - 3+ occurrences: starts at 'high', can be downgraded
    - Amount inconsistency (CV > 0.20): downgrade by one level
    - Date inconsistency: downgrade by one level
    """
    if occurrences <= 0:
        return "low"

    if occurrences == 1:
        return "low"
    elif occurrences == 2:
        base = 2  # medium
    else:
        base = 3  # high

    # Downgrade for amount inconsistency
    if len(amounts) >= 2:
        avg = mean(amounts)
        if avg > 0:
            sd = stdev(amounts)
            cv = sd / avg
            if cv > 0.20:
                base -= 1
        else:
            base -= 1

    # Downgrade for date inconsistency
    if frequency and len(dates_sorted) >= 2:
        if not _check_date_consistency(dates_sorted, frequency, date_tolerance):
            base -= 1

    base = max(1, min(3, base))
    return {1: "low", 2: "medium", 3: "high"}[base]


def _annualize(amount: float, frequency: str) -> float:
    """Convert a per-period amount to annual cost."""
    multipliers = {
        "monthly":     12,
        "quarterly":    4,
        "semi_annual":  2,
        "annual":       1,
    }
    return amount * multipliers.get(frequency, 12)


def _detect_price_change(group_txns: list[dict]) -> dict | None:
    """Detect if the most recent charge differs from recent history."""
    if len(group_txns) < 2:
        return None

    sorted_txns = sorted(group_txns, key=lambda t: t["date"], reverse=True)
    current_amt = sorted_txns[0]["amount"]

    previous_amts = [t["amount"] for t in sorted_txns[1:4]]
    if not previous_amts:
        return None

    prev_avg = mean(previous_amts)

    change = round(current_amt - prev_avg, 2)
    if abs(change) < 0.50:
        return None

    change_pct = round((change / prev_avg) * 100, 1) if prev_avg > 0 else 0.0

    return {
        "previous": round(prev_avg, 2),
        "current": round(current_amt, 2),
        "change": change,
        "change_pct": change_pct,
    }


def _segment_evidence(segments: list[list[dict]]) -> list[dict]:
    """Compact, deterministic evidence for each price-stable segment."""
    evidence: list[dict] = []
    for idx, segment in enumerate(segments, start=1):
        ordered = sorted(segment, key=lambda t: t["date"])
        amounts = [float(t["amount"]) for t in ordered]
        dates = [str(t["date"])[:10] for t in ordered]
        evidence.append({
            "segment": idx,
            "transaction_ids": [t.get("id") for t in ordered if t.get("id") is not None],
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
            "count": len(ordered),
            "avg_amount": round(mean(amounts), 2) if amounts else 0,
            "median_amount": round(median(amounts), 2) if amounts else 0,
            "amount_min": round(min(amounts), 2) if amounts else 0,
            "amount_max": round(max(amounts), 2) if amounts else 0,
        })
    return evidence


def _transaction_evidence(group_txns: list[dict], segments: list[list[dict]] | None = None) -> dict:
    ordered = sorted(group_txns, key=lambda t: t["date"])
    dates = [str(t["date"])[:10] for t in ordered]
    parsed_dates = _parse_dates(dates)
    return {
        "transaction_ids": [t.get("id") for t in ordered if t.get("id") is not None],
        "dates": dates,
        "amounts": [round(float(t["amount"]), 2) for t in ordered],
        "intervals": [
            (parsed_dates[i + 1] - parsed_dates[i]).days
            for i in range(len(parsed_dates) - 1)
        ],
        "price_segments": _segment_evidence(segments or [ordered]),
    }


def _result_evidence_fields(result: dict | None) -> dict:
    if not result:
        return {}
    keys = (
        "transaction_ids", "dates", "amounts", "intervals", "price_segments",
        "segment_amounts", "segment_dates", "segment_count", "segment_size",
        "price_changed",
    )
    return {key: result[key] for key in keys if key in result}


# ══════════════════════════════════════════════════════════════════════════════
# PRICE SEGMENTATION
# ══════════════════════════════════════════════════════════════════════════════

def _segment_by_price(
    group_txns: list[dict],
    segment_cv_threshold: float = 0.05,
    min_segment_size: int = 2,
    credit_filter_pct: float = 0.25,
    credit_filter_floor: float = 1.00,
) -> list[list[dict]]:
    """
    Split a transaction group into price-stable segments.

    Steps:
      1. Sort chronologically.
      2. Filter out micro-credits/adjustments (< 25% of median amount or < $1).
      3. Walk through; start a new segment when adding the next txn would push
         the segment's CV above segment_cv_threshold (0.05).
      4. Return a list of segments (each is a list of txn dicts), ordered
         chronologically (latest segment is [-1]).

    If filtering removes too many transactions, falls back to unfiltered.
    If no valid segments are found, returns the full (filtered) list as one segment.
    """
    if len(group_txns) < 2:
        return [group_txns]

    # Sort by date ascending
    sorted_txns = sorted(group_txns, key=lambda t: t["date"])

    # Step 1: Compute median amount for credit filtering
    all_amounts = [t["amount"] for t in sorted_txns]
    med_amount = median(all_amounts)
    filter_threshold = max(med_amount * credit_filter_pct, credit_filter_floor)

    # Step 2: Filter out micro-credits/adjustments
    filtered = [t for t in sorted_txns if t["amount"] >= filter_threshold]

    # Fallback: if filtering removed too many, use unfiltered
    if len(filtered) < min_segment_size:
        filtered = sorted_txns

    if len(filtered) < min_segment_size:
        return [filtered]

    # Step 3: Walk through and segment at price changes
    segments: list[list[dict]] = []
    current_segment: list[dict] = [filtered[0]]

    for i in range(1, len(filtered)):
        txn = filtered[i]
        # Test if adding this txn keeps the segment consistent
        test_amounts = [t["amount"] for t in current_segment] + [txn["amount"]]

        if len(test_amounts) >= 2:
            test_mean = mean(test_amounts)
            test_sd = stdev(test_amounts)
            test_cv = test_sd / test_mean if test_mean > 0 else 0.0
        else:
            test_cv = 0.0

        if test_cv <= segment_cv_threshold:
            current_segment.append(txn)
        else:
            # Price change: save current segment if large enough, start new
            if len(current_segment) >= min_segment_size:
                segments.append(current_segment)
            current_segment = [txn]

    # Save the final segment
    if len(current_segment) >= min_segment_size:
        segments.append(current_segment)

    # Fallback: if no valid segments, treat everything as one
    if not segments:
        segments = [filtered]

    return segments


# ══════════════════════════════════════════════════════════════════════════════
# GROUP EVALUATION (unified logic for all three layers)
# ══════════════════════════════════════════════════════════════════════════════

def _evaluate_group(
    group_txns: list[dict],
    seed_freq_hint: str | None,
    matched_by: str,
    cv_threshold: float,
    date_tolerance: int,
    today,
) -> dict | None:
    """
    Evaluate a merchant's transaction group for recurring patterns.

    This is the unified evaluation function called by all three layers.
    It handles:
      - Price segmentation (evaluate latest stable segment)
      - CV check on the segmented amounts
      - Frequency detection on the segmented dates
      - Date consistency check
      - Confidence scoring
      - Active/inactive status
      - Price change detection across the full history

    Returns a result dict with all computed fields, or None if the group
    fails validation (not recurring).
    """
    if not group_txns:
        return None

    ordered_group = sorted(group_txns, key=lambda t: t["date"])
    full_amounts = [t["amount"] for t in ordered_group]
    full_dates = sorted(_parse_dates([t["date"] for t in ordered_group]))
    n_total = len(group_txns)

    if not full_dates:
        return None

    # ── Price segmentation ──
    segments = _segment_by_price(group_txns)
    latest_segment = segments[-1]  # Most recent stable price block
    n_segment = len(latest_segment)

    seg_amounts = [t["amount"] for t in latest_segment]
    seg_dates = sorted(_parse_dates([t["date"] for t in latest_segment]))

    if not seg_dates:
        return None

    # ── CV check on the latest segment ──
    avg_amt = mean(seg_amounts)
    if avg_amt <= 0:
        return None

    if n_segment >= 2:
        sd = stdev(seg_amounts)
        cv = sd / avg_amt
        if cv > cv_threshold:
            return None

    # ── Frequency detection on the latest segment ──
    # For single-occurrence segments, rely on seed hint or full-history detection
    if n_segment >= 2:
        detected_freq = _detect_frequency(seg_dates, seed_freq_hint)
    else:
        # Single txn in latest segment — try full history or seed hint
        detected_freq = _detect_frequency(full_dates, seed_freq_hint) if len(full_dates) >= 2 else None

    # For seed-matched single occurrence, use the hint
    if detected_freq is None and matched_by == "seed" and n_total == 1:
        detected_freq = _normalise_seed_frequency(seed_freq_hint)

    if detected_freq is None:
        return None

    # ── Date consistency on the latest segment ──
    if n_segment >= 2:
        if not _check_date_consistency(seg_dates, detected_freq, date_tolerance):
            return None

    # ── Active/inactive status ──
    nominal, grace, _, _ = FREQUENCY_DEFS.get(detected_freq, FREQUENCY_DEFS["monthly"])
    last_date = full_dates[-1]  # Use full history for recency
    is_active = (today - last_date).days <= (nominal + grace)
    next_expected = advance_recurring_date(last_date, detected_freq) if is_active else None

    # ── Confidence scoring (on the segmented data) ──
    confidence_level = _compute_confidence(
        occurrences=n_segment,
        amounts=seg_amounts,
        dates_sorted=seg_dates,
        frequency=detected_freq,
        matched_by=matched_by,
        date_tolerance=date_tolerance,
    )

    # ── Price change detection (across full history) ──
    price_change = _detect_price_change(group_txns)
    price_changed = len(segments) > 1

    # ── Distinct months across full history ──
    distinct_months = len({d.strftime("%Y-%m") for d in full_dates})

    # ── Period ratio check (anti-noise for algorithmic detection) ──
    if matched_by == "algorithm" and n_segment >= 2:
        span_days = max((seg_dates[-1] - seg_dates[0]).days, 1)
        expected_periods = span_days / nominal
        if expected_periods > 0 and (n_segment / expected_periods) > 1.35:
            return None

    return {
        "avg_amount":         round(avg_amt, 2),
        "frequency":          detected_freq,
        "occurrences":        n_total,
        "confidence":         confidence_level,
        "status":             "active" if is_active else "inactive",
        "last_date":          last_date.isoformat(),
        "next_expected_date": next_expected.isoformat() if next_expected else None,
        "months_paid":        distinct_months,
        "annual_cost":        round(_annualize(avg_amt, detected_freq), 2),
        "price_change":       price_change,
        "price_changed":      price_changed,
        "segment_count":      len(segments),
        "segment_size":       n_segment,
        "segment_amounts":    [round(float(t["amount"]), 2) for t in latest_segment],
        "segment_dates":      [str(t["date"])[:10] for t in sorted(latest_segment, key=lambda t: t["date"])],
        **_transaction_evidence(ordered_group, segments),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SEED LOADING WITH TTL CACHE
# ══════════════════════════════════════════════════════════════════════════════

_seeds_cache_store: dict[str, tuple[float, list[dict], set[str]]] = {}
_SEEDS_TTL_SECONDS = 60


def _load_user_declared_subscriptions(get_db_conn, profile: str | None = None) -> list[dict]:
    """
    Load user-declared subscriptions from the database.
    These form Layer 0 — always included, no mathematical validation.
    """
    with get_db_conn() as conn:
        if profile and profile != "household":
            rows = conn.execute(
                """SELECT merchant_name, amount, frequency, profile_id, created_at
                   FROM user_declared_subscriptions
                   WHERE is_active = 1 AND profile_id = ?""",
                (profile,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT merchant_name, amount, frequency, profile_id, created_at
                   FROM user_declared_subscriptions
                   WHERE is_active = 1"""
            ).fetchall()

    return [
        {
            "merchant_name": row[0],
            "amount": row[1],
            "frequency": row[2],
            "profile_id": row[3],
            "created_at": row[4],
        }
        for row in rows
    ]


def _load_dismissed_merchants(get_db_conn, profile: str | None = None) -> set[str]:
    """Load dismissed merchant names for filtering."""
    with get_db_conn() as conn:
        if profile and profile != "household":
            rows = conn.execute(
                "SELECT merchant_name, profile_id FROM dismissed_recurring WHERE profile_id = ?",
                (profile,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT merchant_name, profile_id FROM dismissed_recurring"
            ).fetchall()
    dismissed: set[str] = set()
    scoped_only = not (profile and profile != "household")
    for row in rows:
        profile_id = row[1] or "household"
        for key in merchant_match_keys(row[0]):
            dismissed.add(f"{profile_id}::{key}")
            if not scoped_only:
                dismissed.add(key)
    return dismissed


def _load_cancelled_merchants(get_db_conn, profile: str | None = None) -> dict[str, str]:
    """Load cancelled merchant names and their cancellation dates from v2 first."""
    with get_db_conn() as conn:
        if profile and profile != "household":
            v2_rows = conn.execute(
                """SELECT merchant_key, COALESCE(last_user_action_at, updated_at), profile_id
                   FROM recurring_obligations
                   WHERE state = 'cancelled' AND profile_id = ?""",
                (profile,),
            ).fetchall()
            legacy_rows = conn.execute(
                """SELECT merchant_key, cancelled_at, profile_id
                   FROM merchants
                   WHERE cancelled_by_user = 1 AND profile_id = ?""",
                (profile,),
            ).fetchall()
        else:
            v2_rows = conn.execute(
                """SELECT merchant_key, COALESCE(last_user_action_at, updated_at), profile_id
                   FROM recurring_obligations
                   WHERE state = 'cancelled'"""
            ).fetchall()
            legacy_rows = conn.execute(
                """SELECT merchant_key, cancelled_at, profile_id
                   FROM merchants
                   WHERE cancelled_by_user = 1"""
            ).fetchall()
    cancelled: dict[str, str] = {}
    scoped_only = not (profile and profile != "household")

    def store_cancelled(row, *, overwrite: bool) -> None:
        profile_id = row[2] or "household"
        for key in merchant_match_keys(row[0]):
            scoped_key = f"{profile_id}::{key}"
            if overwrite or scoped_key not in cancelled:
                cancelled[scoped_key] = row[1]
            if not scoped_only and (overwrite or key not in cancelled):
                cancelled[key] = row[1]

    # Legacy rows fill gaps only; v2 obligations are the authority.
    for row in legacy_rows:
        store_cancelled(row, overwrite=False)
    for row in v2_rows:
        store_cancelled(row, overwrite=True)
    return cancelled


def _load_merchants_state(get_db_conn, profile: str | None = None) -> dict[str, dict]:
    """
    Load current recurring state for event comparison.

    The v2 obligation model is authoritative. Legacy merchant subscription
    columns are read only as a compatibility fallback for pre-v2 rows.
    Returns {merchant_key: {subscription_amount, subscription_status, ...}}.
    """
    with get_db_conn() as conn:
        if profile and profile != "household":
            obligation_rows = conn.execute(
                """SELECT merchant_key, display_name, amount_cents, state,
                          frequency, source, profile_id
                   FROM recurring_obligations
                   WHERE profile_id = ?""",
                (profile,),
            ).fetchall()
            legacy_rows = conn.execute(
                """SELECT merchant_key, clean_name, subscription_amount,
                          subscription_status, subscription_frequency,
                          is_subscription, cancelled_by_user,
                          profile_id
                   FROM merchants WHERE profile_id = ?""",
                (profile,),
            ).fetchall()
        else:
            obligation_rows = conn.execute(
                """SELECT merchant_key, display_name, amount_cents, state,
                          frequency, source, profile_id
                   FROM recurring_obligations"""
            ).fetchall()
            legacy_rows = conn.execute(
                """SELECT merchant_key, clean_name, subscription_amount,
                          subscription_status, subscription_frequency,
                          is_subscription, cancelled_by_user, profile_id
                   FROM merchants"""
            ).fetchall()

    result = {}
    scoped_only = not (profile and profile != "household")

    def store_state(row_profile: str, keys: set[str], state: dict, *, overwrite: bool) -> None:
        for key in keys:
            scoped_key = f"{row_profile}::{key}"
            if overwrite or scoped_key not in result:
                result[scoped_key] = state
            if not scoped_only and (overwrite or key not in result):
                result[key] = state

    for row in legacy_rows:
        state = {
            "merchant_key": row[0],
            "clean_name": row[1],
            "subscription_amount": row[2],
            "subscription_status": row[3],
            "subscription_frequency": row[4],
            "is_subscription": bool(row[5]),
            "cancelled_by_user": bool(row[6]),
        }
        profile_id = row[7] or "household"
        store_state(profile_id, merchant_match_keys(row[0]) | merchant_match_keys(row[1]), state, overwrite=False)

    for row in obligation_rows:
        obligation_state = row[3] or "candidate"
        subscription_status = "active" if obligation_state == "confirmed" else obligation_state
        state = {
            "merchant_key": row[0],
            "clean_name": row[1],
            "subscription_amount": round(float(row[2] or 0) / 100.0, 2),
            "subscription_status": subscription_status,
            "subscription_frequency": row[4],
            "is_subscription": obligation_state != "dismissed",
            "cancelled_by_user": obligation_state == "cancelled",
        }
        profile_id = row[6] or "household"
        store_state(profile_id, merchant_match_keys(row[0]) | merchant_match_keys(row[1]), state, overwrite=True)
    return result


def _load_seeds_cached(get_db_conn, profile: str | None = None) -> tuple[list[dict], set[str]]:
    """
    Load subscription seeds from the database with a 60-second TTL cache.
    Respects is_active flag and user suppressions per profile.
    """
    cache_key = profile or "household"
    now = time.time()

    if cache_key in _seeds_cache_store:
        cached_time, cached_seeds, cached_suppressed = _seeds_cache_store[cache_key]
        if now - cached_time < _SEEDS_TTL_SECONDS:
            return cached_seeds, cached_suppressed

    created_by = profile or "household"

    with get_db_conn() as conn:
        suppressed_rows = conn.execute(
            """SELECT pattern FROM subscription_seeds
               WHERE source = 'user' AND is_active = 0
               AND (created_by = ? OR created_by = 'household')""",
            (created_by,),
        ).fetchall()
        suppressed_patterns = {row[0] for row in suppressed_rows}

        rows = conn.execute(
            """SELECT name, pattern, frequency_hint, category, source
               FROM subscription_seeds
               WHERE is_active = 1
                 AND (
                     source = 'system'
                     OR created_by IS NULL
                     OR created_by = ?
                     OR created_by = 'household'
                 )
               ORDER BY source DESC, length(pattern) DESC, id ASC""",
            (created_by,),
        ).fetchall()

    seeds = []
    for row in rows:
        pattern = row[1]
        if len(pattern) < 6:
            compiled = re.compile(r'\b' + re.escape(pattern) + r'\b')
        else:
            compiled = None
        seeds.append({
            "name": row[0],
            "pattern": pattern,
            "frequency_hint": row[2],
            "category": row[3],
            "source": row[4],
            "_compiled": compiled,
        })

    _seeds_cache_store[cache_key] = (now, seeds, suppressed_patterns)
    return seeds, suppressed_patterns


def _match_seed(
    merchant_name: str,
    description: str,
    seeds_cache: list[dict],
    suppressed_cache: set[str],
):
    """Match a single transaction's merchant/description against seed patterns."""
    text_upper = f"{merchant_name} {description}".upper()

    for seed in seeds_cache:
        pattern = seed["pattern"]
        if pattern in suppressed_cache:
            continue

        if seed["_compiled"] is not None:
            if not seed["_compiled"].search(text_upper):
                continue
        else:
            if pattern not in text_upper:
                continue

        seed_dict = {
            "name": seed["name"],
            "frequency_hint": seed["frequency_hint"],
            "category": seed["category"],
            "source": seed["source"],
        }
        return seed_dict, pattern

    return None, None


def _match_seed_from_group(
    group_txns: list[dict],
    seeds_cache: list[dict],
    suppressed_cache: set[str],
):
    """
    Try to match a seed against all unique (merchant_name, description) pairs
    in the group. Returns (seed_dict, pattern) or (None, None).
    """
    seen_texts = set()
    for t in group_txns:
        merchant = t.get("merchant_name", "")
        desc = t.get("description", "")
        text_key = f"{merchant}|{desc}"
        if text_key in seen_texts:
            continue
        seen_texts.add(text_key)

        seed, pat = _match_seed(merchant, desc, seeds_cache, suppressed_cache)
        if seed is not None:
            return seed, pat

    return None, None

# ══════════════════════════════════════════════════════════════════════════════
# EVENT GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def _generate_subscription_events(
    current_items: list[dict],
    previous_state: dict[str, dict],
    cancelled_merchants: dict[str, str],
    profile: str | None = None,
) -> list[dict]:
    """
    Compare current detection results against previous merchant state.
    Generate event dicts for: new_detected, price_increase, price_decrease,
    gone_inactive, zombie_charge.

    Args:
        current_items: list of recurring item dicts from detect()
        previous_state: {merchant_key: {...}} from _load_merchants_state
        cancelled_merchants: {merchant_key: cancelled_at} from _load_cancelled_merchants
        profile: profile_id for event records

    Returns:
        List of event dicts ready for insertion into subscription_events.
    """
    import json as _json

    events = []
    profile_id = profile or "household"
    seen_merchants = set()

    for item in current_items:
        merchant = item.get("merchant", "")
        item_profile = item.get("profile") or profile_id
        merchant_key = recurring_canonical_key(merchant)
        scoped_key = f"{item_profile}::{merchant_key}"
        seen_merchants.add(scoped_key)
        prev = previous_state.get(scoped_key) or previous_state.get(merchant_key)

        # Zombie detection: charge from a cancelled merchant
        cancelled_at = cancelled_merchants.get(scoped_key) or cancelled_merchants.get(merchant_key)
        if cancelled_at and item.get("status") == "active":
            events.append({
                "event_type": "zombie_charge",
                "merchant_name": merchant,
                "profile_id": item_profile,
                "detail": _json.dumps({
                    "cancelled_at": cancelled_at,
                    "new_charge_amount": item.get("avg_amount"),
                    "new_charge_date": item.get("last_date"),
                }),
            })
            continue

        if prev is None or not prev.get("is_subscription"):
            # New subscription detected
            events.append({
                "event_type": "new_detected",
                "merchant_name": merchant,
                "profile_id": item_profile,
                "detail": _json.dumps({
                    "amount": item.get("avg_amount"),
                    "frequency": item.get("frequency"),
                    "confidence": item.get("confidence"),
                }),
            })
        else:
            # Existing subscription — check for price changes
            prev_amount = prev.get("subscription_amount")
            curr_amount = item.get("avg_amount")
            if prev_amount is not None and curr_amount is not None:
                change = round(curr_amount - prev_amount, 2)
                if change >= 0.50:
                    events.append({
                        "event_type": "price_increase",
                        "merchant_name": merchant,
                        "profile_id": item_profile,
                        "detail": _json.dumps({
                            "old_amount": prev_amount,
                            "new_amount": curr_amount,
                            "change": change,
                            "latest_date": item.get("last_date"),
                        }),
                    })
                elif change <= -0.50:
                    events.append({
                        "event_type": "price_decrease",
                        "merchant_name": merchant,
                        "profile_id": item_profile,
                        "detail": _json.dumps({
                            "old_amount": prev_amount,
                            "new_amount": curr_amount,
                            "change": change,
                            "latest_date": item.get("last_date"),
                        }),
                    })

            # Check for status change: was active, now inactive
            prev_status = prev.get("subscription_status")
            curr_status = item.get("status")
            if prev_status == "active" and curr_status == "inactive":
                events.append({
                    "event_type": "gone_inactive",
                    "merchant_name": merchant,
                    "profile_id": item_profile,
                    "detail": _json.dumps({
                        "last_charge_date": item.get("last_date"),
                        "amount": curr_amount,
                    }),
                })

    return events

# ══════════════════════════════════════════════════════════════════════════════
# RECURRING DETECTOR CLASS
# ══════════════════════════════════════════════════════════════════════════════

class RecurringDetector:

    ALGO_MIN_CHARGES         = 2
    ALGO_CV_THRESHOLD        = 0.20
    MAX_TXN_PER_PERIOD_RATIO = 1.35
    DATE_TOLERANCE_DAYS      = 2

    # Categories excluded from algorithmic (Layer 2) and category (Layer 3) detection.
    # Layer 1 (seeds) can bypass this for whitelisted seed categories.
    ALGO_EXCLUDED_CATEGORIES = {
        # Non-spending
        "Savings Transfer", "Credit Card Payment",
        "Income", "Personal Transfer", "Internal Transfer",
        "Tax Payment", "Tax Refund", "Refund",
        # Groceries & dining
        "Groceries", "Food & Dining",
        # Travel & transport
        "Transportation", "Shopping", "Travel",
        # Utilities (parent + specific)
        "Utilities", "Electric", "Gas", "Water",
        # Insurance (parent + specific)
        "Insurance", "Auto Insurance", "Home Insurance",
        "Health Insurance", "Renters Insurance", "Life Insurance",
        # Connectivity
        "Internet", "Wireless", "Cable",
        # Housing
        "Housing", "Rent Payment", "Mortgage",
        # Healthcare (parent + specific)
        "Healthcare", "Therapy", "Telehealth", "Pharmacy",
        # Other non-subscription recurring
        "Taxes",
    }

    # Category-backed recurring bills that should still be eligible after the
    # hard false-positive gates above. These still must pass cadence/amount
    # validation in _evaluate_group; this does not make arbitrary insurance
    # transactions recurring by itself.
    RECURRING_BILL_CATEGORIES = {"Subscriptions", "Insurance"}

    _DISQUALIFY_TOKENS = {
        "ATM", "CASH", "CHECK", "WITHDRAWAL", "DEPOSIT",
        "REFUND", "CREDIT", "REVERSAL", "TRANSFER",
        "PAYMENT THANK YOU", "OVERDRAFT", "NSF",
    }

    def __init__(self, get_db_conn):
        self._get_db_conn = get_db_conn

    # ── Main entry point ──────────────────────────────────────────────────────
    def detect(
        self,
        transactions: list[dict],
        profile: str | None = None,
        merchant_keys: set[str] | None = None,
        generate_events: bool = False,
        today=None,
    ) -> dict:
        """
        Detect recurring charges in a list of transactions.

        Args:
            transactions: list of transaction dicts
            profile: profile filter
            merchant_keys: if provided, only process groups matching these keys
                           (incremental mode). User declarations always included.
            generate_events: if True, compare against stored merchant state and
                             return events list.

        Returns:
            dict with keys: items, count, total_monthly, total_annual, events
        """
        TODAY = today or datetime.now().date()

        # Load dismissed merchants for filtering
        dismissed_merchants = _load_dismissed_merchants(self._get_db_conn, profile)

        # Load previous merchant state for event generation
        previous_state = {}
        cancelled_merchants = {}
        if generate_events:
            previous_state = _load_merchants_state(self._get_db_conn, profile)
            cancelled_merchants = _load_cancelled_merchants(self._get_db_conn, profile)

        expense_txns = []
        for t in transactions:
            if not self._is_expense(t):
                continue
            date_str = t.get("date", "")
            if len(date_str) < 10:
                continue
            desc_upper = (t.get("description") or "").upper()
            if any(token in desc_upper for token in self._DISQUALIFY_TOKENS):
                continue
            # Skip internal transfers — recurring savings transfers etc.
            # should not be detected as subscriptions.
            if t.get("expense_type") == "transfer_internal":
                continue
            expense_txns.append(t)

        recurring: list[dict] = []
        seen_merchants: set[str] = set()

        # Layer 0: User-declared subscriptions (always included)
        user_declared = _load_user_declared_subscriptions(self._get_db_conn, profile)
        for decl in user_declared:
            merchant_name = decl["merchant_name"]
            decl_profile = decl.get("profile_id") or profile or "household"
            if _is_dismissed_match(dismissed_merchants, merchant_name, decl_profile):
                continue
            freq = decl["frequency"]
            amt = decl["amount"]

            recurring.append({
                "merchant":           merchant_name,
                "avg_amount":         round(amt, 2),
                "frequency":          freq,
                "occurrences":        0,
                "category":           "Subscriptions",
                "confidence":         "user",
                "is_subscription":    True,
                "status":             "active",
                "last_date":          None,
                "next_expected_date": None,
                "months_paid":        0,
                "matched_by":         "user",
                "annual_cost":        round(_annualize(amt, freq), 2),
                "price_change":       None,
                "profile":            decl_profile,
            })
            seen_merchants.add(_profile_scope_key(merchant_name, decl_profile))

        if not expense_txns:
            return {
                "items": recurring,
                "count": len(recurring),
                "total_monthly": sum(r["annual_cost"] / 12 for r in recurring if r["status"] == "active"),
                "total_annual": sum(r["annual_cost"] for r in recurring if r["status"] == "active"),
                "events": [],
            }

        merchant_groups, display_names = self._group_by_merchant(expense_txns)

        # Incremental mode: filter to only affected merchant groups
        if merchant_keys is not None:
            filtered_groups = {}
            filtered_names = {}
            for key in merchant_groups:
                base_key = key.split("::", 1)[-1]
                merchant_keys_upper = {mk.upper() for mk in merchant_keys}
                if key in merchant_keys or key.upper() in merchant_keys_upper or base_key.upper() in merchant_keys_upper:
                    filtered_groups[key] = merchant_groups[key]
                    filtered_names[key] = display_names.get(key, key)
            merchant_groups = filtered_groups
            display_names = filtered_names

        seeds_cache, suppressed_cache = _load_seeds_cached(self._get_db_conn, profile)

        self._merge_seed_groups(merchant_groups, display_names, seeds_cache, suppressed_cache)

        # Layer 1: Seed matching
        self._match_seeds(
            merchant_groups, display_names, seeds_cache, suppressed_cache,
            recurring, seen_merchants, TODAY,
        )

        # Layer 2: Algorithmic detection
        self._detect_algorithmically(
            merchant_groups, display_names,
            recurring, seen_merchants, TODAY,
        )

        # Layer 3: Category-based detection
        self._detect_by_category(
            merchant_groups, display_names,
            recurring, seen_merchants, TODAY,
        )

        # Filter out dismissed merchants (Layer 0 already filtered above)
        recurring = [
            r for r in recurring
            if not _is_dismissed_match(dismissed_merchants, r.get("merchant", ""), r.get("profile") or profile)
               and r.get("matched_by") != "dismissed"
        ]

        # Update Layer 0 items with real transaction data if available
        for r in recurring:
            if r.get("matched_by") == "user" and r.get("occurrences", 0) == 0:
                # Look for matching transactions to get last_date, etc.
                merchant_upper = r["merchant"].upper().strip()
                for key, group_txns in merchant_groups.items():
                    if key.upper().strip() == merchant_upper or display_names.get(key, "").upper().strip() == merchant_upper:
                        if group_txns:
                            dates = sorted(_parse_dates([t["date"] for t in group_txns]))
                            if dates:
                                r["last_date"] = dates[-1].isoformat()
                                r["occurrences"] = len(group_txns)
                                r["months_paid"] = len({d.strftime("%Y-%m") for d in dates})
                                freq = r["frequency"]
                                nominal = FREQUENCY_DEFS.get(freq, FREQUENCY_DEFS["monthly"])[0]
                                grace = FREQUENCY_DEFS.get(freq, FREQUENCY_DEFS["monthly"])[1]
                                is_active = (TODAY - dates[-1]).days <= (nominal + grace)
                                r["status"] = "active" if is_active else "inactive"
                                if is_active:
                                    r["next_expected_date"] = advance_recurring_date(dates[-1], freq).isoformat()
                        break

        recurring.sort(
            key=lambda x: (
                0 if x.get("confidence") == "user" else (1 if x["status"] == "active" else 2),
                -x["annual_cost"],
            )
        )

        total_monthly = 0.0
        total_annual = 0.0
        for r in recurring:
            if r["status"] != "active":
                continue
            total_annual += r["annual_cost"]
            total_monthly += r["annual_cost"] / 12

        # Generate events if requested
        events = []
        if generate_events:
            events = _generate_subscription_events(
                current_items=recurring,
                previous_state=previous_state,
                cancelled_merchants=cancelled_merchants,
                profile=profile,
            )

        return {
            "items":         recurring,
            "count":         len(recurring),
            "total_monthly": round(total_monthly, 2),
            "total_annual":  round(total_annual, 2),
            "events":        events,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _is_expense(tx: dict) -> bool:
        amount = float(tx.get("amount", 0))
        cat = tx.get("category", "Other")
        return amount < 0 and cat not in NON_SPENDING_CATEGORIES

    def _group_by_merchant(
        self, expense_txns: list[dict]
    ) -> tuple[dict[str, list[dict]], dict[str, str]]:
        """
        Group transactions by normalized merchant key.
        Prefers explicit merchant_key and uses legacy description extraction
        only as a compatibility fallback for unkeyed historical rows.
        """
        merchant_groups: dict[str, list[dict]] = defaultdict(list)
        display_names: dict[str, str] = {}

        for t in expense_txns:
            raw_merchant = (t.get("merchant_name") or "").strip()
            raw_key = canonicalize_merchant_key(t.get("merchant_key") or "")
            raw_desc = (t.get("description") or "").strip()
            profile_id = t.get("profile_id") or t.get("profile") or ""

            extracted = _extract_merchant_pattern(raw_desc)

            if raw_key:
                key = raw_key
            elif raw_merchant:
                clean = raw_merchant.upper().strip()
                clean = re.sub(r'\.(COM|NET|ORG|IO|CO|AI|TV|FM|APP|ME|US|UK)(\/\S*)?', '', clean)
                clean = re.sub(r',?\s*\b(INC\.?|LLC\.?|L\.?L\.?C\.?|LTD\.?|CORP\.?|CO\.?|INCORPORATED|CORPORATION|S\.?A\.?)\b\.?', '', clean)
                clean = clean.strip(' .,;-')
                key = canonicalize_merchant_key(clean) or (extracted or raw_desc.upper() or "UNKNOWN")
            else:
                key = extracted if extracted else (raw_desc.upper() or "UNKNOWN")

            group_key = f"{profile_id}::{key}" if profile_id else key

            if group_key not in display_names or raw_merchant:
                display_names[group_key] = raw_merchant if raw_merchant else raw_desc
            merchant_groups[group_key].append({
                "id":            t.get("id"),
                "amount":        abs(float(t.get("amount", 0))),
                "date":          t["date"][:10],
                "category":      t.get("category", "Other"),
                "description":   raw_desc,
                "merchant_name": raw_merchant,
                "merchant_key": raw_key,
                "profile_id":    profile_id or None,
            })

        return merchant_groups, display_names

    def _merge_seed_groups(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        seeds_cache: list[dict],
        suppressed_cache: set[str],
    ):
        """
        Split-and-merge: when a seed matches SOME transactions in a broad
        merchant group (e.g., "AMAZON"), extract the matching transactions
        into their own group keyed by the seed name, leaving the rest behind.

        Then merge any groups that map to the same seed name.
        """
        # Phase 1: Split — extract seed-matching txns from broad groups
        splits_to_apply: list[tuple[str, str, list[dict], list[dict]]] = []

        for key in list(merchant_groups.keys()):
            group_txns = merchant_groups[key]
            if not group_txns:
                continue

            seed, matched_pattern = _match_seed_from_group(
                group_txns, seeds_cache, suppressed_cache,
            )
            if seed is None:
                continue

            seed_name = seed["name"]

            # Check each transaction individually — does it match the seed?
            matching_txns = []
            remaining_txns = []

            for t in group_txns:
                t_seed, _ = _match_seed(
                    t.get("merchant_name", ""),
                    t.get("description", ""),
                    seeds_cache,
                    suppressed_cache,
                )
                if t_seed is not None and t_seed["name"] == seed_name:
                    matching_txns.append(t)
                else:
                    remaining_txns.append(t)

            # Only split if there's a meaningful separation
            if matching_txns and remaining_txns:
                splits_to_apply.append((key, seed_name, matching_txns, remaining_txns))
            elif matching_txns and not remaining_txns:
                # Entire group matches — just rename
                display_names[key] = seed_name

        # Apply splits
        for original_key, seed_name, matching, remaining in splits_to_apply:
            # Create a new group for the seed-matching transactions
            seed_profile = _profile_from_group(matching, "")
            seed_key = f"{seed_profile}::__seed__{seed_name}" if seed_profile else f"__seed__{seed_name}"
            if seed_key in merchant_groups:
                # Another group already claimed this seed — merge into it
                merchant_groups[seed_key].extend(matching)
            else:
                merchant_groups[seed_key] = matching
                display_names[seed_key] = seed_name

            # Update the original group to only contain non-matching txns
            if remaining:
                merchant_groups[original_key] = remaining
            else:
                del merchant_groups[original_key]

        # Phase 2: Merge — unify groups that map to the same seed
        seed_merge_map: dict[str, str] = {}
        merge_targets: dict[str, str] = {}

        for key in list(merchant_groups.keys()):
            group_txns = merchant_groups[key]
            seed, _ = _match_seed_from_group(
                group_txns, seeds_cache, suppressed_cache,
            )
            if seed:
                seed_name = seed["name"]
                seed_profile = _profile_from_group(group_txns, "")
                seed_merge_id = f"{seed_profile}::{seed_name}" if seed_profile else seed_name
                if seed_merge_id in seed_merge_map:
                    existing_key = seed_merge_map[seed_merge_id]
                    if key != existing_key:
                        merge_targets[key] = existing_key
                else:
                    seed_merge_map[seed_merge_id] = key
                    display_names[key] = seed_name

        for src_key, dst_key in merge_targets.items():
            if src_key in merchant_groups and dst_key in merchant_groups:
                merchant_groups[dst_key].extend(merchant_groups[src_key])
                del merchant_groups[src_key]

    # ── Layer 1: Seed matching ────────────────────────────────────────────────
    def _match_seeds(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        seeds_cache: list[dict],
        suppressed_cache: set[str],
        recurring: list[dict],
        seen_merchants: set[str],
        today,
    ):
        """
        Layer 1: Match merchant groups against known subscription seeds.

        Category exclusion behavior:
        - If the seed's own category is in _SEED_WHITELISTED_CATEGORIES,
          the match is ALLOWED even if the transaction's LLM-assigned category
          is in ALGO_EXCLUDED_CATEGORIES. This lets "Amazon Prime" (seed category:
          "Membership") survive even when the LLM tagged the transaction as "Shopping".
        - If the seed's category is NOT whitelisted, normal exclusion applies.
        """
        for merchant_key, group_txns in merchant_groups.items():
            if not group_txns:
                continue
            item_profile = _profile_from_group(group_txns)

            seed, _pat = _match_seed_from_group(
                group_txns, seeds_cache, suppressed_cache,
            )
            if seed is None:
                continue

            # Category exclusion with seed whitelist override
            txn_category = (group_txns[0].get("category") or "").strip()
            seed_category = (seed.get("category") or "").strip()

            if txn_category in self.ALGO_EXCLUDED_CATEGORIES:
                # Allow through if the seed's own category is whitelisted
                if seed_category not in _SEED_WHITELISTED_CATEGORIES:
                    continue

            n = len(group_txns)

            # Single occurrence: accept with low confidence, use seed hint
            if n == 1:
                detected_freq = _normalise_seed_frequency(
                    seed.get("frequency_hint", "monthly")
                )
                amounts = [group_txns[0]["amount"]]
                dates_sorted = sorted(_parse_dates([group_txns[0]["date"]]))

                if not dates_sorted:
                    continue

                nominal, grace, _, _ = FREQUENCY_DEFS.get(
                    detected_freq, FREQUENCY_DEFS["monthly"]
                )
                last_date = dates_sorted[-1]
                is_active = (today - last_date).days <= (nominal + grace)
                next_expected = advance_recurring_date(last_date, detected_freq) if is_active else None

                recurring.append({
                    "merchant":           seed.get("name", merchant_key),
                    "avg_amount":         round(amounts[0], 2),
                    "frequency":          detected_freq,
                    "occurrences":        1,
                    "category":           seed_category if seed_category else txn_category,
                    "confidence":         "low",
                    "is_subscription":    True,
                    "status":             "candidate" if is_active else "inactive",
                    "last_date":          last_date.isoformat(),
                    "next_expected_date": next_expected.isoformat() if next_expected else None,
                    "months_paid":        1,
                    "matched_by":         "seed",
                    "annual_cost":        round(_annualize(amounts[0], detected_freq), 2),
                    "price_change":       None,
                    "profile":            item_profile,
                    "seed_pattern":        _pat,
                    **_transaction_evidence(group_txns),
                })
                seen_merchants.add(merchant_key)
                continue

            # 2+ occurrences: full evaluation with segmentation
            result = _evaluate_group(
                group_txns=group_txns,
                seed_freq_hint=seed.get("frequency_hint"),
                matched_by="seed",
                cv_threshold=self.ALGO_CV_THRESHOLD,
                date_tolerance=self.DATE_TOLERANCE_DAYS,
                today=today,
            )

            if result is None:
                # Segmented evaluation failed — still include as low confidence
                # since the seed matched (user should see it)
                amounts = [t["amount"] for t in group_txns]
                dates_sorted = sorted(_parse_dates([t["date"] for t in group_txns]))
                if not dates_sorted:
                    continue

                avg_amt = mean(amounts)
                detected_freq = _normalise_seed_frequency(
                    seed.get("frequency_hint", "monthly")
                )
                nominal, grace, _, _ = FREQUENCY_DEFS.get(
                    detected_freq, FREQUENCY_DEFS["monthly"]
                )
                last_date = dates_sorted[-1]
                is_active = (today - last_date).days <= (nominal + grace)
                next_expected = advance_recurring_date(last_date, detected_freq) if is_active else None
                distinct_months = len({d.strftime("%Y-%m") for d in dates_sorted})

                recurring.append({
                    "merchant":           seed.get("name", merchant_key),
                    "avg_amount":         round(avg_amt, 2),
                    "frequency":          detected_freq,
                    "occurrences":        n,
                    "category":           seed_category if seed_category else txn_category,
                    "confidence":         "low",
                    "is_subscription":    True,
                    "status":             "active" if is_active else "inactive",
                    "last_date":          last_date.isoformat(),
                    "next_expected_date": next_expected.isoformat() if next_expected else None,
                    "months_paid":        distinct_months,
                    "matched_by":         "seed",
                    "annual_cost":        round(_annualize(avg_amt, detected_freq), 2),
                    "price_change":       _detect_price_change(group_txns),
                    "profile":            item_profile,
                    "seed_pattern":        _pat,
                    **_transaction_evidence(group_txns),
                })
                seen_merchants.add(merchant_key)
                continue

            # Successful evaluation — build result
            recurring.append({
                "merchant":           seed.get("name", merchant_key),
                "avg_amount":         result["avg_amount"],
                "frequency":          result["frequency"],
                "occurrences":        result["occurrences"],
                "category":           seed_category if seed_category else txn_category,
                "confidence":         result["confidence"],
                "is_subscription":    True,
                "status":             result["status"],
                "last_date":          result["last_date"],
                "next_expected_date": result["next_expected_date"],
                "months_paid":        result["months_paid"],
                "matched_by":         "seed",
                "annual_cost":        result["annual_cost"],
                "price_change":       result["price_change"],
                "profile":            item_profile,
                "seed_pattern":        _pat,
                **_result_evidence_fields(result),
            })
            seen_merchants.add(merchant_key)

    # ── Layer 2: Algorithmic detection ────────────────────────────────────────
    def _detect_algorithmically(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        recurring: list[dict],
        seen_merchants: set[str],
        today,
    ):
        """
        Layer 2: Detect recurring charges algorithmically.
        Requires minimum occurrence count, passes through CV/date checks
        via _evaluate_group with price segmentation.
        Category exclusions are strictly enforced (no seed override here).
        """
        for merchant_key, group_txns in merchant_groups.items():
            item_profile = _profile_from_group(group_txns)
            if merchant_key in seen_merchants:
                continue
            if len(group_txns) < self.ALGO_MIN_CHARGES:
                continue

            cat = (group_txns[0].get("category") or "").strip()
            if cat in self.ALGO_EXCLUDED_CATEGORIES:
                continue

            result = _evaluate_group(
                group_txns=group_txns,
                seed_freq_hint=None,
                matched_by="algorithm",
                cv_threshold=self.ALGO_CV_THRESHOLD,
                date_tolerance=self.DATE_TOLERANCE_DAYS,
                today=today,
            )

            if result is None:
                continue

            recurring.append({
                "merchant":           display_names.get(merchant_key, merchant_key),
                "avg_amount":         result["avg_amount"],
                "frequency":          result["frequency"],
                "occurrences":        result["occurrences"],
                "category":           cat,
                "confidence":         result["confidence"],
                "is_subscription":    cat == "Subscriptions" or result["frequency"] in (
                    "monthly", "quarterly", "annual", "semi_annual"
                ),
                "status":             result["status"],
                "last_date":          result["last_date"],
                "next_expected_date": result["next_expected_date"],
                "months_paid":        result["months_paid"],
                "matched_by":         "algorithm",
                "annual_cost":        result["annual_cost"],
                "price_change":       result["price_change"],
                "profile":            item_profile,
                **_result_evidence_fields(result),
            })
            seen_merchants.add(merchant_key)

    # ── Layer 3: Category-based detection ─────────────────────────────────────
    def _detect_by_category(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        recurring: list[dict],
        seen_merchants: set[str],
        today,
    ):
        """
        Layer 3: Detect recurring charges for merchants already categorized as
        recurring-bill classes by the LLM/rules. Same CV/date validation as Layer 2.
        """
        for merchant_key, group_txns in merchant_groups.items():
            item_profile = _profile_from_group(group_txns)
            if merchant_key in seen_merchants:
                continue

            cat = (group_txns[0].get("category") or "").strip()
            if cat not in self.RECURRING_BILL_CATEGORIES:
                continue

            if len(group_txns) < 2:
                continue

            result = _evaluate_group(
                group_txns=group_txns,
                seed_freq_hint=None,
                matched_by="category",
                cv_threshold=self.ALGO_CV_THRESHOLD,
                date_tolerance=self.DATE_TOLERANCE_DAYS,
                today=today,
            )

            if result is None:
                continue

            recurring.append({
                "merchant":           display_names.get(merchant_key, merchant_key),
                "avg_amount":         result["avg_amount"],
                "frequency":          result["frequency"],
                "occurrences":        result["occurrences"],
                "category":           cat,
                "confidence":         result["confidence"],
                "is_subscription":    True,
                "status":             result["status"],
                "last_date":          result["last_date"],
                "next_expected_date": result["next_expected_date"],
                "months_paid":        result["months_paid"],
                "matched_by":         "category",
                "annual_cost":        result["annual_cost"],
                "price_change":       result["price_change"],
                "profile":            item_profile,
                **_result_evidence_fields(result),
            })
            seen_merchants.add(merchant_key)

def write_detection_results_to_db(
    get_db_conn,
    items: list[dict],
    events: list[dict],
    profile: str | None = None,
):
    """
    Persist recurring detection results to the merchants table and
    subscription events to the subscription_events table.

    Called after detect() completes, typically from data_manager.py.
    """
    import json as _json
    profile_id = profile or "household"

    with get_db_conn() as conn:
        items_by_profile: dict[str, list[dict]] = defaultdict(list)
        for item in items:
            merchant_name = item.get("merchant", "")
            item_profile = item.get("profile") or profile_id
            merchant_key = recurring_canonical_key(merchant_name)
            if not merchant_key:
                continue
            items_by_profile[item_profile].append(item)

            conn.execute(
                """INSERT INTO merchants
                   (merchant_key, clean_name, category, source,
                    is_subscription, subscription_frequency, subscription_amount,
                    subscription_status, last_charge_date, next_expected_date,
                    charge_count, profile_id)
                   VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(merchant_key, profile_id) DO UPDATE SET
                       clean_name = CASE
                           WHEN merchants.source = 'user' AND merchants.clean_name != ''
                           THEN merchants.clean_name ELSE excluded.clean_name END,
                       is_subscription = 1,
                       subscription_frequency = excluded.subscription_frequency,
                       subscription_amount = excluded.subscription_amount,
                       subscription_status = CASE
                           WHEN merchants.cancelled_by_user = 1
                           THEN COALESCE(merchants.subscription_status, 'cancelled')
                           ELSE excluded.subscription_status END,
                       last_charge_date = excluded.last_charge_date,
                       next_expected_date = CASE
                           WHEN merchants.cancelled_by_user = 1
                           THEN NULL ELSE excluded.next_expected_date END,
                       charge_count = excluded.charge_count,
                       cancelled_by_user = merchants.cancelled_by_user,
                       cancelled_at = merchants.cancelled_at,
                       updated_at = datetime('now')""",
                (
                    merchant_key,
                    merchant_name,
                    item.get("category", "Subscriptions"),
                    item.get("matched_by", "algorithm"),
                    item.get("frequency"),
                    item.get("avg_amount"),
                    item.get("status"),
                    item.get("last_date"),
	                    item.get("next_expected_date"),
	                    item.get("occurrences", 0),
	                    item_profile,
	                ),
	            )

        # Write events
        for event in events:
            event_profile = event.get("profile_id", profile_id)
            try:
                detail_obj = _json.loads(event.get("detail", "{}") or "{}")
            except Exception:
                detail_obj = {}
            detail_obj.setdefault("period_bucket", event_period_bucket(event))
            event_detail = _json.dumps(detail_obj, sort_keys=True)
            event_key = recurring_event_key({**event, "detail": detail_obj}, event_profile)
            merchant_key = recurring_canonical_key(event.get("merchant_name", ""))
            conn.execute(
                """INSERT OR IGNORE INTO subscription_events
	                   (event_type, merchant_name, profile_id, detail, event_key)
	                   VALUES (?, ?, ?, ?, ?)""",
                (
                    event["event_type"],
                    event["merchant_name"],
                    event_profile,
                    event_detail,
                    event_key,
                ),
            )
            if merchant_key:
                conn.execute(
                    """INSERT OR IGNORE INTO recurring_events_v2
                       (merchant_key, profile_id, event_type, period_bucket, payload_json)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        merchant_key,
                        event_profile,
                        event["event_type"],
                        detail_obj["period_bucket"],
                        event_detail,
                    ),
                )

        for scoped_profile, scoped_items in items_by_profile.items():
            sync_detection_results(
                conn,
                scoped_items,
                profile_id=scoped_profile,
                txn_count=0,
                mode="shadow",
            )
