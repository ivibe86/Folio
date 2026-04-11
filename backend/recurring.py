"""
recurring.py
Subscription and recurring charge detection service.
Extracted from main.py for testability and maintainability.
"""

import re
import time
from datetime import datetime, timedelta
from statistics import mean, stdev, median
from collections import defaultdict
from database import _extract_merchant_pattern
from log_config import get_logger

logger = get_logger(__name__)


# ── Frequency definitions (single source of truth) ───────────────
FREQUENCY_DEFS = {
    "monthly":     (30,  15,  25,  38),
    "quarterly":   (91,  30,  80, 105),
    "semi_annual": (182, 45, 160, 210),
    "annual":      (365, 45, 340, 400),
}

FREQ_RANGES = {k: (v[2], v[3]) for k, v in FREQUENCY_DEFS.items()}

TRANSFER_CATEGORIES = {"Savings Transfer", "Personal Transfer", "Credit Card Payment"}
NON_SPENDING_CATEGORIES = TRANSFER_CATEGORIES | {"Income"}


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_dates(date_strings: list[str]) -> list:
    out = []
    for ds in date_strings:
        try:
            out.append(datetime.strptime(ds[:10], "%Y-%m-%d").date())
        except (ValueError, TypeError):
            continue
    return out


def _normalise_seed_frequency(freq_hint: str | None) -> str:
    if not freq_hint:
        return "monthly"
    f = freq_hint.lower().replace("-", "_").strip()
    if f in ("monthly", "quarterly", "semi_annual", "annual"):
        return f
    if f == "monthly_or_annual":
        return "monthly"
    if f in ("semiannual", "semi-annual", "biannual"):
        return "semi_annual"
    if f == "yearly":
        return "annual"
    return "monthly"


def _detect_frequency(dates: list, seed_freq_hint: str | None = None) -> str | None:
    if len(dates) < 2:
        return _normalise_seed_frequency(seed_freq_hint) if seed_freq_hint else None

    intervals = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    if not intervals:
        return None

    med_iv = median(intervals)

    if seed_freq_hint:
        canonical = _normalise_seed_frequency(seed_freq_hint)
        lo, hi = FREQ_RANGES.get(canonical, (25, 38))
        if lo <= med_iv <= hi:
            nominal = (lo + hi) / 2
            ok = sum(
                1 for iv in intervals
                if abs(iv - nominal) / nominal <= 0.35
            )
            if ok / len(intervals) >= 0.50:
                return canonical

    for freq_name, (lo, hi) in FREQ_RANGES.items():
        if lo <= med_iv <= hi:
            nominal = (lo + hi) / 2
            ok = sum(
                1 for iv in intervals
                if abs(iv - nominal) / nominal <= 0.30
            )
            if ok / len(intervals) >= 0.60:
                return freq_name

    return None


def _amount_confidence(amounts: list[float]) -> float:
    if len(amounts) < 2:
        return 1.0
    avg = sum(amounts) / len(amounts)
    if avg == 0:
        return 0.0
    sd = (sum((a - avg) ** 2 for a in amounts) / len(amounts)) ** 0.5
    return round(max(0.0, 1.0 - (sd / avg)), 2)


def _annualize(amount: float, frequency: str) -> float:
    multipliers = {
        "monthly":     12,
        "quarterly":    4,
        "semi_annual":  2,
        "annual":       1,
    }
    return amount * multipliers.get(frequency, 12)


def _detect_price_change(group_txns: list[dict]) -> dict | None:
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


# ══════════════════════════════════════════════════════════════════════════════
# SEED LOADING WITH TTL CACHE
# ══════════════════════════════════════════════════════════════════════════════

_seeds_cache_store: dict[str, tuple[float, list[dict], set[str]]] = {}
_SEEDS_TTL_SECONDS = 60


def _load_seeds_cached(get_db_conn, profile: str | None = None) -> tuple[list[dict], set[str]]:
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
               ORDER BY source DESC, length(pattern) DESC, id ASC"""
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


# ══════════════════════════════════════════════════════════════════════════════
# RECURRING DETECTOR CLASS
# ══════════════════════════════════════════════════════════════════════════════

class RecurringDetector:
    ALGO_MIN_CHARGES         = 3
    ALGO_CV_THRESHOLD        = 0.10
    ALGO_CV_THRESHOLD_LOOSE  = 0.40
    MAX_TXN_PER_PERIOD_RATIO = 1.35
    ALGO_EXCLUDED_CATEGORIES = {
        "Groceries", "Food & Dining", "Transportation", "Shopping",
        "Travel", "Savings Transfer", "Credit Card Payment",
        "Income", "Personal Transfer",
    }
    VARIABLE_AMOUNT_CATEGORIES = {
        "Utilities", "Electric", "Gas", "Water", "Internet",
        "Wireless", "Insurance", "Auto Insurance", "Home Insurance",
        "Health Insurance", "Renters Insurance", "Life Insurance",
    }
    _DISQUALIFY_TOKENS = {
        "ATM", "CASH", "CHECK", "WITHDRAWAL", "DEPOSIT",
        "REFUND", "CREDIT", "REVERSAL", "TRANSFER",
        "PAYMENT THANK YOU", "OVERDRAFT", "NSF",
    }

    def __init__(self, get_db_conn):
        self._get_db_conn = get_db_conn

    def detect(self, transactions: list[dict], profile: str | None = None) -> dict:
        TODAY = datetime.now().date()

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
            expense_txns.append(t)

        if not expense_txns:
            return {
                "items": [],
                "count": 0,
                "total_monthly": 0.0,
                "total_annual": 0.0,
            }

        merchant_groups, display_names = self._group_by_merchant(expense_txns)

        seeds_cache, suppressed_cache = _load_seeds_cached(self._get_db_conn, profile)

        self._merge_seed_groups(merchant_groups, display_names, seeds_cache, suppressed_cache)

        recurring: list[dict] = []
        seen_merchants: set[str] = set()

        self._match_seeds(
            merchant_groups, display_names, seeds_cache, suppressed_cache,
            recurring, seen_merchants, TODAY,
        )

        self._detect_algorithmically(
            merchant_groups, display_names,
            recurring, seen_merchants, TODAY,
        )

        self._detect_by_category(
            merchant_groups, display_names,
            recurring, seen_merchants, TODAY,
        )

        recurring.sort(
            key=lambda x: (0 if x["status"] == "active" else 1, -x["annual_cost"])
        )

        total_monthly = 0.0
        total_annual = 0.0
        for r in recurring:
            if r["status"] != "active":
                continue
            total_annual += r["annual_cost"]
            total_monthly += r["annual_cost"] / 12

        return {
            "items":         recurring,
            "count":         len(recurring),
            "total_monthly": round(total_monthly, 2),
            "total_annual":  round(total_annual, 2),
        }

    @staticmethod
    def _is_expense(tx: dict) -> bool:
        amount = float(tx.get("amount", 0))
        cat = tx.get("category", "Other")
        return amount < 0 and cat not in NON_SPENDING_CATEGORIES

    def _group_by_merchant(
        self, expense_txns: list[dict]
    ) -> tuple[dict[str, list[dict]], dict[str, str]]:
        merchant_groups: dict[str, list[dict]] = defaultdict(list)
        display_names: dict[str, str] = {}

        for t in expense_txns:
            raw_merchant = (t.get("merchant_name") or "").strip()
            raw_desc = (t.get("description") or "").strip()

            extracted = _extract_merchant_pattern(raw_desc)

            if raw_merchant:
                clean = raw_merchant.upper().strip()
                clean = re.sub(r'\.(COM|NET|ORG|IO|CO|AI|TV|FM|APP|ME|US|UK)(\/\S*)?', '', clean)
                clean = re.sub(r',?\s*\b(INC\.?|LLC\.?|L\.?L\.?C\.?|LTD\.?|CORP\.?|CO\.?|INCORPORATED|CORPORATION|S\.?A\.?)\b\.?', '', clean)
                clean = clean.strip(' .,;-')
                key = clean if clean else (extracted or raw_desc.upper() or "UNKNOWN")
            else:
                key = extracted if extracted else (raw_desc.upper() or "UNKNOWN")

            if key not in display_names or raw_merchant:
                display_names[key] = raw_merchant if raw_merchant else raw_desc
            merchant_groups[key].append({
                "amount":        abs(float(t.get("amount", 0))),
                "date":          t["date"][:10],
                "category":      t.get("category", "Other"),
                "description":   raw_desc,
                "merchant_name": raw_merchant,
            })

        return merchant_groups, display_names

    def _merge_seed_groups(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        seeds_cache: list[dict],
        suppressed_cache: set[str],
    ):
        seed_merge_map: dict[str, str] = {}
        merge_targets: dict[str, str] = {}

        for key in list(merchant_groups.keys()):
            sample = merchant_groups[key][0]
            seed, _pat = _match_seed(
                sample["merchant_name"], sample["description"],
                seeds_cache, suppressed_cache,
            )
            if seed:
                seed_name = seed["name"]
                if seed_name in seed_merge_map:
                    canonical = seed_merge_map[seed_name]
                    merge_targets[key] = canonical
                else:
                    seed_merge_map[seed_name] = key
                    display_names[key] = seed_name

        for src_key, dst_key in merge_targets.items():
            merchant_groups[dst_key].extend(merchant_groups[src_key])
            del merchant_groups[src_key]

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
        for merchant_key, group_txns in merchant_groups.items():
            if not group_txns:
                continue
            sample = group_txns[0]
            amounts = [t["amount"] for t in group_txns]
            avg_amt = mean(amounts) if amounts else 0

            seed, _pat = _match_seed(
                sample["merchant_name"], sample["description"],
                seeds_cache, suppressed_cache,
            )
            if seed is None:
                continue

            dates_sorted = sorted(
                _parse_dates([t["date"] for t in group_txns])
            )
            if not dates_sorted:
                continue

            detected_freq = _detect_frequency(dates_sorted, seed.get("frequency_hint"))
            if detected_freq is None:
                detected_freq = _normalise_seed_frequency(
                    seed.get("frequency_hint", "monthly")
                )

            nominal, grace, _, _ = FREQUENCY_DEFS.get(
                detected_freq, FREQUENCY_DEFS["monthly"]
            )
            last_date = dates_sorted[-1]
            is_active = (today - last_date).days <= (nominal + grace)
            next_expected = (
                (last_date + timedelta(days=nominal)) if is_active else None
            )
            distinct_months = len({d.strftime("%Y-%m") for d in dates_sorted})

            recurring.append({
                "merchant":           seed.get("name", merchant_key),
                "avg_amount":         round(avg_amt, 2),
                "frequency":          detected_freq,
                "occurrences":        len(group_txns),
                "category":           sample["category"],
                "confidence":         _amount_confidence(amounts),
                "is_subscription":    True,
                "status":             "active" if is_active else "inactive",
                "last_date":          last_date.isoformat(),
                "next_expected_date": next_expected.isoformat() if next_expected else None,
                "months_paid":        distinct_months,
                "matched_by":         "seed",
                "annual_cost":        round(_annualize(avg_amt, detected_freq), 2),
                "price_change":       _detect_price_change(group_txns),
            })
            seen_merchants.add(merchant_key)

    def _detect_algorithmically(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        recurring: list[dict],
        seen_merchants: set[str],
        today,
    ):
        for merchant_key, group_txns in merchant_groups.items():
            if merchant_key in seen_merchants:
                continue
            if len(group_txns) < self.ALGO_MIN_CHARGES:
                continue

            cat = (group_txns[0].get("category") or "").strip()
            if cat in self.ALGO_EXCLUDED_CATEGORIES:
                continue

            amounts = [t["amount"] for t in group_txns]
            avg_amt = mean(amounts)
            if avg_amt == 0:
                continue

            cv_threshold = (
                self.ALGO_CV_THRESHOLD_LOOSE if cat in self.VARIABLE_AMOUNT_CATEGORIES
                else self.ALGO_CV_THRESHOLD
            )
            if len(amounts) >= 2:
                sd = stdev(amounts)
                cv = sd / avg_amt
                if cv > cv_threshold:
                    continue

            dates_sorted = sorted(
                _parse_dates([t["date"] for t in group_txns])
            )
            if len(dates_sorted) < self.ALGO_MIN_CHARGES:
                continue

            detected_freq = _detect_frequency(dates_sorted, None)
            if detected_freq is None:
                continue

            nominal, grace, _, _ = FREQUENCY_DEFS.get(
                detected_freq, FREQUENCY_DEFS["monthly"]
            )

            span_days = max((dates_sorted[-1] - dates_sorted[0]).days, 1)
            expected_periods = span_days / nominal
            if expected_periods > 0 and (
                len(group_txns) / expected_periods
            ) > self.MAX_TXN_PER_PERIOD_RATIO:
                continue

            last_date = dates_sorted[-1]
            is_active = (today - last_date).days <= (nominal + grace)
            next_expected = (
                (last_date + timedelta(days=nominal)) if is_active else None
            )
            distinct_months = len({d.strftime("%Y-%m") for d in dates_sorted})

            recurring.append({
                "merchant":           display_names.get(merchant_key, merchant_key),
                "avg_amount":         round(avg_amt, 2),
                "frequency":          detected_freq,
                "occurrences":        len(group_txns),
                "category":           cat,
                "confidence":         _amount_confidence(amounts),
                "is_subscription":    cat == "Subscriptions" or detected_freq in ("monthly", "quarterly", "annual", "semi_annual"),
                "status":             "active" if is_active else "inactive",
                "last_date":          last_date.isoformat(),
                "next_expected_date": next_expected.isoformat() if next_expected else None,
                "months_paid":        distinct_months,
                "matched_by":         "algorithm",
                "annual_cost":        round(_annualize(avg_amt, detected_freq), 2),
                "price_change":       _detect_price_change(group_txns),
            })
            seen_merchants.add(merchant_key)

    def _detect_by_category(
        self,
        merchant_groups: dict[str, list[dict]],
        display_names: dict[str, str],
        recurring: list[dict],
        seen_merchants: set[str],
        today,
    ):
        for merchant_key, group_txns in merchant_groups.items():
            if merchant_key in seen_merchants:
                continue

            cat = (group_txns[0].get("category") or "").strip()
            if cat != "Subscriptions":
                continue

            if len(group_txns) < 2:
                continue

            amounts = [t["amount"] for t in group_txns]
            avg_amt = mean(amounts) if amounts else 0
            if avg_amt == 0:
                continue

            dates_sorted = sorted(
                _parse_dates([t["date"] for t in group_txns])
            )
            if len(dates_sorted) < 2:
                continue

            detected_freq = _detect_frequency(dates_sorted, None)
            if detected_freq is None:
                if len(dates_sorted) >= 2:
                    span_days = (dates_sorted[-1] - dates_sorted[0]).days
                    if span_days >= 300:
                        detected_freq = "annual"
                    elif span_days >= 150:
                        detected_freq = "semi_annual"
                    elif span_days >= 70:
                        detected_freq = "quarterly"
                    else:
                        detected_freq = "monthly"
                else:
                    detected_freq = "monthly"

            nominal, grace, _, _ = FREQUENCY_DEFS.get(
                detected_freq, FREQUENCY_DEFS["monthly"]
            )
            last_date = dates_sorted[-1]
            is_active = (today - last_date).days <= (nominal + grace)
            next_expected = (
                (last_date + timedelta(days=nominal)) if is_active else None
            )
            distinct_months = len({d.strftime("%Y-%m") for d in dates_sorted})

            recurring.append({
                "merchant":           display_names.get(merchant_key, merchant_key),
                "avg_amount":         round(avg_amt, 2),
                "frequency":          detected_freq,
                "occurrences":        len(group_txns),
                "category":           cat,
                "confidence":         _amount_confidence(amounts),
                "is_subscription":    True,
                "status":             "active" if is_active else "inactive",
                "last_date":          last_date.isoformat(),
                "next_expected_date": next_expected.isoformat() if next_expected else None,
                "months_paid":        distinct_months,
                "matched_by":         "category",
                "annual_cost":        round(_annualize(avg_amt, detected_freq), 2),
                "price_change":       _detect_price_change(group_txns),
            })
            seen_merchants.add(merchant_key)