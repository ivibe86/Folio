"""
Copilot agent tools. Each tool wraps an existing analytic or persistence
function so the LLM can reach live data through a narrow, named surface.

Tool schemas follow JSON Schema draft-7 (compatible with both Anthropic's
tool-use and Ollama's OpenAI-style tool-use format).

Time windows use a `range` token:
  current_month | last_month | prior_month | YYYY-MM
  this_week    | last_week
  last_7d | last_30d | last_90d | last_180d | last_365d | last_N_months
  ytd | last_year | all
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from database import get_db
from data_manager import (
    get_category_analytics_data,
    get_dashboard_bundle_data,
    get_merchant_insights_data,
    get_monthly_analytics_data,
    get_net_worth_delta_metrics,
    get_net_worth_series_data,
    get_recurring_from_db,
    get_summary_data,
    get_transactions_for_merchant,
    get_transactions_paginated,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Range resolution
# ──────────────────────────────────────────────────────────────────────────────

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    return idx // 12, idx % 12 + 1


def _resolve_range(token: str | None) -> tuple[str | None, str | None, str]:
    """
    Translate a range token to (start_iso_date, end_iso_date, label).
    Returns (None, None, label) for 'all' (no date filter).
    """
    if not token:
        token = "current_month"
    token = token.strip().lower()
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    if token in {"current_month", "this_month", "current"}:
        start, end = _month_bounds(now.year, now.month)
        return start, end, f"{now.year:04d}-{now.month:02d}"

    if token in {"last_month", "prior_month", "prior"}:
        y, m = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
        start, end = _month_bounds(y, m)
        return start, end, f"{y:04d}-{m:02d}"

    if _MONTH_RE.match(token):
        y, m = int(token[:4]), int(token[5:7])
        start, end = _month_bounds(y, m)
        return start, end, token

    if token in {"this_week"}:
        start_dt = now - timedelta(days=now.weekday())
        return start_dt.strftime("%Y-%m-%d"), today, "this_week"

    if token == "last_week":
        this_mon = now - timedelta(days=now.weekday())
        last_mon = this_mon - timedelta(days=7)
        last_sun = this_mon - timedelta(days=1)
        return last_mon.strftime("%Y-%m-%d"), last_sun.strftime("%Y-%m-%d"), "last_week"

    m = re.match(r"^last_(\d+)d$", token)
    if m:
        days = int(m.group(1))
        start_dt = now - timedelta(days=days)
        return start_dt.strftime("%Y-%m-%d"), today, f"last_{days}d"

    m = re.match(r"^last_(\d{1,2})_?months?$", token)
    if m:
        months = max(1, min(int(m.group(1)), 36))
        y, mon = _shift_month(now.year, now.month, -(months - 1))
        start, _ = _month_bounds(y, mon)
        return start, today, f"last_{months}_months"

    if token == "ytd":
        return f"{now.year:04d}-01-01", today, "ytd"

    if token == "last_year":
        start, end = _month_bounds(now.year - 1, 1)
        _, end_of_year = _month_bounds(now.year - 1, 12)
        return start, end_of_year, f"{now.year - 1:04d}"

    if token == "all":
        return None, None, "all"

    # Unknown token → fall back to current month
    start, end = _month_bounds(now.year, now.month)
    return start, end, f"{now.year:04d}-{now.month:02d}"


_RANGE_ENUM = [
    "current_month", "last_month", "prior_month",
    "this_week", "last_week",
    "last_7d", "last_30d", "last_90d", "last_180d", "last_365d",
    "ytd", "last_year", "all",
]
_RANGE_DESC = (
    "Time window. One of: current_month (default), last_month, this_week, last_week, "
    "last_7d, last_30d, last_90d, last_180d, last_365d, ytd, last_year, all, "
    "last_N_months such as last_13_months, or a specific YYYY-MM."
)
_NON_SPENDING_CATEGORIES = ("Savings Transfer", "Personal Transfer", "Credit Card Payment", "Income")


def _profile_filter(profile: str | None) -> tuple[str, list]:
    if not profile or profile == "household":
        return "", []
    return " AND profile_id = ?", [profile]


def _date_filter(start: str | None, end: str | None) -> tuple[str, list]:
    if start and end:
        return " AND date >= ? AND date <= ?", [start, end]
    if start:
        return " AND date >= ?", [start]
    return "", []


def _fmt_money(v) -> float:
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Tool implementations
# ──────────────────────────────────────────────────────────────────────────────

def _t_get_month_summary(args: dict, profile: str | None, conn) -> Any:
    month_arg = args.get("month") or "current"
    _, _, label = _resolve_range(month_arg)
    rows = get_monthly_analytics_data(profile=profile, conn=conn) or []
    if not rows:
        return {"error": "no monthly data"}
    target = next((r for r in rows if r.get("month") == label), None)
    if target is None:
        target = rows[-1]
    idx = rows.index(target)
    prior = rows[idx - 1] if idx > 0 else None
    return {"current": target, "prior": prior}


def _range_to_kwargs(args: dict) -> tuple[str, str | None, str | None, str]:
    """Resolve range token to (month_arg, start_date, end_date, label) for data_manager calls."""
    token = args.get("range") or args.get("month")
    start, end, label = _resolve_range(token)
    # If the label is a YYYY-MM and the range is an exact month, use month= for efficiency
    if _MONTH_RE.match(label or ""):
        return label, None, None, label
    return None, start, end, label


def _t_get_top_categories(args: dict, profile: str | None, conn) -> Any:
    """Delegates to get_category_analytics_data — same aggregation as dashboard."""
    month, start, end, label = _range_to_kwargs(args)
    limit = int(args.get("limit") or 10)
    data = get_category_analytics_data(
        month=month, profile=profile, conn=conn, start_date=start, end_date=end,
    ) or {}
    cats = data.get("categories") or []
    return {
        "range": label,
        "start": start, "end": end,
        "categories": cats[:limit],
    }


def _t_get_top_merchants(args: dict, profile: str | None, conn) -> Any:
    """Delegates to get_merchant_insights_data with include_unenriched=True so partially-enriched merchants are visible."""
    month, start, end, label = _range_to_kwargs(args)
    limit = int(args.get("limit") or 10)
    rows = get_merchant_insights_data(
        month=month, profile=profile, conn=conn,
        start_date=start, end_date=end, include_unenriched=True,
    ) or []
    return {
        "range": label,
        "start": start, "end": end,
        "merchants": rows[:limit],
    }


def _t_get_category_spend(args: dict, profile: str | None, conn) -> Any:
    """Lookup a specific category via the same aggregation as the dashboard, plus recent txns."""
    category = (args.get("category") or "").strip()
    if not category:
        return {"error": "category required"}
    month, start, end, label = _range_to_kwargs(args)

    data = get_category_analytics_data(
        month=month, profile=profile, conn=conn, start_date=start, end_date=end,
    ) or {}
    match = next(
        (c for c in (data.get("categories") or []) if (c.get("category") or "").lower() == category.lower()),
        None,
    )

    # Recent transactions for this category in the same window
    tx_page = get_transactions_paginated(
        month=month, category=category, profile=profile, conn=conn,
        start_date=start, end_date=end, limit=10, offset=0,
    ) or {}

    return {
        "category": category,
        "range": label,
        "start": start, "end": end,
        "total": (match or {}).get("total", 0),
        "gross": (match or {}).get("gross", 0),
        "refunds": (match or {}).get("refunds", 0),
        "percent_of_month": (match or {}).get("percent"),
        "expense_type": (match or {}).get("expense_type"),
        "recent": tx_page.get("data") or [],
        "total_count": tx_page.get("total_count", 0),
    }


def _t_get_merchant_spend(args: dict, profile: str | None, conn) -> Any:
    """Lookup a specific merchant by fragment, using the same include_unenriched path as get_top_merchants."""
    merchant = (args.get("merchant") or "").strip()
    if not merchant:
        return {"error": "merchant required"}
    month, start, end, label = _range_to_kwargs(args)

    all_merchants = get_merchant_insights_data(
        month=month, profile=profile, conn=conn,
        start_date=start, end_date=end, include_unenriched=True,
    ) or []
    needle = merchant.lower()
    matched = [m for m in all_merchants if needle in (m.get("name") or "").lower()]

    search = f"%{merchant.upper()}%"
    params: list[Any] = list(_NON_SPENDING_CATEGORIES)
    where = [
        "amount < 0",
        f"category NOT IN ({','.join('?' for _ in _NON_SPENDING_CATEGORIES)})",
        "(expense_type IS NULL OR expense_type NOT IN ('transfer_internal','transfer_household'))",
        """(
            UPPER(COALESCE(description, '')) LIKE ?
            OR UPPER(COALESCE(raw_description, '')) LIKE ?
            OR UPPER(COALESCE(merchant_key, '')) LIKE ?
            OR UPPER(COALESCE(merchant_name, '')) LIKE ?
            OR UPPER(COALESCE(category, '')) LIKE ?
        )""",
    ]
    params.extend([search, search, search, search, search])
    if profile and profile != "household":
        where.append("profile_id = ?")
        params.append(profile)
    if month:
        where.append("date LIKE ?")
        params.append(month + "%")
    else:
        if start:
            where.append("date >= ?")
            params.append(start)
        if end:
            where.append("date <= ?")
            params.append(end)
    where_sql = " AND ".join(where)
    total_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(amount)), 0) AS total, COUNT(*) AS count
        FROM transactions_visible
        WHERE {where_sql}
        """,
        params,
    ).fetchone()
    recent_rows = conn.execute(
        f"""
        SELECT id as original_id, profile_id as profile, date, description, raw_description,
               amount, category, original_category, categorization_source, confidence,
               transaction_type as type, account_name, account_type, merchant_name, merchant_key,
               enriched, is_excluded, expense_type
        FROM transactions_visible
        WHERE {where_sql}
        ORDER BY date DESC
        LIMIT 10
        """,
        params,
    ).fetchall()
    recent = [dict(row) for row in recent_rows]
    for tx in recent:
        tx["enriched"] = bool(tx.get("enriched", 0))
        tx["is_excluded"] = bool(tx.get("is_excluded", 0))

    return {
        "merchant_query": merchant,
        "range": label,
        "start": start, "end": end,
        "matched_merchants": matched[:5],
        "total": _fmt_money(total_row[0] if total_row else 0),
        "txn_count": int(total_row[1] if total_row else 0),
        "recent": recent,
        "total_matching_transactions": int(total_row[1] if total_row else 0),
    }


def _t_get_transactions(args: dict, profile: str | None, conn) -> Any:
    """General-purpose transaction search — same source as the Transactions page."""
    month, start, end, label = _range_to_kwargs(args) if (args.get("range") or args.get("month")) else (None, None, None, "all")
    return get_transactions_paginated(
        month=month,
        category=args.get("category"),
        account=args.get("account"),
        search=args.get("search"),
        profile=profile,
        limit=int(args.get("limit") or 25),
        offset=int(args.get("offset") or 0),
        conn=conn,
        start_date=start,
        end_date=end,
    ) or {}


def _t_get_category_breakdown(args: dict, profile: str | None, conn) -> Any:
    """Full per-category breakdown (Sankey / cash-waterfall data source)."""
    month, start, end, label = _range_to_kwargs(args)
    return {
        "range": label,
        "start": start, "end": end,
        **(get_category_analytics_data(
            month=month, profile=profile, conn=conn, start_date=start, end_date=end,
        ) or {}),
    }


def _t_get_dashboard_bundle(args: dict, profile: str | None, conn) -> Any:
    """Aggregated dashboard snapshot — same payload that powers the main dashboard."""
    return get_dashboard_bundle_data(profile=profile, conn=conn) or {}


def _t_get_net_worth_delta(args: dict, profile: str | None, conn) -> Any:
    """Month-over-month net worth deltas (same numbers the dashboard shows)."""
    return get_net_worth_delta_metrics(profile=profile, conn=conn) or {}


def _t_get_recurring_summary(args: dict, profile: str | None, conn) -> Any:
    data = get_recurring_from_db(profile=profile, conn=conn) or {}
    items = data.get("items") or []
    status_filter = args.get("status")
    if status_filter:
        items = [i for i in items if (i.get("subscription_status") or "").lower() == status_filter.lower()]
    return {
        "active_count": data.get("active_count", 0),
        "inactive_count": data.get("inactive_count", 0),
        "cancelled_count": data.get("cancelled_count", 0),
        "total_monthly": data.get("total_monthly", 0),
        "total_annual": data.get("total_annual", 0),
        "items": items[: int(args.get("limit") or 25)],
    }


def _t_get_net_worth_trend(args: dict, profile: str | None, conn) -> Any:
    interval = args.get("interval") or "monthly"
    series = get_net_worth_series_data(interval=interval, profile=profile, conn=conn) or []

    range_token = args.get("range")
    if range_token and range_token != "all":
        start, end, label = _resolve_range(range_token)
        if start:
            series = [s for s in series if (s.get("date") or "") >= start and (not end or (s.get("date") or "") <= end)]
    else:
        label = "all"

    limit = int(args.get("limit") or 24)
    return {"interval": interval, "range": label, "series": series[-limit:]}


def _t_get_monthly_spending_trend(args: dict, profile: str | None, conn) -> Any:
    """
    Monthly spending trend for charting. Uses canonical spending semantics and
    optionally narrows to one category.
    """
    try:
        months = max(1, min(int(args.get("months") or 6), 36))
    except (TypeError, ValueError):
        months = 6
    category = (args.get("category") or "").strip()

    now = datetime.now()
    month_keys: list[str] = []
    bounds: list[tuple[str, str, str]] = []
    for offset in range(-(months - 1), 1):
        y, m = _shift_month(now.year, now.month, offset)
        start, end = _month_bounds(y, m)
        label = f"{y:04d}-{m:02d}"
        month_keys.append(label)
        bounds.append((label, start, end))

    start_date = bounds[0][1]
    end_date = bounds[-1][2]
    pclause, pparams = _profile_filter(profile)
    params: list[Any] = [start_date, end_date]
    category_clause = ""
    if category:
        category_clause = " AND LOWER(category) = LOWER(?)"
        params.append(category)
    params.extend(pparams)

    rows = conn.execute(
        f"""
        SELECT substr(date, 1, 7) AS month,
               SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE -ABS(amount) END) AS total
        FROM transactions_visible
        WHERE date >= ? AND date <= ?
          AND is_excluded = 0
          AND category IS NOT NULL
          AND category != ''
          AND category NOT IN ('Savings Transfer','Personal Transfer','Credit Card Payment','Income')
          AND (expense_type IS NULL OR expense_type NOT IN ('transfer_internal','transfer_household'))
          {category_clause}
          {pclause}
        GROUP BY month
        ORDER BY month
        """,
        params,
    ).fetchall()
    by_month = {r["month"]: _fmt_money(r["total"]) for r in rows}
    series = [{"month": m, "total": by_month.get(m, 0.0)} for m in month_keys]
    return {
        "category": category or None,
        "months": months,
        "start": start_date,
        "end": end_date,
        "series": series,
        "labels": [s["month"] for s in series],
        "values": [s["total"] for s in series],
    }


def _t_get_transactions_for_merchant(args: dict, profile: str | None, conn) -> Any:
    merchant = (args.get("merchant") or "").strip()
    if not merchant:
        return {"error": "merchant required"}
    limit = int(args.get("limit") or 25)
    rows = get_transactions_for_merchant(merchant=merchant, profile=profile, limit=limit, conn=conn) or []
    return {"merchant": merchant, "transactions": rows}


def _t_get_summary(args: dict, profile: str | None, conn) -> Any:
    return get_summary_data(profile=profile, conn=conn) or {}


def _t_get_account_balances(args: dict, profile: str | None, conn) -> Any:
    pclause, pparams = _profile_filter(profile)
    rows = conn.execute(
        f"""
        SELECT account_name, account_type, account_subtype, institution_name,
               last_four, current_balance, available_balance, currency,
               profile_id, provider, is_active
        FROM accounts
        WHERE is_active = 1{pclause}
        ORDER BY account_type, institution_name, account_name
        """,
        pparams,
    ).fetchall()
    accounts = [dict(r) for r in rows]

    by_type: dict[str, float] = {}
    for a in accounts:
        t = a.get("account_type") or "other"
        by_type[t] = by_type.get(t, 0.0) + float(a.get("current_balance") or 0.0)

    net = (
        by_type.get("depository", 0.0)
        + by_type.get("investment", 0.0)
        - abs(by_type.get("credit", 0.0))
        - abs(by_type.get("loan", 0.0))
    )

    return {
        "accounts": accounts,
        "totals_by_type": {k: _fmt_money(v) for k, v in by_type.items()},
        "approx_net_worth": _fmt_money(net),
    }


def _t_get_category_rules(args: dict, profile: str | None, conn) -> Any:
    rows = conn.execute(
        "SELECT pattern, category, priority, is_active, source FROM category_rules ORDER BY priority DESC LIMIT ?",
        (int(args.get("limit") or 50),),
    ).fetchall()
    return {"rules": [dict(r) for r in rows]}


def _t_search_saved_insights(args: dict, profile: str | None, conn) -> Any:
    keywords = (args.get("keywords") or "").strip().lower()
    limit = int(args.get("limit") or 10)
    if keywords:
        like = f"%{keywords}%"
        rows = conn.execute(
            """
            SELECT id, question, answer, kind, pinned, created_at
            FROM saved_insights
            WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
              AND (LOWER(question) LIKE ? OR LOWER(answer) LIKE ?)
            ORDER BY pinned DESC, created_at DESC
            LIMIT ?
            """,
            (profile, profile, like, like, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, question, answer, kind, pinned, created_at
            FROM saved_insights
            WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
            ORDER BY pinned DESC, created_at DESC
            LIMIT ?
            """,
            (profile, profile, limit),
        ).fetchall()
    return {"insights": [dict(r) for r in rows]}


def _t_preview_bulk_recategorize(args: dict, profile: str | None, conn) -> Any:
    """Preview moving all transactions for a merchant to a new category.
    Returns a write-preview payload with confirmation_id; user must confirm."""
    from data_manager import bulk_recategorize_preview
    from copilot import store_pending_sql

    merchant = (args.get("merchant") or "").strip()
    category = (args.get("category") or "").strip()
    if not merchant or not category:
        return {"error": "merchant and category are required"}

    data = bulk_recategorize_preview(merchant, category, profile, conn)
    count = data.get("count", 0)
    if count == 0:
        return {
            "operation": "read",
            "count": 0,
            "note": f'No transactions found for "{merchant}" that aren\'t already categorized as "{category}".',
        }
    confirmation_id = store_pending_sql(data["update_sql"], profile)
    return {
        "_write_preview": True,
        "operation": "write_preview",
        "summary": f"Move {count} {merchant} transaction(s) to {category}",
        "confirmation_id": confirmation_id,
        "sql": data["update_sql"],
        "rows_affected": count,
        "samples": data.get("samples", []),
        "preview_changes": [{"column": "category", "raw_value": category, "new_value": category}],
    }


def _t_preview_create_rule(args: dict, profile: str | None, conn) -> Any:
    """Preview creating a category rule (pattern → category)."""
    from data_manager import preview_rule_creation
    from copilot import store_pending_sql

    pattern = (args.get("pattern") or "").strip()
    category = (args.get("category") or "").strip()
    if not pattern or not category:
        return {"error": "pattern and category are required"}

    data = preview_rule_creation(pattern, category, profile, conn)
    count = data.get("count", 0)
    confirmation_id = store_pending_sql(data["insert_sql"], profile)
    existing = data.get("existing_rule")
    return {
        "_write_preview": True,
        "operation": "write_preview",
        "summary": (
            f"Create rule {data.get('pattern') or pattern} → {category} "
            f"(applies to {count} existing + all future matches)"
            + (f". Replaces existing rule → {existing['category']}." if existing else "")
        ),
        "confirmation_id": confirmation_id,
        "sql": data["insert_sql"],
        "rows_affected": count,
        "samples": data.get("samples", []),
        "existing_rule": existing,
        "preview_changes": [{"column": "rule", "raw_value": f"{data.get('pattern') or pattern} → {category}", "new_value": category}],
    }


def _t_preview_rename_merchant(args: dict, profile: str | None, conn) -> Any:
    """Preview renaming a merchant (all its transaction variants)."""
    from data_manager import rename_merchant_variants
    from copilot import store_pending_sql

    old_name = (args.get("old_name") or "").strip()
    new_name = (args.get("new_name") or "").strip()
    if not old_name or not new_name:
        return {"error": "old_name and new_name are required"}

    data = rename_merchant_variants(old_name, new_name, profile, conn)
    count = data.get("count", 0)
    if count == 0:
        return {
            "operation": "read",
            "count": 0,
            "note": f'No transactions found matching "{old_name}".',
        }
    confirmation_id = store_pending_sql(data["update_sql"], profile)
    return {
        "_write_preview": True,
        "operation": "write_preview",
        "summary": f"Rename {count} transaction(s) from {old_name} to {new_name}",
        "confirmation_id": confirmation_id,
        "sql": data["update_sql"],
        "rows_affected": count,
        "samples": data.get("samples", []),
        "preview_changes": [{"column": "merchant_name", "raw_value": new_name, "new_value": new_name}],
    }


def _t_plot_chart(args: dict, profile: str | None, conn) -> Any:
    """
    Emit a chart spec for the UI to render inline. Use when a visualization
    clarifies trends, comparisons, or distributions.
    Types: 'line' (trends over time), 'bar' (comparisons, top-N),
           'donut' (category share / composition).
    """
    # Be defensive for LLM fallback calls. The deterministic chart path already
    # sends the canonical shape, but models sometimes use common charting aliases
    # like chart_type + data.labels/data.values.
    if "type" not in args and "chart_type" in args:
        args = {**args, "type": args.get("chart_type")}
    nested_data = args.get("data")
    if isinstance(nested_data, dict):
        args = {**nested_data, **args}

    chart_type = (args.get("type") or "").strip().lower()
    if chart_type not in ("line", "bar", "donut"):
        return {"error": "type must be one of: line, bar, donut"}

    labels = args.get("labels") or []
    values = args.get("values") or []
    if not isinstance(labels, list) or not isinstance(values, list):
        return {"error": "labels and values must be arrays"}
    if len(labels) != len(values):
        return {"error": f"labels ({len(labels)}) and values ({len(values)}) must have same length"}
    if not labels:
        return {"error": "at least one data point required"}

    try:
        values = [float(v) for v in values]
    except (TypeError, ValueError):
        return {"error": "values must be numeric"}

    return {
        "_chart": True,
        "type": chart_type,
        "title": args.get("title") or "",
        "series_name": args.get("series_name") or "",
        "labels": [str(l) for l in labels],
        "values": values,
        "unit": args.get("unit") or "currency",  # 'currency' | 'number' | 'percent'
    }


def _t_run_sql(args: dict, profile: str | None, conn) -> Any:
    """Read-only SQL escape hatch. Delegates to copilot's existing validator."""
    from copilot import _validate_read_sql, _rewrite_transaction_read_sources

    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query required"}

    rewritten = _rewrite_transaction_read_sources(query)
    ok, err = _validate_read_sql(rewritten)
    if not ok:
        return {"error": f"SQL rejected: {err}"}

    try:
        rows = conn.execute(rewritten).fetchall()
    except Exception as e:
        return {"error": f"SQL execution error: {e}"}

    data = [dict(r) for r in rows[:200]]
    return {"row_count": len(rows), "rows": data, "truncated": len(rows) > 200}


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────

TOOL_REGISTRY: dict[str, dict[str, Any]] = {
    "get_month_summary": {
        "fn": _t_get_month_summary,
        "description": "Income/expense/net/savings for a month, with prior-month comparison. month='current' | 'prior' | 'YYYY-MM'.",
        "parameters": {
            "type": "object",
            "properties": {"month": {"type": "string", "description": "current, prior, or YYYY-MM"}},
        },
    },
    "get_top_categories": {
        "fn": _t_get_top_categories,
        "description": "Top spending categories for a time range, sorted by amount. Use this when the user asks about the biggest/top categories.",
        "parameters": {
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
    "get_top_merchants": {
        "fn": _t_get_top_merchants,
        "description": "Top merchants by spend for a time range. Use this for the biggest/top merchant questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
    "get_category_spend": {
        "fn": _t_get_category_spend,
        "description": "Exact total and recent transactions for a SPECIFIC category by name (e.g. 'Groceries', 'Food & Dining') over a time range. Use this when the user names a specific category.",
        "parameters": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Category name, case-insensitive exact match"},
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
            },
            "required": ["category"],
        },
    },
    "get_merchant_spend": {
        "fn": _t_get_merchant_spend,
        "description": "Exact total and recent transactions for a SPECIFIC merchant by name fragment (e.g. 'Costco', 'BILT', 'Amazon') over a time range. Matches substring in merchant name or description.",
        "parameters": {
            "type": "object",
            "properties": {
                "merchant": {"type": "string", "description": "Merchant name or fragment, substring match"},
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
            },
            "required": ["merchant"],
        },
    },
    "get_recurring_summary": {
        "fn": _t_get_recurring_summary,
        "description": "Active subscriptions/recurring charges with totals. Optional status: 'active'|'inactive'|'cancelled'.",
        "parameters": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "limit": {"type": "integer", "default": 25},
            },
        },
    },
    "get_net_worth_trend": {
        "fn": _t_get_net_worth_trend,
        "description": "Net worth series over time. interval='monthly' (default) or 'weekly'. Optional range filter.",
        "parameters": {
            "type": "object",
            "properties": {
                "interval": {"type": "string", "enum": ["monthly", "weekly"]},
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
                "limit": {"type": "integer", "default": 24},
            },
        },
    },
    "get_monthly_spending_trend": {
        "fn": _t_get_monthly_spending_trend,
        "description": "Monthly spending trend for line charts. Returns labels and values for total spending or one category over the last N calendar months including the current month.",
        "parameters": {
            "type": "object",
            "properties": {
                "months": {"type": "integer", "default": 6, "description": "Number of calendar months to include, 1-36."},
                "category": {"type": "string", "description": "Optional exact category name, e.g. 'Groceries'."},
            },
        },
    },
    "get_transactions_for_merchant": {
        "fn": _t_get_transactions_for_merchant,
        "description": "Recent transactions for a specific merchant (deep drill-down).",
        "parameters": {
            "type": "object",
            "properties": {
                "merchant": {"type": "string"},
                "limit": {"type": "integer", "default": 25},
            },
            "required": ["merchant"],
        },
    },
    "get_summary": {
        "fn": _t_get_summary,
        "description": "Overall financial snapshot: totals, counts, net worth, savings.",
        "parameters": {"type": "object", "properties": {}},
    },
    "get_account_balances": {
        "fn": _t_get_account_balances,
        "description": "Current balances across all connected bank accounts (checking, savings, credit, loan, investment). Use this for 'what's my balance' or 'how much cash do I have' questions.",
        "parameters": {"type": "object", "properties": {}},
    },
    "get_transactions": {
        "fn": _t_get_transactions,
        "description": "Search / list transactions with filters — same source as the Transactions page. Filter by range, category, account, or free-text search.",
        "parameters": {
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
                "category": {"type": "string"},
                "account": {"type": "string"},
                "search": {"type": "string", "description": "Substring match on description or merchant name"},
                "limit": {"type": "integer", "default": 25},
                "offset": {"type": "integer", "default": 0},
            },
        },
    },
    "get_category_breakdown": {
        "fn": _t_get_category_breakdown,
        "description": "Full per-category spending breakdown for a time range — the exact data source used by the dashboard Sankey / cash waterfall. Includes gross, refunds, net, and percent-of-total per category.",
        "parameters": {
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": _RANGE_DESC, "enum": _RANGE_ENUM},
            },
        },
    },
    "get_dashboard_bundle": {
        "fn": _t_get_dashboard_bundle,
        "description": "Complete dashboard snapshot (summary + accounts + monthly analytics + category breakdown + net worth series) in one call. Use when you need broad context.",
        "parameters": {"type": "object", "properties": {}},
    },
    "get_net_worth_delta": {
        "fn": _t_get_net_worth_delta,
        "description": "Month-over-month net-worth change metrics — same numbers shown on the dashboard's net worth card.",
        "parameters": {"type": "object", "properties": {}},
    },
    "get_category_rules": {
        "fn": _t_get_category_rules,
        "description": "User/system categorization rules.",
        "parameters": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 50}},
        },
    },
    "search_saved_insights": {
        "fn": _t_search_saved_insights,
        "description": "Search saved insights/decisions the user has pinned previously. Empty keywords lists recent.",
        "parameters": {
            "type": "object",
            "properties": {
                "keywords": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
    "preview_bulk_recategorize": {
        "fn": _t_preview_bulk_recategorize,
        "description": "Preview moving every transaction for a merchant to a new category. Returns a preview with a confirmation ID — the UI will ask the user to confirm before applying. Use when the user says 'recategorize all X as Y', 'move all X to Y', etc.",
        "parameters": {
            "type": "object",
            "properties": {
                "merchant": {"type": "string", "description": "Merchant name (e.g. 'Beverages & More'). Substring match."},
                "category": {"type": "string", "description": "Target category name (e.g. 'Entertainment')."},
            },
            "required": ["merchant", "category"],
        },
    },
    "preview_create_rule": {
        "fn": _t_preview_create_rule,
        "description": "Preview creating a category rule: when a transaction description matches PATTERN, auto-assign CATEGORY. Applies to past AND future matches. Use when the user says 'create a rule for X', 'always categorize X as Y'.",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Substring/regex pattern that will match transaction descriptions."},
                "category": {"type": "string", "description": "Target category name."},
            },
            "required": ["pattern", "category"],
        },
    },
    "preview_rename_merchant": {
        "fn": _t_preview_rename_merchant,
        "description": "Preview renaming a merchant (cleans up all transaction variants). Use when the user says 'rename X to Y' or 'clean up the merchant name for X'.",
        "parameters": {
            "type": "object",
            "properties": {
                "old_name": {"type": "string", "description": "Current merchant name / fragment (e.g. 'BILT PAYMENT DES:BILTRENT')."},
                "new_name": {"type": "string", "description": "Cleaned display name (e.g. 'Bilt Rent')."},
            },
            "required": ["old_name", "new_name"],
        },
    },
    "plot_chart": {
        "fn": _t_plot_chart,
        "description": "Render a chart inline in the chat. Call this AFTER fetching the underlying numbers with another tool. Use 'line' for trends over time (net worth, monthly spending), 'bar' for comparisons (top merchants, category totals), 'donut' for composition (category share of month).",
        "parameters": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["line", "bar", "donut"]},
                "title": {"type": "string"},
                "labels": {"type": "array", "items": {"type": "string"}, "description": "X-axis labels (months, merchants, categories, etc.)"},
                "values": {"type": "array", "items": {"type": "number"}, "description": "Numeric values matching labels by index"},
                "series_name": {"type": "string", "description": "Name of the data series (e.g. 'Spending', 'Net worth')"},
                "unit": {"type": "string", "enum": ["currency", "number", "percent"], "description": "How values should be formatted. Default 'currency'."},
            },
            "required": ["type", "labels", "values"],
        },
    },
    "run_sql": {
        "fn": _t_run_sql,
        "description": "Escape hatch: run a read-only SELECT against SQLite. Use only when no other tool fits. Tables: transactions_visible, accounts, categories, category_rules, merchants, net_worth_history, saved_insights.",
        "parameters": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
}


def _selected_tool_items(names: list[str] | tuple[str, ...] | set[str] | None = None):
    if not names:
        return TOOL_REGISTRY.items()
    wanted = set(names)
    return [(name, spec) for name, spec in TOOL_REGISTRY.items() if name in wanted]


def tools_for_anthropic(names: list[str] | tuple[str, ...] | set[str] | None = None) -> list[dict]:
    return [
        {"name": name, "description": spec["description"], "input_schema": spec["parameters"]}
        for name, spec in _selected_tool_items(names)
    ]


def tools_for_ollama(names: list[str] | tuple[str, ...] | set[str] | None = None) -> list[dict]:
    return [
        {"type": "function", "function": {"name": name, "description": spec["description"], "parameters": spec["parameters"]}}
        for name, spec in _selected_tool_items(names)
    ]


def execute_tool(name: str, args: dict, profile: str | None, cache: dict | None = None) -> Any:
    spec = TOOL_REGISTRY.get(name)
    if spec is None:
        return {"error": f"unknown tool: {name}"}

    cache_key = (name, json.dumps(args, sort_keys=True, default=str), profile)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    def _execute_uncached():
        try:
            with get_db() as conn:
                return spec["fn"](args or {}, profile, conn)
        except Exception as e:
            logger.exception("tool %s failed", name)
            return {"error": f"tool execution failed: {e}"}

    try:
        import copilot_cache
        result = copilot_cache.get_hot_tool_result(name, args or {}, profile, _execute_uncached)
    except Exception:
        result = _execute_uncached()

    if cache is not None:
        cache[cache_key] = result
    return result
