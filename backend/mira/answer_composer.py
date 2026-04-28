from __future__ import annotations

import json
from calendar import monthrange
from datetime import datetime
from typing import Any


NUMERIC_ACTIONS = {
    "SpendTotal",
    "TransactionSearch",
    "CompareSpend",
    "BudgetStatus",
    "MonthlyTrend",
    "NetWorthTrend",
}


def compose_finance_answer(
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict | None,
    profile: str | None,
) -> str | None:
    """Compose deterministic finance answers from completed domain action traces."""
    action = (route or {}).get("domain_action") if isinstance(route, dict) else None
    if not isinstance(action, dict) or action.get("status") != "ready":
        return None
    name = str(action.get("name") or "")
    if name not in NUMERIC_ACTIONS:
        return None
    trace = trace or []
    cache = cache or {}
    if not trace:
        return None

    calls = [(call, _result_for_call(call, cache, profile)) for call in trace]
    if name == "SpendTotal":
        return _compose_spend_total(calls[0][0], calls[0][1])
    if name == "TransactionSearch":
        return _compose_transaction_search(calls[0][0], calls[0][1])
    if name in {"CompareSpend", "BudgetStatus"}:
        return _compose_comparison(action, calls, budget_status=name == "BudgetStatus")
    if name == "MonthlyTrend":
        return _compose_monthly_trend(action, calls)
    if name == "NetWorthTrend":
        return _compose_net_worth_trend(calls)
    return None


def _result_for_call(call: dict[str, Any], cache: dict, profile: str | None) -> dict[str, Any]:
    name = call.get("name")
    args = call.get("args") or {}
    key = (name, json.dumps(args, sort_keys=True, default=str), profile)
    result = cache.get(key)
    return result if isinstance(result, dict) else {}


def _money(value: Any) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _format_amount(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _range_label(result: dict[str, Any], args: dict[str, Any] | None = None) -> str:
    args = args or {}
    label = str(result.get("range") or args.get("range") or "").strip()
    if label == "all":
        return "all time"
    if label == "current_month":
        return "this month"
    if label == "last_month":
        return "last month"
    if label.startswith("last_") and label.endswith("_months"):
        return f"the last {label[5:-7]} months"
    if label.startswith("last_") and label.endswith("d"):
        return f"the last {label[5:-1]} days"
    if label == "ytd":
        return "year to date"
    if label == "last_year":
        return "last year"
    if label:
        return label
    return "the selected period"


def _count_transactions(result: dict[str, Any]) -> int:
    for key in ("total_count", "txn_count", "total_matching_transactions", "row_count"):
        try:
            if result.get(key) is not None:
                return int(result.get(key) or 0)
        except (TypeError, ValueError):
            return 0
    for key in ("transactions", "data", "recent"):
        rows = result.get(key)
        if isinstance(rows, list):
            return len(rows)
    return 0


def _total(result: dict[str, Any]) -> float:
    try:
        return float(result.get("total") or 0)
    except (TypeError, ValueError):
        return 0.0


def _direction(delta: float) -> str:
    if abs(delta) < 0.005:
        return "flat with"
    return "above" if delta > 0 else "below"


def _compose_spend_total(call: dict[str, Any], result: dict[str, Any]) -> str | None:
    if result.get("error"):
        return f"I couldn't get a clean spend total: {result['error']}"
    args = call.get("args") if isinstance(call.get("args"), dict) else {}
    total = _money(result.get("total"))
    label = _range_label(result, args)
    count = _count_transactions(result)
    if call.get("name") == "get_category_spend":
        category = result.get("category") or args.get("category") or "that category"
        suffix = ""
        gross = result.get("gross")
        refunds = result.get("refunds")
        if gross not in (None, 0) or refunds not in (None, 0):
            suffix = f" Gross spending was {_money(gross)} and refunds were {_money(refunds)}."
        return f"You spent {total} on {category} for {label}, across {count} transaction{'s' if count != 1 else ''}.{suffix}"
    merchant = result.get("merchant_query") or args.get("merchant") or "that merchant"
    return f"You spent {total} at {merchant} for {label}, across {count} transaction{'s' if count != 1 else ''}."


def _compose_transaction_search(call: dict[str, Any], result: dict[str, Any]) -> str | None:
    if result.get("error"):
        return f"I couldn't get a clean transaction list: {result['error']}"
    args = call.get("args") if isinstance(call.get("args"), dict) else {}
    rows = result.get("transactions") if call.get("name") in {"get_transactions_for_merchant", "find_transactions"} else result.get("data")
    if not isinstance(rows, list) or not rows:
        return "I couldn't find matching transactions."

    total_count = _count_transactions(result) or len(rows)
    first = rows[0]
    desc = first.get("description") or first.get("merchant_display_name") or first.get("merchant_name") or "Unknown transaction"
    date = first.get("date") or "unknown date"
    amount = _format_amount(first.get("amount"))
    category = first.get("category") or "Uncategorized"
    account = first.get("account_name")
    account_part = f" from {account}" if account else ""

    try:
        limit = int(args.get("limit") or len(rows) or 0)
    except (TypeError, ValueError):
        limit = len(rows)
    if limit == 1:
        return f"Your latest transaction is {desc} on {date} for {amount}, categorized as {category}{account_part}."
    if call.get("name") == "get_transactions_for_merchant":
        merchant = args.get("merchant") or result.get("merchant") or "that merchant"
        return (
            f"I found {total_count} matching transaction{'s' if total_count != 1 else ''} for {merchant}. "
            f"The most recent is {desc} on {date} for {amount}."
        )
    return (
        f"I found {total_count} matching transaction{'s' if total_count != 1 else ''}. "
        f"The most recent is {desc} on {date} for {amount}."
    )


def _compose_comparison(
    action: dict[str, Any],
    calls: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    budget_status: bool,
) -> str | None:
    semantic_compare = next((result for call, result in calls if call.get("name") == "compare_periods"), None)
    if isinstance(semantic_compare, dict):
        subject = semantic_compare.get("subject") or "that subject"
        total_a = float(semantic_compare.get("total_a") or 0)
        total_b = float(semantic_compare.get("total_b") or 0)
        delta = total_a - total_b
        return (
            f"For {subject}, {semantic_compare.get('range_a') or 'the first period'} is {_money(total_a)}, "
            f"versus {_money(total_b)} for {semantic_compare.get('range_b') or 'the comparison period'}. "
            f"That is {_money(abs(delta))} {_direction(delta)} the comparison period."
        )

    semantic_budget = next((result for call, result in calls if call.get("name") == "get_budget_status"), None)
    if isinstance(semantic_budget, dict):
        category = semantic_budget.get("category") or "that category"
        if not semantic_budget.get("has_budget"):
            return f"I don't see a budget set for {category}."
        remaining = float(semantic_budget.get("remaining") or 0)
        direction = "left" if remaining >= 0 else "over"
        return (
            f"For {category}, the budget is {_money(semantic_budget.get('budget'))}, "
            f"actual spend is {_money(semantic_budget.get('actual'))}, so you are "
            f"{_money(abs(remaining))} {direction} for {_range_label(semantic_budget)}."
        )

    semantic_subject = next((result for call, result in calls if call.get("name") == "analyze_subject"), None)
    if isinstance(semantic_subject, dict):
        subject = semantic_subject.get("subject") or "that subject"
        total = float(semantic_subject.get("total") or 0)
        count = int(semantic_subject.get("count") or 0)
        budget = semantic_subject.get("budget") if isinstance(semantic_subject.get("budget"), dict) else None
        budget_part = ""
        if budget and budget.get("has_budget"):
            remaining = float(budget.get("remaining") or 0)
            budget_part = f" Budget remaining is {_money(remaining)}."
        return f"For {subject}, spend is {_money(total)} for {_range_label(semantic_subject)}, across {count} transaction(s).{budget_part}"

    spend_calls = [(call, result) for call, result in calls if call.get("name") in {"get_category_spend", "get_merchant_spend"}]
    if len(spend_calls) < 2:
        return None
    current_call, current = spend_calls[0]
    comparison_call, comparison = spend_calls[1]
    if current.get("error") or comparison.get("error"):
        return None

    slots = action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}
    subject = slots.get("subject") or _subject_label(current_call.get("args") or {})
    current_total = _total(current)
    current_count = _count_transactions(current)
    comparison_total = _total(comparison)
    comparison_count = _count_transactions(comparison)
    kind = str(slots.get("plan_kind") or "")

    if kind == "current_vs_previous":
        delta = current_total - comparison_total
        return (
            f"For {subject}, this month is {_money(current_total)} across {current_count} transaction(s), "
            f"versus {_money(comparison_total)} last month across {comparison_count} transaction(s). "
            f"That is {_money(abs(delta))} {_direction(delta)} last month."
        )

    try:
        months = max(1, min(int(slots.get("months") or 6), 12))
    except (TypeError, ValueError):
        months = 6
    prior_total = max(0.0, comparison_total - current_total)
    prior_average = prior_total / months
    delta = current_total - prior_average

    if not budget_status:
        return (
            f"For {subject}, this month is {_money(current_total)} across {current_count} transaction(s). "
            f"The prior {months}-month average is {_money(prior_average)} per month, so this month is "
            f"{_money(abs(delta))} {_direction(delta)} that average."
        )

    today = datetime.now().date()
    days_in_month = monthrange(today.year, today.month)[1]
    day = max(1, min(today.day, days_in_month))
    projected = current_total / day * days_in_month
    projected_delta = projected - prior_average
    return (
        f"For {subject}, you have spent {_money(current_total)} so far this month across {current_count} transaction(s). "
        f"At day {day} of {days_in_month}, that pace projects to about {_money(projected)} for the month. "
        f"Your prior {months}-month average is {_money(prior_average)}, so you are pacing "
        f"{_money(abs(projected_delta))} {_direction(projected_delta)} that average."
    )


def _compose_monthly_trend(action: dict[str, Any], calls: list[tuple[dict[str, Any], dict[str, Any]]]) -> str | None:
    result = next((result for call, result in calls if call.get("name") == "get_monthly_spending_trend"), {})
    if result.get("error"):
        return f"I couldn't get a clean monthly trend: {result['error']}"
    values = [float(v or 0) for v in result.get("values") or []]
    labels = [str(label) for label in result.get("labels") or []]
    if not values:
        return "I couldn't find matching spending trend data."
    latest = values[-1]
    latest_label = labels[-1] if labels else "the latest month"
    average = sum(values) / len(values)
    peak_idx = max(range(len(values)), key=lambda idx: values[idx])
    peak_label = labels[peak_idx] if labels else "the peak month"
    slots = action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}
    category = slots.get("category") or result.get("category")
    subject = f"{category} spending" if category else "spending"
    return (
        f"For {subject}, {latest_label} is {_money(latest)}. "
        f"The {len(values)}-month average is {_money(average)}, and the peak was {peak_label} at {_money(values[peak_idx])}."
    )


def _compose_net_worth_trend(calls: list[tuple[dict[str, Any], dict[str, Any]]]) -> str | None:
    result = next((result for call, result in calls if call.get("name") == "get_net_worth_trend"), {})
    if result.get("error"):
        return f"I couldn't get a clean net worth trend: {result['error']}"
    series = result.get("series") if isinstance(result.get("series"), list) else []
    if not series:
        return "I couldn't find net worth trend points."
    first = series[0]
    latest = series[-1]
    first_value = _series_value(first)
    latest_value = _series_value(latest)
    delta = latest_value - first_value
    first_label = first.get("date") or first.get("month") or "the first point"
    latest_label = latest.get("date") or latest.get("month") or "the latest point"
    return (
        f"Your latest net worth point is {_money(latest_value)} on {latest_label}. "
        f"Across {len(series)} point(s), that is {_money(abs(delta))} {_direction(delta)} {first_label}."
    )


def _series_value(row: dict[str, Any]) -> float:
    for key in ("net_worth", "value", "total", "balance"):
        try:
            if row.get(key) is not None:
                return float(row.get(key) or 0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _subject_label(args: dict[str, Any]) -> str:
    return str(args.get("category") or args.get("merchant") or "that area")
