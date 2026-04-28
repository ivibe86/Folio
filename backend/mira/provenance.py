from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any


LEDGER_ACTIONS = {
    "SpendTotal",
    "TransactionSearch",
    "CompareSpend",
    "BudgetStatus",
    "MonthlyTrend",
    "NetWorthTrend",
}
_LEDGER_TTL_SECONDS = 30 * 60
_LEDGER_BY_ID: dict[str, dict[str, Any]] = {}
_LATEST_BY_PROFILE: dict[str, str] = {}


def record_completed_action(
    *,
    profile: str | None,
    question: str,
    answer: str,
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict | None,
) -> dict[str, Any] | None:
    action = (route or {}).get("domain_action") if isinstance(route, dict) else None
    if not isinstance(action, dict) or action.get("status") != "ready":
        return None
    action_name = str(action.get("name") or "")
    if action_name not in LEDGER_ACTIONS:
        return None
    trace = trace or []
    if not trace:
        return None

    tool_calls = []
    all_ranges: list[dict[str, Any]] = []
    all_samples: list[str] = []
    total_rows = 0
    for call in trace:
        result = _result_for_call(call, cache or {}, profile)
        tool_record = _tool_record(call, result, action_name)
        tool_calls.append(tool_record)
        total_rows += int(tool_record.get("row_count") or 0)
        for item in tool_record.get("sample_transaction_ids") or []:
            if item not in all_samples:
                all_samples.append(item)
        date_range = tool_record.get("date_range")
        if isinstance(date_range, dict) and date_range not in all_ranges:
            all_ranges.append(date_range)

    entry = {
        "version": 1,
        "id": _entry_id(profile, question, action_name, trace),
        "profile": profile or "household",
        "created_at": time.time(),
        "question": str(question or ""),
        "answer": str(answer or ""),
        "action": action_name,
        "grounded_entities": copy.deepcopy(action.get("grounded_entities") or []),
        "validated_slots": copy.deepcopy(action.get("validated_slots") or {}),
        "date_ranges": all_ranges,
        "filters": _entry_filters(action, tool_calls),
        "tool_calls": tool_calls,
        "row_count": total_rows,
        "sample_transaction_ids": all_samples[:8],
        "calculation_basis": _action_calculation_basis(action_name, action, tool_calls),
    }
    _LEDGER_BY_ID[entry["id"]] = entry
    _LATEST_BY_PROFILE[_profile_key(profile)] = entry["id"]
    _prune()
    return public_entry(entry)


def attach_completed_action(
    result: dict[str, Any],
    *,
    profile: str | None,
    question: str,
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict | None,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result
    entry = record_completed_action(
        profile=profile,
        question=question,
        answer=str(result.get("answer") or ""),
        route=route,
        trace=trace,
        cache=cache,
    )
    if entry:
        result["provenance"] = entry
    return result


def public_entry(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    return {
        "version": entry.get("version") or 1,
        "id": entry.get("id"),
        "action": entry.get("action"),
        "grounded_entities": copy.deepcopy(entry.get("grounded_entities") or []),
        "validated_slots": copy.deepcopy(entry.get("validated_slots") or {}),
        "date_ranges": copy.deepcopy(entry.get("date_ranges") or []),
        "filters": copy.deepcopy(entry.get("filters") or {}),
        "tool_calls": copy.deepcopy(entry.get("tool_calls") or []),
        "row_count": entry.get("row_count") or 0,
        "sample_transaction_ids": copy.deepcopy(entry.get("sample_transaction_ids") or []),
        "calculation_basis": entry.get("calculation_basis") or "",
    }


def latest_entry(profile: str | None, context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    _prune()
    entry_id = None
    if isinstance(context, dict):
        entry_id = context.get("provenance_id")
    if not entry_id:
        entry_id = _LATEST_BY_PROFILE.get(_profile_key(profile))
    entry = _LEDGER_BY_ID.get(str(entry_id or ""))
    return entry if _is_fresh(entry) else None


def explain_last_answer(profile: str | None, context: dict[str, Any] | None = None) -> str | None:
    entry = latest_entry(profile, context)
    if not entry:
        return None
    action = entry.get("action") or "finance action"
    tools = entry.get("tool_calls") or []
    tool_bits = []
    for tool in tools[:4]:
        args = tool.get("args") or {}
        row_count = int(tool.get("row_count") or 0)
        sample_ids = tool.get("sample_transaction_ids") or []
        sample_part = f"; sample transaction IDs: {', '.join(sample_ids[:3])}" if sample_ids else ""
        tool_bits.append(f"{tool.get('name')}({json.dumps(args, sort_keys=True, default=str)}) returned {row_count} row(s){sample_part}")
    grounding = _grounding_summary(entry.get("grounded_entities") or [])
    ranges = _range_summary(entry.get("date_ranges") or [])
    basis = entry.get("calculation_basis") or "The answer was computed from Folio tool results."
    parts = [f"I used the provenance ledger for the last finance answer: {action}."]
    if grounding:
        parts.append(f"Grounding: {grounding}.")
    if ranges:
        parts.append(f"Date range: {ranges}.")
    if tool_bits:
        parts.append("Tool trace: " + " ".join(tool_bits) + ".")
    parts.append(f"Calculation basis: {basis}")
    return " ".join(parts)


def clear(profile: str | None = None) -> None:
    if profile is None:
        _LEDGER_BY_ID.clear()
        _LATEST_BY_PROFILE.clear()
        return
    key = _profile_key(profile)
    _LATEST_BY_PROFILE.pop(key, None)
    for entry_id, entry in list(_LEDGER_BY_ID.items()):
        if entry.get("profile") == key:
            _LEDGER_BY_ID.pop(entry_id, None)


def _result_for_call(call: dict[str, Any], cache: dict, profile: str | None) -> dict[str, Any]:
    name = call.get("name")
    args = call.get("args") if isinstance(call.get("args"), dict) else {}
    key = (name, json.dumps(args, sort_keys=True, default=str), profile)
    result = cache.get(key)
    return result if isinstance(result, dict) else {}


def _tool_record(call: dict[str, Any], result: dict[str, Any], action_name: str) -> dict[str, Any]:
    args = copy.deepcopy(call.get("args") if isinstance(call.get("args"), dict) else {})
    name = str(call.get("name") or "")
    semantic = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    samples = _sample_transaction_ids(result)
    if semantic.get("sample_transaction_ids"):
        samples = list(semantic.get("sample_transaction_ids") or [])[:5]
    return {
        "name": name,
        "args": args,
        "duration_ms": call.get("duration_ms"),
        "date_range": {
            "range": semantic.get("range"),
            "start": semantic.get("start"),
            "end": semantic.get("end"),
        } if semantic else _date_range(args, result),
        "filters": copy.deepcopy(semantic.get("filters") or {}) if semantic else _filters(args),
        "row_count": int(semantic.get("row_count") or 0) if semantic else _row_count(name, result),
        "sample_transaction_ids": samples,
        "calculation_basis": semantic.get("calculation_basis") or _tool_calculation_basis(name, action_name),
    }


def _date_range(args: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    return {
        "range": result.get("range") or args.get("range") or args.get("month") or "all",
        "start": result.get("start"),
        "end": result.get("end"),
    }


def _filters(args: dict[str, Any]) -> dict[str, Any]:
    excluded = {"range", "month", "offset"}
    return {k: copy.deepcopy(v) for k, v in args.items() if k not in excluded and v not in (None, "", [])}


def _entry_filters(action: dict[str, Any], tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    validated = action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}
    for key in ("merchant", "category", "subject_type", "subject", "plan_kind", "months", "interval", "limit"):
        if validated.get(key) not in (None, "", []):
            filters[key] = copy.deepcopy(validated.get(key))
    for call in tool_calls:
        for key, value in (call.get("filters") or {}).items():
            filters.setdefault(key, copy.deepcopy(value))
    return filters


def _row_count(name: str, result: dict[str, Any]) -> int:
    for key in ("total_count", "txn_count", "total_matching_transactions", "active_count"):
        try:
            if result.get(key) is not None:
                return int(result.get(key) or 0)
        except (TypeError, ValueError):
            return 0
    if name == "plot_chart":
        values = result.get("values")
        if isinstance(values, list):
            return len(values)
        series = result.get("series")
        if isinstance(series, list):
            return len(series)
    for key in ("transactions", "data", "recent", "series", "values", "items", "categories", "merchants"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def _sample_transaction_ids(result: dict[str, Any]) -> list[str]:
    samples: list[str] = []
    for key in ("recent", "transactions", "data"):
        rows = result.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            tx_id = row.get("original_id") or row.get("id") or row.get("transaction_id")
            if tx_id and str(tx_id) not in samples:
                samples.append(str(tx_id))
            if len(samples) >= 5:
                return samples
    return samples


def _tool_calculation_basis(name: str, action_name: str) -> str:
    if name == "get_category_spend":
        return "Dashboard category aggregation for spending transactions in the requested range, with gross/refund details when available."
    if name == "get_merchant_spend":
        return "Merchant spending total from matching merchant identity and descriptions, counting negative spending transactions and excluding transfers/income."
    if name == "get_transactions":
        return "Transactions-page query with the requested filters, ordered by most recent transactions."
    if name == "get_transactions_for_merchant":
        return "Canonical merchant-key transaction lookup, ordered by most recent transactions."
    if name == "find_transactions":
        return "Semantic transaction search using Folio's transactions-page filters, with row count and sample transaction IDs."
    if name == "analyze_subject":
        return "Semantic subject analysis assembled from deterministic spend, transaction, trend, and budget tools where available."
    if name == "compare_periods":
        return "Semantic period comparison assembled from deterministic spend totals for two validated ranges."
    if name == "get_budget_status":
        return "Semantic budget status from category budget settings plus deterministic category spend."
    if name in {"get_dashboard_snapshot", "explain_metric", "get_recurring_changes"}:
        return "Dashboard-level semantic tool result with compact provenance embedded in the tool payload."
    if name == "get_monthly_spending_trend":
        return "Monthly spending totals grouped by month, excluding transfer and income categories, with an optional category filter."
    if name == "get_net_worth_trend":
        return "Net worth time series from Folio's account and balance history data."
    if name == "plot_chart":
        return "Chart rendering from the already computed finance series; it does not change the finance calculation."
    return f"Tool result used by {action_name}."


def _action_calculation_basis(action_name: str, action: dict[str, Any], tool_calls: list[dict[str, Any]]) -> str:
    if action_name == "SpendTotal":
        return "The answer uses one spend tool total and its matching transaction count for the grounded merchant or category."
    if action_name == "TransactionSearch":
        return "The answer uses the transaction lookup result, row count, and most recent matching row."
    if action_name == "CompareSpend":
        return "The answer compares deterministic spend totals from the current range and comparison range, then computes the difference or average in Python."
    if action_name == "BudgetStatus":
        return "The answer compares current-month spend pace against a prior-month average computed from deterministic spend totals."
    if action_name == "MonthlyTrend":
        return "The answer summarizes the monthly spending series returned by the trend tool."
    if action_name == "NetWorthTrend":
        return "The answer summarizes the net worth series returned by the net worth trend tool."
    return "The answer was computed from Folio tool results."


def _grounding_summary(entities: list[dict[str, Any]]) -> str:
    parts = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        label = entity.get("display_name") or entity.get("value")
        entity_type = entity.get("entity_type") or "entity"
        kind = entity.get("kind") or "grounded"
        if label:
            parts.append(f"{entity_type} {label} ({kind})")
    return ", ".join(parts)


def _range_summary(ranges: list[dict[str, Any]]) -> str:
    parts = []
    for item in ranges:
        if not isinstance(item, dict):
            continue
        label = str(item.get("range") or "all")
        start = item.get("start")
        end = item.get("end")
        if start or end:
            parts.append(f"{label} ({start or 'beginning'} to {end or 'today'})")
        else:
            parts.append(label)
    return ", ".join(parts)


def _entry_id(profile: str | None, question: str, action_name: str, trace: list[dict[str, Any]]) -> str:
    raw = json.dumps(
        {
            "profile": profile or "household",
            "question": question,
            "action": action_name,
            "trace": trace,
            "ts": time.time(),
        },
        sort_keys=True,
        default=str,
    )
    return "prov_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _profile_key(profile: str | None) -> str:
    return profile or "household"


def _is_fresh(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    try:
        return (time.time() - float(entry.get("created_at") or 0)) <= _LEDGER_TTL_SECONDS
    except (TypeError, ValueError):
        return False


def _prune() -> None:
    stale = [entry_id for entry_id, entry in _LEDGER_BY_ID.items() if not _is_fresh(entry)]
    for entry_id in stale:
        _LEDGER_BY_ID.pop(entry_id, None)
    for profile, entry_id in list(_LATEST_BY_PROFILE.items()):
        if entry_id not in _LEDGER_BY_ID:
            _LATEST_BY_PROFILE.pop(profile, None)
