from __future__ import annotations

import json
import re
from typing import Any


_MONEY_RE = re.compile(r"(?<![\w])(?:-?\$\s?\d[\d,]*(?:\.\d{1,2})?|-?\d[\d,]*(?:\.\d{1,2})?\s?dollars)\b", re.I)
_PERCENT_RE = re.compile(r"(?<![\w])-?\d+(?:\.\d+)?\s?%")
_DATE_RE = re.compile(
    r"\b(?:\d{4}-\d{2}(?:-\d{2})?|\d{1,2}/\d{1,2}/\d{2,4}|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?)\b",
    re.I,
)
_COUNT_RE = re.compile(r"\b\d+\s+(?:transaction|transactions|row|rows|point|points|candidate|candidates|item|items)\b", re.I)
_NUMERIC_KEYS = {
    "total",
    "gross",
    "refunds",
    "amount",
    "budget",
    "actual",
    "remaining",
    "total_a",
    "total_b",
    "delta",
    "net_worth",
    "value",
    "balance",
    "current_balance",
    "available_balance",
    "monthly_total",
    "total_monthly",
    "total_annual",
}
_ENTITY_KEYS = {
    "merchant",
    "merchant_query",
    "merchant_name",
    "display_counterparty",
    "category",
    "top_level_category",
    "leaf_category",
    "account",
    "account_name",
    "institution_name",
    "description",
    "purpose",
    "subject",
}


def guard_finance_numbers(
    answer: str,
    *,
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict | None,
    profile: str | None,
) -> str:
    """Reject unsupported money figures in tool-backed finance answers."""
    answer = str(answer or "")
    trace = trace or []
    if not answer or not trace:
        return answer
    emitted = _money_values_in_text(answer)
    if not emitted:
        return answer
    supported = _supported_money_values(trace, cache or {}, profile)
    if emitted <= supported:
        return answer

    try:
        from mira import answer_composer

        deterministic = answer_composer.compose_finance_answer(route, trace, cache or {}, profile)
    except Exception:
        deterministic = None
    if deterministic:
        deterministic_values = _money_values_in_text(deterministic)
        if not deterministic_values or deterministic_values <= supported:
            return deterministic

    return (
        "I got the Folio tool result, but I won't restate a finance number that I cannot verify "
        "against the current tool output. Please ask me to rerun the exact merchant, category, and range."
    )


def guard_preserved_facts(
    original: str,
    candidate: str,
    *,
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict | None,
    profile: str | None,
) -> str:
    """Fall back if a wording layer drops protected facts from the source answer."""
    original = str(original or "")
    candidate = str(candidate or "")
    if not original or not candidate:
        return original
    required = _protected_text_tokens(original, route=route, trace=trace, cache=cache or {}, profile=profile)
    normalized_candidate = _normalize_text(candidate)
    for token in required:
        if _normalize_text(token) not in normalized_candidate:
            return original
    return candidate


def _money_values_in_text(text: str) -> set[str]:
    values: set[str] = set()
    for match in _MONEY_RE.finditer(text or ""):
        raw = match.group(0).lower().replace("dollars", "").replace("$", "").replace(",", "").strip()
        try:
            amount = round(float(raw), 2)
        except (TypeError, ValueError):
            continue
        values.add(_money_key(amount))
    return values


def _protected_text_tokens(
    text: str,
    *,
    route: dict[str, Any] | None,
    trace: list[dict[str, Any]] | None,
    cache: dict,
    profile: str | None,
) -> set[str]:
    tokens: set[str] = set()
    for regex in (_MONEY_RE, _PERCENT_RE, _DATE_RE, _COUNT_RE):
        for match in regex.finditer(text or ""):
            tokens.add(match.group(0).strip())
    _collect_route_entities(route or {}, tokens)
    for call in trace or []:
        name = call.get("name")
        args = call.get("args") if isinstance(call.get("args"), dict) else {}
        _collect_entities(args, tokens)
        result = cache.get((name, json.dumps(args, sort_keys=True, default=str), profile))
        _collect_entities(result, tokens)
    return {token for token in tokens if len(token.strip()) >= 2 and _normalize_text(token) in _normalize_text(text)}


def _collect_route_entities(route: dict[str, Any], tokens: set[str]) -> None:
    _collect_entities(route.get("args") if isinstance(route.get("args"), dict) else {}, tokens)
    action = route.get("domain_action") if isinstance(route.get("domain_action"), dict) else {}
    _collect_entities(action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}, tokens)
    for entity in action.get("grounded_entities") or []:
        if isinstance(entity, dict):
            _collect_entities(entity, tokens)


def _collect_entities(value: Any, tokens: set[str], key: str = "") -> None:
    if isinstance(value, dict):
        for child_key, child in value.items():
            _collect_entities(child, tokens, str(child_key))
        return
    if isinstance(value, list):
        for item in value:
            _collect_entities(item, tokens, key)
        return
    if key not in _ENTITY_KEYS or value in (None, "", []):
        return
    text = str(value).strip()
    if 2 <= len(text) <= 80:
        tokens.add(text)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").lower()).strip()


def _supported_money_values(trace: list[dict[str, Any]], cache: dict, profile: str | None) -> set[str]:
    values: set[str] = set()
    for call in trace:
        name = call.get("name")
        args = call.get("args") if isinstance(call.get("args"), dict) else {}
        result = cache.get((name, json.dumps(args, sort_keys=True, default=str), profile))
        _collect_numeric_values(result, values)
    expanded = set(values)
    for value in values:
        try:
            expanded.add(_money_key(abs(float(value))))
        except (TypeError, ValueError):
            pass
    return expanded


def _collect_numeric_values(value: Any, values: set[str], key: str = "") -> None:
    if isinstance(value, dict):
        for child_key, child in value.items():
            _collect_numeric_values(child, values, str(child_key))
        return
    if isinstance(value, list):
        for item in value:
            _collect_numeric_values(item, values, key)
        return
    if key not in _NUMERIC_KEYS:
        return
    try:
        values.add(_money_key(float(value or 0)))
    except (TypeError, ValueError):
        return


def _money_key(value: float) -> str:
    return f"{round(float(value), 2):.2f}"
