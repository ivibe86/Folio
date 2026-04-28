from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Any

import llm_client

VALID_DISPATCH_INTENTS = {
    "overview",
    "spending",
    "transactions",
    "chart",
    "write",
    "sql",
    "chat",
    "drilldown",  # compatibility for forced intent/debug scripts
    "error",
}
DISPATCHER_ENV = "COPILOT_USE_DISPATCHER"
LEGACY_ENV = "COPILOT_USE_LEGACY_AGENT"

_SPENDING_TOOLS = {"get_category_spend", "get_merchant_spend"}
_TRANSACTION_TOOLS = {"get_transactions", "get_transactions_for_merchant"}
_CHART_FETCH_TOOLS = {"get_monthly_spending_trend", "get_net_worth_trend"}
_WRITE_TOOLS = {"preview_bulk_recategorize", "preview_create_rule", "preview_rename_merchant"}
_SQL_TOOLS = {"run_sql"}


def dispatcher_enabled() -> bool:
    if os.getenv(LEGACY_ENV, "").strip().lower() in {"1", "true", "yes", "on"}:
        return False
    value = os.getenv(DISPATCHER_ENV)
    if value is None or not value.strip():
        return True
    return value.strip().lower() not in {"0", "false", "no", "off"}


def estimate_tokens(text: str) -> int:
    return max(1, len(text or "") // 4)


def selected_schema_tokens(tool_names: list[str] | tuple[str, ...]) -> int:
    if not tool_names:
        return 0
    try:
        import copilot_tools

        return estimate_tokens(json.dumps(copilot_tools.tools_for_ollama(tool_names), default=str))
    except Exception:
        return 0


def default_tools_for_intent(intent: str) -> list[str]:
    import copilot_agent as core

    if intent in {"drilldown", "spending"}:
        return list(core.DRILLDOWN_TOOLS)
    if intent == "transactions":
        return ["get_transactions", "get_transactions_for_merchant"]
    if intent == "chart":
        return list(core.TREND_CHART_TOOLS)
    if intent == "write":
        return list(core.WRITE_TOOLS)
    if intent == "sql":
        return ["run_sql"]
    return []


def planned_tools_for_route(route: dict) -> list[str]:
    tool_name = route.get("tool_name")
    if isinstance(tool_name, str) and tool_name:
        if tool_name == "get_monthly_spending_trend":
            return ["get_monthly_spending_trend", "plot_chart"]
        if tool_name == "get_net_worth_trend":
            return ["get_net_worth_trend", "plot_chart"]
        return [tool_name]
    return default_tools_for_intent(str(route.get("intent") or ""))


def _parse_jsonish(raw: str) -> Any:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    return json.loads(raw)


def _history_lines(history: list[dict] | None, limit: int = 4) -> str:
    if not history:
        return "(none)"
    lines: list[str] = []
    for turn in history[-limit:]:
        role = turn.get("role")
        content = " ".join((turn.get("content") or "").split())
        if role in {"user", "assistant"} and content:
            lines.append(f"{role}: {content[:260]}")
    return "\n".join(lines) or "(none)"


def _candidate_payload(question: str, profile: str | None) -> dict[str, list[str]]:
    try:
        from .drilldown import _candidate_names, _load_categories, _load_merchants

        merchants = _load_merchants(profile)
        categories = _load_categories(profile)
        return {
            "merchants": _candidate_names(question, merchants, limit=40) or merchants[:60],
            "categories": _candidate_names(question, categories, limit=30) or categories[:50],
        }
    except Exception:
        return {"merchants": [], "categories": []}


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _coerce_confidence(value: Any) -> float:
    try:
        return max(0.0, min(float(value), 1.0))
    except (TypeError, ValueError):
        return 0.0


def _has_explicit_time_scope(question: str) -> bool:
    q = (question or "").lower()
    return bool(re.search(
        r"\b(this|current|last|previous|prior|past|since|from|between|ytd|year\s+to\s+date|all\s+time|alltime|ever|lifetime|today|yesterday|week|month|year|days?)\b|\b\d{4}-\d{2}\b|\b\d{1,2}\s+(?:days?|months?|years?)\b",
        q,
    ))


def _months_since_named_month(question: str) -> int | None:
    match = re.search(
        r"\bsince\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+(20\d{2}))?\b",
        (question or "").lower(),
    )
    if not match:
        return None
    lookup = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    now = datetime.now()
    month = lookup[match.group(1)]
    year = int(match.group(2) or now.year)
    if not match.group(2) and month > now.month:
        year -= 1
    return max(1, min((now.year - year) * 12 + now.month - month + 1, 36))


def _range_override(question: str) -> str | None:
    q = (question or "").lower()
    if re.search(r"\b(all\s+time|alltime|ever|lifetime|till\s+now|until\s+now|to\s+date)\b", q):
        return "all"
    if re.search(r"\blast\s+year\b", q) and not re.search(r"\b(past|previous|prior)\s+year\b|\blast\s+12\s+months\b", q):
        return "last_year"
    if re.search(r"\b(past|previous|prior)\s+year\b|\blast\s+12\s+months\b|\bover\s+the\s+past\s+year\b", q):
        return "last_12_months"
    return None


def _looks_like_followup(question: str) -> bool:
    return bool(re.search(
        r"\b(what\s+about|how\s+about|and\s+for|now\s+for|that|those|same|it|them)\b",
        (question or "").lower(),
    ))


def _write_tool_override(question: str, tool_name: str | None, args: dict) -> tuple[str | None, dict]:
    q = (question or "").lower()
    if re.search(r"\b(always\s+categorize|auto-?categorize|future\s+transactions?|create\s+(?:a\s+)?rule)\b", q):
        pattern = args.get("pattern") or args.get("merchant") or args.get("old_name")
        category = args.get("category")
        if pattern and category:
            return "preview_create_rule", {"pattern": pattern, "category": category}
    return tool_name, args


def _is_categorization_debug(question: str) -> bool:
    return bool(
        re.search(r"\b(why|how)\s+(?:is|was|did|does)\b", question or "", re.I)
        and re.search(r"\bcategor(?:y|ized|ised|ization|isation)\b", question or "", re.I)
    )


def _clean_args(tool_name: str | None, args: Any, question: str = "") -> dict:
    if not isinstance(args, dict):
        return {}
    cleaned = {k: v for k, v in args.items() if v not in (None, "", [])}
    if tool_name == "get_transactions":
        if "limit" in cleaned:
            try:
                cleaned["limit"] = max(1, min(int(cleaned["limit"]), 50))
            except (TypeError, ValueError):
                cleaned["limit"] = 25
        if "offset" in cleaned:
            try:
                cleaned["offset"] = max(0, int(cleaned["offset"]))
            except (TypeError, ValueError):
                cleaned.pop("offset", None)
    if tool_name in {"get_category_spend", "get_merchant_spend", "get_transactions"}:
        override = _range_override(question)
        if override:
            cleaned["range"] = override
    if tool_name == "get_monthly_spending_trend":
        try:
            cleaned["months"] = max(1, min(int(cleaned.get("months") or 6), 36))
        except (TypeError, ValueError):
            cleaned["months"] = 6
        if not _has_explicit_time_scope(question):
            cleaned["months"] = 6
        since_months = _months_since_named_month(question)
        if since_months is not None:
            cleaned["months"] = since_months
    return cleaned


def _normalize_tool(intent: str, operation: str, tool_name: str | None) -> str | None:
    tool_name = (tool_name or "").strip() or None
    if tool_name:
        return tool_name
    if intent == "transactions":
        return "get_transactions"
    if intent == "chart" and operation == "net_worth_chart":
        return "get_net_worth_trend"
    if intent == "chart":
        return "get_monthly_spending_trend"
    if intent == "write" and operation == "bulk_recategorize":
        return "preview_bulk_recategorize"
    if intent == "write" and operation == "create_rule":
        return "preview_create_rule"
    if intent == "write" and operation == "rename_merchant":
        return "preview_rename_merchant"
    if intent == "sql":
        return "run_sql"
    return None


def _tool_allowed(intent: str, tool_name: str | None) -> bool:
    if not tool_name:
        return intent in {"overview", "chat", "error"}
    if intent in {"spending", "drilldown"}:
        return tool_name in _SPENDING_TOOLS
    if intent == "transactions":
        return tool_name in _TRANSACTION_TOOLS
    if intent == "chart":
        return tool_name in _CHART_FETCH_TOOLS
    if intent == "write":
        return tool_name in _WRITE_TOOLS
    if intent == "sql":
        return tool_name in _SQL_TOOLS
    return False


def _normalize_route(parsed: dict, *, question: str, raw: str, elapsed_ms: float, classifier_ms: float, error: str | None = None) -> dict:
    intent = str(parsed.get("intent") or "chat").strip().lower()
    if intent == "drilldown":
        intent = "spending"
    if intent not in VALID_DISPATCH_INTENTS:
        intent = "chat"

    operation = str(parsed.get("operation") or intent).strip().lower()
    tool_name = _normalize_tool(intent, operation, parsed.get("tool_name"))
    args = _clean_args(tool_name, parsed.get("args") or {}, question)
    if _is_categorization_debug(question):
        intent = "sql"
        operation = "categorization_debug"
        tool_name = "run_sql"
        args = {}
    uses_history = _coerce_bool(parsed.get("uses_history"))
    if tool_name in {"get_category_spend", "get_merchant_spend"}:
        if not _range_override(question) and not _has_explicit_time_scope(question) and not _looks_like_followup(question):
            args["range"] = "current_month"
            uses_history = False
    if intent == "write":
        tool_name, args = _write_tool_override(question, tool_name, args)
    confidence = _coerce_confidence(parsed.get("confidence", 0.0))
    needs_clarification = _coerce_bool(parsed.get("needs_clarification"))

    if not _tool_allowed(intent, tool_name):
        intent = "chat"
        operation = "chat"
        tool_name = None
        args = {}
        needs_clarification = False

    return {
        "intent": intent,
        "operation": operation,
        "tool_name": tool_name,
        "args": args,
        "uses_history": uses_history,
        "confidence": confidence,
        "needs_clarification": needs_clarification,
        "clarification_question": (parsed.get("clarification_question") or "").strip(),
        "shortcut": None,
        "route_ms": round(elapsed_ms, 2),
        "classifier_ms": round(classifier_ms, 2),
        "raw": raw,
        "error": error,
    }


def _forced_route(intent: str, start: float) -> dict:
    normalized = intent if intent in VALID_DISPATCH_INTENTS else "legacy"
    return {
        "intent": normalized,
        "operation": normalized,
        "tool_name": None,
        "args": {},
        "uses_history": False,
        "confidence": 1.0 if normalized != "legacy" else 0.0,
        "needs_clarification": False,
        "shortcut": "forced",
        "route_ms": round((time.perf_counter() - start) * 1000, 2),
        "classifier_ms": 0,
        "raw": None,
        "error": None if normalized != "legacy" else f"unknown forced intent: {intent}",
    }


def route_question(
    question: str,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
    profile: str | None = None,
) -> dict:
    start = time.perf_counter()
    if forced_intent:
        return _forced_route(forced_intent, start)

    if not llm_client.is_available():
        return {
            "intent": "error",
            "operation": "configuration_error",
            "tool_name": None,
            "args": {},
            "uses_history": False,
            "confidence": 1.0,
            "needs_clarification": False,
            "shortcut": None,
            "route_ms": round((time.perf_counter() - start) * 1000, 2),
            "classifier_ms": 0,
            "raw": None,
            "error": "Mira needs a configured Ollama or Anthropic provider for natural-language routing.",
        }

    candidates = _candidate_payload(question, profile)
    today = datetime.now().strftime("%Y-%m-%d")
    prompt = f"""You are Mira's routing layer inside Folio, a local-first finance app.
Classify ONLY the latest user message. Return JSON only. Do not answer the user.
Today is {today}.

Schema:
{{
  "intent": "chat|overview|spending|transactions|chart|write|sql",
  "operation": "chat|watch|category_total|merchant_total|list_transactions|monthly_spending_chart|net_worth_chart|bulk_recategorize|create_rule|rename_merchant|categorization_debug|run_sql",
  "tool_name": "get_category_spend|get_merchant_spend|get_transactions|get_monthly_spending_trend|get_net_worth_trend|preview_bulk_recategorize|preview_create_rule|preview_rename_merchant|run_sql|null",
  "args": object,
  "uses_history": boolean,
  "confidence": number,
  "needs_clarification": boolean,
  "clarification_question": string
}}

Tool contracts:
- spending/category_total -> get_category_spend args {{"category": exact category, "range": range}}
- spending/merchant_total -> get_merchant_spend args {{"merchant": merchant, "range": range}}
- transactions/list_transactions -> get_transactions args {{"limit": number, "range": optional, "category": optional, "account": optional, "search": optional}}
- chart/monthly_spending_chart -> get_monthly_spending_trend args {{"months": 1-36, "category": optional exact category}}
- chart/net_worth_chart -> get_net_worth_trend args {{"interval": "monthly", "limit": optional}}
- write/bulk_recategorize -> preview_bulk_recategorize args {{"merchant": merchant, "category": target category}}
- write/create_rule -> preview_create_rule args {{"pattern": text, "category": target category}}
- write/rename_merchant -> preview_rename_merchant args {{"old_name": merchant, "new_name": new display name}}
- sql/categorization_debug or run_sql -> run_sql only when no specific tool fits.

Routing rules:
- "latest transaction", "last transaction", "most recent transaction", "which transaction occurred last", or "just 1 transaction" is transactions/list_transactions with get_transactions limit 1.
- Transaction listing/search questions are transactions, not spending totals.
- Spending totals require "how much/spend/spent/paid/charges/expenses" plus a merchant/category subject.
- Charts require chart/plot/graph/visualize/trend or an explicit chart follow-up.
- Use history only for true follow-ups such as "what about last month?", "chart that", "show those". If the latest message names a fresh subject or operation, uses_history must be false.
- Never reuse a previous category/merchant/chart type unless uses_history is true and the latest message is a clear follow-up.
- If information is missing for a write or spend request, set needs_clarification=true.

Ranges: current_month, last_month, this_week, last_week, ytd, last_year, all, last_Nd, last_N_months, or YYYY-MM.
"past year" means last_12_months. "last year" means the previous calendar year.
For chart month counts such as "since October", count calendar months inclusively from that month through the current month.

Merchant candidates: {json.dumps(candidates["merchants"][:40], ensure_ascii=True)}
Category candidates: {json.dumps(candidates["categories"][:30], ensure_ascii=True)}

Recent context:
{_history_lines(history)}

Latest message: {question}
JSON:"""

    raw = ""
    classifier_start = time.perf_counter()
    try:
        raw = llm_client.complete(prompt, max_tokens=260, purpose="copilot")
        classifier_ms = (time.perf_counter() - classifier_start) * 1000
        parsed = _parse_jsonish(raw)
        if not isinstance(parsed, dict):
            raise ValueError("router did not return a JSON object")
        return _normalize_route(
            parsed,
            question=question,
            raw=raw,
            elapsed_ms=(time.perf_counter() - start) * 1000,
            classifier_ms=classifier_ms,
        )
    except Exception as exc:
        classifier_ms = (time.perf_counter() - classifier_start) * 1000
        return {
            "intent": "error",
            "operation": "router_error",
            "tool_name": None,
            "args": {},
            "uses_history": False,
            "confidence": 0.0,
            "needs_clarification": False,
            "shortcut": None,
            "route_ms": round((time.perf_counter() - start) * 1000, 2),
            "classifier_ms": round(classifier_ms, 2),
            "raw": raw,
            "error": f"Mira could not route that request cleanly: {exc}",
        }
