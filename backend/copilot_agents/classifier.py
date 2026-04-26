from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import llm_client

VALID_DISPATCH_INTENTS = {"overview", "drilldown", "chart", "write", "sql", "chat"}
DISPATCHER_ENV = "COPILOT_USE_DISPATCHER"
LEGACY_ENV = "COPILOT_USE_LEGACY_AGENT"


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

    if intent == "drilldown":
        return list(core.DRILLDOWN_TOOLS)
    if intent == "chart":
        return list(core.TREND_CHART_TOOLS)
    if intent == "write":
        return list(core.WRITE_TOOLS)
    if intent == "sql":
        return ["run_sql"]
    return []


def planned_tools_for_route(route: dict) -> list[str]:
    if route.get("shortcut") in {"watch", "chart_net_worth", "chart_spending", "drilldown_spend", "drilldown_followup"}:
        return []
    return default_tools_for_intent(str(route.get("intent") or ""))


def _parse_jsonish(raw: str) -> Any:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    return json.loads(raw)


def fallback_intent(question: str) -> str:
    import copilot_agent as core

    q = (question or "").lower()
    if re.search(r"\b(plot|chart|graph|visualize)\b", q):
        return "chart"
    if re.search(r"\b(move all|recategorize|reclassify|rename|create (?:a )?rule|always categorize)\b", q):
        return "write"
    if re.search(r"\b(why|how)\s+(?:is|was)\b", q) and re.search(r"\bcategor(?:y|ized|ised|ization|isation)\b", q):
        return "sql"
    if re.search(r"\b(how much|spend|spent|paid|pay|charges?|expenses?|transactions?)\b", q):
        return "drilldown"
    words = re.findall(r"\w+", question or "")
    if len(words) <= 6 and not core._looks_financial(question):
        return "chat"
    if core._looks_financial(question):
        return "overview"
    return "chat"


def conservative_shortcut(question: str, history: list[dict] | None = None) -> dict | None:
    import copilot_agent as core

    q = (question or "").lower().strip()
    if core._is_watch_question(question):
        return {"intent": "overview", "shortcut": "watch"}
    if re.search(r"\b(plot|chart|graph|visualize)\b", q) and re.search(r"\b(spending|spent|expenses?|groceries|grocery|dining|shopping|travel|subscriptions?)\b", q):
        return {"intent": "chart", "shortcut": "chart_spending"}
    if re.search(r"\b(plot|chart|graph|visualize|show)\b", q) and re.search(r"\bnet\s*worth|networth\b", q) and not re.search(r"\bnot\s+(?:my\s+)?(?:net\s*worth|networth)\b", q):
        return {"intent": "chart", "shortcut": "chart_net_worth"}
    if re.search(r"\b(move all|recategorize|reclassify|rename|create (?:a )?rule|always categorize)\b", q):
        return {"intent": "write", "shortcut": "write"}
    if re.search(r"\b(why|how)\s+(?:is|was)\b", q) and re.search(r"\bcategor(?:y|ized|ised|ization|isation)\b", q):
        return {"intent": "sql", "shortcut": "sql_categorization_debug"}
    try:
        from .drilldown import looks_like_drilldown_shortcut

        if looks_like_drilldown_shortcut(question):
            return {"intent": "drilldown", "shortcut": "drilldown_spend"}
    except Exception:
        pass
    if re.search(r"\b(what\s+about|how\s+about|and\s+for|now\s+for)\b", q):
        recent = core._history_text(history, limit=4).lower()
        if re.search(r"\b(spend|spent|paid|merchant|category|groceries|costco|transactions?)\b", recent):
            return {"intent": "drilldown", "shortcut": "drilldown_followup"}
    return None


def route_question(question: str, history: list[dict] | None = None, forced_intent: str | None = None) -> dict:
    import copilot_agent as core

    start = time.perf_counter()
    if forced_intent:
        intent = forced_intent if forced_intent in VALID_DISPATCH_INTENTS else "legacy"
        return {
            "intent": intent,
            "shortcut": "forced",
            "route_ms": round((time.perf_counter() - start) * 1000, 2),
            "classifier_ms": 0,
            "raw": None,
            "error": None if intent != "legacy" else f"unknown forced intent: {forced_intent}",
        }

    shortcut = conservative_shortcut(question, history)
    if shortcut:
        return {
            **shortcut,
            "route_ms": round((time.perf_counter() - start) * 1000, 2),
            "classifier_ms": 0,
            "raw": None,
            "error": None,
        }

    if not llm_client.is_available():
        intent = fallback_intent(question)
        return {
            "intent": intent,
            "shortcut": "fallback_no_llm",
            "route_ms": round((time.perf_counter() - start) * 1000, 2),
            "classifier_ms": 0,
            "raw": None,
            "error": "llm unavailable",
        }

    recent = core._history_text(history, limit=4)
    prompt = f"""Classify the latest message into one intent.

Intents:
overview: broad finance summary, watch/worry/runway/monthly check-in
drilldown: specific merchant/category/transaction/list amount question
chart: plot, graph, visualize, trend
write: recategorize, rename merchant, create rule, change data
sql: reconciliation/debug/why-is/arbitrary database read
chat: greeting or non-finance conversation

Return JSON only: {{"intent":"overview|drilldown|chart|write|sql|chat"}}

Recent context:
{recent or "(none)"}

Latest message: {question}
JSON:"""
    classifier_start = time.perf_counter()
    raw = ""
    try:
        raw = llm_client.complete(prompt, max_tokens=8, purpose="copilot")
        classifier_ms = round((time.perf_counter() - classifier_start) * 1000, 2)
        parsed = _parse_jsonish(raw)
        intent = parsed.get("intent") if isinstance(parsed, dict) else None
        if intent not in VALID_DISPATCH_INTENTS:
            raise ValueError(f"invalid intent: {intent}")
        error = None
        shortcut_name = None
    except Exception as exc:
        classifier_ms = round((time.perf_counter() - classifier_start) * 1000, 2)
        intent = fallback_intent(question)
        error = str(exc)
        shortcut_name = "fallback_parse"

    return {
        "intent": intent,
        "shortcut": shortcut_name,
        "route_ms": round((time.perf_counter() - start) * 1000, 2),
        "classifier_ms": classifier_ms,
        "raw": raw,
        "error": error,
    }
