from __future__ import annotations

import json
import re
import time
from datetime import datetime
from typing import Any

import llm_client
from mira import controller as conversation_controller
from mira.context_policy import apply_turn_state_context_route
from mira.dialogue_routes import (
    _answer_context_followup_route,
    _answer_context_from_history,
    _confirmed_merchant_route,
    _context_direct_route,
    _context_policy_callbacks,
    _dialogue_state_from_history,
    _dialogue_state_route,
    _is_merchant_selection_prompt,
    _plain_history_finance_context,
)
from mira.fast_paths import (
    _category_spend_shortcut,
    _chart_shortcut,
    _ground_untrusted_subject_route,
    _looks_like_followup,
    _merchant_confirmation_route,
    _merchant_spend_shortcut,
    _missing_spend_subject_route,
    _planner_shortcut,
    _postprocess_route,
    _shortcut_base,
    _transaction_shortcut,
    _unresolved_finance_comparison,
    _write_budget_shortcut,
    _write_recategorize_shortcut,
    _write_rename_shortcut,
    _write_rule_shortcut,
)
from mira.router_prompt import build_router_prompt
from mira import cashflow_forecast
from mira import memory_v2
from range_parser import chart_months, contains, has_explicit_time_scope, parse_range, words

VALID_DISPATCH_INTENTS = {
    "overview",
    "spending",
    "transactions",
    "chart",
    "write",
    "plan",
    "chat",
    "memory",
    "drilldown",  # compatibility for forced intent/debug scripts
    "error",
}
_SPENDING_TOOLS = {"get_category_spend", "get_merchant_spend"}
_TRANSACTION_TOOLS = {"get_transactions", "get_transactions_for_merchant"}
_CHART_FETCH_TOOLS = {"get_monthly_spending_trend", "get_net_worth_trend"}
_SEMANTIC_TOOLS = {
    "get_dashboard_snapshot",
    "analyze_subject",
    "compare_periods",
    "get_budget_status",
    "find_transactions",
    "explain_metric",
    "get_recurring_changes",
    "get_data_health_summary",
    "get_cashflow_forecast",
    "predict_shortfall",
    "check_affordability",
    "remember_user_context",
    "retrieve_relevant_memories",
    "update_memory",
    "forget_memory",
    "list_mira_memories",
    "find_low_confidence_transactions",
    "explain_transaction_enrichment",
    "get_enrichment_quality_summary",
}
_WRITE_TOOLS = {
    "preview_bulk_recategorize",
    "preview_create_rule",
    "preview_rename_merchant",
    "preview_set_budget",
    "preview_create_goal",
    "preview_update_goal_target",
    "preview_mark_goal_funded",
    "preview_set_transaction_note",
    "preview_set_transaction_tags",
    "preview_mark_reviewed",
    "preview_bulk_mark_reviewed",
    "preview_update_manual_account_balance",
    "preview_split_transaction",
    "preview_confirm_recurring_obligation",
    "preview_dismiss_recurring_obligation",
    "preview_cancel_recurring",
    "preview_restore_recurring",
}
_PROVIDER_NAMES = {
    "anthropic", "claude", "openai", "chatgpt", "gpt", "gemini", "sonnet", "opus", "haiku"
}
_PROVIDER_ACTIONS = {
    "switch", "change", "use", "using", "set", "configure", "select", "provider", "model", "llm", "cloud"
}
_ACK_TOKENS = {"thanks", "thank", "thx", "cool", "ok", "okay", "gotcha", "nice", "great"}
_SENSITIVE_SUPPORT_RE = re.compile(
    r"\b("
    r"scared|terrified|worried|anxious|panic|panicking|drowning|ashamed|"
    r"can't\s+pay|cannot\s+pay|cant\s+pay|won't\s+be\s+able\s+to\s+pay|"
    r"overdraft|overdrawn|evict|eviction|can't\s+afford|cannot\s+afford"
    r")\b",
    re.I,
)


def dispatcher_enabled() -> bool:
    return True


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
        return ["find_transactions", "get_transactions", "get_transactions_for_merchant"]
    if intent == "chart":
        return list(core.TREND_CHART_TOOLS)
    if intent == "write":
        return list(core.WRITE_TOOLS)
    if intent == "plan":
        return [
            "analyze_subject",
            "compare_periods",
            "get_budget_status",
            "get_category_spend",
            "get_merchant_spend",
            "get_monthly_spending_trend",
            "get_transactions",
            "get_recurring_summary",
            "get_net_worth_trend",
            "get_net_worth_delta",
            "get_cashflow_forecast",
            "predict_shortfall",
            "check_affordability",
        ]
    if intent == "overview":
        return [
            "get_dashboard_snapshot",
            "explain_metric",
            "get_recurring_changes",
            "get_data_health_summary",
            "get_cashflow_forecast",
            "predict_shortfall",
            "find_low_confidence_transactions",
            "explain_transaction_enrichment",
            "get_enrichment_quality_summary",
        ]
    if intent == "memory":
        return [
            "remember_user_context",
            "retrieve_relevant_memories",
            "update_memory",
            "forget_memory",
            "list_mira_memories",
        ]
    return []


def planned_tools_for_route(route: dict) -> list[str]:
    tool_name = route.get("tool_name")
    if isinstance(tool_name, str) and tool_name:
        if tool_name == "run_sql":
            return []
        if tool_name == "get_monthly_spending_trend":
            return ["get_monthly_spending_trend", "plot_chart"]
        if tool_name == "get_net_worth_trend":
            return ["get_net_worth_trend", "plot_chart"]
        return [tool_name]
    return default_tools_for_intent(str(route.get("intent") or ""))


def answer_context_for_route(
    route: dict | None,
    trace: list[dict] | None = None,
    provenance: dict | None = None,
) -> dict | None:
    route = route or {}
    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    trace = trace or []
    subject_type = ""
    subject = ""
    tool_name = str(route.get("tool_name") or "")

    if route.get("intent") == "plan":
        subject_type = str(args.get("subject_type") or "")
        subject = str(args.get("subject") or "")
        if tool_name == "check_affordability" and args.get("category"):
            subject_type = "category"
            subject = str(args.get("category") or "")
    elif tool_name == "get_merchant_spend":
        subject_type = "merchant"
        subject = str(args.get("merchant") or "")
    elif tool_name == "get_category_spend":
        subject_type = "category"
        subject = str(args.get("category") or "")
    elif tool_name == "get_transactions_for_merchant":
        subject_type = "merchant"
        subject = str(args.get("merchant") or "")
    elif tool_name == "get_monthly_spending_trend" and args.get("category"):
        subject_type = "category"
        subject = str(args.get("category") or "")

    if not subject and trace:
        first_args = trace[0].get("args") if isinstance(trace[0].get("args"), dict) else {}
        if trace[0].get("name") == "get_merchant_spend":
            subject_type = "merchant"
            subject = str(first_args.get("merchant") or "")
        elif trace[0].get("name") == "get_category_spend":
            subject_type = "category"
            subject = str(first_args.get("category") or "")
        elif trace[0].get("name") == "get_transactions_for_merchant":
            subject_type = "merchant"
            subject = str(first_args.get("merchant") or "")
        elif trace[0].get("name") == "get_monthly_spending_trend" and first_args.get("category"):
            subject_type = "category"
            subject = str(first_args.get("category") or "")

    if subject_type not in {"merchant", "category"} or not subject:
        if not isinstance(provenance, dict) or not provenance.get("id"):
            return None
        grounded = provenance.get("grounded_entities") if isinstance(provenance.get("grounded_entities"), list) else []
        first_entity = grounded[0] if grounded and isinstance(grounded[0], dict) else {}
        subject_type = str(first_entity.get("entity_type") or "")
        subject = str(first_entity.get("display_name") or first_entity.get("value") or "")

    if subject_type not in {"merchant", "category"} and not (isinstance(provenance, dict) and provenance.get("id")):
        return None

    ranges: list[str] = []
    tools: list[dict] = []
    for call in trace:
        call_args = call.get("args") if isinstance(call.get("args"), dict) else {}
        tool = str(call.get("name") or "")
        if tool:
            tools.append({"name": tool, "args": call_args})
        range_token = str(call_args.get("range") or "").strip()
        if range_token and range_token not in ranges:
            ranges.append(range_token)

    context = {
        "version": 1,
        "kind": "finance_answer_context",
        "subject_type": subject_type,
        "subject": subject,
        "intent": route.get("intent"),
        "operation": route.get("operation"),
        "tool_name": tool_name or (tools[0]["name"] if tools else None),
        "ranges": ranges,
        "tools": tools[:4],
    }
    if isinstance(provenance, dict) and provenance.get("id"):
        context["provenance_id"] = provenance.get("id")
        context["provenance_action"] = provenance.get("action")
    return context


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


def _looks_like_provider_config_request(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    if not (token_set & _PROVIDER_NAMES):
        return False
    if token_set & _PROVIDER_ACTIONS:
        return True
    return contains(tokens, ("can", "you")) and contains(tokens, ("to", "anthropic"))


def _looks_like_grounding_question(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    if (
        contains(tokens, ("how", "did", "you", "get", "that"))
        or contains(tokens, ("where", "did", "that", "come", "from"))
        or contains(tokens, ("where", "did", "you", "get", "that"))
    ):
        return True
    asks_how = "how" in token_set or contains(tokens, ("where", "did"))
    info_terms = {"information", "info", "numbers", "number", "data", "source", "sources", "transactions", "proof"}
    get_terms = {"get", "got", "find", "derive", "calculate", "computed", "answer"}
    return asks_how and bool(token_set & info_terms) and bool(token_set & get_terms)


def _looks_like_sensitive_support_turn(question: str) -> bool:
    tokens = set(words(question))
    sensitive_topic = bool(tokens & {"rent", "debt", "overdraft", "overdrawn", "eviction", "evict", "medical", "taxes", "income"})
    explicit_total = ("how" in tokens and "much" in tokens) or bool(tokens & {"spent", "spend", "spending", "transactions", "chart", "plot"})
    return sensitive_topic and bool(_SENSITIVE_SUPPORT_RE.search(question or "")) and not explicit_total


def _with_controller_act(route: dict, act) -> dict:
    if act:
        route["controller_act"] = act.as_dict() if hasattr(act, "as_dict") else act
    return route


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
    return has_explicit_time_scope(question)


def _months_since_named_month(question: str) -> int | None:
    months = parse_range(question).chart_months
    return months


def _range_override(question: str) -> str | None:
    parsed = parse_range(question)
    return parsed.token if parsed.explicit else None


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


def _transaction_enrichment_shortcut(question: str, start: float) -> dict | None:
    q = (question or "").lower()
    mentions_enrichment = "enrichment" in q or "enriched" in q
    mentions_transaction = "transaction" in q or "transactions" in q
    if mentions_transaction and "low confidence" in q:
        route = _shortcut_base("overview", "low_confidence_transactions", start)
        route["tool_name"] = "find_low_confidence_transactions"
        route["args"] = {"threshold": 0.7, "limit": 25}
        return route
    if mentions_enrichment and any(term in q for term in ("coverage", "quality", "summary", "health")):
        route = _shortcut_base("overview", "enrichment_quality_summary", start)
        route["tool_name"] = "get_enrichment_quality_summary"
        route["args"] = {}
        return route
    if mentions_transaction and (mentions_enrichment or re.search(r"\bcategor(?:y|ized|ised|ization|isation)\b", q)):
        if re.search(r"\b(why|explain|how)\b", q):
            match = re.search(r"\btransaction(?:\s+id)?\s*[:#-]?\s*([A-Za-z0-9_.:-]{3,})", question or "", re.I)
            route = _shortcut_base("overview", "transaction_enrichment_explanation", start)
            route["tool_name"] = "explain_transaction_enrichment"
            route["args"] = {"transaction_id": match.group(1)} if match else {}
            return route
    return None


def _data_health_shortcut(question: str, start: float) -> dict | None:
    q = (question or "").lower()
    if not re.search(r"\b(data|db|database|sync|health|integrity|stale|freshness|caveat|limitation|coverage)\b", q):
        return None
    if re.search(r"\b(health|integrity|stale|fresh|freshness|sync|trust|caveats?|limitations?|coverage)\b", q):
        route = _shortcut_base("overview", "data_health", start)
        route["tool_name"] = "get_data_health_summary"
        route["args"] = {}
        return route
    return None


def _phase5_cashflow_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    q = (question or "").lower()
    if re.search(r"\b(afford|buy|spend another|can i spend)\b", q):
        try:
            from copilot_agents.drilldown import _load_categories

            categories = _load_categories(profile)
        except Exception:
            categories = []
        args = cashflow_forecast.extract_affordability_args(question, categories) or {}
        if not args.get("amount"):
            return None
        route = _shortcut_base("plan", "affordability", start)
        route["tool_name"] = "check_affordability"
        route["args"] = args
        return route
    if re.search(r"\b(run short|shortfall|overdraw|overdraft|before my next paycheck|before next paycheck)\b", q):
        route = _shortcut_base("plan", "shortfall", start)
        route["tool_name"] = "predict_shortfall"
        route["args"] = {}
        return route
    if re.search(r"\b(cash[- ]?flow|forecast|project(?:ed)? balance|next paycheck)\b", q):
        route = _shortcut_base("plan", "cashflow_forecast", start)
        route["tool_name"] = "get_cashflow_forecast"
        route["args"] = {}
        return route
    return None


def _memory_shortcut(question: str, start: float) -> dict | None:
    command = memory_v2.parse_memory_command(question)
    if not command:
        return None
    operation = str(command.get("operation") or "")
    route = _shortcut_base("memory", operation, start)
    route["tool_name"] = operation
    route["args"] = command.get("args") if isinstance(command.get("args"), dict) else {}
    route["confidence"] = 1.0
    route["uses_history"] = operation == "forget_memory" and "text" not in route["args"]
    return route


def _clean_args(tool_name: str | None, args: Any, question: str = "") -> dict:
    if not isinstance(args, dict):
        return {}
    cleaned = {k: v for k, v in args.items() if v not in (None, "", [])}
    if tool_name in {"get_transactions", "find_transactions", "get_transactions_for_merchant"}:
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
    if tool_name in {
        "get_category_spend",
        "get_merchant_spend",
        "get_transactions",
        "find_transactions",
        "get_transactions_for_merchant",
        "analyze_subject",
        "compare_periods",
        "get_budget_status",
        "explain_metric",
        "find_low_confidence_transactions",
        "explain_transaction_enrichment",
        "get_enrichment_quality_summary",
        "get_cashflow_forecast",
        "predict_shortfall",
        "check_affordability",
    }:
        override = _range_override(question)
        if override:
            if tool_name == "compare_periods":
                cleaned.setdefault("range_a", override)
            else:
                cleaned["range"] = override
    if tool_name == "get_monthly_spending_trend":
        try:
            cleaned["months"] = max(1, min(int(cleaned.get("months") or 6), 36))
        except (TypeError, ValueError):
            cleaned["months"] = 6
        if not _has_explicit_time_scope(question):
            cleaned["months"] = 6
        cleaned["months"] = chart_months(question, fallback=cleaned["months"])
    return cleaned


def _normalize_tool(intent: str, operation: str, tool_name: str | None) -> str | None:
    tool_name = (tool_name or "").strip() or None
    if tool_name:
        return tool_name
    semantic_operation_map = {
        "dashboard_snapshot": "get_dashboard_snapshot",
        "subject_analysis": "analyze_subject",
        "period_comparison": "compare_periods",
        "compare": "compare_periods",
        "budget_status": "get_budget_status",
        "on_track": "get_budget_status",
        "find_transactions": "find_transactions",
        "metric_explanation": "explain_metric",
        "recurring_changes": "get_recurring_changes",
        "data_health": "get_data_health_summary",
        "cashflow_forecast": "get_cashflow_forecast",
        "shortfall": "predict_shortfall",
        "affordability": "check_affordability",
        "low_confidence_transactions": "find_low_confidence_transactions",
        "transaction_enrichment_explanation": "explain_transaction_enrichment",
        "enrichment_quality_summary": "get_enrichment_quality_summary",
        "remember_user_context": "remember_user_context",
        "retrieve_relevant_memories": "retrieve_relevant_memories",
        "update_memory": "update_memory",
        "forget_memory": "forget_memory",
        "list_mira_memories": "list_mira_memories",
    }
    if operation in semantic_operation_map:
        return semantic_operation_map[operation]
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
    write_operation_map = {
        "set_budget": "preview_set_budget",
        "create_goal": "preview_create_goal",
        "update_goal_target": "preview_update_goal_target",
        "mark_goal_funded": "preview_mark_goal_funded",
        "set_transaction_note": "preview_set_transaction_note",
        "set_transaction_tags": "preview_set_transaction_tags",
        "mark_reviewed": "preview_mark_reviewed",
        "bulk_mark_reviewed": "preview_bulk_mark_reviewed",
        "update_manual_account_balance": "preview_update_manual_account_balance",
        "split_transaction": "preview_split_transaction",
        "confirm_recurring_obligation": "preview_confirm_recurring_obligation",
        "dismiss_recurring_obligation": "preview_dismiss_recurring_obligation",
        "cancel_recurring": "preview_cancel_recurring",
        "restore_recurring": "preview_restore_recurring",
    }
    if intent == "write" and operation in write_operation_map:
        return write_operation_map[operation]
    if intent == "plan":
        return None
    return None


def _tool_allowed(intent: str, tool_name: str | None) -> bool:
    if not tool_name:
        return intent in {"overview", "chat", "plan", "error"}
    if intent in {"spending", "drilldown"}:
        return tool_name in (_SPENDING_TOOLS | {"analyze_subject"})
    if intent == "transactions":
        return tool_name in (_TRANSACTION_TOOLS | {"find_transactions"})
    if intent == "chart":
        return tool_name in _CHART_FETCH_TOOLS
    if intent == "write":
        return tool_name in _WRITE_TOOLS
    if intent in {"overview", "plan"}:
        return tool_name in _SEMANTIC_TOOLS
    if intent == "memory":
        return tool_name in {"remember_user_context", "retrieve_relevant_memories", "update_memory", "forget_memory", "list_mira_memories"}
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
    if _looks_like_provider_config_request(question):
        intent = "chat"
        operation = "local_only_provider"
        tool_name = None
        args = {}
    if _is_categorization_debug(question):
        intent = "chat"
        operation = "categorization_debug"
        tool_name = None
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
    turn_interpretation: dict | None = None

    def finish(route: dict) -> dict:
        route["route_ms"] = route.get("route_ms") or round((time.perf_counter() - start) * 1000, 2)
        if turn_interpretation and not route.get("turn_interpretation"):
            route["turn_interpretation"] = dict(turn_interpretation)
        return conversation_controller.finalize_route(route, profile)

    if forced_intent:
        return finish(_forced_route(forced_intent, start))

    answer_context = _answer_context_from_history(history, profile)
    active_state, _last_assistant, _active_source = conversation_controller.active_clarification_from_history(profile, history)
    has_active_dialogue = bool(active_state)
    if not has_active_dialogue and history:
        for turn in reversed(history[-3:]):
            if turn.get("role") != "assistant":
                continue
            if isinstance(turn.get("dialogue_state"), dict):
                has_active_dialogue = turn["dialogue_state"].get("kind") == "merchant_clarification"
                break
            if _is_merchant_selection_prompt(turn.get("content") or ""):
                has_active_dialogue = True
                break
    turn = conversation_controller.interpret_turn_state(
        question,
        answer_context=answer_context,
        has_active_dialogue=has_active_dialogue,
    )
    turn_interpretation = turn.as_dict()

    if turn.turn_kind == "provenance":
        route = _shortcut_base("chat", "explain_grounding", start)
        route["uses_history"] = bool(answer_context)
        return finish(route)

    if _looks_like_provider_config_request(question):
        return finish(_shortcut_base("chat", "local_only_provider", start))

    memory_route = _memory_shortcut(question, start)
    if memory_route is not None:
        return finish(memory_route)

    enrichment_route = _transaction_enrichment_shortcut(question, start)
    if enrichment_route is not None:
        return finish(enrichment_route)

    data_health_route = _data_health_shortcut(question, start)
    if data_health_route is not None:
        return finish(data_health_route)

    cashflow_route = _phase5_cashflow_shortcut(question, profile, start)
    if cashflow_route is not None:
        return finish(cashflow_route)

    if _looks_like_sensitive_support_turn(question):
        return finish(_shortcut_base("chat", "chat", start))

    if _is_categorization_debug(question):
        return finish(_shortcut_base("chat", "categorization_debug", start))

    if turn.turn_kind == "general_chat":
        if set(words(question)) <= _ACK_TOKENS:
            return finish(_context_direct_route("context_acknowledge", start, "Got it."))
        return finish(_shortcut_base("chat", "chat", start))

    had_dialogue_state = False
    if turn.turn_kind in {"confirm", "cancel", "correction", "unclear"}:
        had_dialogue_state = _dialogue_state_from_history(history, profile)[0] is not None
        dialogue_route = _dialogue_state_route(question, history, profile, start)
        if dialogue_route is not None:
            return finish(dialogue_route)

    if turn.turn_kind in {"correction", "followup_range_shift", "followup_subject_shift", "followup_action_shift"}:
        context_for_turn = answer_context or _plain_history_finance_context(history, profile)
        context_route = apply_turn_state_context_route(
            question,
            context_for_turn,
            start,
            turn_interpretation,
            _context_policy_callbacks(profile),
        )
        if context_route is not None:
            return finish(context_route)

    if turn.turn_kind == "unclear":
        answer_context_route = _answer_context_followup_route(question, history, profile, start)
        if answer_context_route is not None:
            return finish(answer_context_route)

    if not had_dialogue_state and turn.turn_kind != "new_finance_task":
        confirmed_route = _confirmed_merchant_route(question, history, profile, start)
        if confirmed_route is not None:
            return finish(confirmed_route)

    if _looks_like_grounding_question(question):
        return finish(_shortcut_base("chat", "explain_grounding", start))

    transaction_route = _transaction_shortcut(question, profile, start)
    if transaction_route is not None:
        return finish(transaction_route)

    chart_route = _chart_shortcut(question, profile, start)
    if chart_route is not None:
        return finish(chart_route)

    category_spend_route = _category_spend_shortcut(question, profile, start)
    if category_spend_route is not None:
        return finish(category_spend_route)

    merchant_spend_route = _merchant_spend_shortcut(question, profile, start)
    if merchant_spend_route is not None:
        return finish(merchant_spend_route)

    merchant_confirmation = _merchant_confirmation_route(question, profile, start)
    if merchant_confirmation is not None:
        return finish(merchant_confirmation)

    missing_spend_subject = _missing_spend_subject_route(question, profile, start)
    if missing_spend_subject is not None:
        return finish(missing_spend_subject)

    write_recategorize_route = _write_recategorize_shortcut(question, profile, start)
    if write_recategorize_route is not None:
        return finish(write_recategorize_route)

    write_rename_route = _write_rename_shortcut(question, profile, start)
    if write_rename_route is not None:
        return finish(write_rename_route)

    write_budget_route = _write_budget_shortcut(question, profile, start)
    if write_budget_route is not None:
        return finish(write_budget_route)

    write_rule_route = _write_rule_shortcut(question, profile, start)
    if write_rule_route is not None:
        return finish(write_rule_route)

    planner_route = _planner_shortcut(question, profile, start)
    if planner_route is not None:
        return finish(planner_route)

    unresolved_comparison = _unresolved_finance_comparison(question, profile, start)
    if unresolved_comparison is not None:
        return finish(unresolved_comparison)

    if not llm_client.is_available():
        return finish({
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
            "error": "Mira needs a configured local Ollama provider for natural-language routing.",
        })

    today = datetime.now().strftime("%Y-%m-%d")
    prompt = build_router_prompt(
        question=question,
        recent_context=_history_lines(history),
        today=today,
    )

    raw = ""
    classifier_start = time.perf_counter()
    try:
        raw = llm_client.complete(prompt, max_tokens=260, purpose="controller")
        classifier_ms = (time.perf_counter() - classifier_start) * 1000
        parsed = _parse_jsonish(raw)
        if not isinstance(parsed, dict):
            raise ValueError("router did not return a JSON object")
        route = _normalize_route(
            parsed,
            question=question,
            raw=raw,
            elapsed_ms=(time.perf_counter() - start) * 1000,
            classifier_ms=classifier_ms,
        )
        route = _postprocess_route(route, question, profile)
        return finish(_ground_untrusted_subject_route(route, question, profile))
    except Exception as exc:
        classifier_ms = (time.perf_counter() - classifier_start) * 1000
        return finish({
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
        })
