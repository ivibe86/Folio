from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from range_parser import resolve_followup_range

from mira.agentic.semantic_catalog import canonical_semantic_tool_name
from mira.agentic.semantic_frames import latest_prior_frame
from mira.agentic.vnext_args import UNIVERSAL_ARG_FIELDS, adapt_universal_args
from mira.agentic.vnext_manifest import (
    all_tool_schemas,
    build_family_detail_manifest,
    build_grouped_tool_manifest,
    selected_family_name,
    selector_manifest_coverage,
    tools_by_name,
)


SelectorCompleter = Callable[[str, int, str], str]

_SINGLE_RANGE_TOOLS = {
    "query_transactions",
    "summarize_spending",
    "finance_overview",
    "review_budget",
    "review_net_worth",
}
_CHAT_ROUTES = {"chat", "explain_last_answer"}
_TOOL_ROUTES = {"finance_tool", "memory", "write_preview"}
_MEMORY_SEMANTIC_TOOLS = {"manage_memory"}
_WRITE_PREVIEW_SEMANTIC_TOOLS = {"preview_finance_change"}
_FINANCE_SEMANTIC_TOOLS = {
    "query_transactions",
    "summarize_spending",
    "finance_overview",
    "review_budget",
    "review_cashflow",
    "check_affordability",
    "review_recurring",
    "review_net_worth",
    "review_data_quality",
    "make_chart",
}
_ROUTE_ALIASES = {
    "": "",
    "answer": "chat",
    "chat": "chat",
    "general": "chat",
    "general_answer": "chat",
    "no_tool": "chat",
    "none": "chat",
    "explain": "explain_last_answer",
    "explain_last_answer": "explain_last_answer",
    "tool": "finance_tool",
    "tools": "finance_tool",
    "finance": "finance_tool",
    "finance_tool": "finance_tool",
    "finance_tools": "finance_tool",
    "memory": "memory",
    "write": "write_preview",
    "write_preview": "write_preview",
    "preview": "write_preview",
}

SELECTOR_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "calls": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    **{field: {"type": ["string", "number", "boolean", "object", "array", "null"]} for field in UNIVERSAL_ARG_FIELDS},
                },
                "required": ["tool", "view", "range", "filters", "payload", "context_action", "range_source"],
                "additionalProperties": False,
            },
        },
        "family": {"type": "string"},
        "intent": {"type": "string"},
        "need_detail": {"type": "boolean"},
        "needs_folio_evidence": {"type": "boolean"},
        "frame_patch": {
            "type": ["object", "null"],
            "properties": {
                "frame_action": {"type": "string"},
                "subject_action": {"type": "string"},
                "subject": {
                    "type": ["object", "string", "null"],
                    "properties": {
                        "raw": {"type": "string"},
                        "type_hint": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                "range_action": {"type": "string"},
                "range": {"type": "string"},
                "output_action": {"type": "string"},
                "requested_output": {"type": "string"},
                "clarification_choice": {"type": "string"},
            },
            "additionalProperties": False,
        },
        "discourse_action": {"type": "string"},
        "subject": {
            "type": ["object", "string", "null"],
            "properties": {
                "raw": {"type": "string"},
                "type_hint": {"type": "string"},
            },
            "additionalProperties": False,
        },
        "range": {"type": "string"},
        "requested_output": {"type": "string"},
        "clarification_choice": {"type": "string"},
        "route": {"type": "string"},
        "answer": {"type": "string"},
    },
    "required": ["route", "intent", "needs_folio_evidence", "calls"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class SelectorResult:
    calls: list[dict[str, Any]]
    decision: dict[str, Any]
    raw: str
    prompt: str
    manifest: str
    attempts: int
    llm_calls: int
    status: str
    error: str = ""
    family_detail_used: bool = False
    repair_used: bool = False
    trace: dict[str, Any] = field(default_factory=dict)


def build_selector_system_prompt() -> str:
    return """You are Mira: companion first, Folio finance expert second. Return JSON only.

Routes:
- chat: greetings, reactions, general knowledge, writing/life help, or finance concepts without Folio data. calls=[].
- finance_tool: user's Folio data: transactions, spending, budgets, cashflow, recurring, net worth, charts, data quality.
- memory: explicit remember/recall/list/update/forget memory.
- write_preview: finance data changes; preview only.
- explain_last_answer: provenance for the prior answer; no fresh tools.

Top JSON: {"route":"chat|finance_tool|memory|write_preview|explain_last_answer","intent":"short_snake_case","needs_folio_evidence":false,"frame_patch":null,"calls":[]}
Call envelope fields: tool, view, range, range_a, range_b, filters, payload, limit, offset, sort, context_action, range_source.
Use filters for read selectors (merchant/category/account/search/transaction_id). Use payload for metrics, amounts, write details, chart source.

Tools/views:
query_transactions(latest,list,search,detail); summarize_spending(period_total,entity_total,top,breakdown,trend,compare); finance_overview(snapshot,priorities,explain_metric); review_budget(plan,category_status,savings_capacity); review_cashflow(forecast,shortfall); check_affordability(purchase); review_recurring(summary,changes); review_net_worth(balances,trend,delta); review_data_quality(health,enrichment_summary,low_confidence,explain_transaction); manage_memory(remember,retrieve,list,update,forget); preview_finance_change(preview); make_chart(line,bar,donut).
Use only these exact view names.

Routing hints:
- spend total at/on X -> summarize_spending entity_total with merchant/category; metric=expenses.
- expenses/income/refunds total -> summarize_spending period_total with payload.metric.
- top/biggest -> summarize_spending top with payload.group_by=merchant or category.
- why/recent/latest transaction/deposit/income timing -> query_transactions.
- budget/category budget -> review_budget. save monthly/savings capacity -> review_budget savings_capacity. cashflow/shortfall -> review_cashflow. afford -> check_affordability.
- subscriptions/recurring -> review_recurring. balances/net worth -> review_net_worth.
- chart/plot/graph -> evidence call first, then make_chart with payload.source_step_id.
- recategorize/move/categorize/set/update finance data -> write_preview preview_finance_change.

Ranges: current_month, last_month, ytd, all, YYYY-MM, last_6_months, last_30d, etc. Month names become YYYY-MM. Default unstated finance reads to current_month unless trend/all-time naturally fits.

Follow-ups: if prior context exists and the user is changing subject/range/output, prefer frame_patch and calls=[].
Patch fields: frame_action=patch_prior|clarification_reply; subject_action=inherit|replace|clear; subject.raw/type_hint; range_action=inherit|replace|previous_of_prior|next_of_prior|clear; range; output_action=inherit|replace; requested_output=scalar_total|rows|chart|comparison|status|preview; clarification_choice.
Do not emit half-filled calls for patch_prior. Latest user words override prior context."""


def build_selector_repair_system_prompt() -> str:
    return """Repair Mira controller JSON. Return JSON only.
Use exact tool names from the manifest. Do not shorten or invent tool names.
Preserve the user's intent and arguments when possible.
Latest user message wins over prior context for any field it mentions or corrects.
Explicit month names must become YYYY-MM.
Keep context_action and range_source accurate.
If no Folio tool is needed, return {"route":"chat","intent":"chat","needs_folio_evidence":false,"frame_patch":null,"calls":[]}.
If saving explicit user memory, use manage_memory view=remember and include payload.text.
If a finance tool is needed, return route=finance_tool with needed envelope fields.
For a follow-up that only changes the raw subject, use frame_patch.frame_action=patch_prior, subject_action=replace, subject.raw, range_action=inherit, and calls=[].
If a write is requested, return route=write_preview and preview_finance_change only."""


def build_selector_prompt(*, question: str, manifest: str, recent_context: str = "", today: str | None = None) -> str:
    today = today or datetime.now().date().isoformat()
    context_block = (
        "Context for follow-ups only:\n"
        + recent_context
        + "\nPreserve omitted prior slots; latest user words override.\n\n"
        if recent_context.strip()
        else ""
    )
    return (
        build_selector_system_prompt()
        + "\n\nManifest:\n"
        + manifest
        + "\n\nToday:\n"
        + today
        + "\n\n"
        + context_block
        + "User question:\n"
        + str(question or "")
        + "\n\nReturn the JSON object now."
    )


def build_repair_prompt(
    *,
    question: str,
    manifest: str,
    recent_context: str,
    invalid_decision: dict[str, Any],
    today: str | None = None,
) -> str:
    today = today or datetime.now().date().isoformat()
    return (
        build_selector_repair_system_prompt()
        + "\n\nManifest:\n"
        + manifest
        + "\n\nToday:\n"
        + today
        + "\n\nUser question:\n"
        + str(question or "")
        + ("\n\nRecent context:\n" + recent_context if recent_context.strip() else "")
        + "\n\nInvalid selector output:\n"
        + str(invalid_decision.get("raw_response") or "")
        + "\n\nValidation errors:\n"
        + json.dumps(invalid_decision.get("validation_errors") or invalid_decision.get("error") or [], ensure_ascii=True, default=str)
        + "\n\nReturn repaired selector JSON now."
    )


def run_selector(
    *,
    question: str,
    history: list[dict[str, Any]] | None = None,
    base_tools: list[dict[str, Any]] | None = None,
    completer: SelectorCompleter | None = None,
    max_tokens: int = 220,
) -> SelectorResult:
    tools = base_tools or all_tool_schemas()
    started = time.perf_counter()
    manifest = build_grouped_tool_manifest(tools)
    recent_context = format_recent_context(history)
    complete = completer or _default_completer
    attempts = 0
    llm_calls = 0
    family_detail_used = False
    repair_used = False

    prompt = build_selector_prompt(question=question, manifest=manifest, recent_context=recent_context)
    raw = complete(prompt, max_tokens, "controller")
    attempts += 1
    llm_calls += 1
    calls, decision = normalize_selector_decision(raw=raw, base_tools=tools)
    calls, decision = apply_discourse_frames(calls=calls, decision=decision, history=history, tools=tools)
    calls, decision = apply_context_semantics(calls=calls, decision=decision, history=history, question=question)

    if decision_needs_family_detail(decision, calls):
        family_detail_used = True
        detail_manifest = build_family_detail_manifest(tools, selected_family(decision))
        prompt = build_selector_prompt(question=question, manifest=detail_manifest, recent_context=recent_context)
        raw = complete(prompt, max_tokens, "controller")
        attempts += 1
        llm_calls += 1
        calls, decision = normalize_selector_decision(raw=raw, base_tools=tools)
        calls, decision = apply_discourse_frames(calls=calls, decision=decision, history=history, tools=tools)
        calls, decision = apply_context_semantics(calls=calls, decision=decision, history=history, question=question)
        decision["family_detail_used"] = True

    if selector_needs_repair(decision, calls):
        repair_used = True
        repair_prompt = build_repair_prompt(
            question=question,
            manifest=manifest,
            recent_context=recent_context,
            invalid_decision=decision,
        )
        raw = complete(repair_prompt, max_tokens, "controller")
        attempts += 1
        llm_calls += 1
        calls, repaired = normalize_selector_decision(raw=raw, base_tools=tools)
        calls, repaired = apply_discourse_frames(calls=calls, decision=repaired, history=history, tools=tools)
        calls, repaired = apply_context_semantics(calls=calls, decision=repaired, history=history, question=question)
        repaired["repair_of"] = {
            "validation_errors": decision.get("validation_errors") or [],
            "error": decision.get("error") or "",
        }
        decision = repaired
        prompt = repair_prompt

    status = selector_status(decision, calls)
    error = selector_error(decision, calls) if status == "clarify" else ""
    trace = {
        "runtime": "agentic_vnext",
        "stage": "selector",
        "status": status,
        "attempts": attempts,
        "llm_calls": llm_calls,
        "selector_ms": round((time.perf_counter() - started) * 1000, 2),
        "repair_used": repair_used,
        "family_detail_used": family_detail_used,
        "prompt_tokens_est": estimate_tokens(prompt),
        "manifest_tokens_est": estimate_tokens(manifest),
        "coverage": selector_manifest_coverage(tools),
    }
    return SelectorResult(
        calls=calls,
        decision=decision,
        raw=raw,
        prompt=prompt,
        manifest=manifest,
        attempts=attempts,
        llm_calls=llm_calls,
        status=status,
        error=error,
        family_detail_used=family_detail_used,
        repair_used=repair_used,
        trace=trace,
    )


def normalize_selector_decision(
    *,
    raw: str,
    base_tools: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        decision = parse_json_object(raw)
    except Exception as exc:
        if str(raw or "").lstrip().startswith('{"answer"'):
            decision = {
                "route": "general_answer",
                "selector_answer_truncated": True,
                "raw_response": raw,
                "validation_errors": [],
            }
            return [], decision
        decision = {
            "calls": [],
            "error": f"selector JSON parse failed: {exc}",
        }

    if not isinstance(decision, dict):
        decision = {"calls": [], "error": "selector response must be a JSON object"}
    decision = normalize_frame_patch_decision(decision)

    by_name = tools_by_name(base_tools)
    calls = []
    validation_errors: list[str] = []
    raw_calls = decision.get("calls")
    if raw_calls is None:
        raw_calls = decision.get("tool_calls") or []
    if raw_calls and not isinstance(raw_calls, list):
        validation_errors.append("calls must be an array")
        raw_calls = []
    if _frame_patch_action(decision) in {"patch_prior", "clarification_reply"}:
        route = _ROUTE_ALIASES.get(str(decision.get("route") or "").strip().lower(), str(decision.get("route") or "").strip().lower())
        if route in {"memory", "write_preview"}:
            decision.pop("frame_patch", None)
        else:
            if raw_calls:
                decision["dropped_calls_due_to_frame_patch"] = len(raw_calls)
            raw_calls = []

    for index, raw_call in enumerate(raw_calls or [], start=1):
        if not isinstance(raw_call, dict):
            validation_errors.append(f"call {index} must be an object")
            continue
        raw_name = str(raw_call.get("tool") or raw_call.get("name") or "").strip()
        name = canonical_semantic_tool_name(raw_name)
        if name not in by_name:
            validation_errors.append(f"unknown tool name: {raw_name or '<empty>'}")
            continue
        nested_args = raw_call.get("args") if isinstance(raw_call.get("args"), dict) else {}
        normalized_call = {**nested_args, **{key: value for key, value in raw_call.items() if key != "args"}, "tool": name}
        _repair_structured_call_from_selector_intent(normalized_call, decision)
        args, validation_error = adapt_universal_args(name, normalized_call, by_name[name])
        if validation_error:
            validation_errors.append(validation_error)
        calls.append({
            "id": str(raw_call.get("id") or f"selector_call_{index}"),
            "name": name,
            "universal_args": {
                key: normalized_call.get(key)
                for key in UNIVERSAL_ARG_FIELDS
                if normalized_call.get(key) not in (None, "", [], {})
            },
            "args": args,
            "validation_error": validation_error,
        })

    calls, validation_errors = append_chart_call_from_structured_intent(
        calls=calls,
        decision=decision,
        by_name=by_name,
        validation_errors=validation_errors,
    )

    selector_answer = str(decision.get("answer") or decision.get("direct_answer") or "").strip()
    if selector_answer and not calls:
        decision["route"] = "general_answer"
        decision["selector_answer_chars"] = len(selector_answer)

    calls, validation_errors = apply_controller_route_permissions(
        decision=decision,
        calls=calls,
        validation_errors=validation_errors,
    )

    if (
        not calls
        and not selected_family(decision)
        and not decision_requests_general_answer(decision)
        and not decision_has_discourse_frame(decision)
    ):
        validation_errors.append("selector returned no calls, family, or route")

    decision["calls"] = calls
    decision["validation_errors"] = validation_errors
    decision["raw_response"] = raw
    return calls, decision


def _repair_structured_call_from_selector_intent(call: dict[str, Any], decision: dict[str, Any]) -> None:
    tool_name = canonical_semantic_tool_name(str(call.get("tool") or ""))
    intent = str(decision.get("intent") or "").strip().lower()
    if tool_name == "query_transactions":
        view = str(call.get("view") or "").strip().lower()
        if view in {"", "list", "search"} and "latest" in intent and "transaction" in intent:
            call["view"] = "latest"
            call.setdefault("limit", 1)
            call.setdefault("sort", "date_desc")
        return

    if tool_name != "summarize_spending":
        return
    if str(call.get("view") or "").strip().lower() != "top":
        return
    payload = call.get("payload") if isinstance(call.get("payload"), dict) else {}
    if payload.get("group_by"):
        return
    if "merchant" in intent:
        payload["group_by"] = "merchant"
    elif "categor" in intent:
        payload["group_by"] = "category"
    if payload:
        call["payload"] = payload


def append_chart_call_from_structured_intent(
    *,
    calls: list[dict[str, Any]],
    decision: dict[str, Any],
    by_name: dict[str, dict[str, Any]],
    validation_errors: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    if not _decision_requests_chart(decision):
        return calls, validation_errors
    if any(str(call.get("name") or "") == "make_chart" for call in calls):
        return calls, validation_errors
    if "make_chart" not in by_name:
        return calls, validation_errors
    source = next((call for call in calls if str(call.get("name") or "") in _FINANCE_SEMANTIC_TOOLS and str(call.get("name") or "") != "make_chart"), None)
    if not source:
        return calls, validation_errors
    source_id = str(source.get("id") or "selector_call_1")
    title = "Net worth trend" if source.get("name") == "review_net_worth" else "Chart"
    raw_call = {
        "tool": "make_chart",
        "view": "line",
        "payload": {"source_step_id": source_id, "title": title},
        "context_action": "new",
        "range_source": "none",
    }
    args, validation_error = adapt_universal_args("make_chart", raw_call, by_name["make_chart"])
    if validation_error:
        validation_errors.append(validation_error)
        return calls, validation_errors
    return [
        *calls,
        {
            "id": f"selector_call_{len(calls) + 1}",
            "name": "make_chart",
            "universal_args": {
                key: raw_call.get(key)
                for key in UNIVERSAL_ARG_FIELDS
                if raw_call.get(key) not in (None, "", [], {})
            },
            "args": args,
            "validation_error": "",
        },
    ], validation_errors


def _decision_requests_chart(decision: dict[str, Any]) -> bool:
    requested_output = str(decision.get("requested_output") or "").strip().lower()
    if requested_output == "chart":
        return True
    intent = str(decision.get("intent") or "").strip().lower()
    return "chart" in intent or "plot" in intent or "graph" in intent


def apply_controller_route_permissions(
    *,
    decision: dict[str, Any],
    calls: list[dict[str, Any]],
    validation_errors: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    route = canonical_controller_route(decision, calls)
    decision["controller_route"] = route
    if route in _CHAT_ROUTES:
        if calls:
            decision["dropped_calls_due_to_route"] = [
                str(call.get("name") or call.get("tool") or "")
                for call in calls
            ]
        decision["calls"] = []
        decision["route"] = "general_answer"
        if route == "explain_last_answer":
            decision["answer_path"] = "explain_last_answer"
        return [], validation_errors

    if not route:
        return calls, validation_errors

    if route not in _TOOL_ROUTES:
        validation_errors.append(f"unsupported route: {route}")
        return calls, validation_errors

    allowed = allowed_tools_for_controller_route(route)
    disallowed = [
        str(call.get("name") or call.get("tool") or "")
        for call in calls
        if str(call.get("name") or call.get("tool") or "") not in allowed
    ]
    if disallowed:
        validation_errors.append(f"{route} route cannot call: {', '.join(sorted(set(disallowed)))}")
    if not calls and not decision_has_discourse_frame(decision):
        validation_errors.append(f"{route} route needs at least one tool call")
    decision["route"] = route
    return calls, validation_errors


def canonical_controller_route(decision: dict[str, Any], calls: list[dict[str, Any]]) -> str:
    raw_route = str(decision.get("route") or decision.get("answer_path") or "").strip().lower()
    route = _ROUTE_ALIASES.get(raw_route, raw_route)
    if route:
        return route

    if calls:
        names = {str(call.get("name") or call.get("tool") or "").strip() for call in calls}
        names.discard("")
        if names and names <= _MEMORY_SEMANTIC_TOOLS:
            return "memory"
        if names and names <= _WRITE_PREVIEW_SEMANTIC_TOOLS:
            return "write_preview"
        return "finance_tool"

    answer = str(decision.get("answer") or decision.get("direct_answer") or "").strip()
    if answer:
        return "chat"
    return ""


def allowed_tools_for_controller_route(route: str) -> set[str]:
    if route == "finance_tool":
        return set(_FINANCE_SEMANTIC_TOOLS)
    if route == "memory":
        return set(_MEMORY_SEMANTIC_TOOLS)
    if route == "write_preview":
        return set(_WRITE_PREVIEW_SEMANTIC_TOOLS)
    return set()


def normalize_frame_patch_decision(decision: dict[str, Any]) -> dict[str, Any]:
    patch = _normalize_frame_patch(decision.get("frame_patch") if isinstance(decision.get("frame_patch"), dict) else {})
    if not patch:
        patch = _legacy_frame_patch_from_decision(decision)
    if patch:
        return {**decision, "frame_patch": patch}
    return decision


def _normalize_frame_patch(patch: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(patch, dict):
        return {}
    frame_action = str(patch.get("frame_action") or "").strip().lower()
    if frame_action not in {"new", "patch_prior", "clarification_reply"}:
        frame_action = ""
    subject_action = str(patch.get("subject_action") or "").strip().lower()
    if subject_action not in {"inherit", "replace", "clear"}:
        subject_action = ""
    range_action = str(patch.get("range_action") or "").strip().lower()
    if range_action not in {"inherit", "replace", "previous_of_prior", "next_of_prior", "clear"}:
        range_action = ""
    output_action = str(patch.get("output_action") or "").strip().lower()
    if output_action not in {"inherit", "replace"}:
        output_action = ""
    subject = _subject_from_value(patch.get("subject"))
    requested_output = str(patch.get("requested_output") or "").strip().lower()
    if requested_output not in {"scalar_total", "rows", "chart", "comparison", "status", "summary", "preview"}:
        requested_output = ""
    range_value = str(patch.get("range") or "").strip()
    if range_action == "replace" and _is_previous_of_prior_range_token(range_value):
        range_action = "previous_of_prior"
        range_value = ""
    if not any((frame_action, subject_action, subject.get("raw"), range_action, str(patch.get("range") or "").strip(), output_action, requested_output, str(patch.get("clarification_choice") or "").strip())):
        return {}
    if subject_action == "replace" and not subject.get("raw"):
        subject_action = "inherit"
    out = {
        "frame_action": frame_action,
        "subject_action": subject_action,
        "subject": subject if subject.get("raw") else {},
        "range_action": range_action,
        "range": range_value,
        "output_action": output_action,
        "requested_output": requested_output,
        "clarification_choice": str(patch.get("clarification_choice") or "").strip(),
    }
    return {key: value for key, value in out.items() if value not in (None, "", {}, [])}


def _is_previous_of_prior_range_token(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"last_month_before", "month_before_last_month", "previous_of_prior"}


def _legacy_frame_patch_from_decision(decision: dict[str, Any]) -> dict[str, Any]:
    action = str(decision.get("discourse_action") or "").strip().lower()
    if not action:
        return {}
    if action == "clarification_reply":
        return _normalize_frame_patch({
            "frame_action": "clarification_reply",
            "clarification_choice": decision.get("clarification_choice") or _decision_subject(decision).get("raw"),
        })
    if action not in {"followup", "followup_same_subject", "followup_replace_subject", "correction"}:
        return {}
    subject = _decision_subject(decision)
    range_value = str(decision.get("range") or "").strip()
    range_action = "inherit"
    if range_value.lower() in {"inherited", "same", "prior"}:
        range_action = "inherit"
        range_value = ""
    elif range_value:
        range_action = "replace"
    return _normalize_frame_patch({
        "frame_action": "patch_prior",
        "subject_action": "replace" if subject.get("raw") else "inherit",
        "subject": subject,
        "range_action": range_action,
        "range": range_value,
        "output_action": "replace" if decision.get("requested_output") else "inherit",
        "requested_output": decision.get("requested_output") or "",
    })


def _frame_patch_from_calls(calls: list[dict[str, Any]]) -> dict[str, Any]:
    if not calls:
        return {}
    call = calls[0]
    action = _call_context_action(call)
    if action not in {"followup", "followup_same_subject", "followup_replace_subject", "correction"}:
        return {}
    args = copy_args(call)
    universal = call.get("universal_args") if isinstance(call.get("universal_args"), dict) else {}
    subject = _subject_from_discourse_call(call)
    range_source = str(universal.get("range_source") or "").strip().lower()
    range_value = str(args.get("range") or universal.get("range") or "").strip()
    range_action = "inherit"
    if range_source in {"previous_of_prior", "next_of_prior"}:
        range_action = range_source
        range_value = ""
    elif action == "followup" and range_value:
        range_action = "replace"
    elif action == "followup_same_subject" and range_value:
        range_action = "replace"
    elif action in {"followup_replace_subject", "correction"}:
        range_action = "inherit"
        range_value = ""
    return _normalize_frame_patch({
        "frame_action": "patch_prior",
        "subject_action": "replace" if subject.get("raw") else "inherit",
        "subject": subject,
        "range_action": range_action,
        "range": range_value,
        "output_action": "inherit",
    })


def _frame_patch_action(decision: dict[str, Any]) -> str:
    patch = decision.get("frame_patch") if isinstance(decision.get("frame_patch"), dict) else {}
    return str(patch.get("frame_action") or "").strip().lower()


def decision_has_discourse_frame(decision: dict[str, Any]) -> bool:
    if not isinstance(decision, dict):
        return False
    if _frame_patch_action(decision):
        return True
    if str(decision.get("discourse_action") or "").strip():
        return True
    if str(decision.get("requested_output") or "").strip():
        return True
    return bool(_decision_subject(decision).get("raw"))


def apply_discourse_frames(
    *,
    calls: list[dict[str, Any]],
    decision: dict[str, Any],
    history: list[dict[str, Any]] | None,
    tools: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    route = canonical_controller_route(decision, calls)
    if route != "finance_tool":
        return calls, decision

    patch = decision.get("frame_patch") if isinstance(decision.get("frame_patch"), dict) else {}
    if not patch:
        patch = _frame_patch_from_calls(calls)
        if patch:
            decision = {**decision, "frame_patch": patch}
    action = str(patch.get("frame_action") or "").strip().lower()
    if not action or action == "new":
        return calls, decision

    pending = latest_pending_clarification(history)
    if action == "clarification_reply":
        if not pending:
            return [], {
                **decision,
                "calls": [],
                "route": "",
                "validation_errors": ["There is no pending clarification to resolve."],
            }
        resolved = _call_from_pending_clarification(decision=decision, pending=pending, tools=tools)
        if resolved:
            return [resolved], {
                **decision,
                "calls": [resolved],
                "frame_patch_compiled": True,
                "pending_clarification_resolved": True,
            }
        return [], {
            **decision,
            "calls": [],
            "route": "",
            "validation_errors": ["I need one of the pending clarification options before continuing."],
        }

    if action != "patch_prior":
        return calls, decision

    prior_frame = latest_conversation_frame(history)
    if not prior_frame:
        return [], {
            **decision,
            "calls": [],
            "route": "",
            "validation_errors": ["I need prior finance context before applying that follow-up."],
        }
    if _frame_patch_is_noop(patch):
        return [], {
            **decision,
            "calls": [],
            "route": "general_answer",
            "frame_patch_rejected": "noop",
        }

    compiled = _call_from_frame_patch(patch=patch, decision=decision, prior_frame=prior_frame, tools=tools)
    if not compiled:
        return [], {
            **decision,
            "calls": [],
            "route": "",
            "validation_errors": ["I need one more detail before applying that follow-up."],
        }
    call, conversation_frame = compiled
    return [call], {
        **decision,
        "calls": [call],
        "frame_patch_compiled": True,
        "compiled_conversation_frame": conversation_frame,
    }


def latest_conversation_frame(history: list[dict[str, Any]] | None) -> dict[str, Any]:
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        frame = answer_context.get("conversation_frame") if isinstance(answer_context.get("conversation_frame"), dict) else {}
        if frame:
            return json.loads(json.dumps(frame, default=str))
        current_frame = answer_context.get("current_frame") if isinstance(answer_context.get("current_frame"), dict) else {}
        converted = _conversation_frame_from_semantic_frame(current_frame)
        if converted:
            return converted
    prior = latest_prior_frame(history)
    return _conversation_frame_from_semantic_frame(prior)


def _conversation_frame_from_semantic_frame(frame: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(frame, dict) or not frame.get("tool"):
        return {}
    filters = frame.get("filters") if isinstance(frame.get("filters"), dict) else {}
    subject: dict[str, str] = {}
    if filters.get("merchant"):
        subject = {"type": "merchant", "canonical": str(filters.get("merchant")), "raw": str(filters.get("merchant"))}
    elif filters.get("category"):
        subject = {"type": "category", "canonical": str(filters.get("category")), "raw": str(filters.get("category"))}
    elif frame.get("entity") and frame.get("entity_type"):
        subject = {
            "type": str(frame.get("entity_type") or ""),
            "canonical": str(frame.get("entity") or ""),
            "raw": str(frame.get("entity") or ""),
        }
    output = "scalar_total" if str(frame.get("view") or "").strip().lower() in {"entity_total", "period_total"} else "summary"
    return {
        "intent": "spend_total" if frame.get("tool") == "summarize_spending" else str(frame.get("tool") or ""),
        "tool": str(frame.get("tool") or ""),
        "view": str(frame.get("view") or ""),
        "subject": subject,
        "range": str(frame.get("range") or frame.get("range_b") or frame.get("range_a") or ""),
        "requested_output": output,
        "payload": frame.get("payload") if isinstance(frame.get("payload"), dict) else {},
    }


def _frame_patch_is_noop(patch: dict[str, Any]) -> bool:
    subject_action = str(patch.get("subject_action") or "").strip().lower()
    range_action = str(patch.get("range_action") or "").strip().lower()
    output_action = str(patch.get("output_action") or "").strip().lower()
    if subject_action in {"replace", "clear"}:
        return False
    if range_action in {"replace", "previous_of_prior", "next_of_prior", "clear"}:
        return False
    if output_action == "replace":
        return False
    return True


def _call_from_frame_patch(
    *,
    patch: dict[str, Any],
    decision: dict[str, Any],
    prior_frame: dict[str, Any],
    tools: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    subject = _patched_subject(patch, prior_frame)
    if subject is None:
        return None
    range_value = _patched_range(patch, prior_frame)
    if range_value is None:
        return None
    requested_output = _patched_output(patch, prior_frame)
    tool_name, view = _tool_view_from_frame_patch(decision=decision, prior_frame=prior_frame, subject=subject, requested_output=requested_output)
    if not tool_name:
        return None

    args: dict[str, Any] = {
        "view": view,
        "context_action": "patch_prior",
        "range_source": _compiled_range_source(patch),
    }
    filters = _filters_from_patched_subject(subject)
    if filters:
        args["filters"] = filters
    if range_value and tool_name in _SINGLE_RANGE_TOOLS:
        args["range"] = range_value
    payload = _payload_from_conversation(tool_name=tool_name, prior_frame=prior_frame, requested_output=requested_output)
    if payload:
        args["payload"] = payload
    call = _override_call(tool_name, args, tools)
    if not call:
        return None
    conversation_frame = {
        "intent": str(decision.get("intent") or prior_frame.get("intent") or "").strip(),
        "tool": tool_name,
        "view": view,
        "subject": subject,
        "range": range_value,
        "requested_output": requested_output,
        "payload": payload,
    }
    return call, conversation_frame


def _patched_subject(patch: dict[str, Any], prior_frame: dict[str, Any]) -> dict[str, str] | None:
    action = str(patch.get("subject_action") or "inherit").strip().lower()
    if action == "clear":
        return {}
    if action == "replace":
        subject = _subject_from_value(patch.get("subject"))
        if not subject.get("raw"):
            return None
        return {"type": str(subject.get("type_hint") or "unknown"), "canonical": "", "raw": str(subject.get("raw") or "")}
    prior_subject = prior_frame.get("subject") if isinstance(prior_frame.get("subject"), dict) else {}
    if prior_subject:
        return {
            "type": str(prior_subject.get("type") or prior_subject.get("type_hint") or "unknown"),
            "canonical": str(prior_subject.get("canonical") or prior_subject.get("value") or prior_subject.get("raw") or ""),
            "raw": str(prior_subject.get("raw") or prior_subject.get("canonical") or prior_subject.get("value") or ""),
        }
    return {}


def _patched_range(patch: dict[str, Any], prior_frame: dict[str, Any]) -> str | None:
    action = str(patch.get("range_action") or "inherit").strip().lower()
    prior_range = str(prior_frame.get("range") or "").strip()
    if action == "clear":
        return ""
    if action == "inherit":
        return prior_range
    if action == "replace":
        value = str(patch.get("range") or "").strip()
        return value or None
    if action == "previous_of_prior":
        return resolve_followup_range("month before", prior_range).token if prior_range else None
    if action == "next_of_prior":
        return resolve_followup_range("month after", prior_range).token if prior_range else None
    return prior_range


def _patched_output(patch: dict[str, Any], prior_frame: dict[str, Any]) -> str:
    if str(patch.get("output_action") or "").strip().lower() == "replace":
        value = str(patch.get("requested_output") or "").strip().lower()
        if value:
            return value
    return str(prior_frame.get("requested_output") or patch.get("requested_output") or "scalar_total").strip().lower()


def _tool_view_from_frame_patch(
    *,
    decision: dict[str, Any],
    prior_frame: dict[str, Any],
    subject: dict[str, str],
    requested_output: str,
) -> tuple[str, str]:
    intent = str(decision.get("intent") or prior_frame.get("intent") or "").strip().lower()
    prior_tool = canonical_semantic_tool_name(str(prior_frame.get("tool") or ""))
    prior_view = str(prior_frame.get("view") or "").strip().lower()
    if requested_output == "rows":
        return "query_transactions", "list"
    if requested_output == "chart":
        return "make_chart", "line"
    if prior_tool == "query_transactions":
        return "query_transactions", prior_view if prior_view in {"latest", "list", "search", "detail"} else "list"
    if prior_tool == "review_budget" or "budget" in intent:
        return "review_budget", "category_status" if subject else "plan"
    if prior_tool == "check_affordability" or "afford" in intent:
        return "check_affordability", "purchase"
    if prior_tool == "summarize_spending" or "spend" in intent or "expense" in intent:
        return "summarize_spending", "entity_total" if subject else "period_total"
    return prior_tool or "summarize_spending", prior_view or ("entity_total" if subject else "period_total")


def _filters_from_patched_subject(subject: dict[str, str]) -> dict[str, str]:
    if not subject:
        return {}
    canonical = str(subject.get("canonical") or "").strip()
    entity_type = str(subject.get("type") or subject.get("type_hint") or "").strip().lower()
    raw = str(subject.get("raw") or "").strip()
    if canonical and entity_type in {"merchant", "category", "account"}:
        return {entity_type: canonical}
    if raw:
        filters = {"mention": raw}
        if entity_type in {"merchant", "category", "account"}:
            filters["mention_type"] = entity_type
        return filters
    return {}


def _payload_from_conversation(*, tool_name: str, prior_frame: dict[str, Any], requested_output: str) -> dict[str, Any]:
    if tool_name == "summarize_spending":
        payload = prior_frame.get("payload") if isinstance(prior_frame.get("payload"), dict) else {}
        metric = str(payload.get("metric") or "").strip()
        if not metric and requested_output in {"scalar_total", "summary"}:
            metric = "expenses"
        return {"metric": metric} if metric else {}
    if tool_name == "make_chart":
        source_step_id = str(prior_frame.get("source_step_id") or "").strip()
        return {"source_step_id": source_step_id, "title": "Chart"} if source_step_id else {}
    return {}


def _compiled_range_source(patch: dict[str, Any]) -> str:
    action = str(patch.get("range_action") or "").strip().lower()
    if action == "inherit":
        return "inherited"
    if action in {"previous_of_prior", "next_of_prior"}:
        return action
    if action == "replace":
        return "explicit_user"
    return "none"


def latest_pending_clarification(history: list[dict[str, Any]] | None) -> dict[str, Any]:
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        pending = answer_context.get("pending_clarification") if isinstance(answer_context.get("pending_clarification"), dict) else {}
        if pending:
            return json.loads(json.dumps(pending, default=str))
    return {}


def _decision_subject(decision: dict[str, Any]) -> dict[str, str]:
    subject = _subject_from_value(decision.get("subject"))
    raw = subject.get("raw") or str(decision.get("subject_raw") or "").strip()
    type_hint = subject.get("type_hint") or str(decision.get("subject_type") or decision.get("type_hint") or "").strip().lower()
    if type_hint not in {"merchant", "category", "account", "unknown"}:
        type_hint = "unknown"
    return {"raw": raw, "type_hint": type_hint}


def _subject_from_value(raw_subject: Any) -> dict[str, str]:
    if isinstance(raw_subject, dict):
        raw = str(raw_subject.get("raw") or raw_subject.get("text") or "").strip()
        type_hint = str(raw_subject.get("type_hint") or raw_subject.get("type") or "").strip().lower()
    else:
        raw = str(raw_subject or "").strip()
        type_hint = ""
    if type_hint not in {"merchant", "category", "account", "unknown"}:
        type_hint = "unknown"
    return {"raw": raw, "type_hint": type_hint}


def _subject_from_discourse_call(call: dict[str, Any]) -> dict[str, str]:
    action = _call_context_action(call)
    if action not in {"followup_replace_subject", "correction"}:
        return {}
    name = canonical_semantic_tool_name(str(call.get("name") or call.get("tool") or ""))
    if name not in {"summarize_spending", "query_transactions", "review_budget", "check_affordability"}:
        return {}
    args = copy_args(call)
    filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
    if filters.get("mention"):
        return {"raw": str(filters.get("mention") or "").strip(), "type_hint": "unknown"}
    for key in ("merchant", "category", "account"):
        value = str(filters.get(key) or "").strip()
        if value:
            return {"raw": value, "type_hint": "unknown"}
    return {}


def _call_context_action(call: dict[str, Any]) -> str:
    args = call.get("args") if isinstance(call.get("args"), dict) else {}
    universal = call.get("universal_args") if isinstance(call.get("universal_args"), dict) else {}
    return str(args.get("context_action") or universal.get("context_action") or "").strip().lower()


def _call_from_discourse_frame(
    *,
    decision: dict[str, Any],
    subject: dict[str, str],
    history: list[dict[str, Any]] | None,
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    prior_frame = latest_prior_frame(history)
    tool_name, view = _tool_view_from_discourse(decision=decision, prior_frame=prior_frame)
    if not tool_name:
        return {}
    args: dict[str, Any] = {
        "view": view,
        "filters": _mention_filters(subject),
        "context_action": str(decision.get("discourse_action") or "followup").strip() or "followup",
        "range_source": _range_source_from_discourse(decision),
    }
    range_value = _range_from_discourse(decision=decision, prior_frame=prior_frame, history=history)
    if range_value and tool_name in _SINGLE_RANGE_TOOLS:
        args["range"] = range_value
    payload = _payload_from_discourse(tool_name=tool_name, decision=decision, prior_frame=prior_frame)
    if payload:
        args["payload"] = payload
    return _override_call(tool_name, args, tools)


def _call_with_discourse_subject(
    *,
    call: dict[str, Any],
    decision: dict[str, Any],
    subject: dict[str, str],
    history: list[dict[str, Any]] | None,
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    name = canonical_semantic_tool_name(str(call.get("name") or call.get("tool") or ""))
    if name not in {"summarize_spending", "query_transactions", "review_budget", "check_affordability"}:
        return call
    args = copy_args(call)
    filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
    filters = dict(filters)
    for key in ("merchant", "category", "account", "search"):
        filters.pop(key, None)
    filters.update(_mention_filters(subject))
    args["filters"] = filters

    prior_frame = latest_prior_frame(history)
    requested_output = str(decision.get("requested_output") or "").strip().lower()
    intent = str(decision.get("intent") or "").strip().lower()
    if name == "summarize_spending" and (requested_output == "scalar_total" or "spend" in intent or "expense" in intent):
        args["view"] = "entity_total"
        payload = args.get("payload") if isinstance(args.get("payload"), dict) else {}
        if not payload.get("metric"):
            payload["metric"] = "expenses"
        args["payload"] = payload

    range_value = _range_from_discourse(decision=decision, prior_frame=prior_frame, history=history)
    if range_value and name in _SINGLE_RANGE_TOOLS:
        args["range"] = range_value
    args["context_action"] = str(decision.get("discourse_action") or args.get("context_action") or "followup").strip() or "followup"
    args["range_source"] = _range_source_from_discourse(decision)
    rebuilt = _override_call(name, args, tools)
    if rebuilt:
        rebuilt["id"] = str(call.get("id") or rebuilt.get("id") or "selector_call_1")
    return rebuilt or call


def copy_args(call: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(call.get("args") if isinstance(call.get("args"), dict) else {}, default=str))


def _mention_filters(subject: dict[str, str]) -> dict[str, str]:
    filters = {"mention": str(subject.get("raw") or "").strip()}
    type_hint = str(subject.get("type_hint") or "").strip().lower()
    if type_hint in {"merchant", "category", "account"}:
        filters["mention_type"] = type_hint
    return {key: value for key, value in filters.items() if value}


def _tool_view_from_discourse(*, decision: dict[str, Any], prior_frame: dict[str, Any]) -> tuple[str, str]:
    requested_output = str(decision.get("requested_output") or "").strip().lower()
    intent = str(decision.get("intent") or "").strip().lower()
    prior_tool = canonical_semantic_tool_name(str(prior_frame.get("tool") or "")) if prior_frame else ""
    prior_view = str(prior_frame.get("view") or "").strip().lower()

    if requested_output == "rows":
        return "query_transactions", "list"
    if prior_tool == "query_transactions":
        return "query_transactions", prior_view if prior_view in {"latest", "list", "search", "detail"} else "list"
    if prior_tool == "review_budget" or "budget" in intent:
        return "review_budget", "category_status"
    if prior_tool == "check_affordability" or "afford" in intent:
        return "check_affordability", "purchase"
    if prior_tool == "summarize_spending" or requested_output == "scalar_total" or "spend" in intent or "expense" in intent:
        return "summarize_spending", "entity_total"
    return "summarize_spending", "entity_total"


def _range_from_discourse(
    *,
    decision: dict[str, Any],
    prior_frame: dict[str, Any],
    history: list[dict[str, Any]] | None,
) -> str:
    value = str(decision.get("range") or "").strip()
    if value.lower() in {"inherited", "same", "prior"}:
        return str(prior_frame.get("range") or prior_frame.get("range_b") or prior_frame.get("range_a") or latest_context_range(history) or "").strip()
    return value


def _range_source_from_discourse(decision: dict[str, Any]) -> str:
    value = str(decision.get("range") or "").strip().lower()
    if value in {"inherited", "same", "prior"}:
        return "inherited"
    if value:
        return "explicit_user"
    return "none"


def _payload_from_discourse(*, tool_name: str, decision: dict[str, Any], prior_frame: dict[str, Any]) -> dict[str, Any]:
    if tool_name != "summarize_spending":
        return {}
    prior_payload = prior_frame.get("payload") if isinstance(prior_frame.get("payload"), dict) else {}
    metric = str(prior_payload.get("metric") or "").strip()
    intent = str(decision.get("intent") or "").strip().lower()
    if not metric and ("spend" in intent or "expense" in intent or str(decision.get("requested_output") or "") == "scalar_total"):
        metric = "expenses"
    return {"metric": metric} if metric else {}


def _call_from_pending_clarification(
    *,
    decision: dict[str, Any],
    pending: dict[str, Any],
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    option = _pending_option_from_choice(decision=decision, pending=pending)
    if not option:
        return {}
    resume = pending.get("resume_frame") if isinstance(pending.get("resume_frame"), dict) else {}
    tool_name = str(resume.get("tool") or "summarize_spending").strip()
    args = resume.get("args") if isinstance(resume.get("args"), dict) else {}
    args = json.loads(json.dumps(args, default=str))
    filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
    filters = dict(filters)
    entity_type = str(option.get("type") or option.get("entity_type") or "").strip()
    value = str(option.get("canonical") or option.get("value") or option.get("label") or "").strip()
    if entity_type and value:
        for key in ("mention", "mention_type", "merchant", "category", "account"):
            filters.pop(key, None)
        filters[entity_type] = value
    args["filters"] = filters
    args["context_action"] = "clarification_reply"
    return _override_call(tool_name, args, tools)


def _pending_option_from_choice(*, decision: dict[str, Any], pending: dict[str, Any]) -> dict[str, Any]:
    patch = decision.get("frame_patch") if isinstance(decision.get("frame_patch"), dict) else {}
    choice = str(patch.get("clarification_choice") or decision.get("clarification_choice") or _decision_subject(decision).get("raw") or "").strip().lower()
    if not choice:
        return {}
    options = pending.get("options") if isinstance(pending.get("options"), list) else []
    typed = [item for item in options if isinstance(item, dict) and str(item.get("type") or "").strip().lower() == choice]
    if len(typed) == 1:
        return typed[0]
    for item in options:
        if not isinstance(item, dict):
            continue
        candidates = {
            str(item.get("id") or "").strip().lower(),
            str(item.get("label") or "").strip().lower(),
            str(item.get("canonical") or item.get("value") or "").strip().lower(),
        }
        if choice in candidates:
            return item
    return {}


def apply_context_semantics(
    *,
    calls: list[dict[str, Any]],
    decision: dict[str, Any],
    history: list[dict[str, Any]] | None,
    question: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _ = question
    if decision.get("frame_patch_compiled"):
        return calls, decision
    prior_range = latest_context_range(history)
    if not prior_range:
        return calls, decision

    changed = False
    out_calls: list[dict[str, Any]] = []
    for call in calls:
        call_out = {**call, "args": dict(call.get("args") or {})}
        universal = dict(call.get("universal_args") or {})
        source = str(universal.get("range_source") or "").strip().lower()
        action = str(universal.get("context_action") or "").strip().lower()
        range_value = str(universal.get("range") or call_out["args"].get("range") or "").strip().lower()
        resolved = ""
        if source == "previous_of_prior" or range_value == "previous_of_prior":
            resolved = resolve_followup_range("month before", prior_range).token
            universal["range_source"] = "previous_of_prior"
        elif _is_previous_of_prior_range_token(range_value) and source not in {"inherited", "inherit"}:
            resolved = resolve_followup_range("month before", prior_range).token
            universal["range_source"] = "previous_of_prior"
        elif source == "next_of_prior" or range_value == "next_of_prior":
            resolved = resolve_followup_range("next month", prior_range).token
            universal["range_source"] = "next_of_prior"
        elif (
            prior_range
            and _call_accepts_single_range(call_out)
            and action in {"followup", "followup_same_subject", "followup_replace_subject", "correction"}
            and source in {"inherited", "inherit"}
        ):
            resolved = prior_range
            universal["range_source"] = "inherited"
        if resolved and call_out["args"].get("range") != resolved:
            call_out["args"]["range"] = resolved
            universal["range"] = resolved
            call_out["universal_args"] = universal
            changed = True
        out_calls.append(call_out)

    if changed:
        decision = {
            **decision,
            "calls": out_calls,
            "context_semantics_applied": True,
            "prior_range": prior_range,
        }
    return out_calls, decision


def _call_accepts_single_range(call: dict[str, Any]) -> bool:
    name = str(call.get("name") or call.get("tool") or "").strip()
    return name in _SINGLE_RANGE_TOOLS


def _override_call(tool_name: str, args: dict[str, Any], tools: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = tools_by_name(tools)
    tool_name = canonical_semantic_tool_name(tool_name)
    if tool_name not in by_name:
        return {}
    normalized_call = {
        "tool": tool_name,
        "view": args.get("view") or "list",
        "context_action": "new",
        "range_source": "none",
        **args,
    }
    adapted_args, validation_error = adapt_universal_args(tool_name, normalized_call, by_name[tool_name])
    return {
        "id": "selector_call_1",
        "name": tool_name,
        "universal_args": {
            key: normalized_call.get(key)
            for key in UNIVERSAL_ARG_FIELDS
            if normalized_call.get(key) not in (None, "", [], {})
        },
        "args": adapted_args,
        "validation_error": validation_error,
    }


def latest_context_range(history: list[dict[str, Any]] | None) -> str:
    prior_frame = latest_prior_frame(history)
    value = str(prior_frame.get("range") or prior_frame.get("range_b") or prior_frame.get("range_a") or "").strip()
    if value:
        return value
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        conversation_frame = context.get("conversation_frame") if isinstance(context.get("conversation_frame"), dict) else {}
        value = str(conversation_frame.get("range") or "").strip()
        if value:
            return value
        ranges = context.get("ranges") if isinstance(context.get("ranges"), list) else []
        for item in ranges:
            value = str(item or "").strip()
            if value:
                return value
        tools = context.get("tools") if isinstance(context.get("tools"), list) else []
        for tool in tools:
            args = tool.get("args") if isinstance(tool, dict) and isinstance(tool.get("args"), dict) else {}
            value = str(args.get("range") or "").strip()
            if value:
                return value
        tool_context = turn.get("tool_context") if isinstance(turn.get("tool_context"), list) else []
        for tool in tool_context:
            args = tool.get("args") if isinstance(tool, dict) and isinstance(tool.get("args"), dict) else {}
            value = str(args.get("range") or "").strip()
            if value:
                return value
    return ""


def parse_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = json.loads(_extract_json_object(text))
    if not isinstance(payload, dict):
        raise ValueError("selector response must be a JSON object")
    return payload


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        raise ValueError("selector response did not contain a JSON object")
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_string:
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:index + 1]
    raise ValueError("selector response contained incomplete JSON")


def decision_needs_family_detail(decision: dict[str, Any], calls: list[dict[str, Any]]) -> bool:
    if calls:
        return False
    need_detail = decision.get("need_detail")
    if isinstance(need_detail, str):
        need_detail = need_detail.strip().lower() in {"1", "true", "yes"}
    return bool(need_detail or selected_family(decision))


def decision_requests_general_answer(decision: dict[str, Any]) -> bool:
    controller_route = str(decision.get("controller_route") or "").strip().lower()
    if controller_route in _CHAT_ROUTES:
        return True
    route = str(decision.get("route") or decision.get("answer_path") or "").strip().lower()
    return _ROUTE_ALIASES.get(route, route) in _CHAT_ROUTES


def selected_family(decision: dict[str, Any]) -> str:
    return selected_family_name(decision.get("family") or decision.get("tool_family"))


def selector_needs_repair(decision: dict[str, Any], calls: list[dict[str, Any]]) -> bool:
    if decision_requests_general_answer(decision):
        return False
    if decision.get("error"):
        return True
    if decision.get("validation_errors"):
        return True
    return any(str(call.get("validation_error") or "").strip() for call in calls)


def selector_status(decision: dict[str, Any], calls: list[dict[str, Any]]) -> str:
    if calls and not selector_needs_repair(decision, calls):
        return "tool_calls"
    if decision_requests_general_answer(decision):
        return "general_answer"
    if decision_needs_family_detail(decision, calls):
        return "need_detail"
    return "clarify"


def selector_error(decision: dict[str, Any], calls: list[dict[str, Any]]) -> str:
    if decision.get("error"):
        return str(decision.get("error"))
    errors = [str(item) for item in decision.get("validation_errors") or [] if str(item)]
    errors.extend(str(call.get("validation_error")) for call in calls if str(call.get("validation_error") or ""))
    return "; ".join(errors)


def format_recent_context(history: list[dict[str, Any]] | None, *, limit: int = 3, max_chars: int = 120) -> str:
    lines = []
    for turn in (history or [])[-limit:]:
        role = str(turn.get("role") or "").strip()
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        if answer_context:
            compact_context = _compact_answer_context(answer_context)
            if compact_context:
                lines.append(
                    "ctx: "
                    + json.dumps(
                        compact_context,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        default=str,
                    )
                )
        content = " ".join(str(turn.get("content") or "").split())
        if role not in {"user", "assistant"} or not content:
            continue
        if len(content) > max_chars:
            content = content[:max_chars].rsplit(" ", 1)[0].rstrip() + "..."
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _compact_answer_context(answer_context: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    frame = answer_context.get("conversation_frame") if isinstance(answer_context.get("conversation_frame"), dict) else {}
    if not frame:
        current_frame = answer_context.get("current_frame") if isinstance(answer_context.get("current_frame"), dict) else {}
        frame = _conversation_frame_from_semantic_frame(current_frame)
    compact_frame = _compact_conversation_frame(frame)
    if compact_frame:
        out["frame"] = compact_frame
    else:
        subject = str(answer_context.get("subject") or "").strip()
        ranges = answer_context.get("ranges") if isinstance(answer_context.get("ranges"), list) else []
        if subject:
            out["subject"] = subject
        if answer_context.get("subject_type"):
            out["subject_type"] = answer_context.get("subject_type")
        if ranges:
            out["range"] = ranges[0]

    pending = answer_context.get("pending_clarification") if isinstance(answer_context.get("pending_clarification"), dict) else {}
    compact_pending = _compact_pending_clarification(pending)
    if compact_pending:
        out["pending"] = compact_pending
    return out


def _compact_conversation_frame(frame: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(frame, dict):
        return {}
    subject = frame.get("subject") if isinstance(frame.get("subject"), dict) else {}
    payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
    compact_subject = {
        key: subject.get(key)
        for key in ("type", "canonical", "raw")
        if subject.get(key) not in (None, "", [], {})
    }
    compact_payload = {
        key: payload.get(key)
        for key in ("metric", "group_by")
        if payload.get(key) not in (None, "", [], {})
    }
    out = {
        "intent": frame.get("intent"),
        "tool": frame.get("tool"),
        "view": frame.get("view"),
        "subject": compact_subject,
        "range": frame.get("range"),
        "output": frame.get("requested_output"),
        "payload": compact_payload,
    }
    return {key: value for key, value in out.items() if value not in (None, "", {}, [])}


def _compact_pending_clarification(pending: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(pending, dict) or not pending:
        return {}
    options = []
    raw_options = pending.get("options") if isinstance(pending.get("options"), list) else []
    for option in raw_options[:4]:
        if not isinstance(option, dict):
            continue
        compact = {
            key: option.get(key)
            for key in ("id", "type", "canonical", "label")
            if option.get(key) not in (None, "", [], {})
        }
        if compact:
            options.append(compact)
    out = {
        "kind": pending.get("kind"),
        "raw": pending.get("raw"),
        "options": options,
    }
    return {key: value for key, value in out.items() if value not in (None, "", [], {})}


def estimate_tokens(text: str) -> int:
    return max(1, len(text or "") // 4)


def resolve_selector_num_ctx(
    *,
    selector_model: str,
    answer_model: str,
    selector_num_ctx: int | None,
    answer_num_ctx: int | None,
) -> tuple[int | None, str]:
    if (
        selector_model
        and answer_model
        and selector_model == answer_model
        and selector_num_ctx
        and answer_num_ctx
        and selector_num_ctx != answer_num_ctx
    ):
        return answer_num_ctx, "selector and answer use the same model; using answer_num_ctx for selector to avoid Ollama reloads"
    return selector_num_ctx, ""


def _default_completer(prompt: str, max_tokens: int, purpose: str) -> str:
    import llm_client

    return llm_client.complete(
        prompt,
        max_tokens=max_tokens,
        purpose=purpose,
        response_format=SELECTOR_RESPONSE_SCHEMA,
    )


__all__ = [
    "SELECTOR_RESPONSE_SCHEMA",
    "SelectorResult",
    "apply_context_semantics",
    "apply_controller_route_permissions",
    "apply_discourse_frames",
    "build_repair_prompt",
    "build_selector_prompt",
    "build_selector_repair_system_prompt",
    "build_selector_system_prompt",
    "canonical_controller_route",
    "decision_needs_family_detail",
    "decision_requests_general_answer",
    "format_recent_context",
    "latest_pending_clarification",
    "latest_context_range",
    "normalize_selector_decision",
    "parse_json_object",
    "resolve_selector_num_ctx",
    "run_selector",
    "selected_family",
    "selector_needs_repair",
    "selector_status",
]
