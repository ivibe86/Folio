from __future__ import annotations

from mira import controller as conversation_controller
from mira import domain_actions
from mira import provenance

from . import chat, chart, drilldown, overview, planner, sql, write
from .classifier import answer_context_for_route, planned_tools_for_route, route_question, selected_schema_tokens


def _annotate_route(route: dict, profile: str | None) -> dict:
    conversation_controller.ensure_controller_act(route)
    domain_actions.annotate_route(route, profile)
    route["selected_tools"] = domain_actions.tool_names_for_action(route.get("domain_action")) or planned_tools_for_route(route)
    route["tool_schema_tokens_est"] = selected_schema_tokens(route["selected_tools"])
    return route


def _record_result_state(route: dict, result: dict, profile: str | None) -> dict:
    if isinstance(route.get("dialogue_state"), dict) and result.get("answer"):
        result["dialogue_state"] = route.get("dialogue_state")
        conversation_controller.record_active_clarification(profile, route.get("dialogue_state"), result.get("answer") or "")
    if isinstance(result.get("answer_context"), dict) and result.get("answer"):
        conversation_controller.record_answer_context(profile, result.get("answer") or "", result.get("answer_context"))
    return result


def _progress_event_for_route(route: dict) -> dict:
    action = route.get("domain_action") if isinstance(route.get("domain_action"), dict) else {}
    status = action.get("status")
    tool_plan = action.get("tool_plan") if isinstance(action.get("tool_plan"), list) else []
    intent = route.get("intent")
    if route.get("needs_clarification") or status == "clarify":
        stage = "clarify"
        label = "Checking the grounded match"
    elif status == "ready" and tool_plan:
        stage = "action"
        label = "Checking Folio data"
    elif intent == "chat":
        stage = "model"
        label = "Thinking locally"
    elif intent == "overview":
        stage = "summary"
        label = "Building the finance summary"
    else:
        stage = "route"
        label = "Preparing the answer"
    return {
        "type": "progress",
        "stage": stage,
        "label": label,
        "intent": intent,
        "operation": route.get("operation"),
        "selected_tools": route.get("selected_tools") or [],
        "domain_action_name": action.get("name"),
        "domain_action_status": status,
    }


def _local_only_provider_result(route: dict) -> dict:
    answer = (
        "Mira is local-LLM-only in this build, so I can't switch Folio to Anthropic, Claude, OpenAI, or another cloud LLM provider. "
        "Use Control Center to pick an installed Ollama model for local AI."
    )
    return {
        "answer": answer,
        "tool_trace": [],
        "iterations": 0,
        "error": None,
        "data": None,
        "data_source": None,
        "route": route,
        "llm_calls": 0,
    }


def _grounding_explanation_result(route: dict, profile: str | None, history: list[dict] | None = None) -> dict:
    context = conversation_controller.answer_context_from_history(profile, history)
    answer = provenance.explain_last_answer(profile, context)
    if not answer:
        answer = (
            "That answer should only come from Folio tool results: merchant/category spend tools and the matching transaction rows behind them. "
            "If I gave numbers but then could not show the transactions, treat that prior answer as invalid and ask me to rerun it; I should return the exact tool-backed totals or say I cannot find the merchant."
        )
    return {
        "answer": answer,
        "tool_trace": [],
        "iterations": 0,
        "error": None,
        "data": None,
        "data_source": None,
        "route": route,
        "dialogue_state": route.get("dialogue_state"),
        "llm_calls": 0,
    }


def _direct_answer_result(route: dict) -> dict:
    return {
        "answer": (route.get("args") or {}).get("answer") or "Got it.",
        "tool_trace": [],
        "iterations": 0,
        "error": None,
        "data": None,
        "data_source": None,
        "route": route,
        "dialogue_state": route.get("dialogue_state"),
        "llm_calls": 0,
    }


def _domain_action_clarification_result(route: dict) -> dict:
    action = route.get("domain_action") if isinstance(route.get("domain_action"), dict) else {}
    return {
        "answer": action.get("clarification_question") or "I need one more detail to answer that cleanly.",
        "tool_trace": [],
        "iterations": 0,
        "error": None,
        "data": None,
        "data_source": None,
        "route": route,
        "dialogue_state": route.get("dialogue_state"),
        "llm_calls": 0,
    }


def run_agent(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None) -> dict | None:
    route = _annotate_route(route_question(question, history, forced_intent=forced_intent, profile=profile), profile)
    intent = route.get("intent")
    if intent == "legacy":
        return None
    if intent == "error":
        return {
            "answer": route.get("error") or "Mira could not route that request.",
            "tool_trace": [],
            "iterations": 0,
            "error": route.get("error") or "router error",
            "data": None,
            "data_source": None,
            "route": route,
            "llm_calls": 0,
        }
    if route.get("needs_clarification"):
        return _record_result_state(route, {
            "answer": route.get("clarification_question") or "I need one more detail to answer that cleanly.",
            "tool_trace": [],
            "iterations": 0,
            "error": None,
            "data": None,
            "data_source": None,
            "route": route,
            "dialogue_state": route.get("dialogue_state"),
            "llm_calls": 0,
        }, profile)
    if (route.get("domain_action") or {}).get("status") == "clarify":
        return _record_result_state(route, _domain_action_clarification_result(route), profile)
    if route.get("operation") == "local_only_provider":
        return _record_result_state(route, _local_only_provider_result(route), profile)
    if route.get("operation") == "explain_grounding":
        return _record_result_state(route, _grounding_explanation_result(route, profile, history), profile)
    if route.get("operation") == "context_acknowledge":
        return _record_result_state(route, _direct_answer_result(route), profile)
    if route.get("operation") == "categorization_debug":
        result = sql.run(question, profile, history)
        result["route"] = route
        return result

    if intent == "overview":
        result = overview.run(question, profile, history)
    elif intent == "chart":
        result = chart.run(question, profile, history, route=route)
    elif intent == "chat":
        result = chat.run(question, profile, history)
    elif intent in {"drilldown", "spending", "transactions"}:
        result = drilldown.run(question, profile, history, route=route)
    elif intent == "write":
        result = write.run(question, profile, history, route=route)
    elif intent == "plan":
        result = planner.run(question, profile, history, route=route)
    else:
        return None

    result["route"] = route
    answer_context = answer_context_for_route(route, result.get("tool_trace") or [], provenance=result.get("provenance"))
    if answer_context:
        result["answer_context"] = answer_context
    return _record_result_state(route, result, profile)


def run_agent_stream(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None):
    def _with_route(events, route: dict):
        for event in events:
            if event.get("type") == "done":
                event = {**event, "route": route, "intent": route.get("intent")}
                answer_context = answer_context_for_route(route, event.get("tool_trace") or [], provenance=event.get("provenance"))
                if answer_context:
                    event["answer_context"] = answer_context
                    conversation_controller.record_answer_context(profile, event.get("answer") or "", answer_context)
                if isinstance(route.get("dialogue_state"), dict) and event.get("answer"):
                    event["dialogue_state"] = route.get("dialogue_state")
                    conversation_controller.record_active_clarification(profile, route.get("dialogue_state"), event.get("answer") or "")
            yield event

    def _events():
        yield {
            "type": "routing_started",
            "stage": "routing",
            "label": "Routing the request",
        }
        route = _annotate_route(route_question(question, history, forced_intent=forced_intent, profile=profile), profile)
        if route.get("intent") == "legacy":
            yield {"type": "error", "message": "Mira could not route that request cleanly."}
            return
        intent = route["intent"]
        yield {
            "type": "route",
            "intent": intent,
            "operation": route.get("operation"),
            "shortcut": route.get("shortcut"),
            "tool_name": route.get("tool_name"),
            "args": route.get("args") or {},
            "uses_history": route.get("uses_history"),
            "confidence": route.get("confidence"),
            "controller_act": route.get("controller_act"),
            "needs_clarification": route.get("needs_clarification"),
            "route_ms": route.get("route_ms"),
            "classifier_ms": route.get("classifier_ms"),
            "selected_tools": route.get("selected_tools"),
            "tool_schema_tokens_est": route.get("tool_schema_tokens_est"),
            "dialogue_state": route.get("dialogue_state"),
            "domain_action": route.get("domain_action"),
        }
        yield {
            "type": "controller",
            "controller_act": route.get("controller_act"),
            "route_ms": route.get("route_ms"),
            "classifier_ms": route.get("classifier_ms"),
        }
        yield {
            "type": "action",
            "domain_action": route.get("domain_action"),
            "selected_tools": route.get("selected_tools") or [],
            "tool_schema_tokens_est": route.get("tool_schema_tokens_est"),
        }
        yield _progress_event_for_route(route)
        if intent == "error":
            yield {"type": "error", "message": route.get("error") or "Mira could not route that request."}
        elif route.get("needs_clarification"):
            yield {
                "type": "done",
                "answer": route.get("clarification_question") or "I need one more detail to answer that cleanly.",
                "data": None,
                "data_source": None,
                "tool_trace": [],
                "iterations": 0,
                "llm_calls": 0,
                "route": route,
                "intent": intent,
                "dialogue_state": route.get("dialogue_state"),
            }
        elif (route.get("domain_action") or {}).get("status") == "clarify":
            result = _record_result_state(route, _domain_action_clarification_result(route), profile)
            yield {
                "type": "done",
                **result,
                "intent": intent,
            }
        elif route.get("operation") == "local_only_provider":
            result = _record_result_state(route, _local_only_provider_result(route), profile)
            yield {
                "type": "done",
                **result,
                "intent": intent,
            }
        elif route.get("operation") == "explain_grounding":
            result = _record_result_state(route, _grounding_explanation_result(route, profile, history), profile)
            yield {
                "type": "done",
                **result,
                "intent": intent,
            }
        elif route.get("operation") == "context_acknowledge":
            result = _record_result_state(route, _direct_answer_result(route), profile)
            yield {
                "type": "done",
                **result,
                "intent": intent,
            }
        elif route.get("operation") == "categorization_debug":
            yield from _with_route(sql.stream(question, profile, history), route)
        elif intent == "overview":
            yield from _with_route(overview.stream(question, profile, history), route)
        elif intent == "chart":
            yield from _with_route(chart.stream(question, profile, history, route=route), route)
        elif intent == "chat":
            yield from _with_route(chat.stream(question, profile, history), route)
        elif intent in {"drilldown", "spending", "transactions"}:
            yield from _with_route(drilldown.stream(question, profile, history, route=route), route)
        elif intent == "write":
            yield from _with_route(write.stream(question, profile, history, route=route), route)
        elif intent == "plan":
            yield from _with_route(planner.stream(question, profile, history, route=route), route)

    return _events()
