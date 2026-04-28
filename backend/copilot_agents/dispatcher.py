from __future__ import annotations

from . import chat, chart, drilldown, overview, sql, write
from .classifier import planned_tools_for_route, route_question, selected_schema_tokens


def _annotate_route(route: dict) -> dict:
    route["selected_tools"] = planned_tools_for_route(route)
    route["tool_schema_tokens_est"] = selected_schema_tokens(route["selected_tools"])
    return route


def run_agent(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None) -> dict | None:
    route = _annotate_route(route_question(question, history, forced_intent=forced_intent, profile=profile))
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
        }
    if route.get("needs_clarification"):
        return {
            "answer": route.get("clarification_question") or "I need one more detail to answer that cleanly.",
            "tool_trace": [],
            "iterations": 0,
            "error": None,
            "data": None,
            "data_source": None,
            "route": route,
        }

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
    elif intent == "sql":
        result = sql.run(question, profile, history)
    else:
        return None

    result["route"] = route
    return result


def run_agent_stream(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None):
    route = _annotate_route(route_question(question, history, forced_intent=forced_intent, profile=profile))
    if route.get("intent") == "legacy":
        return None

    def _with_route(events):
        for event in events:
            if event.get("type") == "done":
                event = {**event, "route": route, "intent": route.get("intent")}
            yield event

    def _events():
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
            "needs_clarification": route.get("needs_clarification"),
            "route_ms": route.get("route_ms"),
            "classifier_ms": route.get("classifier_ms"),
            "selected_tools": route.get("selected_tools"),
            "tool_schema_tokens_est": route.get("tool_schema_tokens_est"),
        }
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
            }
        elif intent == "overview":
            yield from _with_route(overview.stream(question, profile, history))
        elif intent == "chart":
            yield from _with_route(chart.stream(question, profile, history, route=route))
        elif intent == "chat":
            yield from _with_route(chat.stream(question, profile, history))
        elif intent in {"drilldown", "spending", "transactions"}:
            yield from _with_route(drilldown.stream(question, profile, history, route=route))
        elif intent == "write":
            yield from _with_route(write.stream(question, profile, history, route=route))
        elif intent == "sql":
            yield from _with_route(sql.stream(question, profile, history))

    return _events()
