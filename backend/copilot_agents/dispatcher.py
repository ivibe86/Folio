from __future__ import annotations

from . import chat, chart, drilldown, overview, sql, write
from .classifier import planned_tools_for_route, route_question, selected_schema_tokens


def _annotate_route(route: dict) -> dict:
    route["selected_tools"] = planned_tools_for_route(route)
    route["tool_schema_tokens_est"] = selected_schema_tokens(route["selected_tools"])
    return route


def run_agent(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None) -> dict | None:
    route = _annotate_route(route_question(question, history, forced_intent=forced_intent))
    intent = route.get("intent")
    if intent == "legacy":
        return None

    if intent == "overview":
        result = overview.run(question, profile, history)
    elif intent == "chart":
        result = chart.run(question, profile, history)
    elif intent == "chat":
        result = chat.run(question, profile, history)
    elif intent == "drilldown":
        result = drilldown.run(question, profile, history)
    elif intent == "write":
        result = write.run(question, profile, history)
    elif intent == "sql":
        result = sql.run(question, profile, history)
    else:
        return None

    result["route"] = route
    return result


def run_agent_stream(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None):
    route = _annotate_route(route_question(question, history, forced_intent=forced_intent))
    if route.get("intent") == "legacy":
        return None

    def _events():
        intent = route["intent"]
        yield {
            "type": "route",
            "intent": intent,
            "shortcut": route.get("shortcut"),
            "route_ms": route.get("route_ms"),
            "classifier_ms": route.get("classifier_ms"),
            "selected_tools": route.get("selected_tools"),
            "tool_schema_tokens_est": route.get("tool_schema_tokens_est"),
        }
        if intent == "overview":
            yield from overview.stream(question, profile, history)
        elif intent == "chart":
            yield from chart.stream(question, profile, history)
        elif intent == "chat":
            yield from chat.stream(question, profile, history)
        elif intent == "drilldown":
            yield from drilldown.stream(question, profile, history)
        elif intent == "write":
            yield from write.stream(question, profile, history)
        elif intent == "sql":
            yield from sql.stream(question, profile, history)

    return _events()
