from __future__ import annotations

from typing import Any


def route_question(
    question: str,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """Compatibility route payload for old diagnostics.

    The legacy classifier/current/shadow runtimes have been staged under
    retired/. Production dispatch goes directly through Mira vNext.
    """
    return {
        "intent": "agentic_vnext",
        "operation": forced_intent or "vnext",
        "question": question,
        "uses_history": bool(history),
        "profile": profile,
        "selected_tools": [],
        "tool_schema_tokens_est": 0,
        "legacy_router_used": False,
    }


def answer_context_for_route(route: dict, tool_trace: list[dict], provenance: dict | None = None) -> dict | None:
    return None


def planned_tools_for_route(route: dict) -> list[str]:
    selected = route.get("selected_tools") if isinstance(route, dict) else None
    return [str(tool) for tool in selected] if isinstance(selected, list) else []


def selected_schema_tokens(tool_names: list[str] | tuple[str, ...]) -> int:
    return 0


def run_agent(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None) -> dict:
    from mira.agentic.vnext_runtime import run_vnext_result

    return run_vnext_result(question, profile, history, forced_intent=forced_intent)


def run_agent_stream(question: str, profile: str | None, history: list[dict] | None = None, forced_intent: str | None = None):
    from mira.agentic.vnext_runtime import run_vnext_stream

    return run_vnext_stream(question, profile, history, forced_intent=forced_intent)


__all__ = [
    "answer_context_for_route",
    "planned_tools_for_route",
    "route_question",
    "run_agent",
    "run_agent_stream",
    "selected_schema_tokens",
]
