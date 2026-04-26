from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import llm_client
from copilot_tools import execute_tool

from .base import emit_done_with_memory

logger = logging.getLogger(__name__)


def _summarize_tool_payload(result: Any, *, key: str, limit: int = 5) -> Any:
    import copilot_agent as core

    return core._summarize_tool_payload(result, key=key, limit=limit)


def build_prompt(question: str, profile: str | None) -> tuple[dict, list[dict], str, str]:
    import copilot_agent as core

    cache: dict = {}
    payload: dict[str, Any] = {}
    trace: list[dict] = []
    calls = [
        ("get_recurring_summary", {"limit": 5}, "recurring", "items"),
        ("get_top_merchants", {"range": "current_month", "limit": 5}, "top_merchants", "merchants"),
        ("get_top_categories", {"range": "current_month", "limit": 5}, "top_categories", "categories"),
    ]
    for name, args, payload_key, list_key in calls:
        start = datetime.now()
        result = execute_tool(name, args, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": name, "args": args, "duration_ms": duration_ms})
        payload[payload_key] = _summarize_tool_payload(result, key=list_key)
    prompt = (
        "User asked: " + question + "\n\n"
        "Live tool results:\n" + json.dumps(payload, ensure_ascii=True, default=str)
    )
    return cache, trace, prompt, core._fast_watch_system(profile)


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    cache, trace, prompt, system = build_prompt(question, profile)
    response = llm_client.chat_with_tools(
        messages=[{"role": "user", "content": prompt}],
        tools=[],
        system=system,
        max_tokens=420,
        purpose="copilot",
    )
    result = core._finalize_answer(
        question=question,
        profile=profile,
        raw_answer=(response.get("content") or "").strip(),
        trace=trace,
        cache=cache,
        iterations=0,
        run_detector=True,
    )
    result["llm_calls"] = 1
    return result


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    cache: dict = {}
    trace: list[dict] = []
    payload: dict[str, Any] = {}
    yield {"type": "reset_text"}
    calls = [
        ("get_recurring_summary", {"limit": 5}, "recurring", "items"),
        ("get_top_merchants", {"range": "current_month", "limit": 5}, "top_merchants", "merchants"),
        ("get_top_categories", {"range": "current_month", "limit": 5}, "top_categories", "categories"),
    ]
    for name, args, payload_key, list_key in calls:
        yield {"type": "tool_call", "name": name, "args": args}
        start = datetime.now()
        result = execute_tool(name, args, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": name, "args": args, "duration_ms": duration_ms})
        payload[payload_key] = _summarize_tool_payload(result, key=list_key)
        yield {"type": "tool_result", "name": name, "duration_ms": duration_ms}
    prompt = (
        "User asked: " + question + "\n\n"
        "Live tool results:\n" + json.dumps(payload, ensure_ascii=True, default=str)
    )
    final_parts: list[str] = []
    try:
        for event_type, payload_part in llm_client.chat_with_tools_stream(
            messages=[{"role": "user", "content": prompt}],
            tools=[],
            system=core._fast_watch_system(profile),
            max_tokens=420,
            purpose="copilot",
        ):
            if event_type == "text":
                final_parts.append(payload_part)
                yield {"type": "token", "text": payload_part}
    except Exception as e:
        logger.exception("overview specialist stream failed")
        yield {"type": "error", "message": f"Copilot hit an error: {e}"}
        return
    yield from emit_done_with_memory(
        question=question,
        profile=profile,
        final_answer="".join(final_parts).strip() or "I couldn't land on a confident answer from the available data.",
        trace=trace,
        cache=cache,
        iterations=0,
        llm_calls=1,
    )
