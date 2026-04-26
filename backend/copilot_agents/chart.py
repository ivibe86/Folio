from __future__ import annotations

import re

from .base import emit_done_with_memory, tool_loop_result, tool_loop_stream


def chart_kind_from_question(question: str, history: list[dict] | None = None) -> str | None:
    import copilot_agent as core

    current = (question or "").lower()
    if re.search(r"\b(spending|spent|expenses?|groceries|grocery|dining|shopping|travel|subscriptions?)\b", current):
        return "spending"
    if re.search(r"\bnet\s*worth|networth\b", current):
        return "net_worth"
    if re.search(r"\b(that|those|same|every month|monthly|chart|graph|plot|trend)\b", current):
        if core._extract_chart_spending_category(question, history):
            return "spending"
    context = core._history_text(history).lower()
    if re.search(r"\b(spending|spent|expenses?|groceries|grocery|dining|shopping|travel|subscriptions?)\b", context):
        return "spending"
    if re.search(r"\bnet\s*worth|networth\b", context):
        return "net_worth"
    return None


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    cache: dict = {}
    chart_kind = chart_kind_from_question(question, history)
    if chart_kind:
        answer, trend, trace, chart = core._execute_chart_plan(chart_kind, question, profile, history, cache)
        result = core._finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            run_detector=True,
        )
        if chart.get("_chart"):
            result["chart"] = chart
        result["data"] = trend.get("series") or result.get("data")
        result["data_source"] = trace[0]["name"] if trace else result.get("data_source")
        result["llm_calls"] = 0
        return result

    return tool_loop_result(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.TREND_CHART_TOOLS),
        system=core._build_system_prompt(profile, list(core.TREND_CHART_TOOLS)),
    )


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    chart_kind = chart_kind_from_question(question, history)
    if chart_kind:
        cache: dict = {}
        yield {"type": "reset_text"}
        answer, trend, trace, chart = core._execute_chart_plan(chart_kind, question, profile, history, cache)
        for call in trace:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            yield {"type": "tool_result", "name": call["name"], "duration_ms": call.get("duration_ms", 0)}
        chart_payload = chart if chart.get("_chart") else None
        if chart_payload:
            yield {"type": "chart", "chart": chart_payload}
        yield from emit_done_with_memory(
            question=question,
            profile=profile,
            final_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            pending_chart=chart_payload,
            data=trend.get("series"),
            data_source=trace[0]["name"] if trace else None,
            llm_calls=0,
        )
        return

    yield from tool_loop_stream(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.TREND_CHART_TOOLS),
        system=core._build_system_prompt(profile, list(core.TREND_CHART_TOOLS)),
    )
