from __future__ import annotations

from copilot_tools import execute_tool
from mira import answer_composer
from mira import provenance
from range_parser import words as range_words
from .base import emit_done_with_memory, tool_loop_result, tool_loop_stream


def chart_kind_from_question(question: str, history: list[dict] | None = None) -> str | None:
    import copilot_agent as core

    current_tokens = set(range_words(question))
    spending_terms = {"spending", "spent", "expense", "expenses", "groceries", "grocery", "dining", "shopping", "travel", "subscription", "subscriptions"}
    if current_tokens & spending_terms:
        return "spending"
    if "networth" in current_tokens or {"net", "worth"}.issubset(current_tokens):
        return "net_worth"
    if current_tokens & {"that", "those", "same", "monthly", "chart", "graph", "plot", "trend"} or {"every", "month"}.issubset(current_tokens):
        if core._extract_chart_spending_category(question, history):
            return "spending"
    context_tokens = set(range_words(core._history_text(history)))
    if context_tokens & spending_terms:
        return "spending"
    if "networth" in context_tokens or {"net", "worth"}.issubset(context_tokens):
        return "net_worth"
    return None


def _build_monthly_spending_chart_from_route(route: dict, question: str, profile: str | None, cache: dict) -> tuple[str, dict, list[dict], dict]:
    import copilot_agent as core
    from datetime import datetime

    args = dict(route.get("args") or {})
    try:
        months = max(1, min(int(args.get("months") or 6), 36))
    except (TypeError, ValueError):
        months = 6
    category = (args.get("category") or "").strip()
    trend_args = {"months": months}
    if category:
        trend_args["category"] = category

    trace: list[dict] = []
    start = datetime.now()
    trend = execute_tool("get_monthly_spending_trend", trend_args, profile, cache=cache)
    trace.append({
        "name": "get_monthly_spending_trend",
        "args": trend_args,
        "duration_ms": int((datetime.now() - start).total_seconds() * 1000),
    })

    labels = trend.get("labels", []) if isinstance(trend, dict) else []
    values = trend.get("values", []) if isinstance(trend, dict) else []
    subject = f"{category} spending" if category else "spending"
    chart_args = {
        "type": "line",
        "title": f"{subject.title()} - Last {months} Months",
        "series_name": subject.title(),
        "labels": labels,
        "values": values,
        "unit": "currency",
    }
    start = datetime.now()
    chart = execute_tool("plot_chart", chart_args, profile, cache=cache)
    trace.append({
        "name": "plot_chart",
        "args": chart_args,
        "duration_ms": int((datetime.now() - start).total_seconds() * 1000),
    })

    if values:
        peak_idx = max(range(len(values)), key=lambda i: values[i])
        answer = (
            f"Done - here's your {subject} trend for the last {months} months. "
            f"Latest month is ${values[-1]:,.2f}; peak was {labels[peak_idx]} at ${values[peak_idx]:,.2f}."
        )
    else:
        answer = f"I couldn't find {subject} data for the last {months} months."
    return answer, trend if isinstance(trend, dict) else {}, trace, chart if isinstance(chart, dict) else {}


def _chart_from_route(route: dict | None, question: str, profile: str | None, cache: dict) -> tuple[str, dict, list[dict], dict] | None:
    if not route or route.get("intent") != "chart":
        return None
    import copilot_agent as core

    if route.get("operation") == "net_worth_chart" or route.get("tool_name") == "get_net_worth_trend":
        return core._build_net_worth_chart(profile, cache)
    if route.get("operation") == "monthly_spending_chart" or route.get("tool_name") == "get_monthly_spending_trend":
        return _build_monthly_spending_chart_from_route(route, question, profile, cache)
    return None


def run(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None) -> dict:
    import copilot_agent as core

    cache: dict = {}
    routed_chart = _chart_from_route(route, question, profile, cache)
    chart_kind = None if routed_chart else chart_kind_from_question(question, history)
    if routed_chart or chart_kind:
        answer, trend, trace, chart = routed_chart or core._execute_chart_plan(chart_kind, question, profile, history, cache)
        answer = answer_composer.compose_finance_answer(route, trace, cache, profile) or answer
        result = core._finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            run_detector=True,
            route=route,
        )
        if chart.get("_chart"):
            result["chart"] = chart
        result["data"] = trend.get("series") or result.get("data")
        result["data_source"] = trace[0]["name"] if trace else result.get("data_source")
        result["llm_calls"] = 0
        return provenance.attach_completed_action(
            result,
            profile=profile,
            question=question,
            route=route,
            trace=trace,
            cache=cache,
        )

    return tool_loop_result(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.TREND_CHART_TOOLS),
        system=core._build_system_prompt(profile, list(core.TREND_CHART_TOOLS)),
    )


def stream(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None):
    import copilot_agent as core

    cache: dict = {}
    routed_chart = _chart_from_route(route, question, profile, cache)
    chart_kind = None if routed_chart else chart_kind_from_question(question, history)
    if routed_chart or chart_kind:
        yield {"type": "reset_text"}
        answer, trend, trace, chart = routed_chart or core._execute_chart_plan(chart_kind, question, profile, history, cache)
        for call in trace:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            yield {"type": "tool_result", "name": call["name"], "duration_ms": call.get("duration_ms", 0)}
        chart_payload = chart if chart.get("_chart") else None
        if chart_payload:
            yield {"type": "chart", "chart": chart_payload}
        yield from emit_done_with_memory(
            question=question,
            profile=profile,
            final_answer=answer_composer.compose_finance_answer(route, trace, cache, profile) or answer,
            trace=trace,
            cache=cache,
            iterations=0,
            pending_chart=chart_payload,
            data=trend.get("series"),
            data_source=trace[0]["name"] if trace else None,
            llm_calls=0,
            route=route,
        )
        return

    yield from tool_loop_stream(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.TREND_CHART_TOOLS),
        system=core._build_system_prompt(profile, list(core.TREND_CHART_TOOLS)),
    )
