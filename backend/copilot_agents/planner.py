from __future__ import annotations

import json
from calendar import monthrange
from datetime import datetime
from typing import Any

import llm_client
from copilot_tools import execute_tool
from mira import answer_composer
from mira import domain_actions
from mira import memory_v2
from mira import provenance

from .base import emit_done_with_memory
from .drilldown import _parse_jsonish


ALLOWED_PLAN_TOOLS = {
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
}
MAX_PLAN_STEPS = 3


def _plan_prompt(question: str, history: list[dict] | None, route: dict | None) -> str:
    import copilot_agent as core

    return f"""Create a compact read-only Mira finance plan.
Return JSON only with this shape:
{{"steps":[{{"tool":"name","args":{{}}}}],"reason":"short"}}

Rules:
- Use at most {MAX_PLAN_STEPS} steps.
- Allowed tools only: {", ".join(sorted(ALLOWED_PLAN_TOOLS))}.
- Do not use run_sql or write-preview tools.
- Prefer direct spend tools for totals and get_monthly_spending_trend for averages/trends.
- Preserve exact category/merchant names from the router args or recent context when available.
- If the question is not answerable with these tools, return {{"steps":[],"reason":"unsupported"}}.

Router route:
{json.dumps(route or {}, ensure_ascii=True, default=str)}

Recent context:
{core._history_text(history, limit=4) or "(none)"}

Latest message: {question}
JSON:"""


def _validated_steps(parsed: dict | None) -> list[dict]:
    if not isinstance(parsed, dict):
        return []
    raw_steps = parsed.get("steps")
    if not isinstance(raw_steps, list):
        return []
    steps: list[dict] = []
    for item in raw_steps[:MAX_PLAN_STEPS]:
        if not isinstance(item, dict):
            continue
        tool = str(item.get("tool") or item.get("name") or "").strip()
        args = item.get("args") if isinstance(item.get("args"), dict) else {}
        if tool in ALLOWED_PLAN_TOOLS:
            steps.append({"name": tool, "args": args})
    return steps


def _subject_tool(subject_type: str) -> str | None:
    if subject_type == "category":
        return "get_category_spend"
    if subject_type == "merchant":
        return "get_merchant_spend"
    return None


def _subject_args(subject_type: str, subject: str, range_token: str) -> dict:
    key = "category" if subject_type == "category" else "merchant"
    return {key: subject, "range": range_token}


def _deterministic_steps(route: dict | None) -> list[dict]:
    action_steps = domain_actions.tool_plan_for_route(route, {"CompareSpend", "BudgetStatus", "CashFlowForecast", "Affordability"})
    if action_steps:
        return action_steps
    args = (route or {}).get("args") or {}
    kind = str(args.get("plan_kind") or "").strip()
    subject_type = str(args.get("subject_type") or "").strip()
    subject = str(args.get("subject") or "").strip()
    tool = _subject_tool(subject_type)
    if not kind or not tool or not subject:
        return []
    try:
        months = max(1, min(int(args.get("months") or 6), 12))
    except (TypeError, ValueError):
        months = 6

    if kind == "current_vs_previous":
        return [
            {"name": tool, "args": _subject_args(subject_type, subject, "current_month")},
            {"name": tool, "args": _subject_args(subject_type, subject, "last_month")},
        ]
    if kind in {"current_vs_average", "on_track"}:
        return [
            {"name": tool, "args": _subject_args(subject_type, subject, "current_month")},
            {"name": tool, "args": _subject_args(subject_type, subject, f"last_{months + 1}_months")},
        ]
    return []


def _make_plan(question: str, profile: str | None, history: list[dict] | None, route: dict | None) -> tuple[list[dict], int]:
    deterministic = _deterministic_steps(route)
    if deterministic:
        return deterministic, 0
    if not llm_client.is_available():
        return [], 0
    raw = llm_client.complete(_plan_prompt(question, history, route), max_tokens=420, purpose="controller")
    return _validated_steps(_parse_jsonish(raw)), 1


def _money(value: Any) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _range(result: dict, args: dict) -> str:
    return str(result.get("range") or args.get("range") or "selected period")


def _result_for_call(call: dict, cache: dict, profile: str | None) -> dict:
    key = (call["name"], json.dumps(call.get("args") or {}, sort_keys=True, default=str), profile)
    result = cache.get(key)
    return result if isinstance(result, dict) else {}


def _spend_total(result: dict) -> float:
    try:
        return float(result.get("total") or 0)
    except (TypeError, ValueError):
        return 0.0


def _spend_count(result: dict) -> int:
    try:
        return int(result.get("total_count") or result.get("txn_count") or result.get("total_matching_transactions") or 0)
    except (TypeError, ValueError):
        return 0


def _direction(delta: float) -> str:
    if abs(delta) < 0.005:
        return "flat with"
    return "above" if delta > 0 else "below"


def _subject_label(args: dict) -> str:
    return str(args.get("category") or args.get("merchant") or "that area")


def _deterministic_answer(route: dict | None, trace: list[dict], cache: dict, profile: str | None) -> str | None:
    route_args = (route or {}).get("args") or {}
    kind = route_args.get("plan_kind")
    if kind not in {"current_vs_previous", "current_vs_average", "on_track"} or len(trace) < 2:
        return None

    current_call, comparison_call = trace[0], trace[1]
    current = _result_for_call(current_call, cache, profile)
    comparison = _result_for_call(comparison_call, cache, profile)
    if current.get("error") or comparison.get("error"):
        return None

    subject = str(route_args.get("subject") or _subject_label(current_call.get("args") or {}))
    current_total = _spend_total(current)
    current_count = _spend_count(current)
    comparison_total = _spend_total(comparison)

    if kind == "current_vs_previous":
        previous_count = _spend_count(comparison)
        delta = current_total - comparison_total
        return (
            f"For {subject}, this month is {_money(current_total)} across {current_count} transaction(s), "
            f"versus {_money(comparison_total)} last month across {previous_count} transaction(s). "
            f"That is {_money(abs(delta))} {_direction(delta)} last month."
        )

    try:
        months = max(1, min(int(route_args.get("months") or 6), 12))
    except (TypeError, ValueError):
        months = 6
    prior_total = max(0.0, comparison_total - current_total)
    prior_average = prior_total / months
    delta = current_total - prior_average

    if kind == "current_vs_average":
        return (
            f"For {subject}, this month is {_money(current_total)} across {current_count} transaction(s). "
            f"The prior {months}-month average is {_money(prior_average)} per month, so this month is "
            f"{_money(abs(delta))} {_direction(delta)} that average."
        )

    today = datetime.now().date()
    days_in_month = monthrange(today.year, today.month)[1]
    day = max(1, min(today.day, days_in_month))
    projected = current_total / day * days_in_month
    projected_delta = projected - prior_average
    return (
        f"For {subject}, you have spent {_money(current_total)} so far this month across {current_count} transaction(s). "
        f"At day {day} of {days_in_month}, that pace projects to about {_money(projected)} for the month. "
        f"Your prior {months}-month average is {_money(prior_average)}, so you are pacing "
        f"{_money(abs(projected_delta))} {_direction(projected_delta)} that average."
    )


def _step_summary(name: str, args: dict, result: Any) -> str:
    if not isinstance(result, dict):
        return f"{name}: no structured result."
    if result.get("error"):
        return f"{name}: {result['error']}"
    if name == "get_category_spend":
        subject = result.get("category") or args.get("category") or "category"
        count = int(result.get("total_count") or 0)
        return f"{subject} spend for {_range(result, args)} was {_money(result.get('total'))} across {count} transaction(s)."
    if name == "get_merchant_spend":
        subject = result.get("merchant_query") or args.get("merchant") or "merchant"
        count = int(result.get("txn_count") or result.get("total_matching_transactions") or 0)
        return f"{subject} spend for {_range(result, args)} was {_money(result.get('total'))} across {count} transaction(s)."
    if name == "get_monthly_spending_trend":
        values = [float(v or 0) for v in result.get("values") or []]
        labels = result.get("labels") or []
        if not values:
            return "Monthly trend had no matching spending."
        avg = sum(values) / len(values)
        latest = values[-1]
        delta = latest - avg
        latest_label = labels[-1] if labels else "latest month"
        direction = "above" if delta >= 0 else "below"
        return f"{latest_label} was {_money(latest)}, {direction} the {len(values)}-month average of {_money(avg)} by {_money(abs(delta))}."
    if name == "get_transactions":
        rows = result.get("data") or result.get("transactions") or []
        if not rows:
            return "No matching transactions were found."
        first = rows[0]
        return f"Found {len(rows)} transaction(s); latest is {first.get('description')} on {first.get('date')} for {_money(first.get('amount'))}."
    if name == "get_recurring_summary":
        items = result.get("items") or []
        total = result.get("monthly_total") or result.get("total_monthly") or 0
        return f"Recurring summary has {len(items)} item(s), about {_money(total)} monthly."
    if name == "get_net_worth_delta":
        return f"Net worth delta: month-over-month {_money(result.get('mom'))}, YTD {_money(result.get('ytd'))}."
    if name == "get_net_worth_trend":
        series = result.get("series") or []
        if not series:
            return "No net worth trend points were found."
        latest = series[-1]
        return f"Latest net worth point is {_money(latest.get('net_worth') or latest.get('value'))} on {latest.get('date') or latest.get('month')}."
    return f"{name}: result captured."


def _answer(question: str, trace: list[dict], cache: dict, profile: str | None, route: dict | None = None) -> str:
    if not trace:
        return "I need a clearer finance comparison or planning question to answer that with data."
    deterministic = _deterministic_answer(route, trace, cache, profile)
    if deterministic:
        return deterministic
    lines = []
    for call in trace:
        lines.append(_step_summary(call["name"], call.get("args") or {}, _result_for_call(call, cache, profile)))
    return " ".join(lines)


def _retrieve_memory_context(question: str, profile: str | None, route: dict | None) -> tuple[dict | None, dict | None]:
    from database import get_db

    try:
        with get_db() as conn:
            result = memory_v2.retrieve_relevant_memories(
                conn=conn,
                profile=profile,
                question=question,
                route=route,
                limit=4,
            )
    except Exception:
        return None, None
    trace = result.get("memory_trace") if isinstance(result, dict) else None
    packet = result.get("compact_memory") if isinstance(result, dict) else None
    return packet if isinstance(packet, dict) else None, trace if isinstance(trace, dict) else None


def _with_memory_context(answer: str, packet: dict | None) -> str:
    if not isinstance(packet, dict) or not packet.get("used"):
        return answer
    useful = [item for item in packet.get("items") or [] if isinstance(item, dict)]
    if not useful:
        return answer
    first = useful[0]
    memory_type = str(first.get("type") or "")
    summary = str(first.get("summary") or "").strip().rstrip(".")
    if first.get("sensitivity") == "sensitive":
        return f"{answer} I'll keep the tone gentle here and avoid making light of this."
    if memory_type in {"goal", "commitment", "constraint"} and summary:
        phrase = _memory_summary_clause(summary)
        return f"{answer} Since you {phrase}, I would keep that in the decision."
    if memory_type == "tone_preference":
        return f"{answer} I'll keep your saved tone preference in mind."
    return f"{answer} I'll use your saved coaching context as soft guidance."


def _memory_summary_clause(summary: str) -> str:
    phrase = (summary or "").strip().rstrip(".")
    replacements = (
        ("Wants to ", "want to "),
        ("Prefers ", "prefer "),
        ("Is trying to ", "are trying to "),
        ("Is ", "are "),
        ("Does not want ", "do not want "),
    )
    for prefix, replacement in replacements:
        if phrase.startswith(prefix):
            return replacement + phrase[len(prefix):]
    return phrase[0].lower() + phrase[1:] if phrase else phrase


def _execute(question: str, profile: str | None, history: list[dict] | None, route: dict | None) -> dict:
    import copilot_agent as core

    steps, llm_calls = _make_plan(question, profile, history, route)
    cache: dict = {}
    trace: list[dict] = []
    for step in steps:
        start = datetime.now()
        result = execute_tool(step["name"], step.get("args") or {}, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": step["name"], "args": step.get("args") or {}, "duration_ms": duration_ms})
        if isinstance(result, dict) and result.get("error"):
            break
    answer = answer_composer.compose_finance_answer(route, trace, cache, profile) or _answer(question, trace, cache, profile, route=route)
    memory_packet, memory_trace = _retrieve_memory_context(question, profile, route)
    answer = _with_memory_context(answer, memory_packet)
    finalized = core._finalize_answer(
        question=question,
        profile=profile,
        raw_answer=answer,
        trace=trace,
        cache=cache,
        iterations=0,
        run_detector=True,
        route=route,
        memory_trace=memory_trace,
    )
    finalized["llm_calls"] = llm_calls
    if memory_trace:
        finalized["memory_trace"] = memory_trace
        finalized["answer_context"] = {
            "version": 2,
            "kind": "memory_context",
            "memory_trace": memory_trace,
            "compact_memory_trace": memory_packet,
        }
    return provenance.attach_completed_action(
        finalized,
        profile=profile,
        question=question,
        route=route,
        trace=trace,
        cache=cache,
    )


def run(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None) -> dict:
    return _execute(question, profile, history, route)


def stream(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None):
    steps, llm_calls = _make_plan(question, profile, history, route)
    cache: dict = {}
    trace: list[dict] = []
    yield {"type": "reset_text"}
    for step in steps:
        yield {"type": "tool_call", "name": step["name"], "args": step.get("args") or {}}
        start = datetime.now()
        result = execute_tool(step["name"], step.get("args") or {}, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": step["name"], "args": step.get("args") or {}, "duration_ms": duration_ms})
        yield {"type": "tool_result", "name": step["name"], "duration_ms": duration_ms}
        if isinstance(result, dict) and result.get("error"):
            break
    stream_answer = answer_composer.compose_finance_answer(route, trace, cache, profile) or _answer(question, trace, cache, profile, route=route)
    memory_packet, memory_trace = _retrieve_memory_context(question, profile, route)
    yield from emit_done_with_memory(
        question=question,
        profile=profile,
        final_answer=_with_memory_context(stream_answer, memory_packet),
        trace=trace,
        cache=cache,
        iterations=0,
        llm_calls=llm_calls,
        route=route,
        memory_trace=memory_trace,
        compact_memory_trace=memory_packet,
    )
