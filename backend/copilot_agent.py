"""
Copilot agent loop. Provider-agnostic tool-use via llm_client.chat_with_tools.

- System prompt pre-loads a live orientation block so cheap questions resolve
  in a single LLM turn with zero tool calls.
- Up to MAX_ITERATIONS rounds of tool calls, then forced final answer.
- Request-scoped cache dedups identical tool+args within a run.
- Optional conversation history: previous user turns + assistant final answers.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

import llm_client
from copilot_context import build_copilot_context
from copilot_tools import TOOL_REGISTRY, execute_tool
from database import get_db
import memory

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 3
TOOL_RESULT_PAYLOAD_LIMIT = 12000  # chars
HISTORY_TURN_LIMIT = 6              # last N user/assistant pairs

SUMMARY_TOOLS = (
    "get_month_summary",
    "get_top_categories",
    "get_top_merchants",
    "get_recurring_summary",
    "get_summary",
    "get_account_balances",
)
DRILLDOWN_TOOLS = (
    "get_category_spend",
    "get_merchant_spend",
    "get_transactions",
    "get_transactions_for_merchant",
    "get_top_categories",
    "get_top_merchants",
)
WRITE_TOOLS = ("preview_bulk_recategorize", "preview_create_rule", "preview_rename_merchant")
CHART_TOOLS = ("plot_chart",)
TREND_CHART_TOOLS = (
    "get_monthly_spending_trend",
    "get_net_worth_trend",
    "get_top_categories",
    "get_category_spend",
    "get_top_merchants",
    "plot_chart",
)
ADVANCED_TOOLS = (
    "get_month_summary",
    "get_top_categories",
    "get_top_merchants",
    "get_category_spend",
    "get_merchant_spend",
    "get_recurring_summary",
    "get_monthly_spending_trend",
    "get_net_worth_trend",
    "get_transactions_for_merchant",
    "get_summary",
    "get_account_balances",
    "get_transactions",
    "get_category_breakdown",
    "get_dashboard_bundle",
    "get_net_worth_delta",
    "get_category_rules",
    "search_saved_insights",
    "plot_chart",
    "run_sql",
)

TOOL_CAPABILITY_MANIFEST = """Available Copilot capabilities:
- monthly summaries and top spending: get_month_summary, get_top_categories, get_top_merchants, get_summary
- specific category/merchant/transaction answers: get_category_spend, get_merchant_spend, get_transactions, get_transactions_for_merchant
- recurring/subscriptions: get_recurring_summary
- account balances and cash: get_account_balances
- net worth history/charts: get_net_worth_trend, get_net_worth_delta
- monthly spending/category spending charts: get_monthly_spending_trend
- dashboard-wide context: get_dashboard_bundle, get_category_breakdown
- category rules and saved insights: get_category_rules, search_saved_insights
- write previews: preview_bulk_recategorize, preview_create_rule, preview_rename_merchant
- chart rendering: plot_chart
- advanced read-only SQL fallback: run_sql
"""

SQL_SCHEMA_ON_DEMAND = """SQL schema notes for run_sql:
- Prefer specialized tools first. Use run_sql only when no specific tool fits.
- Read transactions through transactions_visible. Spending filter: amount < 0, category NOT IN ('Savings Transfer','Personal Transfer','Credit Card Payment','Income'), and expense_type not internal/household transfer.
- Useful tables: transactions_visible(id, account_id, profile_id, date, description, amount, category, merchant_name, is_excluded, expense_type), accounts(id, profile_id, account_name, account_type, current_balance), categories(name, expense_type), category_rules(pattern, category), merchants(clean_name, category, total_spent), net_worth_history(date, profile_id, total_assets, total_owed, net_worth), saved_insights(question, answer, kind, pinned).
- profile='household' means omit a profile filter.
"""

CHART_RECIPE_ON_DEMAND = """Chart recipe:
- To chart net worth: call get_net_worth_trend, then plot_chart(type='line') using date/month labels and net_worth values.
- To chart spending over months: call get_monthly_spending_trend, then plot_chart(type='line') using labels and values.
- To chart top categories/merchants: fetch the data first, then plot_chart(type='bar' or 'donut').
"""

# Keys in tool results that typically carry user-displayable list data,
# ordered by preference (first match wins).
_DISPLAY_LIST_KEYS = (
    "transactions",  # get_transactions_for_merchant, get_transactions inner
    "data",          # get_transactions (paginated shape: {data, total_count})
    "recent",        # get_category_spend / get_merchant_spend recent txns
    "merchants",     # get_top_merchants, get_merchant_spend matched_merchants
    "matched_merchants",
    "categories",    # get_top_categories, get_category_breakdown
    "items",         # get_recurring_summary
    "accounts",      # get_account_balances
    "rows",          # run_sql
    "series",        # get_net_worth_trend
    "rules",         # get_category_rules
    "insights",      # search_saved_insights
)


def _uniq_tools(*groups: tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for group in groups:
        for name in group:
            if name not in seen:
                seen.add(name)
                ordered.append(name)
    return ordered


def _looks_financial(question: str) -> bool:
    q = question.lower()
    terms = (
        "spend", "spent", "income", "budget", "merchant", "category", "transaction",
        "subscription", "recurring", "cash", "balance", "account", "net worth",
        "saving", "savings", "month", "week", "ytd", "money", "card", "payment",
        "rent", "grocery", "groceries", "restaurant", "dining", "watch", "runway",
    )
    return "$" in q or any(t in q for t in terms)


def _is_watch_question(question: str) -> bool:
    q = question.lower().strip()
    has_watch = any(p in q for p in ("what should i watch", "watch this month", "watch out", "keep an eye", "anything concerning", "anything i should worry"))
    has_period = any(p in q for p in ("this month", "current month", "month", "right now", "lately"))
    return has_watch and has_period


def _history_text(history: list[dict] | None, limit: int = 4) -> str:
    if not history:
        return ""
    parts: list[str] = []
    for turn in history[-limit:]:
        content = (turn.get("content") or "").strip()
        if content:
            parts.append(content)
    return "\n".join(parts)


def _extract_month_count(question: str, history: list[dict] | None = None) -> int:
    context = f"{_history_text(history)}\n{question}".lower()
    match = re.search(r"(?:last|past)\s+(\d{1,2})\s+months?", context)
    if match:
        return max(1, min(int(match.group(1)), 36))
    if "year" in context or "12 months" in context:
        return 12
    if "all time" in context or "all-time" in context:
        return 36
    return 6


def _extract_spending_category(question: str, history: list[dict] | None = None) -> str | None:
    context = f"{_history_text(history)}\n{question}".lower()
    category_aliases = {
        "groceries": "Groceries",
        "grocery": "Groceries",
        "food": "Food & Dining",
        "dining": "Food & Dining",
        "restaurants": "Food & Dining",
        "restaurant": "Food & Dining",
        "travel": "Travel",
        "taxes": "Taxes",
        "tax": "Taxes",
        "shopping": "Shopping",
        "utilities": "Utilities",
        "housing": "Housing",
        "rent": "Housing",
        "healthcare": "Healthcare",
        "medical": "Healthcare",
        "transportation": "Transportation",
        "subscriptions": "Subscriptions",
        "subscription": "Subscriptions",
    }
    for needle, category in category_aliases.items():
        if re.search(rf"\b{re.escape(needle)}\b", context):
            return category
    return None


def _valid_tool_names(names: Any) -> list[str]:
    if not isinstance(names, list):
        return []
    valid = set(TOOL_REGISTRY)
    cleaned: list[str] = []
    for name in names:
        if isinstance(name, str) and name in valid and name not in cleaned:
            cleaned.append(name)
    return cleaned


def _route_tools_with_llm(question: str, history: list[dict] | None = None) -> list[str] | None:
    if not llm_client.is_available():
        return None

    recent = _history_text(history, limit=4)
    prompt = f"""Choose the minimum tool set for the user's latest request.

{TOOL_CAPABILITY_MANIFEST}

Rules:
- Return ONLY JSON: {{"tools":["tool_name"],"reason":"short"}}
- Choose [] for normal non-finance/general chat.
- For any chart request, include plot_chart plus the data tool needed to produce values.
- For net worth/networth charts, choose get_net_worth_trend and plot_chart.
- For spending/category spending over months, choose get_monthly_spending_trend and plot_chart.
- For ambiguous finance questions, choose a safe small bundle rather than no tools.

Recent context:
{recent or "(none)"}

Latest user request:
{question}

JSON:"""
    try:
        raw = llm_client.complete(prompt, max_tokens=180, purpose="copilot")
    except Exception:
        logger.debug("tool router LLM failed", exc_info=True)
        return None
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.debug("tool router returned non-JSON: %r", raw[:200])
        return None
    tools = _valid_tool_names(parsed.get("tools") if isinstance(parsed, dict) else parsed)
    return tools


def _select_tools(question: str, history: list[dict] | None = None) -> list[str]:
    routed = _route_tools_with_llm(question, history)
    if routed is not None:
        return routed

    context = f"{_history_text(history)}\n{question}".lower()
    if not _looks_financial(context):
        return []

    if any(w in context for w in ("recategorize", "categorize", "create rule", "always categorize", "rename", "move all", "change all")):
        return list(WRITE_TOOLS)

    groups: list[tuple[str, ...]] = []
    if any(w in context for w in ("chart", "graph", "plot", "visual", "trend", "over the months", "monthly")):
        groups.append(TREND_CHART_TOOLS)
    if any(w in context for w in ("transaction", "merchant", "category", "costco", "amazon", "where did", "show me", "list", "grocery", "groceries")):
        groups.append(DRILLDOWN_TOOLS)
    if any(w in context for w in ("balance", "cash", "account", "net worth", "runway")):
        groups.append(("get_account_balances", "get_net_worth_trend", "get_net_worth_delta", "get_summary"))
    if any(w in context for w in ("sql", "query", "rule", "policy", "why is", "debug", "reconcile")):
        groups.append(ADVANCED_TOOLS)
    if not groups:
        groups.append(SUMMARY_TOOLS)

    return _uniq_tools(*groups)


def _extra_prompt_for_tools(tool_names: list[str]) -> str:
    parts = [TOOL_CAPABILITY_MANIFEST]
    if "run_sql" in tool_names:
        parts.append(SQL_SCHEMA_ON_DEMAND)
    if "plot_chart" in tool_names:
        parts.append(CHART_RECIPE_ON_DEMAND)
    return "\n".join(parts)


def _claims_missing_capability(answer: str) -> bool:
    text = (answer or "").lower()
    return any(
        phrase in text
        for phrase in (
            "i don't have access",
            "i do not have access",
            "i don't have the tool",
            "i do not have the tool",
            "i don't have a tool",
            "i do not have a tool",
            "can't access your",
            "cannot access your",
        )
    )


def _fast_watch_system(profile: str | None) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return (
        "You're the user's close friend and senior financial advisor. "
        "Summarize the provided live finance data in 2-4 warm, direct sentences. "
        "Call out the biggest current-month watch item, cite concrete numbers, "
        "and do not invent data. No markdown table. "
        f"Today is {today}. Active profile: {profile or 'household'}."
    )


def _summarize_tool_payload(result: Any, *, key: str, limit: int = 5) -> Any:
    if not isinstance(result, dict):
        return result
    cloned = dict(result)
    value = cloned.get(key)
    if isinstance(value, list):
        cloned[key] = value[:limit]
    return cloned


def _execute_fast_watch_tools(profile: str | None, cache: dict) -> tuple[dict[str, Any], list[dict]]:
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
    return payload, trace


def _build_monthly_spending_chart(question: str, profile: str | None, history: list[dict] | None = None, cache: dict | None = None) -> tuple[str, dict, list[dict], dict]:
    cache = cache if cache is not None else {}
    months = _extract_month_count(question, history)
    category = _extract_spending_category(question, history)
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
        latest = values[-1]
        peak_idx = max(range(len(values)), key=lambda i: values[i])
        answer = (
            f"Done — here's your {subject} trend for the last {months} months. "
            f"Latest month is ${latest:,.2f}; peak was {labels[peak_idx]} at ${values[peak_idx]:,.2f}."
        )
    else:
        answer = f"I couldn't find {subject} data for the last {months} months."
    return answer, trend if isinstance(trend, dict) else {}, trace, chart if isinstance(chart, dict) else {}


def _latest_monthly_points(series: list[dict]) -> tuple[list[str], list[float], list[dict]]:
    by_month: dict[str, dict] = {}
    for point in series:
        date = str(point.get("date") or point.get("month") or "")
        if not date:
            continue
        month = date[:7]
        by_month[month] = point
    labels = sorted(by_month)
    values: list[float] = []
    rows: list[dict] = []
    for month in labels:
        point = by_month[month]
        raw = point.get("net_worth", point.get("value", point.get("total", 0)))
        try:
            value = round(float(raw or 0), 2)
        except (TypeError, ValueError):
            value = 0.0
        values.append(value)
        rows.append({"month": month, "net_worth": value})
    return labels, values, rows


def _build_net_worth_chart(profile: str | None, cache: dict | None = None) -> tuple[str, dict, list[dict], dict]:
    cache = cache if cache is not None else {}
    trace: list[dict] = []
    args = {"interval": "monthly", "limit": 24}
    start = datetime.now()
    trend = execute_tool("get_net_worth_trend", args, profile, cache=cache)
    trace.append({
        "name": "get_net_worth_trend",
        "args": args,
        "duration_ms": int((datetime.now() - start).total_seconds() * 1000),
    })

    raw_series = trend.get("series", []) if isinstance(trend, dict) else []
    labels, values, rows = _latest_monthly_points(raw_series)
    chart_args = {
        "type": "line",
        "title": "Net Worth Trend",
        "series_name": "Net Worth",
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
        answer = f"Done — here's your net worth trend. Latest month is ${values[-1]:,.2f}."
    else:
        answer = "I couldn't find net worth history to chart."
    trend_payload = {"series": rows, "labels": labels, "values": values}
    return answer, trend_payload, trace, chart if isinstance(chart, dict) else {}


def _can_execute_chart_plan(tool_names: list[str]) -> str | None:
    tools = set(tool_names)
    if {"get_net_worth_trend", "plot_chart"}.issubset(tools):
        return "net_worth"
    if {"get_monthly_spending_trend", "plot_chart"}.issubset(tools):
        return "spending"
    return None


def _execute_chart_plan(kind: str, question: str, profile: str | None, history: list[dict] | None, cache: dict) -> tuple[str, dict, list[dict], dict]:
    if kind == "net_worth":
        return _build_net_worth_chart(profile, cache)
    return _build_monthly_spending_chart(question, profile, history, cache)


def _finalize_answer(
    *,
    question: str,
    profile: str | None,
    raw_answer: str,
    trace: list[dict],
    cache: dict,
    iterations: int,
    run_detector: bool,
) -> dict:
    display_rows, display_source = _extract_display_data(trace, cache, profile)
    cleaned_answer, agent_props_raw, observations_logged, proposals_created = _persist_agent_tags(
        raw_answer=raw_answer, profile=profile,
    )
    if run_detector:
        detector_props = _persist_detector_signals(
            user_question=question,
            cleaned_answer=cleaned_answer or raw_answer,
            profile=profile,
            agent_proposals_raw=agent_props_raw,
        )
        proposals_created.extend(detector_props)
    answer_text = _fallback_when_empty(cleaned_answer, bool(proposals_created))
    return {
        "answer": answer_text,
        "tool_trace": trace,
        "iterations": iterations,
        "error": None,
        "data": display_rows,
        "data_source": display_source,
        "memory_proposals": proposals_created,
        "memory_observations": observations_logged,
    }


def _extract_display_data(trace: list[dict], tool_cache: dict, profile: str | None) -> tuple[list[dict] | None, str | None]:
    """
    Scan the tool trace back-to-front for the most recent list-shaped result.
    Returns (rows, source_tool_name) — rows for inline table render, or (None, None).
    """
    for call in reversed(trace):
        name = call.get("name")
        args = call.get("args") or {}
        cache_key = (name, json.dumps(args, sort_keys=True, default=str), profile)
        result = tool_cache.get(cache_key)
        if not isinstance(result, dict):
            continue
        for key in _DISPLAY_LIST_KEYS:
            value = result.get(key)
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value[:50], name
    return None, None


SYSTEM_PROMPT = """You're the user's close friend whose day job is senior financial advisor. Friend first, expert second: warm, direct, lightly wry, and useful.

PERSONA
- You can help with finance, code, life, general knowledge, and normal conversation. Do not redirect everything to money.
- When money comes up, use fresh data, cite concrete numbers, and never invent transactions, merchants, or balances.
- Avoid heavy caveats. For personalized regulated advice, add one short parenthetical at the end only if needed.
- Style: usually 2-5 sentences, candid and specific.

TOOL DISCIPLINE
- If you need a tool, emit no visible prose before the tool call.
- The orientation block is only a truncated preview. For any specific time period, merchant, category, account balance, transaction list, write, or chart, call an appropriate tool.
- Time windows: current_month, last_month, this_week, last_week, last_7d/30d/90d/180d/365d, ytd, last_year, all, or YYYY-MM. Never reuse old turn numbers for a changed time window.
- Prefer specialized tools over run_sql. Use run_sql only when no specific tool fits.
- Specialized tools match dashboard aggregation. If a result disagrees with the UI, trust the dashboard and say you'll double-check.

FINANCE SEMANTICS
- Spending excludes Savings Transfer, Personal Transfer, Credit Card Payment, Income, and internal/household transfers.
- Negative amount = money out. Positive amount = income/refund.
- Net category spend subtracts refunds. Merchant identity uses merchant_name, falling back to description.
- "Watch / keep an eye on" means financial concerns here. "Park my money" means where to hold cash. "Burn rate/runway" means spending velocity.

WRITES
- You may preview data changes only when the user explicitly asks to change data.
- Use preview_bulk_recategorize, preview_create_rule, or preview_rename_merchant. The UI shows Confirm/Cancel from the returned confirmation ID.
- After a preview, summarize the change briefly and stop.

CHARTS
- Use plot_chart only after fetching the underlying numbers. One chart max.
- line = trends, bar = comparisons, donut = composition. Skip charts for single-number answers.
- For monthly spending charts, call get_monthly_spending_trend, then call plot_chart with its labels and values. Use the user's requested months; if omitted, default to 6 months.

PERSISTENT MEMORY
- The orientation may include persistent memory about the user. Use it as background; do not restate it verbatim.
- At the very end only, after visible prose, you may emit hidden tags that the app strips:
  <observation theme="short_kebab_case">one-line note</observation>
  <memory_proposal section="identity|preferences|goals|concerns|open_questions" confidence="stated" evidence="quote or reason">one-line entry</memory_proposal>
- Use memory_proposal only for explicit user-stated enduring facts, preferences, commitments, goals, or contradictions. Most turns produce no tags.
"""


_OBSERVATION_THEME_TO_SECTION: dict[str, str] = {
    "runway_anxiety": "concerns",
    "response_length_pushback": "preferences",
}


def _section_for_observation(theme: str) -> str:
    """Best-effort mapping from observation theme → memory section. Defaults to concerns."""
    return _OBSERVATION_THEME_TO_SECTION.get(theme, "concerns")


def _persist_agent_tags(
    *,
    raw_answer: str,
    profile: str | None,
    source_conversation_id: int | None = None,
) -> tuple[str, list[dict], list[dict], list[dict]]:
    """
    FAST phase. Parses <observation>/<memory_proposal> tags out of the agent's
    reply, persists what's there, runs the cheap threshold check on observations.
    No LLM call — runs in the streaming critical path.

    Returns (cleaned_answer, agent_proposals_raw, observations_logged, proposals_created).
    `agent_proposals_raw` is needed by the slow phase to dedup detector output against.
    """
    cleaned, observations, agent_proposals = memory.parse_agent_memory_tags(raw_answer)
    obs_logged: list[dict] = []
    props_created: list[dict] = []

    if not observations and not agent_proposals:
        return cleaned, agent_proposals, obs_logged, props_created

    try:
        with get_db() as conn:
            for obs in observations:
                try:
                    obs_id = memory.log_observation(
                        profile=profile,
                        theme=obs["theme"],
                        note=obs["note"],
                        source_conversation_id=source_conversation_id,
                        conn=conn,
                    )
                    obs_logged.append({"id": obs_id, **obs})
                    promoted = memory.maybe_promote_observation(
                        profile=profile,
                        theme=obs["theme"],
                        section=_section_for_observation(obs["theme"]),
                        body=obs["note"],
                        source_conversation_id=source_conversation_id,
                        conn=conn,
                    )
                    if promoted:
                        promo = memory.get_proposal(promoted, conn) or {}
                        props_created.append(promo)
                except Exception:
                    logger.debug("failed to log observation %s", obs, exc_info=True)

            for prop in agent_proposals:
                try:
                    pid = memory.create_proposal(
                        profile=profile,
                        section=prop["section"],
                        body=prop["body"],
                        confidence=prop["confidence"],
                        evidence=prop.get("evidence", ""),
                        theme=prop.get("theme"),
                        source="agent",
                        source_conversation_id=source_conversation_id,
                        conn=conn,
                    )
                    created = memory.get_proposal(pid, conn) or {}
                    props_created.append(created)
                except Exception:
                    logger.debug("failed to create agent proposal %s", prop, exc_info=True)
    except Exception:
        logger.exception("agent-tag persistence failed")

    return cleaned, agent_proposals, obs_logged, props_created


def _persist_detector_signals(
    *,
    user_question: str,
    cleaned_answer: str,
    profile: str | None,
    agent_proposals_raw: list[dict],
    source_conversation_id: int | None = None,
) -> list[dict]:
    """
    SLOW phase. Runs the dedicated post-turn detector and persists any
    proposals it produces (deduplicated against agent-emitted ones).
    Safe to call after the streaming `done` event has been sent.
    """
    if not user_question:
        return []
    detected = memory.detect_memory_signals(user_question, cleaned_answer or "")
    if not detected:
        return []

    existing_bodies = {p.get("body", "").strip().lower() for p in agent_proposals_raw if p.get("body")}
    new_proposals: list[dict] = []
    try:
        with get_db() as conn:
            for sig in detected:
                if sig["body"].strip().lower() in existing_bodies:
                    continue
                try:
                    pid = memory.create_proposal(
                        profile=profile,
                        section=sig["section"],
                        body=sig["body"],
                        confidence=sig["confidence"],
                        evidence=sig.get("evidence", ""),
                        theme=None,
                        source="agent",
                        source_conversation_id=source_conversation_id,
                        conn=conn,
                    )
                    created = memory.get_proposal(pid, conn) or {}
                    new_proposals.append(created)
                except Exception:
                    logger.debug("failed to persist detector signal %s", sig, exc_info=True)
    except Exception:
        logger.exception("detector persistence failed")
    return new_proposals


def _fallback_when_empty(cleaned: str, has_signals: bool) -> str:
    """
    The agent occasionally emits a reply that's entirely tags + whitespace, leaving
    nothing visible after cleanup. If we have a proposal to show, give the user a
    short anchor sentence. Otherwise admit we have nothing.
    """
    stripped = (cleaned or "").strip()
    if stripped:
        return stripped
    if has_signals:
        return "Got it — I noted that."
    return "I didn't land on a confident answer this turn."


def _truncate_for_model(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=True, default=str)
    if len(text) > TOOL_RESULT_PAYLOAD_LIMIT:
        return text[:TOOL_RESULT_PAYLOAD_LIMIT] + "...[truncated]"
    return text


def _build_system_prompt(profile: str | None, tool_names: list[str] | None = None) -> str:
    try:
        with get_db() as conn:
            orientation = build_copilot_context(profile, conn)
    except Exception:
        logger.exception("Failed to build orientation block")
        orientation = ""

    now = datetime.now().strftime("%Y-%m-%d")
    header = f"Today is {now}. Active profile: {profile or 'household'}."
    tool_context = _extra_prompt_for_tools(tool_names or [])
    blocks = [SYSTEM_PROMPT, header, tool_context]
    if orientation:
        blocks.append(orientation)
    return "\n\n".join(block for block in blocks if block)


def _normalize_history(history: list[dict] | None) -> list[dict]:
    """Keep only user/assistant turns with text content. Drop tool traces."""
    if not history:
        return []
    cleaned = []
    for turn in history[-HISTORY_TURN_LIMIT * 2:]:
        role = turn.get("role")
        content = (turn.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            cleaned.append({"role": role, "content": content})
    return cleaned


def run_agent(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    if _is_watch_question(question):
        cache: dict = {}
        payload, trace = _execute_fast_watch_tools(profile, cache)
        prompt = (
            "User asked: " + question + "\n\n"
            "Live tool results:\n" + json.dumps(payload, ensure_ascii=True, default=str)
        )
        try:
            response = llm_client.chat_with_tools(
                messages=[{"role": "user", "content": prompt}],
                tools=[],
                system=_fast_watch_system(profile),
                max_tokens=420,
                purpose="copilot",
            )
            final_answer = (response.get("content") or "").strip()
        except Exception as e:
            logger.exception("fast watch summary failed")
            return {
                "answer": f"Copilot hit an error while summarizing: {e}",
                "tool_trace": trace,
                "iterations": 0,
                "error": str(e),
            }
        return _finalize_answer(
            question=question,
            profile=profile,
            raw_answer=final_answer,
            trace=trace,
            cache=cache,
            iterations=0,
            run_detector=True,
        )

    messages: list[dict] = _normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer = ""
    selected_tools = _select_tools(question, history)
    chart_plan = _can_execute_chart_plan(selected_tools)
    if chart_plan:
        answer, trend, plan_trace, chart = _execute_chart_plan(chart_plan, question, profile, history, cache)
        result = _finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=plan_trace,
            cache=cache,
            iterations=0,
            run_detector=True,
        )
        if chart.get("_chart"):
            result["chart"] = chart
        result["data"] = trend.get("series") or result.get("data")
        result["data_source"] = plan_trace[0]["name"] if plan_trace else result.get("data_source")
        return result

    system = _build_system_prompt(profile, selected_tools)
    retried_with_expanded_tools = False

    for iteration in range(MAX_ITERATIONS + 1):
        force_final = iteration == MAX_ITERATIONS
        if force_final:
            messages.append({
                "role": "user",
                "content": "Iteration cap reached. Provide your best final answer now using what you already know. Do not call any more tools.",
            })

        try:
            response = llm_client.chat_with_tools(
                messages=messages,
                tools=selected_tools,
                system=system,
                max_tokens=1400,
                purpose="copilot",
            )
        except Exception as e:
            logger.exception("agent chat failed at iteration %d", iteration)
            return {
                "answer": f"Copilot hit an error while reasoning: {e}",
                "tool_trace": trace,
                "iterations": iteration,
                "error": str(e),
            }

        content = response.get("content") or ""
        tool_calls = response.get("tool_calls") or []

        if not tool_calls or force_final:
            final_answer = content.strip() if content and content.strip() else ""
            if (
                final_answer
                and _claims_missing_capability(final_answer)
                and selected_tools != list(ADVANCED_TOOLS)
                and not retried_with_expanded_tools
            ):
                retried_with_expanded_tools = True
                selected_tools = list(ADVANCED_TOOLS)
                system = _build_system_prompt(profile, selected_tools)
                messages = _normalize_history(history)
                messages.append({"role": "user", "content": question})
                final_answer = ""
                continue
            break

        messages.append({
            "role": "assistant",
            "content": content,
            "tool_calls": tool_calls,
        })

        for call in tool_calls:
            start = datetime.now()
            result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            trace.append({
                "name": call["name"],
                "args": call.get("args") or {},
                "duration_ms": duration_ms,
            })
            messages.append({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": _truncate_for_model(result),
            })

    return _finalize_answer(
        question=question,
        profile=profile,
        raw_answer=final_answer,
        trace=trace,
        cache=cache,
        iterations=iteration,
        run_detector=True,
    )


def run_agent_stream(question: str, profile: str | None, history: list[dict] | None = None):
    """
    Streaming variant of run_agent. Yields event dicts suitable for SSE:
      {type: 'status', message: str}
      {type: 'tool_call', name, args}
      {type: 'tool_result', name, duration_ms}
      {type: 'token', text}                      # incremental text from current turn
      {type: 'reset_text'}                       # emitted before a new turn that may supersede
      {type: 'done', answer, data, data_source, tool_trace, iterations}
      {type: 'error', message}
    """
    if _is_watch_question(question):
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
                system=_fast_watch_system(profile),
                max_tokens=420,
                purpose="copilot",
            ):
                if event_type == "text":
                    final_parts.append(payload_part)
                    yield {"type": "token", "text": payload_part}
        except Exception as e:
            logger.exception("fast watch stream failed")
            yield {"type": "error", "message": f"Copilot hit an error: {e}"}
            return

        final_answer = "".join(final_parts).strip() or "I couldn't land on a confident answer from the available data."
        display_rows, display_source = _extract_display_data(trace, cache, profile)
        cleaned_answer, agent_props_raw, observations_logged, proposals_created = _persist_agent_tags(
            raw_answer=final_answer, profile=profile,
        )
        answer_text = _fallback_when_empty(cleaned_answer, bool(proposals_created))
        yield {
            "type": "done",
            "answer": answer_text,
            "data": display_rows,
            "data_source": display_source,
            "tool_trace": trace,
            "iterations": 0,
            "memory_proposals": proposals_created,
            "memory_observations": observations_logged,
        }
        detector_props = _persist_detector_signals(
            user_question=question,
            cleaned_answer=cleaned_answer or final_answer,
            profile=profile,
            agent_proposals_raw=agent_props_raw,
        )
        if detector_props:
            yield {"type": "memory_update", "memory_proposals": detector_props}
        return

    messages: list[dict] = _normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer_parts: list[str] = []
    pending_write: dict | None = None
    pending_chart: dict | None = None
    selected_tools = _select_tools(question, history)
    chart_plan = _can_execute_chart_plan(selected_tools)
    if chart_plan:
        cache: dict = {}
        yield {"type": "reset_text"}
        if chart_plan == "net_worth":
            first_tool = "get_net_worth_trend"
            answer, trend, plan_trace, chart = _build_net_worth_chart(profile, cache)
        else:
            first_tool = "get_monthly_spending_trend"
            answer, trend, plan_trace, chart = _build_monthly_spending_chart(question, profile, history, cache)
        for call in plan_trace:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            yield {"type": "tool_result", "name": call["name"], "duration_ms": call.get("duration_ms", 0)}
        if chart.get("_chart"):
            yield {"type": "chart", "chart": chart}
        cleaned_answer, agent_props_raw, observations_logged, proposals_created = _persist_agent_tags(
            raw_answer=answer, profile=profile,
        )
        answer_text = _fallback_when_empty(cleaned_answer, bool(proposals_created))
        yield {
            "type": "done",
            "answer": answer_text,
            "data": trend.get("series"),
            "data_source": first_tool,
            "tool_trace": plan_trace,
            "iterations": 0,
            "memory_proposals": proposals_created,
            "memory_observations": observations_logged,
            "chart": chart if chart.get("_chart") else None,
        }
        detector_props = _persist_detector_signals(
            user_question=question,
            cleaned_answer=cleaned_answer or answer,
            profile=profile,
            agent_proposals_raw=agent_props_raw,
        )
        if detector_props:
            yield {"type": "memory_update", "memory_proposals": detector_props}
        return

    system = _build_system_prompt(profile, selected_tools)
    retried_with_expanded_tools = False

    for iteration in range(MAX_ITERATIONS + 1):
        force_final = iteration == MAX_ITERATIONS
        if force_final:
            messages.append({
                "role": "user",
                "content": "Iteration cap reached. Provide your best final answer now using what you already know. Do not call any more tools.",
            })

        pending_tool_calls: list[dict] = []
        text_buffer: list[str] = []
        # If this turn ends up having tool calls, text is just reasoning; clear any prior partial.
        yield {"type": "reset_text"}

        try:
            for event_type, payload in llm_client.chat_with_tools_stream(
                messages=messages, tools=selected_tools, system=system, max_tokens=1400, purpose="copilot",
            ):
                if event_type == "text":
                    text_buffer.append(payload)
                    yield {"type": "token", "text": payload}
                elif event_type == "tool_call":
                    pending_tool_calls.append(payload)
                # 'stop' events are informational; loop naturally ends when iter_lines closes
        except Exception as e:
            logger.exception("agent stream failed at iteration %d", iteration)
            yield {"type": "error", "message": f"Copilot hit an error: {e}"}
            return

        content = "".join(text_buffer)

        if not pending_tool_calls or force_final:
            candidate_answer = content.strip() if content else ""
            if (
                candidate_answer
                and _claims_missing_capability(candidate_answer)
                and selected_tools != list(ADVANCED_TOOLS)
                and not retried_with_expanded_tools
            ):
                retried_with_expanded_tools = True
                selected_tools = list(ADVANCED_TOOLS)
                system = _build_system_prompt(profile, selected_tools)
                messages = _normalize_history(history)
                messages.append({"role": "user", "content": question})
                final_answer_parts = []
                yield {"type": "reset_text"}
                continue
            final_answer_parts.append(content)
            break

        # Any text emitted before the tool call was just reasoning — tell client to discard
        yield {"type": "reset_text"}

        messages.append({
            "role": "assistant",
            "content": content,
            "tool_calls": pending_tool_calls,
        })

        for call in pending_tool_calls:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            start = datetime.now()
            result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            trace.append({
                "name": call["name"],
                "args": call.get("args") or {},
                "duration_ms": duration_ms,
            })
            yield {"type": "tool_result", "name": call["name"], "duration_ms": duration_ms}

            # Capture write-preview payloads so the UI can render Confirm/Cancel
            if isinstance(result, dict) and result.get("_write_preview"):
                pending_write = {
                    "confirmation_id": result.get("confirmation_id"),
                    "sql": result.get("sql"),
                    "rows_affected": result.get("rows_affected"),
                    "samples": result.get("samples", []),
                    "preview_changes": result.get("preview_changes", []),
                    "summary": result.get("summary"),
                }

            # Capture chart specs — most recent wins if multiple are emitted
            if isinstance(result, dict) and result.get("_chart"):
                pending_chart = {
                    "type": result.get("type"),
                    "title": result.get("title"),
                    "series_name": result.get("series_name"),
                    "labels": result.get("labels", []),
                    "values": result.get("values", []),
                    "unit": result.get("unit", "currency"),
                }
                yield {"type": "chart", "chart": pending_chart}

            messages.append({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": _truncate_for_model(result),
            })

    _raw_answer = "".join(final_answer_parts).strip()
    if _raw_answer:
        final_answer = _raw_answer
    elif pending_chart:
        title = pending_chart.get("title") or "chart"
        final_answer = f"Done — here's the {title}."
    else:
        final_answer = "I couldn't land on a confident answer from the available data."
    display_rows, display_source = _extract_display_data(trace, cache, profile)

    # FAST phase — runs in critical path. Just XML parsing + threshold check.
    cleaned_answer, agent_props_raw, observations_logged, proposals_created = _persist_agent_tags(
        raw_answer=final_answer, profile=profile,
    )
    answer_text = _fallback_when_empty(cleaned_answer, bool(proposals_created))

    done_event = {
        "type": "done",
        "answer": answer_text,
        "data": display_rows,
        "data_source": display_source,
        "tool_trace": trace,
        "iterations": iteration,
        "memory_proposals": proposals_created,
        "memory_observations": observations_logged,
    }
    if pending_write:
        done_event["pending_write"] = pending_write
        # Prefer showing the preview samples inline over any other list data
        if pending_write.get("samples"):
            done_event["data"] = pending_write["samples"]
            done_event["data_source"] = "write_preview"
    if pending_chart:
        done_event["chart"] = pending_chart
    yield done_event

    # SLOW phase — runs after the user has already seen the answer. Catches turns
    # the conversational agent forgot to tag. Streams the late-arriving proposals
    # back as a separate event so the UI can append them inline.
    detector_props = _persist_detector_signals(
        user_question=question,
        cleaned_answer=cleaned_answer or final_answer,
        profile=profile,
        agent_proposals_raw=agent_props_raw,
    )
    if detector_props:
        yield {"type": "memory_update", "memory_proposals": detector_props}
