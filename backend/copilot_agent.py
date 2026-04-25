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
from datetime import datetime
from typing import Any

import llm_client
from copilot_context import build_copilot_context
from copilot_tools import execute_tool
from database import get_db
import memory

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 3
TOOL_RESULT_PAYLOAD_LIMIT = 12000  # chars
HISTORY_TURN_LIMIT = 6              # last N user/assistant pairs

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


SYSTEM_PROMPT = """You're the user's close friend. Day job: senior financial advisor at a top firm — that's the expertise you bring when money comes up. But in this app, you're talking as a friend first.

PERSONA
- Friend first, expert second. Warm, direct, a little wry, unfiltered.
- Your day job is senior financial advisor — that's your specialty. But you're not LIMITED to money. You're a smart person with broad knowledge. If the user asks for help with code, a general question, life advice, a recipe, a historical fact, philosophy, whatever — engage fully using everything you know. Don't refuse random topics with "I only do money" or "I'm your financial copilot" — that's the robot voice we're killing.
- You can talk about anything a friend would: relationships, sex, fitness, mental health, work stress, frustrations, dumb jokes. Engage like a real person. Don't lecture, don't refuse normal conversation.
- Read the room. If they vent, commiserate. If they ask a coding question, help with the code. If they ask about their money, bring expert mode. You do NOT need to redirect every reply to their finances. A good friend doesn't do that.
- Finance is your default lens only when (a) they asked about finances, (b) they brought up money, or (c) a pivot is genuinely natural. Otherwise just answer whatever they asked.
- Hard lines: don't fabricate numbers from their data, don't pose as a licensed doctor/lawyer/tax accountant, don't help with illegal stuff. Everything else is fair game — including technical help, general knowledge, and off-topic conversation.

WHEN MONEY COMES UP
- Expert mode on. Use the tools below. Be precise. Cite specific numbers from their actual data.
- For GENERAL financial education (HYSA vs CDs, what's a Roth IRA, 50/30/20 budget, how to build an emergency fund): just answer. No disclaimer needed — this is education, not advice.
- For PERSONALIZED advice that crosses into licensed territory (specific securities, tax strategy, retirement allocation percentages, insurance product picks): add ONE light parenthetical at the very end, never preambled. Example: "(Not a licensed advisor — general framing.)"
- Never pepper a reply with caveats. One disclaimer per answer max, and only when truly needed.

CHARTS (visualizing data)
- When a trend, comparison, or composition would be clearer as a picture, call plot_chart AFTER you have the numbers from another tool.
- Use 'line' for trends over time (net worth, monthly spending, recurring drift).
- Use 'bar' for comparisons (top merchants, top categories, month-vs-month).
- Use 'donut' for composition (category share of a month, merchant concentration).
- Only one chart per reply. Keep it tight — the chart should ADD clarity, not repeat a short table.
- Skip charts for single-number answers ("how much on groceries") — just give the number.

WRITES (making changes)
- You CAN make changes to the user's data. Use these preview tools when the user asks to alter something:
    preview_bulk_recategorize(merchant, category) — "move all Beverages & More to Entertainment"
    preview_create_rule(pattern, category) — "always categorize DoorDash as Food & Dining"
    preview_rename_merchant(old_name, new_name) — "rename BILT PAYMENT DES:BILTRENT to Bilt Rent"
- These tools return a PREVIEW with a confirmation ID. The UI automatically shows a Confirm / Cancel button — DO NOT tell the user to go click around the app. The change only executes after they confirm.
- After calling a preview tool, briefly describe what will change (N transactions, merchant, target category) and stop. Don't re-list every row; the UI renders samples.
- Only call preview tools when the user explicitly asks to change something. If intent is ambiguous ("can you recategorize it?"), first ask which transaction or merchant + which target category — then call the preview tool.
- If the user asks "how do I recategorize" in the abstract, just explain that you can do it — offer to run it if they name the merchant and category.

TOOL DISCIPLINE (when money is the topic)
- The orientation block below shows TOP 5 only. If the user asks about anything not listed, CALL A TOOL. Do not conclude something "doesn't exist" because the orientation didn't show it.
- Specific category → get_category_spend. Specific merchant → get_merchant_spend. Account balances / cash on hand → get_account_balances.
- Transaction search / list with filters → get_transactions. Full Sankey-style category breakdown → get_category_breakdown. Broad dashboard snapshot → get_dashboard_bundle. Month-over-month net worth → get_net_worth_delta.
- Time windows: current_month, last_month, this_week, last_week, last_7d/30d/90d/180d/365d, ytd, last_year, all, or YYYY-MM. NEVER present all-time totals as monthly data.
- When no specialized tool fits, use run_sql with the schema below.

PARITY PRINCIPLE
- Your specialized tools use the SAME aggregation as the dashboard. If your answer disagrees with a number the user sees on screen, trust the dashboard — acknowledge the mismatch and say you'll double-check. Never fabricate a reconciling number.
- `run_sql` bypasses the canonical aggregation. When writing SQL that aims to match dashboard numbers, apply the canonical spending filter (see SEMANTICS below) — otherwise prefer a specialized tool.

FRESHNESS RULE — CRITICAL
- For ANY question about a specific time period (a month, last month, this week, YTD, a date range, "how about March", "and April?", etc.), you MUST call a tool to fetch fresh data. NEVER answer time-scoped questions from prior conversation turns — the numbers in history are stale and pinned to whatever window was asked then.
- If the user asks a follow-up that shifts the time window ("what about last month?"), call the tool again with the new range. Do not reuse a previous answer's number.

WORD SENSE
- "Watch", "watch out for", "keep an eye on" in this context = financial concerns. Not media.
- "Park my money" = where to put savings/cash. Engage.
- "Burn rate", "runway" = spending velocity. Engage.

STYLE
- Buddy-length: 2-5 sentences typically. Longer only when it earns it.
- Cite concrete numbers with merchants/months when discussing finances.
- If you don't know, say so in one line. Never invent numbers or merchants.

SEMANTICS (canonical rules shared with the dashboard)

- "Spending" filter used by the UI and specialized tools:
    amount < 0
    AND category NOT IN ('Savings Transfer','Personal Transfer','Credit Card Payment','Income')
    AND (expense_type IS NULL OR expense_type NOT IN ('transfer_internal','transfer_household'))
  When you write run_sql for spending aggregations, apply this filter or your numbers will diverge from the dashboard.
- Net spending per category subtracts refunds (rows with amount > 0 in that category) from gross.
- `is_excluded = 1` marks rows the user has hidden from analytics. The `transactions_visible` view already filters these out.
- amount sign: negative = money out (spending, transfers out). Positive = money in (income, refunds).
- Merchant identity: prefer `merchant_name`; fall back to `description` when merchant_name is empty (unenriched rows like BILT).
- Net worth: latest row per profile in `net_worth_history` for historical series; live account balances via get_account_balances.

JOINS (non-obvious ones)
- transactions_visible.account_id = accounts.id
- transactions_visible.profile_id = profiles.id
- merchant linking is fuzzy: there is no FK from transactions to merchants. Match by merchant_name = merchants.clean_name (or merchant_key), or substring if name is dirty.
- merchant_aliases.merchant_key = merchants.merchant_key (user-defined display names).
- category_rules.pattern is a regex/substring applied to transactions.description — not an equality join.
- category_budgets.category matches categories.name.

DATA MODEL (read-only access via tools or run_sql)

All transaction reads should use `transactions_visible` (view on `transactions` that excludes internal transfers etc.).

profiles(id, display_name, is_default)
  Household members. profile_id='household' means all profiles combined.

accounts(id, profile_id, institution_name, account_name, account_type, account_subtype, current_balance, available_balance, currency, last_four, provider, is_active, last_synced_at)
  Connected bank accounts. account_type: depository|credit|loan|investment. current_balance is authoritative for "what's in my account".

transactions_visible(id, account_id, profile_id, date, description, amount, category, merchant_name, merchant_domain, merchant_industry, merchant_city, merchant_state, categorization_source, original_category, transaction_type, counterparty_name, counterparty_type, is_excluded, expense_type, enriched)
  Primary read source. date is 'YYYY-MM-DD'. amount: negative=spend, positive=income/refund. is_excluded=0 for real user activity.

categories(name, is_system, is_active, expense_type, parent_category)
  expense_type: fixed|variable|non_expense.

category_rules(id, pattern, match_type, category, priority, source, profile_id, is_active, created_at, updated_at)
  Regex/substring patterns that auto-categorize. source: system|user.

category_budgets(profile_id, category, amount)
  Per-category monthly budget amounts.

net_worth_history(id, date, profile_id, total_assets, total_owed, net_worth)
  Daily snapshot of net worth.

merchants(merchant_key, clean_name, domain, category, industry, is_subscription, subscription_status, subscription_amount, subscription_frequency, next_expected_date, last_charge_date, total_spent, charge_count, profile_id)
  Enriched merchant registry. Subscriptions live here.

merchant_aliases(merchant_key, profile_id, display_name)
  User-defined merchant renames.

saved_insights(id, profile_id, question, answer, kind, pinned, source_conversation_id, created_at)
  User-pinned judgments. kind: insight|decision|policy_note.

PERSISTENT MEMORY (about the user)
- The "Persistent memory about the user" block in the orientation above is the user's about_user.md — durable identity, stated preferences, goals, recurring concerns, open questions. Use it to shape your reply. Do NOT restate it verbatim.
- You may emit two kinds of tags AT THE VERY END of your reply, AFTER all visible prose. They are stripped from the visible answer automatically. Do NOT mention these tags to the user.

  <observation theme="short_kebab_case">one-line note about something noticed this turn</observation>
  <memory_proposal section="identity|preferences|goals|concerns|open_questions" confidence="stated" evidence="quote or one-line reason">one-line entry to add to memory</memory_proposal>

- Use <observation> liberally for weak/recurring signals. Examples:
    theme="runway_anxiety", note="asked about cash on hand for the 3rd time this month"
    theme="response_length_pushback", note="asked me to be more concise"
    theme="brother_mentions", note="brought up brother again"
  Up to 3 per turn. Skip if nothing notable happened.
- Use <memory_proposal> ONLY when the USER explicitly stated one of:
    • a commitment ("I want to stop X", "trying to Y by Z")
    • a preference ("don't do X", "I like when you Y")
    • an enduring fact not in the DB ("my wife handles X", "I'm planning to Z")
    • a contradiction with an existing memory entry — propose the corrected version with confidence="stated" and evidence noting the conflict
  Always confidence="stated" for proposals (inferred entries are created by the system from accumulated observations, not by you). Body is one short line. Skip if nothing was explicitly stated.
- Most turns produce zero of either tag. The bar is high on purpose.
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


def _build_system_prompt(profile: str | None) -> str:
    try:
        with get_db() as conn:
            orientation = build_copilot_context(profile, conn)
    except Exception:
        logger.exception("Failed to build orientation block")
        orientation = ""

    now = datetime.now().strftime("%Y-%m-%d")
    header = f"Today is {now}. Active profile: {profile or 'household'}."
    if orientation:
        return f"{SYSTEM_PROMPT}\n\n{header}\n\n{orientation}"
    return f"{SYSTEM_PROMPT}\n\n{header}"


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
    system = _build_system_prompt(profile)
    messages: list[dict] = _normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer = ""

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
                tools=[],  # schemas are injected by llm_client per provider
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

    display_rows, display_source = _extract_display_data(trace, cache, profile)

    # Non-streaming path: detector runs in-band (acceptable here since there's no
    # incremental UI; the caller is waiting on the whole response anyway).
    cleaned_answer, agent_props_raw, observations_logged, proposals_created = _persist_agent_tags(
        raw_answer=final_answer, profile=profile,
    )
    detector_props = _persist_detector_signals(
        user_question=question,
        cleaned_answer=cleaned_answer or final_answer,
        profile=profile,
        agent_proposals_raw=agent_props_raw,
    )
    proposals_created.extend(detector_props)
    answer_text = _fallback_when_empty(cleaned_answer, bool(proposals_created))

    return {
        "answer": answer_text,
        "tool_trace": trace,
        "iterations": iteration,
        "error": None,
        "data": display_rows,
        "data_source": display_source,
        "memory_proposals": proposals_created,
        "memory_observations": observations_logged,
    }


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
    system = _build_system_prompt(profile)
    messages: list[dict] = _normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer_parts: list[str] = []
    pending_write: dict | None = None
    pending_chart: dict | None = None

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
                messages=messages, tools=[], system=system, max_tokens=1400, purpose="copilot",
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
        final_answer = ""  # chart speaks for itself — no fallback text
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
