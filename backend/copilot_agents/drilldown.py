from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import llm_client
from database import get_db
from copilot_tools import execute_tool

from .base import emit_done_with_memory, tool_loop_result, tool_loop_stream


SPEND_TOTAL_TOOLS = {"get_merchant_spend", "get_category_spend"}
SPEND_WORD_RE = re.compile(r"\b(how much|spend|spent|paid|pay|charges?|expenses?|transactions?|bought|purchase[ds]?)\b", re.I)
CHART_WORD_RE = re.compile(r"\b(plot|chart|graph|visualize|trend)\b", re.I)
WRITE_WORD_RE = re.compile(r"\b(move all|recategorize|reclassify|rename|create (?:a )?rule|always categorize)\b", re.I)
STOP_SUBJECTS = {
    "a", "an", "and", "any", "at", "by", "did", "do", "for", "from", "how", "i", "in",
    "last", "me", "month", "months", "much", "my", "of", "on", "over", "paid", "past",
    "pay", "spend", "spent", "the", "this", "to", "what", "with", "year",
}
CATEGORY_SYNONYMS = {
    "grocery": "Groceries",
    "groceries": "Groceries",
    "food": "Food & Dining",
    "dining": "Food & Dining",
    "food and dining": "Food & Dining",
    "food dining": "Food & Dining",
    "restaurants": "Food & Dining",
    "restaurant": "Food & Dining",
    "subscriptions": "Subscriptions",
    "subscription": "Subscriptions",
}


def _money(value: Any) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())).strip()


def _tokens(text: str) -> set[str]:
    return {t for t in _norm(text).split() if t and t not in STOP_SUBJECTS}


def _canonical_key(text: str) -> str:
    return " ".join(t for t in _norm(text).split() if t != "and")


def _parse_jsonish(raw: str) -> dict | None:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _range_from_question(question: str) -> str:
    q = _norm(question)

    if re.search(r"\b(this|current)\s+month\b", q):
        return "current_month"
    if re.search(r"\b(last|previous|prior)\s+month\b", q):
        return "last_month"
    if re.search(r"\b(this|current)\s+week\b", q):
        return "this_week"
    if re.search(r"\b(last|previous|prior)\s+week\b", q):
        return "last_week"
    if re.search(r"\b(ytd|year\s+to\s+date|this\s+year)\b", q):
        return "ytd"
    if re.search(r"\blast\s+year\b", q):
        return "last_year"
    if re.search(r"\b(past|previous|prior)\s+year\b", q) or re.search(r"\bover\s+the\s+past\s+year\b", q):
        return "last_12_months"
    if re.search(r"\b(all\s+time|alltime|ever|lifetime|till\s+now|until\s+now|to\s+date)\b", q):
        return "all"
    if re.search(r"\bso\s+far\b", q) and not re.search(r"\b(this|current)\s+(month|week|year)\b", q):
        return "all"

    half_year = re.search(r"\b(?:last|past|previous|prior|over\s+the\s+last|over\s+the\s+past)\s+half\s+(?:a\s+)?year\b", q)
    if half_year:
        return "last_6_months"

    months = re.search(r"\b(?:last|past|previous|prior|over\s+the\s+last|over\s+the\s+past|in\s+the\s+last)\s+(\d{1,2})\s+months?\b", q)
    if months:
        n = max(1, min(int(months.group(1)), 36))
        return f"last_{n}_months"

    days = re.search(r"\b(?:last|past|previous|prior|over\s+the\s+last|over\s+the\s+past|in\s+the\s+last)\s+(\d{1,3})\s+days?\b", q)
    if days:
        n = max(1, min(int(days.group(1)), 365))
        return f"last_{n}d"

    return "current_month"


def _has_explicit_time_phrase(question: str) -> bool:
    q = _norm(question)
    return bool(re.search(
        r"\b(this|current|last|previous|prior|past|all\s+time|alltime|ever|lifetime|till\s+now|until\s+now|to\s+date|so\s+far|ytd|year\s+to\s+date|\d{1,3}\s+days?|\d{1,2}\s+months?)\b",
        q,
    ))


def _range_label(result: dict) -> str:
    label = str(result.get("range") or "").strip()
    if label == "all":
        return "all time"
    if label == "current_month":
        return "this month"
    if label == "last_month":
        return "last month"
    if label.startswith("last_") and label.endswith("_months"):
        return f"the last {label[5:-7]} months"
    if label.startswith("last_") and label.endswith("d"):
        return f"the last {label[5:-1]} days"
    if re.match(r"^\d{4}-\d{2}$", label):
        now = datetime.now()
        current = f"{now.year:04d}-{now.month:02d}"
        prior_year, prior_month = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
        prior = f"{prior_year:04d}-{prior_month:02d}"
        if label == current:
            return "this month"
        if label == prior:
            return "last month"
        return label
    if label:
        return label
    return "the selected period"


def _spend_answer(tool_name: str, args: dict, result: dict) -> str:
    if result.get("error"):
        return f"I couldn't get a clean spend total: {result['error']}"

    total = _money(result.get("total"))
    label = _range_label(result)
    if tool_name == "get_category_spend":
        category = result.get("category") or args.get("category") or "that category"
        count = int(result.get("total_count") or 0)
        gross = result.get("gross")
        refunds = result.get("refunds")
        suffix = ""
        if gross not in (None, 0) or refunds not in (None, 0):
            suffix = f" Gross spending was {_money(gross)} and refunds were {_money(refunds)}."
        return f"You spent {total} on {category} for {label}, across {count} transaction{'s' if count != 1 else ''}.{suffix}"

    merchant = result.get("merchant_query") or args.get("merchant") or "that merchant"
    count = int(result.get("txn_count") or result.get("total_matching_transactions") or 0)
    return f"You spent {total} at {merchant} for {label}, across {count} transaction{'s' if count != 1 else ''}."


def _load_categories(profile: str | None) -> list[str]:
    try:
        with get_db() as conn:
            params: list[Any] = []
            where = "category IS NOT NULL AND category != ''"
            if profile and profile != "household":
                where += " AND profile_id = ?"
                params.append(profile)
            rows = conn.execute(
                f"""
                SELECT DISTINCT category
                FROM transactions_visible
                WHERE {where}
                ORDER BY LENGTH(category) DESC, category
                """,
                params,
            ).fetchall()
        return [str(r[0]) for r in rows if r[0]]
    except Exception:
        return []


def _load_merchants(profile: str | None) -> list[str]:
    try:
        with get_db() as conn:
            params: list[Any] = []
            where = """
                COALESCE(NULLIF(merchant_key, ''), NULLIF(merchant_name, '')) IS NOT NULL
                AND COALESCE(NULLIF(merchant_key, ''), NULLIF(merchant_name, '')) != ''
                AND amount < 0
                AND is_excluded = 0
                AND category NOT IN ('Savings Transfer','Personal Transfer','Credit Card Payment','Income')
                AND (expense_type IS NULL OR expense_type NOT IN ('transfer_internal','transfer_household'))
            """
            if profile and profile != "household":
                where += " AND profile_id = ?"
                params.append(profile)
            rows = conn.execute(
                f"""
                SELECT COALESCE(NULLIF(merchant_name, ''), merchant_key) AS merchant_name, COUNT(*) AS count
                FROM transactions_visible
                WHERE {where}
                GROUP BY COALESCE(NULLIF(merchant_key, ''), NULLIF(merchant_name, ''))
                ORDER BY LENGTH(merchant_name) DESC, count DESC
                """,
                params,
            ).fetchall()
        names = []
        seen = set()
        for row in rows:
            name = str(row[0] or "").strip()
            key = name.lower()
            if len(_norm(name)) >= 3 and key not in seen:
                names.append(name)
                seen.add(key)
        return names
    except Exception:
        return []


def _candidate_names(question: str, names: list[str], limit: int = 20) -> list[str]:
    q_norm = _norm(question)
    q_key = _canonical_key(question)
    q_tokens = _tokens(question)
    scored: list[tuple[int, int, str]] = []
    for name in names:
        n_norm = _norm(name)
        n_key = _canonical_key(name)
        n_tokens = _tokens(name)
        if not n_key or n_key in STOP_SUBJECTS:
            continue
        score = 0
        if re.search(rf"(?<!\w){re.escape(n_norm)}(?!\w)", q_norm):
            score += 100
        if n_key != n_norm and re.search(rf"(?<!\w){re.escape(n_key)}(?!\w)", q_key):
            score += 95
        overlap = len(q_tokens & n_tokens)
        if overlap:
            score += overlap * 20
        if score:
            scored.append((score, len(n_norm), name))
    scored.sort(reverse=True)
    return [name for _, _, name in scored[:limit]]


def _contains_phrase(normalized_question: str, phrase: str) -> bool:
    normalized = _norm(phrase)
    if not normalized or normalized in STOP_SUBJECTS:
        return False
    return re.search(rf"(?<!\w){re.escape(normalized)}(?!\w)", normalized_question) is not None


def _match_category(question: str, profile: str | None) -> str | None:
    q = _norm(question)
    for phrase, category in sorted(CATEGORY_SYNONYMS.items(), key=lambda item: len(item[0]), reverse=True):
        if _contains_phrase(q, phrase):
            return category
    for category in _load_categories(profile):
        if _contains_phrase(q, category):
            return category
        if category.endswith("s") and _contains_phrase(q, category[:-1]):
            return category
    return None


def _match_merchant(question: str, profile: str | None) -> str | None:
    q = _norm(question)
    q_key = _canonical_key(question)
    candidates = []
    for merchant in _load_merchants(profile):
        normalized = _norm(merchant)
        canonical = _canonical_key(merchant)
        if len(normalized) < 3 or normalized in STOP_SUBJECTS:
            continue
        if _contains_phrase(q, merchant) or (canonical and re.search(rf"(?<!\w){re.escape(canonical)}(?!\w)", q_key)):
            candidates.append((len(normalized), merchant))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _history_subject(history: list[dict] | None, profile: str | None) -> tuple[str, str] | None:
    if not history:
        return None
    for turn in reversed(history[-8:]):
        content = (turn.get("content") or "").strip()
        if not content:
            continue
        category = _match_category(content, profile)
        merchant = _match_merchant(content, profile)
        if merchant:
            return "get_merchant_spend", merchant
        if category:
            return "get_category_spend", category
    return None


def _resolve_name(subject_type: str, subject_text: str | None, profile: str | None, candidates: dict[str, list[str]]) -> str | None:
    subject_text = (subject_text or "").strip()
    if not subject_text:
        return None
    pool = candidates["categories"] if subject_type == "category" else candidates["merchants"]
    if not pool:
        pool = _load_categories(profile) if subject_type == "category" else _load_merchants(profile)

    subject_norm = _norm(subject_text)
    subject_key = _canonical_key(subject_text)
    for name in pool:
        if _norm(name) == subject_norm or _canonical_key(name) == subject_key:
            return name
    for name in pool:
        name_key = _canonical_key(name)
        if name_key and (name_key in subject_key or subject_key in name_key):
            return name
    return None


def _recent_context(history: list[dict] | None) -> str:
    if not history:
        return "(none)"
    lines = []
    for turn in history[-4:]:
        role = turn.get("role")
        content = (turn.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            lines.append(f"{role}: {content[:240]}")
    return "\n".join(lines) or "(none)"


def _parser_candidates(question: str, profile: str | None) -> dict[str, list[str]]:
    categories = _candidate_names(question, _load_categories(profile), limit=16)
    merchants = _candidate_names(question, _load_merchants(profile), limit=24)
    if not categories:
        categories = _load_categories(profile)[:16]
    return {"categories": categories, "merchants": merchants}


def structured_spend_plan(question: str, profile: str | None, history: list[dict] | None = None) -> tuple[dict | None, int]:
    if not llm_client.is_available():
        return None, 0

    candidates = _parser_candidates(question, profile)
    prompt = f"""Parse the latest user message for Folio dispatch.

Return JSON only with this schema:
{{"intent":"drilldown|chart|write|overview|sql|chat","operation":"spend_total|list_transactions|other","subject_type":"merchant|category|none","subject_text":string|null,"range":string|null,"confidence":number,"needs_clarification":boolean}}

Rules:
- Use intent=chat for non-finance/general knowledge/code/science/life questions.
- Use operation=spend_total for questions asking how much money was spent/paid/wasted/charged/expensed.
- Pick subject_text from the candidate lists when possible. Preserve exact candidate spelling.
- Use the latest message's subject when it names one. Use recent context only for true follow-ups like "what about last month?".
- Ranges: current_month, last_month, this_week, last_week, ytd, last_year, all, last_Nd, last_N_months.
- "past year" or "last 12 months" means last_12_months. "last year" means the previous calendar year.
- If a subject is named but not in candidates, still put the raw subject_text and lower confidence.
- If subject/range is unclear, set needs_clarification=true.

Merchant candidates: {json.dumps(candidates["merchants"][:24])}
Category candidates: {json.dumps(candidates["categories"][:16])}

Recent context:
{_recent_context(history)}

Latest message: {question}
JSON:"""
    raw = llm_client.complete(prompt, max_tokens=180, purpose="copilot")
    parsed = _parse_jsonish(raw)
    if not parsed:
        return None, 1
    if parsed.get("intent") != "drilldown" or parsed.get("operation") != "spend_total":
        return None, 1
    if parsed.get("needs_clarification"):
        return None, 1

    subject_type = str(parsed.get("subject_type") or "").strip().lower()
    if subject_type not in {"merchant", "category"}:
        inherited = _history_subject(history, profile)
        if not inherited:
            return None, 1
        tool_name, subject = inherited
    else:
        subject = _resolve_name(subject_type, parsed.get("subject_text"), profile, candidates)
        if not subject:
            return None, 1
        tool_name = "get_category_spend" if subject_type == "category" else "get_merchant_spend"

    range_token = str(parsed.get("range") or "").strip() or "current_month"
    range_hint = _range_from_question(question)
    if _has_explicit_time_phrase(question):
        range_token = range_hint
    else:
        range_token = "current_month"
    if range_token in {"past_year"}:
        range_token = "last_12_months"
    arg_name = "category" if tool_name == "get_category_spend" else "merchant"
    return {"name": tool_name, "args": {arg_name: subject, "range": range_token}, "source": "structured_parser"}, 1


def direct_spend_plan(question: str, profile: str | None, history: list[dict] | None = None) -> dict | None:
    if CHART_WORD_RE.search(question or "") or WRITE_WORD_RE.search(question or ""):
        return None
    if not SPEND_WORD_RE.search(question or "") and not re.search(r"\bwhat\s+about\b", question or "", re.I):
        return None

    category = _match_category(question, profile)
    merchant = _match_merchant(question, profile)
    q = _norm(question)
    tool_name: str | None = None
    subject: str | None = None

    if merchant and re.search(rf"\b(at|from)\s+{re.escape(_norm(merchant))}\b", q):
        tool_name, subject = "get_merchant_spend", merchant
    elif category and re.search(rf"\b(on|for|in)\s+{re.escape(_norm(category))}\b", q):
        tool_name, subject = "get_category_spend", category
    elif category and not merchant:
        tool_name, subject = "get_category_spend", category
    elif merchant and not category:
        tool_name, subject = "get_merchant_spend", merchant
    elif category and merchant:
        if re.search(r"\b(at|from)\b", q):
            tool_name, subject = "get_merchant_spend", merchant
        else:
            tool_name, subject = "get_category_spend", category

    if not tool_name or not subject:
        explicit_subject_hint = re.search(r"\b(at|from|on|for)\s+([a-z0-9][a-z0-9\s&'._-]{2,})", q)
        if explicit_subject_hint:
            return None
        inherited = _history_subject(history, profile)
        if inherited:
            tool_name, subject = inherited

    if not tool_name or not subject:
        return None

    range_token = _range_from_question(question)
    arg_name = "category" if tool_name == "get_category_spend" else "merchant"
    return {"name": tool_name, "args": {arg_name: subject, "range": range_token}, "source": "direct_resolver"}


def looks_like_drilldown_shortcut(question: str) -> bool:
    q = question or ""
    return bool(SPEND_WORD_RE.search(q)) and not CHART_WORD_RE.search(q) and not WRITE_WORD_RE.search(q)


def _first_tool_result(question: str, profile: str | None, history: list[dict] | None) -> tuple[dict | None, list[dict], dict, int]:
    import copilot_agent as core

    cache: dict = {}
    trace: list[dict] = []
    messages = core._normalize_history(history) + [{"role": "user", "content": question}]
    response = llm_client.chat_with_tools(
        messages=messages,
        tools=list(core.DRILLDOWN_TOOLS),
        system=core._build_system_prompt(profile, list(core.DRILLDOWN_TOOLS)),
        max_tokens=800,
        purpose="copilot",
    )
    tool_calls = response.get("tool_calls") or []
    if len(tool_calls) != 1:
        return None, trace, cache, 1

    call = tool_calls[0]
    start = datetime.now()
    result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
    duration_ms = int((datetime.now() - start).total_seconds() * 1000)
    trace.append({"name": call["name"], "args": call.get("args") or {}, "duration_ms": duration_ms})
    return {"call": call, "result": result}, trace, cache, 1


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    structured, parser_calls = structured_spend_plan(question, profile, history)
    direct = structured or direct_spend_plan(question, profile, history)
    if direct:
        cache: dict = {}
        start = datetime.now()
        result_payload = execute_tool(direct["name"], direct["args"], profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace = [{"name": direct["name"], "args": direct["args"], "duration_ms": duration_ms}]
        answer = _spend_answer(direct["name"], direct["args"], result_payload if isinstance(result_payload, dict) else {})
        result = core._finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            run_detector=True,
        )
        result["llm_calls"] = parser_calls
        return result

    first, trace, cache, llm_calls = _first_tool_result(question, profile, history)
    if first and first["call"].get("name") in SPEND_TOTAL_TOOLS and isinstance(first.get("result"), dict):
        answer = _spend_answer(first["call"]["name"], first["call"].get("args") or {}, first["result"])
        result = core._finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            run_detector=True,
        )
        result["llm_calls"] = llm_calls
        return result

    return tool_loop_result(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.DRILLDOWN_TOOLS),
        system=core._build_system_prompt(profile, list(core.DRILLDOWN_TOOLS)),
    )


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    structured, parser_calls = structured_spend_plan(question, profile, history)
    direct = structured or direct_spend_plan(question, profile, history)
    if direct:
        cache: dict = {}
        trace: list[dict] = []
        yield {"type": "reset_text"}
        yield {"type": "tool_call", "name": direct["name"], "args": direct["args"]}
        start = datetime.now()
        result_payload = execute_tool(direct["name"], direct["args"], profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": direct["name"], "args": direct["args"], "duration_ms": duration_ms})
        yield {"type": "tool_result", "name": direct["name"], "duration_ms": duration_ms}
        answer = _spend_answer(direct["name"], direct["args"], result_payload if isinstance(result_payload, dict) else {})
        yield from emit_done_with_memory(
            question=question,
            profile=profile,
            final_answer=answer,
            trace=trace,
            cache=cache,
            iterations=0,
            llm_calls=parser_calls,
        )
        return

    cache: dict = {}
    trace: list[dict] = []
    messages = core._normalize_history(history) + [{"role": "user", "content": question}]
    pending_tool_calls: list[dict] = []
    yield {"type": "reset_text"}
    try:
        for event_type, payload in llm_client.chat_with_tools_stream(
            messages=messages,
            tools=list(core.DRILLDOWN_TOOLS),
            system=core._build_system_prompt(profile, list(core.DRILLDOWN_TOOLS)),
            max_tokens=800,
            purpose="copilot",
        ):
            if event_type == "tool_call":
                pending_tool_calls.append(payload)
    except Exception as e:
        yield {"type": "error", "message": f"Copilot hit an error: {e}"}
        return

    if len(pending_tool_calls) == 1:
        call = pending_tool_calls[0]
        yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
        start = datetime.now()
        result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": call["name"], "args": call.get("args") or {}, "duration_ms": duration_ms})
        yield {"type": "tool_result", "name": call["name"], "duration_ms": duration_ms}

        if call["name"] in SPEND_TOTAL_TOOLS and isinstance(result, dict):
            answer = _spend_answer(call["name"], call.get("args") or {}, result)
            yield from emit_done_with_memory(
                question=question,
                profile=profile,
                final_answer=answer,
                trace=trace,
                cache=cache,
                iterations=0,
                llm_calls=1,
            )
            return

    yield from tool_loop_stream(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.DRILLDOWN_TOOLS),
        system=core._build_system_prompt(profile, list(core.DRILLDOWN_TOOLS)),
    )
