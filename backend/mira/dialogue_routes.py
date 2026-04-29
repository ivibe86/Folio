from __future__ import annotations

from typing import Any

from merchant_aliases import exact_merchant_for_text, resolve_merchant_with_llm
from mira import controller as conversation_controller
from mira.context_policy import (
    ContextRouteCallbacks,
    context_range_token,
    route_for_context_slots,
)
from mira.fast_paths import (
    _average_months,
    _candidate_payload,
    _exact_category_for_text,
    _looks_like_followup,
    _merchant_confirmation_question,
    _merchant_dialogue_state,
    _merchant_names,
    _plan_kind,
    _shortcut_base,
    _with_dialogue_state,
)
from mira.grounding import BROAD_CATEGORY_NAMES as _BROAD_CATEGORY_NAMES, ground_category, ground_merchant, normalize_text
from range_parser import contains, parse_range, words

_PLAN_TERMS = {
    "compare", "compared", "versus", "vs", "average", "avg", "usual", "normal",
    "track", "pace", "projected", "projection", "forecast", "higher", "lower",
}
_ACK_TOKENS = {"thanks", "thank", "thx", "cool", "ok", "okay", "gotcha", "nice", "great"}
_CONTEXT_REF_TOKENS = {
    "again", "back", "behind", "breakdown", "details", "detail", "it", "same", "that", "those", "this",
}
_NEGATIVE_FEEDBACK_TOKENS = {"wrong", "incorrect", "off", "false", "bad", "nonsense", "invalid"}
_TRANSACTION_DETAIL_TOKENS = {
    "behind", "breakdown", "details", "detail", "rows", "row", "transactions", "transaction",
    "charges", "charge", "purchases", "purchase", "receipts", "receipt",
}
_CHART_TOKENS = {"chart", "plot", "graph", "visualize", "trend", "line", "bar"}


def _history_lines(history: list[dict] | None, limit: int = 4) -> str:
    if not history:
        return "(none)"
    lines: list[str] = []
    for turn in history[-limit:]:
        role = turn.get("role")
        content = " ".join((turn.get("content") or "").split())
        if role in {"user", "assistant"} and content:
            lines.append(f"{role}: {content[:260]}")
    return "\n".join(lines) or "(none)"


def _with_controller_act(route: dict, act) -> dict:
    if act:
        route["controller_act"] = act.as_dict() if hasattr(act, "as_dict") else act
    return route


def _looks_like_grounding_question(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    if (
        contains(tokens, ("how", "did", "you", "get", "that"))
        or contains(tokens, ("where", "did", "that", "come", "from"))
        or contains(tokens, ("where", "did", "you", "get", "that"))
    ):
        return True
    asks_how = "how" in token_set or contains(tokens, ("where", "did"))
    info_terms = {"information", "info", "numbers", "number", "data", "source", "sources", "transactions", "proof"}
    get_terms = {"get", "got", "find", "derive", "calculate", "computed", "answer"}
    return asks_how and bool(token_set & info_terms) and bool(token_set & get_terms)


def _dialogue_state_from_history(history: list[dict] | None, profile: str | None) -> tuple[dict | None, str]:
    server_state, last_assistant, _source = conversation_controller.active_clarification_from_history(profile, history)
    if server_state:
        return server_state, last_assistant
    if not history or len(history) < 2:
        return None, ""
    last = history[-1]
    if last.get("role") != "assistant":
        return None, ""
    state = last.get("dialogue_state")
    if isinstance(state, dict) and state.get("kind") in {"merchant_clarification", "entity_type_clarification"}:
        return dict(state), last.get("content") or ""

    content = last.get("content") or ""
    if not _is_merchant_selection_prompt(content):
        return None, ""
    merchants = _merchant_names(profile)
    candidates = _merchant_names_in_text(content, merchants)
    if not candidates:
        return None, ""
    original = _original_user_for_merchant_selection(history)
    return _merchant_dialogue_state(original, candidates), content

def _answer_context_from_history(history: list[dict] | None, profile: str | None) -> dict | None:
    server_context = conversation_controller.answer_context_from_history(profile, history)
    if server_context:
        return server_context
    return None

def _plain_history_finance_context(history: list[dict] | None, profile: str | None) -> dict | None:
    if not history:
        return None
    merchants = _merchant_names(profile)
    for turn in reversed(history[-8:]):
        content = str(turn.get("content") or "")
        if not content.strip():
            continue
        merchant = exact_merchant_for_text(content, merchants)
        category = _exact_category_for_text(content, profile)
        if not merchant and not category:
            continue
        subject_type = "merchant" if merchant else "category"
        subject = merchant or category or ""
        parsed_range = parse_range(content)
        range_token = parsed_range.token if parsed_range.explicit else "current_month"
        tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
        arg_name = "merchant" if subject_type == "merchant" else "category"
        return {
            "version": 1,
            "kind": "finance_answer_context",
            "subject_type": subject_type,
            "subject": subject,
            "intent": "spending",
            "operation": "merchant_total" if subject_type == "merchant" else "category_total",
            "tool_name": tool_name,
            "ranges": [range_token],
            "tools": [{"name": tool_name, "args": {arg_name: subject, "range": range_token}}],
        }
    return None

def _has_context_reference(tokens: list[str]) -> bool:
    token_set = set(tokens)
    if token_set & _CONTEXT_REF_TOKENS:
        return True
    return (
        contains(tokens, ("what", "about"))
        or contains(tokens, ("how", "about"))
        or contains(tokens, ("same", "for"))
        or contains(tokens, ("now", "for"))
        or contains(tokens, ("for", "that"))
        or contains(tokens, ("asking", "about"))
        or contains(tokens, ("asked", "about"))
    )

def _asks_for_transaction_details(tokens: list[str]) -> bool:
    token_set = set(tokens)
    if token_set & _TRANSACTION_DETAIL_TOKENS:
        return True
    return (
        contains(tokens, ("show", "me", "more"))
        or contains(tokens, ("show", "more"))
        or contains(tokens, ("what", "made"))
        or contains(tokens, ("made", "up"))
    )

def _explicit_subject_type(question: str) -> str:
    tokens = words(question)
    token_set = set(tokens)
    if contains(tokens, ("not", "merchant")) or contains(tokens, ("in", "category")) or "category" in token_set:
        return "category"
    if contains(tokens, ("not", "category")) or "merchant" in token_set or bool(token_set & {"at", "from"}):
        return "merchant"
    if "on" in token_set:
        return "category"
    return ""

def _context_subject(question: str, context: dict, profile: str | None) -> tuple[str, str]:
    merchant = exact_merchant_for_text(question, _merchant_names(profile))
    category = _exact_category_for_text(question, profile)
    explicit_type = _explicit_subject_type(question)
    if explicit_type == "category" and category:
        subject_type = "category"
        subject = category
    elif explicit_type == "merchant" and merchant:
        subject_type = "merchant"
        subject = merchant
    elif category and category.lower() in _BROAD_CATEGORY_NAMES:
        subject_type = "category"
        subject = category
    else:
        subject_type = "merchant" if merchant else ("category" if category else str(context.get("subject_type") or ""))
        subject = merchant or category or str(context.get("subject") or "")
    if subject_type not in {"merchant", "category"}:
        return "", ""
    return subject_type, subject

def _context_plan_route(question: str, subject_type: str, subject: str, start: float) -> dict:
    tokens = words(question)
    kind = _plan_kind(question)
    if not kind:
        token_set = set(tokens)
        if token_set & {"average", "avg", "usual", "normal"}:
            kind = "current_vs_average"
        elif token_set & {"track", "pace", "projected", "projection", "forecast"}:
            kind = "on_track"
        else:
            kind = "current_vs_previous"
    months = _average_months(tokens) or (parse_range(question).chart_months if kind not in {"current_vs_average", "on_track"} else None) or 6
    route = _shortcut_base(
        "plan",
        kind,
        start,
        args={
            "plan_kind": kind,
            "subject_type": subject_type,
            "subject": subject,
            "months": max(1, min(int(months), 12)),
        },
    )
    route["shortcut"] = "context_controller_plan"
    route["uses_history"] = True
    return route

def _context_chart_route(subject_type: str, subject: str, start: float) -> dict:
    if subject_type == "category":
        route = _shortcut_base(
            "chart",
            "monthly_spending_chart",
            start,
            args={"category": subject, "months": 6},
        )
        route["tool_name"] = "get_monthly_spending_trend"
        route["shortcut"] = "context_controller_chart"
        route["uses_history"] = True
        return route
    route = _shortcut_base("chat", "merchant_chart_needs_plan", start)
    route["needs_clarification"] = True
    route["clarification_question"] = (
        f"I can compare {subject} across periods or show the transactions behind it. "
        "Merchant trend charts are not available yet. Which view do you want?"
    )
    route["uses_history"] = True
    return route

def _context_direct_route(operation: str, start: float, answer: str) -> dict:
    route = _shortcut_base("chat", operation, start, args={"answer": answer})
    route["uses_history"] = True
    route["shortcut"] = operation
    return route

def _context_policy_callbacks(profile: str | None) -> ContextRouteCallbacks:
    def ground_subject_shift(
        question: str,
        clue: str,
        context: dict,
        start: float,
        *,
        action: str,
        range_token: str,
    ) -> dict | None:
        return _ground_context_subject_shift(
            question,
            clue,
            context,
            profile,
            start,
            action=action,
            range_token=range_token,
        )

    return ContextRouteCallbacks(
        shortcut_base=_shortcut_base,
        chart_route=_context_chart_route,
        plan_route=_context_plan_route,
        ground_subject_shift=ground_subject_shift,
    )

def _context_missing_subject_route(question: str, context: dict, clue: str, range_token: str, start: float, *, action: str = "use") -> dict:
    previous_subject = str(context.get("subject") or "").strip()
    route = _shortcut_base("chat", "context_subject_not_found", start)
    route["needs_clarification"] = True
    prior_note = f" I won't reuse {previous_subject} for that." if previous_subject else ""
    route["clarification_question"] = (
        f"I don't see a merchant or category matching `{clue}` in your transactions.{prior_note} "
        "Can you give me another clue, like part of the transaction description, amount, date, or category?"
    )
    route["uses_history"] = True
    state = _merchant_dialogue_state(
        question,
        [],
        matched_text=clue,
        rejected_candidates=[],
        action="show_transactions" if action == "transactions" else "use",
    )
    state["range_token"] = range_token
    return _with_dialogue_state(route, state)

def _ground_context_subject_shift(
    question: str,
    clue: str,
    context: dict,
    profile: str | None,
    start: float,
    *,
    action: str,
    range_token: str,
) -> dict | None:
    clue = (clue or "").strip()
    if not clue:
        return None

    category_result = ground_category(clue, _candidate_payload(clue, profile).get("categories") or [], limit=3)
    merchant_result = ground_merchant(clue, _merchant_names(profile), profile=profile, include_transaction_evidence=True, limit=3)
    explicit_type = _explicit_subject_type(question)
    broad_category = _broad_category_from_result(category_result, clue)

    subject_type = ""
    subject = ""
    if _is_material_entity_type_collision(merchant_result, category_result, profile, range_token) and not explicit_type:
        return _entity_type_collision_route(
            question,
            clue,
            merchant_result.value or "",
            category_result.value or "",
            range_token,
            action,
            profile,
            start,
        )
    if explicit_type == "category" and (category_result.value or broad_category):
        subject_type, subject = "category", category_result.value or broad_category
    elif explicit_type == "merchant" and merchant_result.kind == "exact" and merchant_result.value:
        subject_type, subject = "merchant", merchant_result.value
    elif broad_category:
        subject_type, subject = "category", broad_category
    elif merchant_result.kind == "exact" and merchant_result.value:
        subject_type, subject = "merchant", merchant_result.value
    elif category_result.kind == "exact" and category_result.value:
        subject_type, subject = "category", category_result.value

    if subject_type and subject:
        route = route_for_context_slots(
            question,
            subject_type,
            subject,
            action,
            range_token,
            start,
            _context_policy_callbacks(profile),
        )
        if route:
            route["shortcut"] = "turn_state_subject_shift"
        return route

    if merchant_result.kind in {"approximate", "ambiguous"} and (merchant_result.value or merchant_result.candidates):
        candidates = []
        if merchant_result.value:
            candidates.append(merchant_result.value)
        for candidate in merchant_result.candidates:
            name = str(candidate.get("display_name") or candidate.get("value") or "").strip()
            if name and name not in candidates:
                candidates.append(name)
        candidate = candidates[0] if candidates else ""
        if candidate:
            route = _shortcut_base("chat", "confirm_context_subject_switch", start)
            route["needs_clarification"] = True
            prior_subject = str(context.get("subject") or "").strip()
            prior_note = f" I won't reuse {prior_subject} for that." if prior_subject else ""
            route["clarification_question"] = _merchant_confirmation_question(
                candidate,
                matched_text=(merchant_result.candidates[0].get("matched_text") if merchant_result.candidates else clue) or clue,
                profile=profile,
                action="show_transactions" if action == "transactions" else "use",
            ) + prior_note
            route["args"] = {
                "candidates": candidates[:5],
                "matched_text": clue,
                "confidence": merchant_result.confidence,
            }
            route["uses_history"] = True
            state = _merchant_dialogue_state(
                question,
                candidates[:5],
                matched_text=clue,
                action="show_transactions" if action == "transactions" else "use",
            )
            state["range_token"] = range_token
            return _with_dialogue_state(route, state)

    return _context_missing_subject_route(question, context, clue, range_token, start, action=action)

def _broad_category_from_result(category_result: Any, clue: str = "") -> str:
    value = str(getattr(category_result, "value", "") or "")
    if value and value.lower() in _BROAD_CATEGORY_NAMES:
        return value
    clue_norm = normalize_text(clue)
    for candidate in getattr(category_result, "candidates", []) or []:
        name = str(candidate.get("display_name") or candidate.get("value") or "").strip()
        if name and name.lower() in _BROAD_CATEGORY_NAMES and normalize_text(name) == clue_norm:
            return name
    for candidate in getattr(category_result, "candidates", []) or []:
        name = str(candidate.get("display_name") or candidate.get("value") or "").strip()
        if name and name.lower() in _BROAD_CATEGORY_NAMES:
            return name
    return ""

def _subject_total(subject_type: str, subject: str, range_token: str, profile: str | None) -> tuple[float, int]:
    try:
        from copilot_tools import execute_tool

        tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
        key = "merchant" if subject_type == "merchant" else "category"
        result = execute_tool(tool_name, {key: subject, "range": range_token}, profile, cache={})
        if not isinstance(result, dict):
            return 0.0, 0
        total = float(result.get("total") or 0)
        count = int(result.get("txn_count") or result.get("total_count") or result.get("total_matching_transactions") or 0)
        return total, count
    except Exception:
        return 0.0, 0

def _is_material_entity_type_collision(merchant_result: Any, category_result: Any, profile: str | None, range_token: str) -> bool:
    if not (
        getattr(merchant_result, "kind", "") == "exact"
        and getattr(category_result, "kind", "") == "exact"
        and getattr(merchant_result, "value", None)
        and getattr(category_result, "value", None)
    ):
        return False
    merchant_name = str(merchant_result.value)
    category_name = str(category_result.value)
    if normalize_text(merchant_name) != normalize_text(category_name):
        return False
    if category_name.lower() in _BROAD_CATEGORY_NAMES:
        return False
    merchant_total, merchant_count = _subject_total("merchant", merchant_name, range_token, profile)
    category_total, category_count = _subject_total("category", category_name, range_token, profile)
    return abs(merchant_total - category_total) >= 0.01 or merchant_count != category_count

def _entity_type_state(question: str, subject: str, merchant: str, category: str, range_token: str, action: str) -> dict[str, Any]:
    return {
        "version": 1,
        "kind": "entity_type_clarification",
        "original_question": question,
        "subject": subject,
        "merchant": merchant,
        "category": category,
        "range_token": range_token,
        "action": action or "spend",
        "pending_action": action or "spend",
        "rejected_interpretations": [],
    }

def _entity_type_collision_route(
    question: str,
    clue: str,
    merchant: str,
    category: str,
    range_token: str,
    action: str,
    profile: str | None,
    start: float,
) -> dict:
    merchant_total, merchant_count = _subject_total("merchant", merchant, range_token, profile)
    category_total, category_count = _subject_total("category", category, range_token, profile)
    route = _shortcut_base("chat", "entity_type_collision", start)
    route["needs_clarification"] = True
    route["clarification_question"] = (
        f"I found `{clue}` as both a merchant and a category. "
        f"Merchant `{merchant}` is ${merchant_total:,.2f} across {merchant_count} transaction(s); "
        f"category `{category}` is ${category_total:,.2f} across {category_count} transaction(s). "
        "Did you mean the merchant or the category?"
    )
    route["args"] = {
        "matched_text": clue,
        "merchant": merchant,
        "category": category,
        "range": range_token,
        "merchant_total": merchant_total,
        "category_total": category_total,
    }
    route["uses_history"] = True
    return _with_dialogue_state(route, _entity_type_state(question, clue, merchant, category, range_token, action))

def _entity_type_dialogue_route(question: str, state: dict, start: float, profile: str | None) -> dict:
    tokens = words(question)
    token_set = set(tokens)
    if token_set <= {"cancel", "stop", "nevermind", "forget"}:
        route = _shortcut_base("chat", "dialogue_cancelled", start)
        route["needs_clarification"] = True
        route["clarification_question"] = "Okay, I won't use that interpretation."
        route["uses_history"] = True
        return route
    chosen = _explicit_subject_type(question)
    if not chosen and token_set <= {"merchant", "merch"}:
        chosen = "merchant"
    if not chosen and token_set <= {"category", "cat"}:
        chosen = "category"
    if chosen == "merchant":
        subject = str(state.get("merchant") or state.get("subject") or "").strip()
    elif chosen == "category":
        subject = str(state.get("category") or state.get("subject") or "").strip()
    else:
        subject = ""
    if chosen in {"merchant", "category"} and subject:
        route = route_for_context_slots(
            question,
            chosen,
            subject,
            str(state.get("action") or "spend"),
            str(state.get("range_token") or "current_month"),
            start,
            _context_policy_callbacks(profile),
        )
        if route:
            route["uses_history"] = True
            route["shortcut"] = "entity_type_correction"
            return route
    route = _shortcut_base("chat", "clarify_entity_type", start)
    route["needs_clarification"] = True
    route["clarification_question"] = "I still need the type: should I use the merchant or the category?"
    route["uses_history"] = True
    return _with_dialogue_state(route, state)

def _should_run_context_interpreter(tokens: list[str], parsed_range_explicit: bool) -> bool:
    if not tokens:
        return False
    token_set = set(tokens)
    if parsed_range_explicit or _has_context_reference(tokens):
        return True
    if len(tokens) <= 8 and (token_set & {"again", "more", "why", "no", "not", "wrong", "chart", "plot", "average", "usual"}):
        return True
    return False

def _answer_context_followup_route(question: str, history: list[dict] | None, profile: str | None, start: float) -> dict | None:
    context = _answer_context_from_history(history, profile)
    if not context:
        return None

    parsed_range = parse_range(question)
    tokens = words(question)
    token_set = set(tokens)
    if not tokens:
        return None

    if _looks_like_grounding_question(question) or ("why" in token_set and token_set & {"zero", "0", "wrong"}):
        route = _shortcut_base("chat", "explain_grounding", start)
        route["uses_history"] = True
        return route

    if token_set <= _ACK_TOKENS:
        return _context_direct_route("context_acknowledge", start, "Got it.")

    has_context_ref = _has_context_reference(tokens) or _looks_like_followup(question)
    asks_transactions = _asks_for_transaction_details(tokens)
    asks_chart = bool(token_set & _CHART_TOKENS)
    asks_plan = bool(token_set & _PLAN_TERMS) or bool(token_set & {"average", "avg", "usual", "normal", "pace", "projected", "projection"})
    has_negative_feedback = bool(token_set & _NEGATIVE_FEEDBACK_TOKENS) or contains(tokens, ("not", "right"))
    exact_merchant = exact_merchant_for_text(question, _merchant_names(profile))
    exact_category = _exact_category_for_text(question, profile)
    explicit_subject_named = bool(exact_merchant or exact_category)

    subject_type, subject = _context_subject(question, context, profile)
    if subject_type not in {"merchant", "category"} or not subject:
        return None

    if has_negative_feedback and not (parsed_range.explicit or asks_transactions or asks_chart or asks_plan or explicit_subject_named):
        route = _shortcut_base("chat", "context_correction_needed", start)
        route["needs_clarification"] = True
        route["clarification_question"] = (
            "Got it, I won't rely on that prior answer. "
            "Tell me the merchant or category and the range you want me to rerun."
        )
        route["uses_history"] = True
        return route

    if asks_plan and (has_context_ref or explicit_subject_named or parsed_range.explicit or len(tokens) <= 6):
        return _context_plan_route(question, subject_type, subject, start)

    if asks_chart and (has_context_ref or explicit_subject_named or len(tokens) <= 5):
        return _context_chart_route(subject_type, subject, start)

    if asks_transactions:
        contextual_details = has_context_ref or explicit_subject_named or len(tokens) <= 4
        if not contextual_details:
            return None
        if subject_type == "merchant":
            route = _shortcut_base("transactions", "list_transactions", start, args={"merchant": subject, "limit": 25})
            route["tool_name"] = "get_transactions_for_merchant"
        else:
            route = _shortcut_base("transactions", "list_transactions", start, args={"category": subject, "limit": 25})
            route["tool_name"] = "get_transactions"
        route["uses_history"] = True
        route["shortcut"] = "answer_context_followup"
        return route

    if parsed_range.explicit or has_context_ref or explicit_subject_named:
        range_token = context_range_token(parsed_range, context)
        arg_name = "merchant" if subject_type == "merchant" else "category"
        tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
        route = _shortcut_base(
            "spending",
            "merchant_total" if subject_type == "merchant" else "category_total",
            start,
            args={arg_name: subject, "range": range_token},
        )
        route["tool_name"] = tool_name
        route["uses_history"] = True
        route["shortcut"] = "answer_context_followup"
        return route

    if _should_run_context_interpreter(tokens, parsed_range.explicit):
        act = conversation_controller.interpret_context_turn(question, context, _history_lines(history, limit=4))
        if act.act == "same_subject_transactions":
            if subject_type == "merchant":
                route = _shortcut_base("transactions", "list_transactions", start, args={"merchant": subject, "limit": 25})
                route["tool_name"] = "get_transactions_for_merchant"
            else:
                route = _shortcut_base("transactions", "list_transactions", start, args={"category": subject, "limit": 25})
                route["tool_name"] = "get_transactions"
            route["uses_history"] = True
            route["shortcut"] = "context_controller_transactions"
            return route
        if act.act == "same_subject_plan":
            return _context_plan_route(question, subject_type, subject, start)
        if act.act == "same_subject_chart":
            return _context_chart_route(subject_type, subject, start)
        if act.act == "same_subject_spend":
            arg_name = "merchant" if subject_type == "merchant" else "category"
            tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
            route = _shortcut_base(
                "spending",
                "merchant_total" if subject_type == "merchant" else "category_total",
                start,
                args={arg_name: subject, "range": context_range_token(parsed_range, context)},
            )
            route["tool_name"] = tool_name
            route["uses_history"] = True
            route["shortcut"] = "context_controller_spend"
            return route
        if act.act == "explain_grounding":
            route = _shortcut_base("chat", "explain_grounding", start)
            route["uses_history"] = True
            return route
        if act.act == "answer_directly":
            return _context_direct_route("context_acknowledge", start, "Got it.")

    return None

def _merge_rejected_candidates(state: dict, candidates: list[str] | tuple[str, ...]) -> list[str]:
    merged: list[str] = []
    for candidate in list(state.get("rejected_candidates") or []) + list(candidates or []):
        name = str(candidate or "").strip()
        if name and name not in merged:
            merged.append(name)
    return merged

def _available_merchants(profile: str | None, rejected: list[str]) -> list[str]:
    rejected_set = {item.lower() for item in rejected}
    return [name for name in _merchant_names(profile) if name.lower() not in rejected_set]

def _rejected_text(rejected: list[str]) -> str:
    if not rejected:
        return ""
    if len(rejected) == 1:
        return f" I've ruled out {rejected[0]}."
    return f" I've ruled out {', '.join(rejected[:-1])}, and {rejected[-1]}."

def _ask_for_merchant_clue_route(state: dict, rejected: list[str], start: float, *, correction_text: str = "") -> dict:
    if correction_text:
        question = (
            f"I don't see a merchant matching `{correction_text}` in your transactions."
            f"{_rejected_text(rejected)} Can you give me another clue, like part of the transaction description, amount, date, or category?"
        )
        shortcut = "merchant_correction_not_found"
    else:
        target = "that merchant" if len(rejected) == 1 else "those merchant candidates"
        question = (
            f"Got it, I won't use {target}.{_rejected_text(rejected)} "
            "What merchant should I look for instead? You can give me part of the transaction description, amount, date, or category."
        )
        shortcut = "merchant_rejected_need_clue"
    route = _shortcut_base("chat", shortcut, start)
    route["needs_clarification"] = True
    route["clarification_question"] = question
    route["uses_history"] = True
    next_state = _merchant_dialogue_state(
        str(state.get("original_question") or ""),
        [],
        matched_text=str(state.get("matched_text") or ""),
        rejected_candidates=rejected,
        action=str(state.get("action") or "use"),
    )
    if state.get("range_token"):
        next_state["range_token"] = state.get("range_token")
    return _with_dialogue_state(route, next_state)

def _corrected_merchant_route(correction: str, state: dict, rejected: list[str], profile: str | None, start: float) -> dict:
    original = str(state.get("original_question") or "")
    action = str(state.get("action") or "use")
    available = _available_merchants(profile, rejected)
    exact = exact_merchant_for_text(correction, available)
    if exact:
        range_token = str(state.get("range_token") or "") or None
        return _route_for_confirmed_merchant(exact, original, start, range_token=range_token)

    resolution = resolve_merchant_with_llm(correction, available, profile=profile, include_transaction_evidence=True)
    if resolution.name and resolution.confidence >= 0.55:
        candidates = [resolution.name]
        question_text = _merchant_confirmation_question(
            resolution.name,
            matched_text=resolution.matched_text or correction,
            profile=profile,
            action=action,
        )
        route = _shortcut_base("chat", "confirm_corrected_merchant", start)
        route["needs_clarification"] = True
        route["clarification_question"] = question_text
        route["args"] = {
            "candidates": candidates,
            "matched_text": resolution.matched_text or correction,
            "confidence": resolution.confidence,
        }
        route["uses_history"] = True
        next_state = _merchant_dialogue_state(
            original,
            candidates,
            matched_text=resolution.matched_text or correction,
            rejected_candidates=rejected,
            action=action,
        )
        if state.get("range_token"):
            next_state["range_token"] = state.get("range_token")
        return _with_dialogue_state(route, next_state)

    return _ask_for_merchant_clue_route(state, rejected, start, correction_text=correction)

def _is_affirmative_confirmation(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    if not tokens:
        return False
    if token_set <= {"yes", "yeah", "yep", "correct", "right", "sure", "ok", "okay"}:
        return True
    return (
        contains(tokens, ("yes", "use", "that"))
        or contains(tokens, ("yes", "that", "one"))
        or (tokens[0] in {"yes", "yeah", "yep", "ok", "okay", "sure"} and len(tokens) <= 4)
    )

def _merchant_names_in_text(text: str, merchant_names: list[str]) -> list[str]:
    lowered = (text or "").lower()
    matches = []
    for name in merchant_names:
        if not name:
            continue
        idx = lowered.find(name.lower())
        if idx >= 0:
            matches.append((idx, -len(name), name))
    matches.sort()
    ordered = []
    for _, _, name in matches:
        if name not in ordered:
            ordered.append(name)
    return ordered

def _candidate_tokens(name: str) -> set[str]:
    return {
        token for token in words(name)
        if token not in {"and", "the", "of", "for", "at", "to", "in", "on", "my"}
    }

def _candidate_from_confirmation_reply(question: str, candidates: list[str]) -> tuple[str | None, bool]:
    tokens = words(question)
    token_set = set(tokens)
    if not candidates:
        return None, False
    if token_set & {"no", "nope", "nah"} or contains(tokens, ("not", "that")):
        return None, True

    ordinal_map = {
        "first": 0, "1": 0,
        "second": 1, "2": 1,
        "third": 2, "3": 2,
    }
    for token in tokens:
        if token in ordinal_map and ordinal_map[token] < len(candidates):
            return candidates[ordinal_map[token]], False

    compact_reply = "".join(tokens)
    exact_matches = []
    token_matches = []
    for candidate in candidates:
        candidate_tokens = _candidate_tokens(candidate)
        compact_candidate = "".join(words(candidate))
        if compact_candidate and compact_candidate in compact_reply:
            exact_matches.append(candidate)
            continue
        if candidate_tokens and candidate_tokens <= token_set:
            exact_matches.append(candidate)
            continue
        if candidate_tokens and token_set & candidate_tokens:
            token_matches.append(candidate)

    if len(exact_matches) == 1:
        return exact_matches[0], False
    if len(exact_matches) > 1:
        return None, True
    if len(token_matches) == 1:
        return token_matches[0], False
    if len(token_matches) > 1:
        return None, True

    if _is_affirmative_confirmation(question) and len(candidates) == 1:
        return candidates[0], False
    return None, True

def _is_merchant_selection_prompt(content: str) -> bool:
    text = content or ""
    return any(
        marker in text
        for marker in (
            "Which one should I use?",
            "Should I use that merchant?",
            "Should I show transactions for that merchant?",
            "Please pick one:",
        )
    )

def _original_user_for_merchant_selection(history: list[dict]) -> str:
    idx = len(history) - 1
    original = ""
    while idx >= 1:
        assistant_turn = history[idx]
        if assistant_turn.get("role") != "assistant" or not _is_merchant_selection_prompt(assistant_turn.get("content") or ""):
            break
        user_turn = history[idx - 1]
        if user_turn.get("role") != "user":
            break
        original = user_turn.get("content") or original
        previous_prompt_idx = idx - 2
        if (
            previous_prompt_idx >= 0
            and history[previous_prompt_idx].get("role") == "assistant"
            and _is_merchant_selection_prompt(history[previous_prompt_idx].get("content") or "")
        ):
            idx = previous_prompt_idx
            continue
        break
    return original

def _route_for_confirmed_merchant(merchant: str, previous_user: str, start: float, *, range_token: str | None = None) -> dict:
    previous_tokens = set(words(previous_user))
    if "transactions" in previous_tokens or "transaction" in previous_tokens:
        route = _shortcut_base("transactions", "list_transactions", start, args={"merchant": merchant, "limit": 25})
        route["tool_name"] = "get_transactions_for_merchant"
        route["shortcut"] = "confirmed_merchant_transactions"
        route["uses_history"] = True
        return route
    kind = _plan_kind(previous_user)
    if kind:
        months = _average_months(words(previous_user)) or (
            parse_range(previous_user).chart_months if kind not in {"current_vs_average", "on_track"} else None
        ) or 6
        route = _shortcut_base(
            "plan",
            kind,
            start,
            args={"plan_kind": kind, "subject_type": "merchant", "subject": merchant, "months": max(1, min(int(months), 12))},
        )
        route["shortcut"] = "confirmed_merchant_plan"
        route["uses_history"] = True
        return route
    route = _shortcut_base(
        "spending",
        "merchant_total",
        start,
        args={"merchant": merchant, "range": range_token or parse_range(previous_user).token},
    )
    route["tool_name"] = "get_merchant_spend"
    route["shortcut"] = "confirmed_merchant_spend"
    route["uses_history"] = True
    return route

def _confirmed_merchant_route(question: str, history: list[dict] | None, profile: str | None, start: float) -> dict | None:
    if not history or len(history) < 2:
        return None
    if history[-1].get("role") != "assistant" or history[-2].get("role") != "user":
        return None
    content = history[-1].get("content") or ""
    if not _is_merchant_selection_prompt(content):
        return None
    merchants = _merchant_names(profile)
    candidates = _merchant_names_in_text(content, merchants)
    if not candidates:
        return None
    merchant, ambiguous = _candidate_from_confirmation_reply(question, candidates)
    if merchant:
        previous_user = _original_user_for_merchant_selection(history) or (history[-2].get("content") or "")
        return _route_for_confirmed_merchant(merchant, previous_user, start)

    names = ", ".join(candidates[:3])
    route = _shortcut_base("chat", "clarify_merchant_selection", start)
    route["needs_clarification"] = True
    route["clarification_question"] = f"I couldn't tell which merchant you meant. Please pick one: {names}. Which one should I use?"
    route["args"] = {"candidates": candidates[:5]}
    route["uses_history"] = True
    route["shortcut"] = "ambiguous_merchant_confirmation" if ambiguous else "missing_merchant_confirmation"
    original = _original_user_for_merchant_selection(history)
    return _with_dialogue_state(route, _merchant_dialogue_state(original, candidates[:5]))

def _dialogue_state_route(question: str, history: list[dict] | None, profile: str | None, start: float) -> dict | None:
    state, last_assistant = _dialogue_state_from_history(history, profile)
    if not state:
        return None
    if state.get("kind") == "entity_type_clarification":
        return _entity_type_dialogue_route(question, state, start, profile)

    act = conversation_controller.interpret_dialogue_reply(question, state, last_assistant)
    controller_act = conversation_controller.controller_act_for_dialogue(act, state)
    if act.act == "new_task":
        return None
    if act.act == "ask_provenance":
        route = _shortcut_base("chat", "explain_grounding", start)
        route["uses_history"] = True
        return _with_controller_act(_with_dialogue_state(route, state), controller_act)
    if act.act == "cancel":
        route = _shortcut_base("chat", "dialogue_cancelled", start)
        route["needs_clarification"] = True
        route["clarification_question"] = "Okay, I won't use that merchant."
        route["uses_history"] = True
        return _with_controller_act(route, controller_act)

    original = str(state.get("original_question") or "")
    current_candidates = [str(item) for item in (state.get("current_candidates") or []) if str(item or "").strip()]
    if act.act in {"confirm", "select_candidate"}:
        selected = act.selected_candidate or (current_candidates[0] if act.act == "confirm" and len(current_candidates) == 1 else "")
        if selected and selected in current_candidates:
            range_token = str(state.get("range_token") or "") or None
            return _with_controller_act(_route_for_confirmed_merchant(selected, original, start, range_token=range_token), controller_act)

    if act.act in {"reject", "reject_and_correct"}:
        rejected = _merge_rejected_candidates(state, list(current_candidates) + list(act.rejected_candidates or ()))
        if act.act == "reject_and_correct" and act.correction_text:
            return _with_controller_act(_corrected_merchant_route(act.correction_text, state, rejected, profile, start), controller_act)
        return _with_controller_act(_ask_for_merchant_clue_route(state, rejected, start), controller_act)

    if not current_candidates:
        correction = (act.correction_text or question).strip()
        rejected = _merge_rejected_candidates(state, act.rejected_candidates)
        if correction:
            return _with_controller_act(_corrected_merchant_route(correction, state, rejected, profile, start), controller_act)
        return _with_controller_act(_ask_for_merchant_clue_route(state, rejected, start), controller_act)

    names = ", ".join(current_candidates[:3])
    route = _shortcut_base("chat", "clarify_merchant_selection", start)
    route["needs_clarification"] = True
    route["clarification_question"] = f"I couldn't tell which merchant you meant. Please pick one: {names}. Which one should I use?"
    route["args"] = {"candidates": current_candidates[:5]}
    route["uses_history"] = True
    return _with_controller_act(_with_dialogue_state(route, state), controller_act)
