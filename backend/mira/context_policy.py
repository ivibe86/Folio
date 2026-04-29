from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from range_parser import resolve_followup_range, words


Route = dict[str, Any]


@dataclass(frozen=True)
class ContextRouteCallbacks:
    shortcut_base: Callable[..., Route]
    chart_route: Callable[[str, str, float], Route]
    plan_route: Callable[[str, str, str, float], Route]
    ground_subject_shift: Callable[..., Route | None]


_SUBJECT_CLUE_FILLERS = {
    "a", "about", "actually", "all", "am", "an", "and", "any", "asking", "at",
    "charge", "charges", "current", "did", "do", "does", "expense", "expenses",
    "for", "from", "how", "i", "in", "instead", "last", "me", "month", "months",
    "much", "my", "no", "not", "now", "of", "on", "paid", "pay", "please",
    "past", "previous", "prior", "show", "spend", "spending", "spent", "that", "the",
    "this", "to", "total", "transaction", "transactions", "what", "with",
    "week", "weeks", "year", "years", "ytd", "today", "date", "far", "so",
}

_RANGE_BOUNDARY_TOKENS = {
    "after", "before", "current", "during", "last", "month", "months", "next",
    "previous", "prior", "since", "this", "today", "week", "weeks", "year", "years",
}


def context_range_token(parsed_range: Any, context: dict[str, Any]) -> str:
    if getattr(parsed_range, "explicit", False):
        return str(parsed_range.token)
    return prior_context_range(context)


def prior_context_range(context: dict[str, Any]) -> str:
    ranges = context.get("ranges")
    if isinstance(ranges, list):
        for item in ranges:
            token = str(item or "").strip()
            if token:
                return token
    tools = context.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            args = tool.get("args") if isinstance(tool, dict) and isinstance(tool.get("args"), dict) else {}
            token = str(args.get("range") or "").strip()
            if token:
                return token
    return "current_month"


def context_action_name(context: dict[str, Any]) -> str:
    tool_name = str(context.get("tool_name") or "")
    intent = str(context.get("intent") or "")
    operation = str(context.get("operation") or "")
    if tool_name in {"get_transactions", "get_transactions_for_merchant"} or intent == "transactions":
        return "transactions"
    if tool_name == "get_monthly_spending_trend" or intent == "chart":
        return "chart"
    if intent == "plan" or operation in {"current_vs_previous", "current_vs_average", "on_track"}:
        return "plan"
    return "spend"


def context_slots(context: dict[str, Any]) -> tuple[str, str, str, str]:
    subject_type = str(context.get("subject_type") or "").strip()
    subject = str(context.get("subject") or "").strip()
    if subject_type not in {"merchant", "category"} or not subject:
        return "", "", "", ""
    return subject_type, subject, context_action_name(context), prior_context_range(context)


def route_for_context_slots(
    question: str,
    subject_type: str,
    subject: str,
    action: str,
    range_token: str,
    start: float,
    callbacks: ContextRouteCallbacks,
) -> Route | None:
    if subject_type not in {"merchant", "category"} or not subject:
        return None
    action = action or "spend"
    range_token = range_token or "current_month"
    if action == "transactions":
        if subject_type == "merchant":
            route = callbacks.shortcut_base(
                "transactions",
                "list_transactions",
                start,
                args={"merchant": subject, "limit": 25},
            )
            route["tool_name"] = "get_transactions_for_merchant"
        else:
            route = callbacks.shortcut_base(
                "transactions",
                "list_transactions",
                start,
                args={"category": subject, "range": range_token, "limit": 25},
            )
            route["tool_name"] = "get_transactions"
        route["uses_history"] = True
        route["shortcut"] = "turn_state_action_shift" if "more" in set(words(question)) else "turn_state_context"
        return route
    if action == "chart":
        return callbacks.chart_route(subject_type, subject, start)
    if action == "plan":
        return callbacks.plan_route(question, subject_type, subject, start)

    arg_name = "merchant" if subject_type == "merchant" else "category"
    tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
    route = callbacks.shortcut_base(
        "spending",
        "merchant_total" if subject_type == "merchant" else "category_total",
        start,
        args={arg_name: subject, "range": range_token},
    )
    route["tool_name"] = tool_name
    route["uses_history"] = True
    route["shortcut"] = "turn_state_context"
    return route


def apply_turn_state_context_route(
    question: str,
    context: dict[str, Any] | None,
    start: float,
    turn: dict[str, Any],
    callbacks: ContextRouteCallbacks,
) -> Route | None:
    if not isinstance(context, dict):
        return None
    subject_type, subject, prior_action, prior_range = context_slots(context)
    if not subject_type or not subject:
        return None

    kind = str(turn.get("turn_kind") or "")
    action = str(turn.get("replace_action") or prior_action or "spend")
    parsed_range = resolve_followup_range(str(turn.get("replace_range_text") or question), prior_range)
    range_token = parsed_range.token if parsed_range.explicit else prior_range
    preserved_action = (
        "spend"
        if prior_action == "plan" and kind in {"correction", "followup_range_shift", "followup_subject_shift"}
        else prior_action
    )

    if kind in {"correction", "followup_range_shift"}:
        route = route_for_context_slots(
            question,
            subject_type,
            subject,
            preserved_action,
            range_token,
            start,
            callbacks,
        )
        if route:
            route["shortcut"] = "turn_state_range_shift" if kind == "followup_range_shift" else "turn_state_correction"
        return route
    if kind == "followup_action_shift":
        route = route_for_context_slots(question, subject_type, subject, action, range_token, start, callbacks)
        if route and action != "plan":
            route["shortcut"] = "turn_state_action_shift"
        return route
    if kind == "followup_subject_shift":
        return callbacks.ground_subject_shift(
            question,
            str(turn.get("replace_subject_text") or ""),
            context,
            start,
            action=preserved_action,
            range_token=range_token,
        )
    return None


def subject_clue_for_missing_finance_subject(question: str) -> str:
    tokens = words(question)
    if not tokens:
        return ""

    start_idx = 0
    for idx, token in enumerate(tokens):
        if token in {"at", "from", "for", "on"} and idx + 1 < len(tokens):
            start_idx = idx + 1
            break

    clue_tokens: list[str] = []
    for token in tokens[start_idx:]:
        if clue_tokens and token in _RANGE_BOUNDARY_TOKENS:
            break
        if token in _SUBJECT_CLUE_FILLERS or token.isdigit():
            continue
        clue_tokens.append(token)
        if len(clue_tokens) >= 5:
            break
    return " ".join(clue_tokens).strip()
