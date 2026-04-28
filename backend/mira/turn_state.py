from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from range_parser import MONTH_LOOKUP, contains, parse_range, resolve_followup_range, words


TURN_KINDS = {
    "general_chat",
    "new_finance_task",
    "correction",
    "followup_range_shift",
    "followup_subject_shift",
    "followup_action_shift",
    "provenance",
    "confirm",
    "cancel",
    "unclear",
}


@dataclass(frozen=True)
class TurnInterpretation:
    turn_kind: str
    finance_related: bool
    preserve_subject: bool = False
    preserve_range: bool = False
    preserve_action: bool = False
    replace_subject_text: str | None = None
    replace_range_text: str | None = None
    replace_action: str | None = None
    reset_finance_context: bool = False
    confidence: float = 0.0
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        kind = self.turn_kind if self.turn_kind in TURN_KINDS else "unclear"
        return {
            "turn_kind": kind,
            "finance_related": bool(self.finance_related),
            "preserve_subject": bool(self.preserve_subject),
            "preserve_range": bool(self.preserve_range),
            "preserve_action": bool(self.preserve_action),
            "replace_subject_text": self.replace_subject_text,
            "replace_range_text": self.replace_range_text,
            "replace_action": self.replace_action,
            "reset_finance_context": bool(self.reset_finance_context),
            "confidence": _coerce_confidence(self.confidence),
            "reason": str(self.reason or ""),
        }


_CONFIRM_TOKENS = {"yes", "yeah", "yep", "correct", "right", "sure", "ok", "okay"}
_CANCEL_TOKENS = {"cancel", "stop", "nevermind", "forget"}
_ACK_TOKENS = {"thanks", "thank", "thx", "cool", "gotcha", "nice", "great"}
_NEGATIVE_TOKENS = {"no", "nope", "nah", "not", "wrong", "incorrect", "off", "false"}

_SPEND_TOKENS = {
    "spend", "spent", "spending", "paid", "pay", "charges", "charge", "expenses",
    "expense", "total", "cost", "costs", "wasted", "waste",
}
_TRANSACTION_TOKENS = {"transaction", "transactions", "purchase", "purchases", "charges", "charge", "rows", "details", "detail"}
_CHART_TOKENS = {"chart", "plot", "graph", "visualize", "trend"}
_PLAN_TOKENS = {"compare", "compared", "versus", "vs", "average", "avg", "usual", "normal", "pace", "projected", "projection"}
_WRITE_TOKENS = {
    "move", "change", "set", "update", "categorize", "categorise", "recategorize",
    "recategorise", "rename", "rule", "budget", "goal", "split", "tag", "note",
    "reviewed", "mark",
}
_FINANCE_NOUN_TOKENS = {
    "merchant", "merchants", "category", "categories", "groceries", "grocery", "dining",
    "restaurant", "restaurants", "subscriptions", "subscription", "tax", "taxes",
    "rent", "housing", "healthcare", "medical", "entertainment", "shopping",
    "travel", "utilities", "income", "budget", "goal", "networth", "worth",
    "balance", "balances", "account", "accounts", "cash", "money",
}
_DISPLAY_TOKENS = {"show", "list", "display", "pull", "find"}
_CONTEXT_REF_TOKENS = {"that", "those", "same", "it", "them", "more", "again"}
_RANGE_FILLER_TOKENS = {
    "a", "about", "actually", "am", "and", "asking", "asked", "before", "after",
    "during", "earlier", "for", "from", "how", "i", "in", "instead", "later",
    "last", "me", "month", "months", "next", "no", "not", "now", "of", "one", "prior",
    "previous", "the", "this", "to", "what",
} | set(MONTH_LOOKUP)
_SUBJECT_STOP_TOKENS = _RANGE_FILLER_TOKENS | _SPEND_TOKENS | _TRANSACTION_TOKENS | _CHART_TOKENS | _PLAN_TOKENS | _WRITE_TOKENS | _DISPLAY_TOKENS | _CONTEXT_REF_TOKENS | {
    "all", "any", "can", "could", "did", "do", "does", "give", "help", "is",
    "just", "kind", "like", "look", "much", "my", "please", "talk", "tell",
    "there", "was", "were", "with", "you",
}


def interpret_turn(
    text: str,
    *,
    answer_context: dict[str, Any] | None = None,
    has_active_dialogue: bool = False,
    now: Any = None,
) -> TurnInterpretation:
    tokens = words(text)
    token_set = set(tokens)
    has_context = isinstance(answer_context, dict) and answer_context.get("kind") == "finance_answer_context"
    prior_range = _prior_range(answer_context)
    parsed_range = resolve_followup_range(text, prior_range, now=now) if has_context else parse_range(text, now=now)
    has_range = bool(parsed_range.explicit)

    if not tokens:
        return _turn("unclear", False, 0.0, "empty turn")
    if _looks_like_provenance(tokens):
        return _turn("provenance", False, 0.95, "provenance request", preserve_subject=has_context, preserve_range=has_context, preserve_action=has_context)
    if token_set <= _CANCEL_TOKENS:
        return _turn("cancel", False, 0.95, "cancel request", reset_finance_context=True)
    if has_active_dialogue and _is_confirmation(tokens):
        return _turn("confirm", True, 0.9, "active clarification confirmation", preserve_subject=True, preserve_range=True, preserve_action=True)
    if has_active_dialogue and not _is_finance_task(tokens):
        if token_set & _NEGATIVE_TOKENS:
            return _turn("correction", True, 0.78, "active clarification correction", preserve_range=True, preserve_action=True)
        if len(tokens) <= 5:
            return _turn("unclear", True, 0.5, "short active clarification reply", preserve_range=True, preserve_action=True)

    if not has_context and has_range and _has_subject_shift_marker(tokens):
        return _turn(
            "followup_range_shift",
            True,
            0.68,
            "plain-history range follow-up",
            preserve_subject=True,
            preserve_action=True,
            replace_range_text=text,
        )
    if not has_context and (token_set & _CONTEXT_REF_TOKENS):
        plain_action_shift = _action_shift(tokens, True)
        if plain_action_shift:
            return _turn(
                "followup_action_shift",
                True,
                0.66,
                f"plain-history action shift to {plain_action_shift}",
                preserve_subject=True,
                preserve_range=True,
                replace_action=plain_action_shift,
            )

    action_shift = _action_shift(tokens, has_context)
    if has_context and action_shift:
        return _turn(
            "followup_action_shift",
            True,
            0.9,
            f"follow-up action shift to {action_shift}",
            preserve_subject=True,
            preserve_range=True,
            replace_action=action_shift,
        )

    if has_context and has_range and (token_set & _NEGATIVE_TOKENS):
        return _turn(
            "correction",
            True,
            0.88,
            "range correction",
            preserve_subject=True,
            preserve_action=True,
            replace_range_text=text,
        )

    range_only = has_context and has_range and not _subject_candidate(text) and not _explicit_finance_action(tokens)
    if range_only:
        if token_set & _NEGATIVE_TOKENS or contains(tokens, ("not", "this", "month")) or contains(tokens, ("asking", "about")):
            return _turn(
                "correction",
                True,
                0.92,
                "range correction",
                preserve_subject=True,
                preserve_action=True,
                replace_range_text=text,
            )
        return _turn(
            "followup_range_shift",
            True,
            0.9,
            "range-only follow-up",
            preserve_subject=True,
            preserve_action=True,
            replace_range_text=text,
        )

    if has_context and _has_subject_shift_marker(tokens):
        subject_text = _subject_candidate(text)
        if subject_text:
            return _turn(
                "followup_subject_shift",
                True,
                0.82,
                "subject-only finance follow-up",
                preserve_range=True,
                preserve_action=True,
                replace_subject_text=subject_text,
            )

    if _is_finance_task(tokens) or (has_range and bool(token_set & _FINANCE_NOUN_TOKENS)):
        return _turn("new_finance_task", True, 0.86, "explicit finance task", reset_finance_context=not has_context)

    if token_set <= _ACK_TOKENS:
        return _turn("general_chat", False, 0.86, "acknowledgement", reset_finance_context=True)

    if has_context and (token_set & _NEGATIVE_TOKENS):
        return _turn("unclear", True, 0.55, "negative feedback on prior finance answer", preserve_subject=True, preserve_range=True, preserve_action=True)

    if not has_context:
        return _turn("general_chat", False, 0.78, "no finance intent", reset_finance_context=True)

    if has_context and not _has_contextual_finance_signal(tokens):
        return _turn("general_chat", False, 0.82, "general chat escapes finance context", reset_finance_context=True)

    return _turn("unclear", False, 0.35, "ambiguous contextual turn")


def _turn(
    kind: str,
    finance_related: bool,
    confidence: float,
    reason: str,
    **kwargs: Any,
) -> TurnInterpretation:
    return TurnInterpretation(
        turn_kind=kind if kind in TURN_KINDS else "unclear",
        finance_related=finance_related,
        confidence=_coerce_confidence(confidence),
        reason=reason,
        **kwargs,
    )


def _prior_range(answer_context: dict[str, Any] | None) -> str:
    if not isinstance(answer_context, dict):
        return ""
    ranges = answer_context.get("ranges")
    if isinstance(ranges, list):
        for item in ranges:
            token = str(item or "").strip()
            if token:
                return token
    tools = answer_context.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            args = tool.get("args") if isinstance(tool, dict) and isinstance(tool.get("args"), dict) else {}
            token = str(args.get("range") or "").strip()
            if token:
                return token
    return ""


def _looks_like_provenance(tokens: list[str]) -> bool:
    token_set = set(tokens)
    if contains(tokens, ("how", "did", "you", "get", "that")):
        return True
    if contains(tokens, ("where", "did", "that", "come", "from")) or contains(tokens, ("where", "did", "you", "get", "that")):
        return True
    asks_how = "how" in token_set or contains(tokens, ("where", "did"))
    info_terms = {"information", "info", "numbers", "number", "data", "source", "sources", "transactions", "proof"}
    get_terms = {"get", "got", "find", "derive", "calculate", "computed", "answer"}
    return asks_how and bool(token_set & info_terms) and bool(token_set & get_terms)


def _is_confirmation(tokens: list[str]) -> bool:
    if not tokens:
        return False
    token_set = set(tokens)
    return token_set <= _CONFIRM_TOKENS or (
        tokens[0] in _CONFIRM_TOKENS and len(tokens) <= 4 and not (token_set & _NEGATIVE_TOKENS)
    )


def _explicit_finance_action(tokens: list[str]) -> bool:
    token_set = set(tokens)
    if token_set & (_SPEND_TOKENS | _TRANSACTION_TOKENS | _PLAN_TOKENS):
        return True
    if token_set & _CHART_TOKENS and (token_set & _FINANCE_NOUN_TOKENS or {"net", "worth"} <= token_set):
        return True
    if token_set & _DISPLAY_TOKENS and token_set & _FINANCE_NOUN_TOKENS:
        return True
    return False


def _is_finance_task(tokens: list[str]) -> bool:
    token_set = set(tokens)
    if contains(tokens, ("how", "much")):
        return True
    if {"net", "worth"} <= token_set:
        return True
    if _explicit_finance_action(tokens):
        return True
    if token_set & _WRITE_TOKENS and (token_set & (_FINANCE_NOUN_TOKENS | _TRANSACTION_TOKENS | _SPEND_TOKENS) or "to" in token_set):
        if "write" in token_set and not (token_set & (_FINANCE_NOUN_TOKENS | _TRANSACTION_TOKENS | _SPEND_TOKENS | {"rename", "move", "recategorize", "categorize", "rule"})):
            return False
        return True
    return False


def _has_contextual_finance_signal(tokens: list[str]) -> bool:
    token_set = set(tokens)
    return bool(
        token_set & (_SPEND_TOKENS | _TRANSACTION_TOKENS | _CHART_TOKENS | _PLAN_TOKENS | _FINANCE_NOUN_TOKENS)
        or _has_subject_shift_marker(tokens)
        or "more" in token_set
        or contains(tokens, ("show", "me", "more"))
    )


def _action_shift(tokens: list[str], has_context: bool) -> str | None:
    if not has_context:
        return None
    token_set = set(tokens)
    if token_set & _CHART_TOKENS and (token_set & _CONTEXT_REF_TOKENS or len(tokens) <= 4):
        return "chart"
    if token_set & _PLAN_TOKENS and (token_set & _CONTEXT_REF_TOKENS or len(tokens) <= 6):
        return "plan"
    if (
        contains(tokens, ("show", "me", "more"))
        or contains(tokens, ("show", "more"))
        or token_set & {"details", "detail", "rows"}
    ) and not _subject_candidate(" ".join(tokens)):
        return "transactions"
    return None


def _has_subject_shift_marker(tokens: list[str]) -> bool:
    return (
        contains(tokens, ("what", "about"))
        or contains(tokens, ("how", "about"))
        or contains(tokens, ("same", "for"))
        or contains(tokens, ("now", "for"))
        or contains(tokens, ("and", "for"))
        or contains(tokens, ("asking", "about"))
        or contains(tokens, ("asked", "about"))
    )


def _subject_candidate(text: str) -> str:
    tokens = words(text)
    if not tokens:
        return ""
    candidate_tokens = [
        token for token in tokens
        if len(token) > 1 and not token.isdigit() and token not in _SUBJECT_STOP_TOKENS
    ]
    if not candidate_tokens:
        return ""
    if len(candidate_tokens) > 5:
        return ""
    return " ".join(candidate_tokens).strip()


def _coerce_confidence(value: Any) -> float:
    try:
        return max(0.0, min(float(value), 1.0))
    except (TypeError, ValueError):
        return 0.0


def copy_interpretation(value: TurnInterpretation | dict[str, Any] | None) -> dict[str, Any] | None:
    if isinstance(value, TurnInterpretation):
        return value.as_dict()
    if isinstance(value, dict):
        return copy.deepcopy(value)
    return None
