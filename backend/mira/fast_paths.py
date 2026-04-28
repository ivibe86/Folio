from __future__ import annotations

import re
import time
from typing import Any

from merchant_aliases import (
    alias_targets_for_text,
    exact_merchant_for_text,
    resolve_merchant_with_llm,
)
from mira.context_policy import subject_clue_for_missing_finance_subject
from mira.grounding import (
    BROAD_CATEGORY_NAMES as _BROAD_CATEGORY_NAMES,
    CATEGORY_SYNONYMS as _CATEGORY_SYNONYMS,
    exact_category_for_text,
    ground_category,
    ground_merchant,
)
from range_parser import chart_months, contains, parse_range, words

_PLAN_TERMS = {
    "compare", "compared", "versus", "vs", "average", "avg", "usual", "normal",
    "track", "pace", "projected", "projection", "forecast", "higher", "lower",
}
_CHART_TOKENS = {"chart", "plot", "graph", "visualize", "trend", "line", "bar"}


def _candidate_payload(question: str, profile: str | None) -> dict[str, list[str]]:
    try:
        from copilot_agents.drilldown import _candidate_names, _load_categories, _load_merchants

        merchants = _load_merchants(profile)
        categories = _load_categories(profile)
        return {
            "merchants": _candidate_names(question, merchants, limit=40) or merchants[:60],
            "categories": _candidate_names(question, categories, limit=30) or categories[:50],
        }
    except Exception:
        return {"merchants": [], "categories": []}

def _merchant_names(profile: str | None) -> list[str]:
    try:
        from copilot_agents.drilldown import _load_merchants

        return _load_merchants(profile)
    except Exception:
        return []

def _shortcut_base(intent: str, operation: str, start: float, *, args: dict | None = None) -> dict:
    return {
        "intent": intent,
        "operation": operation,
        "tool_name": None,
        "args": args or {},
        "uses_history": False,
        "confidence": 1.0,
        "needs_clarification": False,
        "clarification_question": "",
        "shortcut": operation,
        "route_ms": round((time.perf_counter() - start) * 1000, 2),
        "classifier_ms": 0,
        "raw": None,
        "error": None,
    }

def _name_tokens(name: str) -> set[str]:
    return {
        token for token in words(name)
        if token not in {"and", "the", "of", "for", "at", "to", "in", "on", "my"}
    }

def _best_name_match(question_tokens: set[str], names: list[str]) -> str | None:
    scored: list[tuple[int, int, str]] = []
    for name in names:
        tokens = _name_tokens(name)
        if not tokens:
            continue
        overlap = len(tokens & question_tokens)
        if overlap:
            scored.append((overlap, len(tokens), name))
    scored.sort(reverse=True)
    return scored[0][2] if scored else None

def _resolve_plan_subject(question: str, candidates: dict[str, list[str]]) -> tuple[str | None, str | None]:
    tokens = words(question)
    token_set = set(tokens)

    merchant_result = ground_merchant(question, candidates.get("merchants") or [], limit=3)
    category_result = ground_category(question, candidates.get("categories") or [], limit=3)
    if (
        category_result.kind == "exact"
        and category_result.value
        and category_result.value.lower() in _BROAD_CATEGORY_NAMES
    ):
        return "category", category_result.value
    if merchant_result.kind == "exact" and merchant_result.value:
        return "merchant", merchant_result.value
    if category_result.kind == "exact" and category_result.value:
        return "category", category_result.value

    if merchant_result.kind == "approximate" and merchant_result.value:
        return "merchant", merchant_result.value

    category = _best_name_match(token_set, candidates.get("categories") or [])
    if category:
        return "category", category

    merchant = _best_name_match(token_set, candidates.get("merchants") or [])
    if merchant:
        return "merchant", merchant

    return None, None

def _looks_like_finance_read(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    finance_terms = {
        "spend", "spent", "spending", "paid", "pay", "charges", "charge", "expenses",
        "transactions", "transaction", "purchase", "purchases", "compare", "versus",
        "vs", "average", "track", "pace", "chart", "plot", "graph",
    }
    return bool(token_set & finance_terms) or contains(tokens, ("how", "much"))

def _merchant_evidence_text(merchant: str, profile: str | None) -> str:
    try:
        from copilot_tools import execute_tool

        result = execute_tool("get_transactions_for_merchant", {"merchant": merchant, "limit": 1}, profile)
        rows = result.get("transactions") if isinstance(result, dict) else []
        if isinstance(rows, list) and rows:
            row = rows[0]
            desc = row.get("description") or row.get("merchant_display_name") or row.get("merchant_name")
            date = row.get("date")
            if desc and date:
                return f" I found it in your transactions, most recently `{desc}` on {date}."
    except Exception:
        return ""
    return ""

def _merchant_confirmation_question(name: str, *, matched_text: str = "", profile: str | None = None, action: str = "use") -> str:
    hint = f" for `{matched_text}`" if matched_text else ""
    evidence = _merchant_evidence_text(name, profile)
    if action == "show_transactions":
        return f"I found `{name}` as the closest merchant match{hint}.{evidence} Should I show transactions for that merchant?"
    return f"I found `{name}` as the closest merchant match{hint}.{evidence} Should I use that merchant?"

def _pending_action_for_question(question: str, action: str = "use") -> str:
    if action == "show_transactions":
        return "transactions"
    if _plan_kind(question):
        return "plan"
    return "spending"

def _merchant_dialogue_state(
    original_question: str,
    candidates: list[str],
    *,
    matched_text: str = "",
    rejected_candidates: list[str] | None = None,
    action: str = "use",
) -> dict:
    cleaned_candidates = []
    for candidate in candidates:
        name = str(candidate or "").strip()
        if name and name not in cleaned_candidates:
            cleaned_candidates.append(name)
    rejected = []
    for candidate in rejected_candidates or []:
        name = str(candidate or "").strip()
        if name and name not in rejected:
            rejected.append(name)
    return {
        "version": 1,
        "kind": "merchant_clarification",
        "original_question": original_question,
        "subject_slot": "merchant",
        "current_candidates": cleaned_candidates[:5],
        "rejected_candidates": rejected[:10],
        "matched_text": matched_text,
        "action": action,
        "pending_action": _pending_action_for_question(original_question, action),
    }

def _with_dialogue_state(route: dict, state: dict | None) -> dict:
    if state:
        route["dialogue_state"] = state
    return route

def _exact_category_for_text(question: str, profile: str | None) -> str | None:
    categories = _candidate_payload(question, profile).get("categories") or []
    return exact_category_for_text(question, categories) or _category_token_fallback(question, categories)

def _category_token_fallback(question: str, categories: list[str]) -> str | None:
    tokens = words(question)
    if not tokens:
        return None
    by_lower = {str(item).lower(): item for item in categories}
    by_compact = {"".join(words(str(item))): item for item in categories}
    phrase = " ".join(tokens)
    compact = "".join(tokens)
    for key in (phrase, compact):
        if key in by_lower:
            return by_lower[key]
        if key in by_compact:
            return by_compact[key]
    for token in tokens:
        if token in by_lower:
            return by_lower[token]
    synonym = _CATEGORY_SYNONYMS.get(phrase) or _CATEGORY_SYNONYMS.get(compact)
    if synonym:
        return by_lower.get(synonym.lower(), synonym)
    for token in tokens:
        synonym = _CATEGORY_SYNONYMS.get(token)
        if synonym:
            return by_lower.get(synonym.lower(), synonym)
    return None

def _merchant_confirmation_route(question: str, profile: str | None, start: float) -> dict | None:
    if not _looks_like_finance_read(question):
        return None
    tokens = words(question)
    token_set = set(tokens)
    if token_set & {"chart", "plot", "graph", "trend", "visualize"}:
        return None
    if {"net", "worth"} <= token_set:
        return None
    if token_set & {"category", "categories"} and token_set & {"all", "every"}:
        return None
    if token_set & {"transaction", "transactions", "purchase", "purchases"} and token_set & {"show", "list", "display", "find", "pull"}:
        return None
    resolved_type, _ = _resolve_plan_subject(question, _candidate_payload(question, profile))
    if resolved_type == "category":
        return None

    merchants = _merchant_names(profile)
    resolution = resolve_merchant_with_llm(question, merchants, profile=profile, include_transaction_evidence=True)
    if not resolution.name:
        return None
    exact = exact_merchant_for_text(question, merchants)
    if exact and exact == resolution.name:
        return None
    if resolution.confidence < 0.55:
        return None
    candidate_names = list(resolution.candidates[:3])
    if len(candidate_names) > 1 and resolution.confidence < 0.7:
        names = ", ".join(candidate_names)
        question_text = f"I found a few possible merchants: {names}. Which one should I use?"
    else:
        question_text = _merchant_confirmation_question(
            resolution.name,
            matched_text=resolution.matched_text,
            profile=profile,
        )
    route = _shortcut_base("chat", "confirm_grounded_merchant", start)
    route["needs_clarification"] = True
    route["clarification_question"] = question_text
    route["args"] = {
        "candidates": list(resolution.candidates[:5]),
        "match_type": "llm",
        "matched_text": resolution.matched_text,
        "merchant": resolution.name,
        "confidence": resolution.confidence,
    }
    route["shortcut"] = "merchant_confirmation"
    return _with_dialogue_state(
        route,
        _merchant_dialogue_state(
            question,
            candidate_names[:3] if len(candidate_names) > 1 and resolution.confidence < 0.7 else [resolution.name],
            matched_text=resolution.matched_text,
        ),
    )

def _transaction_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    write_terms = {
        "move", "change", "set", "update", "categorize", "categorise", "recategorize",
        "recategorise", "rename", "mark", "split", "tag", "note",
    }
    if token_set & write_terms:
        return None
    spend_terms = {"spend", "spent", "spending", "waste", "wasted", "money", "total", "much"}
    if contains(tokens, ("how", "much")) or (token_set & spend_terms and token_set & {"show", "compare"}):
        return None
    transaction_terms = {"transaction", "transactions", "purchase", "purchases", "charges", "charge"}
    display_terms = {"show", "list", "display", "find", "pull"}
    asks_for_rows = bool(token_set & display_terms) or (
        bool(token_set & {"what", "which"}) and bool(token_set & transaction_terms)
    )
    if not asks_for_rows or not (token_set & transaction_terms):
        return None
    latest_one = (
        bool(token_set & {"latest", "recent", "newest"})
        or contains(tokens, ("occurred", "last"))
        or (("last" in token_set or "most" in token_set) and bool(token_set & {"1", "one", "single", "just"}))
    )
    if latest_one:
        route = _shortcut_base("transactions", "list_transactions", start, args={"limit": 1})
        route["tool_name"] = "get_transactions"
        route["shortcut"] = "latest_transaction"
        return route
    merchants = _merchant_names(profile)
    exact_merchant = exact_merchant_for_text(question, merchants)
    if exact_merchant:
        merchant = exact_merchant
    else:
        resolution = resolve_merchant_with_llm(question, merchants, profile=profile, include_transaction_evidence=True)
        if resolution.name and resolution.confidence >= 0.55:
            route = _shortcut_base("chat", "confirm_grounded_merchant", start)
            route["needs_clarification"] = True
            route["clarification_question"] = _merchant_confirmation_question(
                resolution.name,
                matched_text=resolution.matched_text,
                profile=profile,
                action="show_transactions",
            )
            route["args"] = {
                "merchant": resolution.name,
                "candidates": list(resolution.candidates[:5]),
                "confidence": resolution.confidence,
            }
            route["shortcut"] = "merchant_transaction_confirmation"
            return _with_dialogue_state(
                route,
                _merchant_dialogue_state(
                    question,
                    [resolution.name],
                    matched_text=resolution.matched_text,
                    action="show_transactions",
                ),
            )
        candidates = _candidate_payload(question, profile)
        merchant = _best_name_match(token_set, candidates.get("merchants") or [])
    if not merchant:
        category = _exact_category_for_text(question, profile)
        if category:
            route = _shortcut_base("transactions", "list_transactions", start, args={"category": category, "limit": 25})
            route["tool_name"] = "get_transactions"
            route["shortcut"] = "category_transactions"
            return route
        return None
    args = {"merchant": merchant, "limit": 25}
    route = _shortcut_base("transactions", "list_transactions", start, args=args)
    route["tool_name"] = "get_transactions_for_merchant"
    route["shortcut"] = "merchant_transactions"
    return route

def _category_spend_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if token_set & {"transaction", "transactions", "purchase", "purchases", "charges", "charge"}:
        return None
    if token_set & {"chart", "plot", "graph", "trend", "visualize"}:
        return None
    if token_set & _PLAN_TERMS:
        return None
    if token_set & {"why", "high", "higher", "spike", "spiked", "left", "remaining", "budget"}:
        return None
    category = _exact_category_for_text(question, profile)
    if not category:
        return None
    exact_merchant = exact_merchant_for_text(question, _merchant_names(profile))
    if exact_merchant and category.lower() not in _BROAD_CATEGORY_NAMES and "category" not in token_set:
        return None
    spend_terms = {"spend", "spent", "spending", "paid", "pay", "expenses", "expense", "total", "much"}
    display_terms = {"show", "list", "display", "pull"}
    parsed_range = parse_range(question)
    if not (token_set & spend_terms or token_set & display_terms or contains(tokens, ("how", "much")) or parsed_range.explicit):
        return None
    route = _shortcut_base(
        "spending",
        "category_total",
        start,
        args={"category": category, "range": parsed_range.token},
    )
    route["tool_name"] = "get_category_spend"
    route["shortcut"] = "category_spend"
    return route

def _merchant_spend_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if token_set & {"transaction", "transactions", "purchase", "purchases", "charges", "charge"}:
        return None
    if token_set & {"chart", "plot", "graph", "trend", "visualize"}:
        return None
    if token_set & _PLAN_TERMS:
        return None
    if token_set & {"why", "high", "higher", "spike", "spiked", "left", "remaining", "budget"}:
        return None
    merchant = exact_merchant_for_text(question, _merchant_names(profile))
    if not merchant:
        return None
    category = _exact_category_for_text(question, profile)
    if category and category.lower() in _BROAD_CATEGORY_NAMES:
        return None
    spend_terms = {"spend", "spent", "spending", "paid", "pay", "expenses", "expense", "total", "much", "wasted"}
    display_terms = {"show", "list", "display", "pull"}
    parsed_range = parse_range(question)
    if not (token_set & spend_terms or token_set & display_terms or contains(tokens, ("how", "much")) or parsed_range.explicit):
        return None
    route = _shortcut_base(
        "spending",
        "merchant_total",
        start,
        args={"merchant": merchant, "range": parsed_range.token},
    )
    route["tool_name"] = "get_merchant_spend"
    route["shortcut"] = "merchant_spend"
    return route

def _chart_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    spending_terms = {"spending", "spend", "spent", "expenses", "expense", "charges", "charge"}
    display_terms = {"show", "list", "display", "pull"}
    parsed_range = parse_range(question)
    has_chart_request = bool(token_set & _CHART_TOKENS) or bool(token_set & display_terms and token_set & spending_terms and parsed_range.chart_months)
    if not has_chart_request:
        return None
    category = _exact_category_for_text(question, profile)
    if ({"net", "worth"} <= token_set or "networth" in token_set) and not category and not contains(tokens, ("not", "net", "worth")):
        route = _shortcut_base(
            "chart",
            "net_worth_chart",
            start,
            args={"interval": "monthly", "limit": 24},
        )
        route["tool_name"] = "get_net_worth_trend"
        route["shortcut"] = "net_worth_chart"
        return route
    if not (token_set & spending_terms or category):
        return None
    args = {"months": chart_months(question, fallback=parsed_range.chart_months or 6)}
    if category:
        args["category"] = category
    route = _shortcut_base("chart", "monthly_spending_chart", start, args=args)
    route["tool_name"] = "get_monthly_spending_trend"
    route["shortcut"] = "monthly_spending_chart"
    return route

def _write_recategorize_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if not (token_set & {"move", "change", "recategorize", "recategorise", "categorize", "categorise"}):
        return None
    if "to" not in tokens:
        return None
    to_idx = tokens.index("to")
    if to_idx <= 0 or to_idx >= len(tokens) - 1:
        return None
    source_text = " ".join(tokens[:to_idx])
    target_text = " ".join(tokens[to_idx + 1:])
    category = _exact_category_for_text(target_text, profile)
    if not category:
        category_result = ground_category(target_text, _candidate_payload(target_text, profile).get("categories") or [], limit=3)
        if category_result.kind == "exact" and category_result.value:
            category = category_result.value
    if not category:
        return None
    merchant_result = ground_merchant(source_text, _merchant_names(profile), profile=profile, include_transaction_evidence=True, limit=3)
    if merchant_result.kind != "exact" or not merchant_result.value:
        return None
    route = _shortcut_base("write", "bulk_recategorize", start, args={"merchant": merchant_result.value, "category": category})
    route["tool_name"] = "preview_bulk_recategorize"
    route["shortcut"] = "write_bulk_recategorize"
    return route

def _write_rename_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    if "rename" not in set(words(question)):
        return None
    match = re.search(r"\brename\s+(.+?)\s+(?:to|as)\s+(.+)$", question or "", re.I)
    if not match:
        return None
    old_text = match.group(1).strip(" .,:;\"'")
    new_name = match.group(2).strip(" .,:;\"'")
    if not old_text or not new_name:
        return None
    merchant_result = ground_merchant(old_text, _merchant_names(profile), profile=profile, include_transaction_evidence=True, limit=3)
    if merchant_result.kind != "exact" or not merchant_result.value:
        return None
    route = _shortcut_base("write", "rename_merchant", start, args={"old_name": merchant_result.value, "new_name": new_name})
    route["tool_name"] = "preview_rename_merchant"
    route["shortcut"] = "write_rename_merchant"
    return route

def _write_rule_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    wants_rule = (
        ("always" in token_set and ("categorize" in token_set or "categorise" in token_set))
        or contains(tokens, ("auto", "categorize"))
        or contains(tokens, ("create", "rule"))
        or contains(tokens, ("future", "transactions"))
    )
    if not wants_rule:
        return None
    candidates = _candidate_payload(question, profile)
    category = _best_name_match(token_set, candidates.get("categories") or [])
    merchant = _best_name_match(token_set, candidates.get("merchants") or [])
    if not category:
        for token in tokens:
            synonym = _CATEGORY_SYNONYMS.get(token)
            if synonym:
                category = synonym
                break
    if not merchant or not category:
        return None
    route = _shortcut_base("write", "create_rule", start, args={"pattern": merchant, "category": category})
    route["tool_name"] = "preview_create_rule"
    route["shortcut"] = "write_rule"
    return route

def _first_number(question: str) -> float | None:
    current: list[str] = []
    for char in question or "":
        if char.isdigit() or (char == "." and current and "." not in current):
            current.append(char)
            continue
        if current and any(part.isdigit() for part in current):
            try:
                return float("".join(current))
            except ValueError:
                return None
        current = []
    if current and any(part.isdigit() for part in current):
        try:
            return float("".join(current))
        except ValueError:
            return None
    return None

def _write_budget_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if "budget" not in token_set and "cap" not in token_set:
        return None
    if not (token_set & {"set", "update", "change", "make", "limit"}):
        return None
    amount = _first_number(question)
    if amount is None:
        return None
    candidates = _candidate_payload(question, profile)
    category = _best_name_match(token_set, candidates.get("categories") or [])
    if not category:
        for token in tokens:
            category = _CATEGORY_SYNONYMS.get(token)
            if category:
                break
    if not category:
        return None
    route = _shortcut_base("write", "set_budget", start, args={"category": category, "amount": amount})
    route["tool_name"] = "preview_set_budget"
    route["shortcut"] = "write_budget"
    return route

def _postprocess_route(route: dict, question: str, profile: str | None) -> dict:
    if route.get("intent") != "spending" or route.get("tool_name") != "get_category_spend":
        return route
    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    category = (args.get("category") or "").strip()
    if not category or category.lower() in _BROAD_CATEGORY_NAMES:
        return route
    candidates = _candidate_payload(question, profile)
    merchant = _best_name_match(set(words(question)), candidates.get("merchants") or [])
    if merchant and merchant.lower() == category.lower():
        next_args = dict(args)
        next_args.pop("category", None)
        next_args["merchant"] = merchant
        route = {**route, "operation": "merchant_total", "tool_name": "get_merchant_spend", "args": next_args}
    return route

def _clarification_from_route(route: dict, question: str, *, shortcut: str = "grounding_clarification", args: dict | None = None) -> dict:
    next_route = dict(route)
    next_route.update({
        "intent": "chat",
        "operation": shortcut,
        "tool_name": None,
        "args": args or {},
        "needs_clarification": True,
        "clarification_question": question,
        "shortcut": shortcut,
    })
    return next_route

def _ground_untrusted_subject_route(route: dict, question: str, profile: str | None) -> dict:
    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    tool_name = route.get("tool_name")
    intent = route.get("intent")
    merchant = ""
    if tool_name in {"get_merchant_spend", "get_transactions_for_merchant"}:
        merchant = str(args.get("merchant") or "").strip()
    elif intent == "plan" and args.get("subject_type") == "merchant":
        merchant = str(args.get("subject") or "").strip()

    if merchant:
        merchants = _merchant_names(profile)
        exact_from_question = exact_merchant_for_text(question, merchants)
        if exact_from_question or route.get("uses_history") or _looks_like_followup(question):
            exact = exact_from_question or exact_merchant_for_text(merchant, merchants)
            next_args = dict(args)
            if tool_name in {"get_merchant_spend", "get_transactions_for_merchant"}:
                next_args["merchant"] = exact
            elif intent == "plan":
                next_args["subject"] = exact
            return {**route, "args": next_args}
        resolution = resolve_merchant_with_llm(question, merchants, profile=profile, include_transaction_evidence=True)
        if not resolution.name and merchant in merchants:
            resolution = resolve_merchant_with_llm(f"{question} {merchant}", merchants, profile=profile, include_transaction_evidence=True)
        if resolution.name and resolution.confidence >= 0.55:
            next_route = _clarification_from_route(
                route,
                _merchant_confirmation_question(resolution.name, matched_text=resolution.matched_text or merchant, profile=profile),
                shortcut="confirm_grounded_merchant",
                args={"candidates": list(resolution.candidates[:5]), "matched_text": resolution.matched_text or merchant},
            )
            return _with_dialogue_state(
                next_route,
                _merchant_dialogue_state(question, [resolution.name], matched_text=resolution.matched_text or merchant),
            )
        next_route = _clarification_from_route(
            route,
            f"I couldn't confidently match `{merchant}` to a merchant in your data. Which merchant should I use?",
            shortcut="missing_grounded_merchant",
            args={"merchant": merchant},
        )
        return _with_dialogue_state(next_route, _merchant_dialogue_state(question, [], matched_text=merchant))

    category = ""
    if tool_name == "get_category_spend":
        category = str(args.get("category") or "").strip()
    elif intent == "plan" and args.get("subject_type") == "category":
        category = str(args.get("subject") or "").strip()
    if category:
        if route.get("uses_history") or _looks_like_followup(question):
            return route
        categories = _candidate_payload(question, profile).get("categories") or []
        category_lookup = {str(item).lower(): item for item in categories}
        synonym = _CATEGORY_SYNONYMS.get(category.lower())
        exact_category = category_lookup.get(category.lower()) or (synonym if synonym else None)
        if exact_category:
            next_args = dict(args)
            if tool_name == "get_category_spend":
                next_args["category"] = exact_category
            elif intent == "plan":
                next_args["subject"] = exact_category
            return {**route, "args": next_args}
        return _clarification_from_route(
            route,
            f"I couldn't confidently match `{category}` to a category in your data. Which category should I use?",
            shortcut="missing_grounded_category",
            args={"category": category},
        )

    return route

def _unresolved_finance_comparison(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if not (token_set & {"compare", "versus", "vs", "average", "track", "pace"}):
        return None
    if not (token_set & {"spending", "spend", "spent", "charges", "expenses"}):
        return None
    if alias_targets_for_text(question):
        return None
    candidates = _candidate_payload(question, profile)
    subject_type, subject = _resolve_plan_subject(question, candidates)
    if subject_type and subject:
        return None
    route = _shortcut_base("chat", "needs_grounded_subject", start)
    route["needs_clarification"] = True
    route["clarification_question"] = "Which merchant or category should I compare?"
    route["shortcut"] = "missing_comparison_subject"
    return route

def _missing_spend_subject_route(question: str, profile: str | None, start: float) -> dict | None:
    if not _looks_like_finance_read(question):
        return None
    tokens = words(question)
    token_set = set(tokens)
    if token_set & {"transaction", "transactions", "purchase", "purchases"}:
        return None
    if token_set & {"chart", "plot", "graph", "trend", "visualize"}:
        return None
    if _resolve_plan_subject(question, _candidate_payload(question, profile))[0]:
        return None
    clue = subject_clue_for_missing_finance_subject(question)
    if not clue:
        return None
    route = _shortcut_base("chat", "missing_grounded_subject", start)
    route["needs_clarification"] = True
    route["clarification_question"] = (
        f"I couldn't confidently match `{clue}` to a merchant or category in your data. "
        "Which merchant or category should I use?"
    )
    route["shortcut"] = "missing_grounded_subject"
    route["args"] = {"matched_text": clue}
    return _with_dialogue_state(route, _merchant_dialogue_state(question, [], matched_text=clue))

def _plan_kind(question: str) -> str | None:
    tokens = words(question)
    token_set = set(tokens)
    if "track" in token_set or "pace" in token_set or "projected" in token_set or "projection" in token_set:
        return "on_track"
    if "average" in token_set or "avg" in token_set or "usual" in token_set or "normal" in token_set:
        return "current_vs_average"
    if "versus" in token_set or "vs" in token_set or "compare" in token_set or "compared" in token_set:
        return "current_vs_previous"
    return None

def _average_months(tokens: list[str]) -> int | None:
    for idx, token in enumerate(tokens[:-1]):
        if not token.isdigit():
            continue
        unit = tokens[idx + 1]
        if unit not in {"month", "months"}:
            continue
        try:
            months = int(token)
        except ValueError:
            continue
        tail = set(tokens[idx + 2:idx + 5])
        if tail & {"average", "avg", "usual", "normal"}:
            return max(1, min(months, 12))
    return None

def _planner_shortcut(question: str, profile: str | None, start: float) -> dict | None:
    tokens = words(question)
    token_set = set(tokens)
    if not (token_set & _PLAN_TERMS):
        return None

    candidates = _candidate_payload(question, profile)
    subject_type, subject = _resolve_plan_subject(question, candidates)
    if not subject_type or not subject:
        return None

    kind = _plan_kind(question)
    if not kind:
        return None
    months = _average_months(tokens) or (parse_range(question).chart_months if kind not in {"current_vs_average", "on_track"} else None) or 6
    args = {
        "plan_kind": kind,
        "subject_type": subject_type,
        "subject": subject,
        "months": max(1, min(int(months), 12)),
    }
    return _shortcut_base("plan", kind, start, args=args)

def _looks_like_followup(question: str) -> bool:
    tokens = words(question)
    token_set = set(tokens)
    return bool(token_set & {"that", "those", "same", "it", "them"}) or (
        contains(tokens, ("what", "about"))
        or contains(tokens, ("how", "about"))
        or contains(tokens, ("and", "for"))
        or contains(tokens, ("now", "for"))
    )
