from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Callable

from mira.agentic.schemas import EvidencePacket


AnswerCompleter = Callable[[str, int, str], str]


@dataclass(frozen=True)
class AnswerResult:
    answer: str
    raw: str
    prompt: str
    llm_calls: int
    used_fallback: bool = False
    error: str = ""


def synthesize_answer(
    evidence: EvidencePacket,
    *,
    completer: AnswerCompleter | None = None,
    max_tokens: int = 700,
) -> str:
    return synthesize_answer_result(evidence, completer=completer, max_tokens=max_tokens).answer


def synthesize_answer_result(
    evidence: EvidencePacket,
    *,
    completer: AnswerCompleter | None = None,
    max_tokens: int = 700,
) -> AnswerResult:
    return AnswerResult(
        answer=deterministic_answer(evidence),
        raw="",
        prompt="",
        llm_calls=0,
        used_fallback=True,
        error="legacy LLM answerer retired; vNext uses mira.agentic.vnext_answerer",
    )


def deterministic_answer(evidence: EvidencePacket) -> str:
    if evidence.caveats and not evidence.facts and not evidence.rows:
        return "I do not have usable tool evidence for that yet. " + " ".join(evidence.caveats[:2])
    lines: list[str] = []
    for fact in evidence.facts[:4]:
        summary = str(fact.get("summary") or "").strip()
        if summary:
            lines.append(summary)
            continue
        compact = _compact_fact(fact)
        if compact:
            lines.append(compact)
    if not lines and evidence.rows:
        lines.append(f"Found {len(evidence.rows)} row(s) in the evidence.")
    if not lines and evidence.charts:
        lines.append("I prepared a chart from the tool evidence.")
    if evidence.caveats:
        lines.append("Caveat: " + " ".join(evidence.caveats[:2]))
    return " ".join(lines) if lines else "I do not have tool evidence to answer that yet."


def _compact_fact(fact: dict) -> str:
    tool = str(fact.get("tool") or "tool")
    parts = []
    for key, value in fact.items():
        if key in {"step_id", "tool", "execution_tool"} or value in (None, "", []):
            continue
        if isinstance(value, (str, int, float, bool)):
            parts.append(f"{key}={value}")
    return f"{tool}: " + ", ".join(parts[:8]) if parts else ""


def _contains_unsupported_numbers(answer: str, evidence: EvidencePacket) -> bool:
    answer_numbers = _numbers(answer)
    if not answer_numbers:
        return False
    evidence_numbers = _numbers(json.dumps(evidence.to_dict(), ensure_ascii=True, default=str))
    return any(number not in evidence_numbers for number in answer_numbers)


def _unsupported_entity_terms(answer: str, evidence: EvidencePacket) -> list[str]:
    allowed = _allowed_terms(evidence)
    unsupported: list[str] = []
    tokens = _answer_word_tokens(answer)
    for index, token in enumerate(tokens):
        lower = token.lower()
        previous = tokens[index - 1].lower() if index else ""
        next_token = tokens[index + 1].lower() if index + 1 < len(tokens) else ""
        nearby = [item.lower() for item in tokens[max(0, index - 3): index + 4] if item != token]
        if lower in allowed or lower in _COMMON_ALLOWED_TERMS:
            continue
        if _looks_like_entity_term(token, previous, next_token, nearby):
            unsupported.append(token)
    return unsupported


def _allowed_terms(evidence: EvidencePacket) -> set[str]:
    raw = json.dumps(evidence.to_dict(), ensure_ascii=True, default=str)
    terms = {term.lower() for term in _plain_terms(raw)}
    for tool_name in (
        "get_spending_summary",
        "analyze_subject",
        "find_transactions",
        "compare_periods",
        "get_income_summary",
        "get_budget_status",
        "get_cashflow_forecast",
        "get_savings_capacity",
        "get_recurring_review",
        "get_net_worth_trend",
        "plot_chart",
        "get_data_health_summary",
        "preview_finance_change",
        "remember_user_context",
        "retrieve_relevant_memories",
    ):
        terms.update(part for part in tool_name.split("_") if part)
        terms.add(tool_name)
    return terms


def _plain_terms(text: str) -> list[str]:
    return re.findall(r"[A-Za-z][A-Za-z0-9&'-]*", text or "")


def _answer_word_tokens(answer: str) -> list[str]:
    return _plain_terms(answer)


def _looks_like_entity_term(token: str, previous: str, next_token: str = "", nearby: list[str] | None = None) -> bool:
    if previous in _ENTITY_CUE_WORDS and len(token) > 2:
        return True
    if next_token in _FINANCE_ENTITY_CONTEXT_WORDS and len(token) > 2:
        return True
    if nearby and len(token) > 2 and any(item in _FINANCE_ENTITY_CONTEXT_WORDS for item in nearby):
        return True
    if len(token) > 1 and token.isupper():
        return True
    return bool(token[:1].isupper() and len(token) > 2)


def _numbers(text: str) -> set[str]:
    values: set[str] = set()
    for match in re.findall(r"(?<![A-Za-z])[-+]?\$?\d[\d,]*(?:\.\d+)?%?", text or ""):
        clean = match.replace("$", "").replace(",", "").replace("%", "")
        try:
            number = float(clean)
        except ValueError:
            continue
        values.add(f"{number:.4f}".rstrip("0").rstrip("."))
    return values


_ENTITY_CUE_WORDS = {
    "account",
    "accounts",
    "at",
    "category",
    "from",
    "merchant",
    "merchants",
    "on",
    "to",
    "transaction",
    "transactions",
    "vendor",
    "with",
}

_FINANCE_ENTITY_CONTEXT_WORDS = {
    "account",
    "accounts",
    "balances",
    "budget",
    "budgets",
    "card",
    "cards",
    "category",
    "charge",
    "charges",
    "expense",
    "expenses",
    "income",
    "merchant",
    "merchants",
    "payment",
    "payments",
    "spend",
    "spending",
    "spent",
    "subscription",
    "subscriptions",
    "transaction",
    "transactions",
    "vendor",
    "vendors",
}

_COMMON_ALLOWED_TERMS = {
    "a",
    "about",
    "above",
    "across",
    "after",
    "again",
    "against",
    "all",
    "also",
    "am",
    "an",
    "and",
    "answer",
    "are",
    "around",
    "as",
    "at",
    "available",
    "based",
    "be",
    "because",
    "before",
    "between",
    "but",
    "by",
    "can",
    "caveat",
    "caveats",
    "change",
    "chart",
    "checked",
    "compared",
    "could",
    "data",
    "did",
    "do",
    "does",
    "down",
    "each",
    "evidence",
    "few",
    "folio",
    "for",
    "from",
    "found",
    "had",
    "has",
    "have",
    "here",
    "i",
    "in",
    "is",
    "it",
    "its",
    "limited",
    "looks",
    "may",
    "mira",
    "month",
    "more",
    "need",
    "no",
    "not",
    "of",
    "off",
    "ok",
    "okay",
    "on",
    "only",
    "or",
    "out",
    "over",
    "period",
    "ready",
    "record",
    "records",
    "row",
    "rows",
    "same",
    "scope",
    "see",
    "seems",
    "should",
    "so",
    "spend",
    "spending",
    "spent",
    "steady",
    "still",
    "summary",
    "that",
    "the",
    "there",
    "this",
    "those",
    "through",
    "to",
    "tool",
    "tools",
    "total",
    "transaction",
    "transactions",
    "up",
    "use",
    "used",
    "using",
    "usd",
    "was",
    "were",
    "what",
    "when",
    "with",
    "within",
    "you",
    "your",
}


def _default_completer(prompt: str, max_tokens: int, purpose: str) -> str:
    import llm_client

    return llm_client.complete(prompt, max_tokens=max_tokens, purpose=purpose)
