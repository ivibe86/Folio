from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Callable, Iterable

from mira.agentic.direct_renderer import try_direct_scalar_answer
from mira.agentic.schemas import EvidencePacket, ValidationResult

try:
    from mira.agentic.answerer import (
        deterministic_answer,
        _contains_unsupported_numbers,
        _unsupported_entity_terms,
    )
except ModuleNotFoundError:
    def deterministic_answer(evidence: EvidencePacket) -> str:
        if evidence.facts:
            summary = str(evidence.facts[0].get("summary") or "").strip()
            if summary:
                return summary
        if evidence.rows:
            return f"I found {len(evidence.rows)} matching row(s)."
        if evidence.tool_results:
            return "I collected Folio evidence for that."
        return "I do not have enough Folio evidence to answer that cleanly."

    def _contains_unsupported_numbers(answer: str, evidence: EvidencePacket) -> bool:
        _ = answer, evidence
        return False

    def _unsupported_entity_terms(answer: str, evidence: EvidencePacket) -> list[str]:
        _ = answer, evidence
        return []


AnswerCompleter = Callable[[str, int, str], str]
StreamAnswerCompleter = Callable[[str, int, str], Iterable[str]]

VNEXT_EVIDENCE_MAX_TOKENS = int(os.getenv("MIRA_VNEXT_EVIDENCE_MAX_TOKENS", "900"))
VNEXT_GENERAL_MAX_TOKENS = int(os.getenv("MIRA_VNEXT_GENERAL_MAX_TOKENS", "1800"))


@dataclass(frozen=True)
class VNextAnswerResult:
    answer: str
    path: str
    raw: str = ""
    prompt: str = ""
    llm_calls: int = 0
    used_fallback: bool = False
    error: str = ""
    max_tokens: int = 0


def answer_vnext(
    *,
    question: str,
    route: dict,
    validation: ValidationResult,
    evidence: EvidencePacket,
    history: list[dict] | None = None,
    completer: AnswerCompleter | None = None,
    max_tokens: int | None = None,
) -> VNextAnswerResult:
    if validation.status == "clarify":
        return VNextAnswerResult(
            answer=validation.clarification_question or "I need one more detail to answer that cleanly.",
            path="clarify",
        )
    if validation.status == "blocked":
        return VNextAnswerResult(
            answer=validation.blocked_reason or "I could not safely run that request.",
            path="blocked",
            error=validation.blocked_reason,
        )

    operation = str(route.get("operation") or "")
    if operation == "general_answer":
        return answer_general_question(question=question, history=history, completer=completer, max_tokens=max_tokens)

    direct = try_direct_scalar_answer(question, evidence)
    if direct:
        return VNextAnswerResult(answer=direct, path="direct_scalar")

    return answer_from_evidence(question=question, evidence=evidence, completer=completer, max_tokens=max_tokens)


def answer_from_evidence(
    *,
    question: str,
    evidence: EvidencePacket,
    completer: AnswerCompleter | None = None,
    max_tokens: int | None = None,
) -> VNextAnswerResult:
    resolved_max_tokens = _resolve_answer_max_tokens("evidence_llm", max_tokens)
    prompt = build_evidence_answer_prompt(question=question, evidence=evidence)
    complete = completer or _default_completer
    raw = ""
    try:
        raw = complete(prompt, resolved_max_tokens, "copilot")
        return _evidence_result_from_raw(
            question=question,
            evidence=evidence,
            raw=raw,
            prompt=prompt,
            max_tokens=resolved_max_tokens,
        )
    except Exception as exc:
        return VNextAnswerResult(
            answer=deterministic_answer(evidence),
            path="evidence_llm",
            raw=raw,
            prompt=prompt,
            llm_calls=1,
            used_fallback=True,
            error=str(exc),
            max_tokens=resolved_max_tokens,
        )


def answer_general_question(
    *,
    question: str,
    history: list[dict] | None = None,
    completer: AnswerCompleter | None = None,
    max_tokens: int | None = None,
) -> VNextAnswerResult:
    if is_explain_last_answer_question(question):
        return VNextAnswerResult(
            answer=explain_last_answer_from_history(history),
            path="explain_last_answer",
        )

    resolved_max_tokens = _resolve_answer_max_tokens("general_answer", max_tokens)
    prompt = build_general_answer_prompt(question, history=history)
    complete = completer or _default_completer
    raw = ""
    try:
        raw = complete(prompt, resolved_max_tokens, "copilot")
        return _general_result_from_raw(raw=raw, prompt=prompt, max_tokens=resolved_max_tokens)
    except Exception as exc:
        return VNextAnswerResult(
            answer="I can help with general questions and with Folio finance tasks, but I could not generate that answer locally just now.",
            path="general_answer",
            raw=raw,
            prompt=prompt,
            llm_calls=1,
            used_fallback=True,
            error=str(exc),
            max_tokens=resolved_max_tokens,
        )


def iter_answer_vnext_events(
    *,
    question: str,
    route: dict,
    validation: ValidationResult,
    evidence: EvidencePacket,
    history: list[dict] | None = None,
    stream_completer: StreamAnswerCompleter | None = None,
    max_tokens: int | None = None,
):
    """Yield token events and finish with an internal answer_result event."""
    if validation.status == "clarify":
        yield {
            "type": "_answer_result",
            "answer_result": VNextAnswerResult(
                answer=validation.clarification_question or "I need one more detail to answer that cleanly.",
                path="clarify",
            ),
        }
        return
    if validation.status == "blocked":
        yield {
            "type": "_answer_result",
            "answer_result": VNextAnswerResult(
                answer=validation.blocked_reason or "I could not safely run that request.",
                path="blocked",
                error=validation.blocked_reason,
            ),
        }
        return

    operation = str(route.get("operation") or "")
    if operation == "general_answer":
        if is_explain_last_answer_question(question):
            yield {
                "type": "_answer_result",
                "answer_result": VNextAnswerResult(
                    answer=explain_last_answer_from_history(history),
                    path="explain_last_answer",
                ),
            }
            return

        resolved_max_tokens = _resolve_answer_max_tokens("general_answer", max_tokens)
        prompt = build_general_answer_prompt(question, history=history)
        yield from _iter_streamed_answer_result(
            prompt=prompt,
            finalize=lambda raw: _general_result_from_raw(raw=raw, prompt=prompt, max_tokens=resolved_max_tokens),
            fallback=lambda raw, exc: VNextAnswerResult(
                answer="I can help with general questions and with Folio finance tasks, but I could not generate that answer locally just now.",
                path="general_answer",
                raw=raw,
                prompt=prompt,
                llm_calls=1,
                used_fallback=True,
                error=str(exc),
                max_tokens=resolved_max_tokens,
            ),
            stream_completer=stream_completer,
            max_tokens=resolved_max_tokens,
        )
        return

    direct = try_direct_scalar_answer(question, evidence)
    if direct:
        yield {"type": "_answer_result", "answer_result": VNextAnswerResult(answer=direct, path="direct_scalar")}
        return

    resolved_max_tokens = _resolve_answer_max_tokens("evidence_llm", max_tokens)
    prompt = build_evidence_answer_prompt(question=question, evidence=evidence)
    yield from _iter_streamed_answer_result(
        prompt=prompt,
        finalize=lambda raw: _evidence_result_from_raw(
            question=question,
            evidence=evidence,
            raw=raw,
            prompt=prompt,
            max_tokens=resolved_max_tokens,
        ),
        fallback=lambda raw, exc: VNextAnswerResult(
            answer=deterministic_answer(evidence),
            path="evidence_llm",
            raw=raw,
            prompt=prompt,
            llm_calls=1,
            used_fallback=True,
            error=str(exc),
            max_tokens=resolved_max_tokens,
        ),
        stream_completer=stream_completer,
        max_tokens=resolved_max_tokens,
    )


def _iter_streamed_answer_result(
    *,
    prompt: str,
    finalize: Callable[[str], VNextAnswerResult],
    fallback: Callable[[str, Exception], VNextAnswerResult],
    stream_completer: StreamAnswerCompleter | None,
    max_tokens: int,
):
    stream = stream_completer or _default_stream_completer
    raw_parts: list[str] = []
    emitted = False
    try:
        for chunk in stream(prompt, max_tokens, "copilot"):
            text = str(chunk or "")
            if not text:
                continue
            raw_parts.append(text)
            emitted = True
            yield {"type": "token", "text": text}
        raw = "".join(raw_parts)
        result = finalize(raw)
    except Exception as exc:
        raw = "".join(raw_parts)
        result = fallback(raw, exc)

    displayed_answer = raw.strip()
    if emitted and result.answer and result.answer.strip() != displayed_answer:
        yield {"type": "reset_text"}
        yield {"type": "token", "text": result.answer}
    yield {"type": "_answer_result", "answer_result": result}


def _evidence_result_from_raw(
    *,
    question: str,
    evidence: EvidencePacket,
    raw: str,
    prompt: str,
    max_tokens: int,
) -> VNextAnswerResult:
    answer = str(raw or "").strip()
    if not answer:
        raise ValueError("empty answer")
    if _contains_unsupported_numbers(answer, evidence):
        return VNextAnswerResult(
            answer=deterministic_answer(evidence),
            path="evidence_llm",
            raw=raw,
            prompt=prompt,
            llm_calls=1,
            used_fallback=True,
            error="answer introduced numbers not present in evidence",
            max_tokens=max_tokens,
        )
    unsupported_terms = _unsupported_vnext_entity_terms(answer, evidence)
    if unsupported_terms:
        return VNextAnswerResult(
            answer=deterministic_answer(evidence),
            path="evidence_llm",
            raw=raw,
            prompt=prompt,
            llm_calls=1,
            used_fallback=True,
            error="answer introduced terms not present in evidence: " + ", ".join(unsupported_terms[:4]),
            max_tokens=max_tokens,
        )
    answer = ensure_why_disclaimer(question, answer)
    return VNextAnswerResult(answer=answer, path="evidence_llm", raw=raw, prompt=prompt, llm_calls=1, max_tokens=max_tokens)


def _general_result_from_raw(*, raw: str, prompt: str, max_tokens: int) -> VNextAnswerResult:
    answer = str(raw or "").strip()
    if not answer:
        raise ValueError("empty answer")
    return VNextAnswerResult(answer=answer, path="general_answer", raw=raw, prompt=prompt, llm_calls=1, max_tokens=max_tokens)


def build_evidence_answer_prompt(*, question: str, evidence: EvidencePacket) -> str:
    return (
        build_answer_system_prompt()
        + "\n\nUser question:\n"
        + str(question or "")
        + "\n\nEvidence JSON:\n"
        + json.dumps(evidence.to_dict(), ensure_ascii=True, separators=(",", ":"), default=str)
    )


def build_answer_system_prompt() -> str:
    return """You are Mira, the user's warm, sharp AI companion inside Folio.
Answer warmly and concisely using only the evidence JSON.
Do not introduce any amount, count, date, merchant, category, account, or transaction absent from evidence.
Do not infer the user's intent or motive from transaction data. For "why" questions, say you cannot know why from the data alone, then summarize what the evidence suggests.
Distinguish direct merchant matches from description/memo matches. Do not describe transfers or reimbursements as direct merchant spend unless merchant_name or merchant_key supports that.
Use categories, memos, transaction type, and descriptions as clues, not proof of intent.
If evidence is empty, errored, or caveated, say that plainly."""


def build_general_answer_prompt(question: str, *, history: list[dict] | None = None) -> str:
    context = build_recent_conversation_context(history)
    context_block = (
        "\n\nRecent conversation for follow-up context:\n"
        + context
        + "\nUse this only when the current question depends on it. Preserve prior context only for omitted fields; the latest user message overrides prior context for subject, date/range, item, tone, format, comparison target, or constraint. If the user corrects you, treat the correction as the source of truth."
        if context
        else ""
    )
    return (
        build_general_answer_system_prompt()
        + context_block
        + "\n\nCurrent user question:\n"
        + str(question or "")
    )


def build_general_answer_system_prompt() -> str:
    return """You are Mira, the user's warm, sharp, broadly capable AI companion inside Folio.
Answer normally and conversationally. You can help with general questions, thinking, writing, planning, technology, science, and everyday decisions.
Prefer a complete, focused answer with clear structure when useful. Do not trail off; finish the thought cleanly.
You also have Folio finance tools for spending, transactions, budgets, cash flow, net worth, recurring charges, data health, charts, memories, and safe previews of finance changes.
No live tool evidence is attached to this answer. Do not invent the user's personal finance facts, amounts, balances, transactions, budgets, or forecasts."""


def is_explain_last_answer_question(question: str) -> bool:
    text = " ".join(str(question or "").lower().split())
    if not text:
        return False
    return any(
        phrase in text
        for phrase in (
            "explain last answer",
            "explain your last answer",
            "how did you answer",
            "how did you get",
            "how did you calculate",
            "how did you figure",
            "how i answered",
            "what tools did you use",
            "which tools did you use",
            "where did that come from",
            "why did you say that",
            "show provenance",
        )
    )


def explain_last_answer_from_history(history: list[dict] | None) -> str:
    turn = _last_assistant_turn(history)
    if not turn:
        return "I do not have a previous answer in this chat to explain yet."

    tool_context = turn.get("tool_context") if isinstance(turn.get("tool_context"), list) else []
    answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
    trace = turn.get("trace") if isinstance(turn.get("trace"), dict) else {}
    answer_guard = turn.get("answer_guard") if isinstance(turn.get("answer_guard"), dict) else {}

    lines: list[str] = []
    if tool_context:
        lines.append("I answered from Folio tool evidence rather than guessing.")
        lines.append("Tools I used: " + "; ".join(_tool_context_phrase(tool) for tool in tool_context if isinstance(tool, dict)))
    else:
        lines.append("I answered without running a Folio data tool in the prior turn.")

    subject = str(answer_context.get("subject") or "").strip()
    subject_type = str(answer_context.get("subject_type") or "").strip()
    ranges = [str(item).strip() for item in answer_context.get("ranges") or [] if str(item).strip()]
    context_bits = []
    if subject:
        context_bits.append(f"{subject_type or 'subject'}={subject}")
    if ranges:
        context_bits.append("range=" + ", ".join(ranges))
    if context_bits:
        lines.append("Grounded context: " + "; ".join(context_bits) + ".")

    path = str(answer_guard.get("path") or trace.get("answer_path") or "").strip()
    if path:
        lines.append(f"Answer path: {path}.")

    if any(_tool_context_is_write_preview(tool) for tool in tool_context if isinstance(tool, dict)):
        lines.append("Because this involved a possible edit, I only prepared a preview; a separate confirmation id is required before anything changes.")
    elif tool_context:
        lines.append("I did not apply any writes.")

    return "\n".join(lines)


def _last_assistant_turn(history: list[dict] | None) -> dict | None:
    for turn in reversed(history or []):
        if isinstance(turn, dict) and str(turn.get("role") or "").lower() == "assistant":
            return turn
    return None


def _tool_context_phrase(tool: dict) -> str:
    name = str(tool.get("name") or "unknown_tool")
    args = tool.get("args") if isinstance(tool.get("args"), dict) else {}
    interesting = []
    for key in ("view", "range", "range_a", "range_b", "limit", "sort", "entity_type", "entity", "merchant", "category", "subject", "metric", "group_by", "amount", "transaction_id", "change_type"):
        value = args.get(key)
        if value not in (None, "", [], {}):
            interesting.append(f"{key}={value}")
    filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
    for key in ("merchant", "category", "account", "search"):
        value = filters.get(key)
        if value not in (None, "", [], {}):
            interesting.append(f"filters.{key}={value}")
    payload = args.get("payload") if isinstance(args.get("payload"), dict) else {}
    for key in ("metric", "group_by", "amount", "purpose", "change_type", "source_step_id"):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            interesting.append(f"payload.{key}={value}")
    return f"{name}({', '.join(interesting)})" if interesting else name


def _tool_context_is_write_preview(tool: dict) -> bool:
    name = str((tool or {}).get("name") or "")
    if name.startswith("preview_") or name == "preview_finance_change":
        return True
    args = tool.get("args") if isinstance(tool.get("args"), dict) else {}
    return bool(args.get("change_type"))


def _resolve_answer_max_tokens(operation: str, max_tokens: int | None) -> int:
    if max_tokens is not None:
        return max_tokens
    if operation == "general_answer":
        return VNEXT_GENERAL_MAX_TOKENS
    return VNEXT_EVIDENCE_MAX_TOKENS


def build_recent_conversation_context(
    history: list[dict] | None,
    *,
    limit: int = 6,
    max_chars_per_turn: int = 420,
) -> str:
    lines: list[str] = []
    for turn in (history or [])[-limit:]:
        if not isinstance(turn, dict):
            continue
        role = str(turn.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _compact_text(turn.get("content"), max_chars=max_chars_per_turn)
        if not content:
            continue
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _compact_text(value: object, *, max_chars: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0].rstrip() + "..."


def _default_completer(prompt: str, max_tokens: int, purpose: str) -> str:
    import llm_client

    return llm_client.complete(prompt, max_tokens=max_tokens, purpose=purpose)


def _default_stream_completer(prompt: str, max_tokens: int, purpose: str):
    import llm_client

    yield from llm_client.complete_stream(prompt, max_tokens=max_tokens, purpose=purpose)


def ensure_why_disclaimer(question: str, answer: str) -> str:
    text = str(question or "").lower()
    if "why" not in text:
        return answer
    lowered = str(answer or "").lower()
    if any(phrase in lowered for phrase in ("cannot know why", "can't know why", "data alone", "cannot prove intent", "can't prove intent")):
        return answer
    return "I can't know why from the data alone. " + str(answer or "").lstrip()


def _unsupported_vnext_entity_terms(answer: str, evidence: EvidencePacket) -> list[str]:
    # The shared guard is intentionally conservative and can flag ordinary
    # lowercase verbs near finance words. For vNext, keep it focused on likely
    # invented entities while the numeric guard handles invented amounts.
    return [
        term
        for term in _unsupported_entity_terms(answer, evidence)
        if term[:1].isupper() or term.isupper()
    ]


__all__ = [
    "VNEXT_EVIDENCE_MAX_TOKENS",
    "VNEXT_GENERAL_MAX_TOKENS",
    "VNextAnswerResult",
    "answer_from_evidence",
    "answer_general_question",
    "answer_vnext",
    "build_answer_system_prompt",
    "build_evidence_answer_prompt",
    "build_general_answer_prompt",
    "build_general_answer_system_prompt",
    "build_recent_conversation_context",
    "explain_last_answer_from_history",
    "ensure_why_disclaimer",
    "is_explain_last_answer_question",
    "iter_answer_vnext_events",
]
