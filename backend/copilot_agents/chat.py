from __future__ import annotations

import logging

import llm_client
from mira import memory_v2
from mira import persona

from .base import emit_done_with_memory

logger = logging.getLogger(__name__)


def _memory_context(profile: str | None, question: str, route: dict | None) -> tuple[str, dict | None, dict | None]:
    from database import get_db

    try:
        with get_db() as conn:
            result = memory_v2.retrieve_relevant_memories(
                conn=conn,
                profile=profile,
                question=question,
                route=route,
                limit=5,
            )
    except Exception:
        logger.debug("memory v2 retrieval failed for chat specialist", exc_info=True)
        return "", None, None
    trace = result.get("memory_trace") if isinstance(result, dict) else None
    packet = result.get("compact_memory") if isinstance(result, dict) else None
    return (
        memory_v2.context_block_from_packet(packet if isinstance(packet, dict) else None),
        trace if isinstance(trace, dict) else None,
        packet if isinstance(packet, dict) else None,
    )


def system_prompt(profile: str | None, question: str = "", route: dict | None = None) -> tuple[str, dict | None]:
    from datetime import datetime

    memory_v2_block, memory_trace, compact_memory_trace = _memory_context(profile, question, route)

    today = datetime.now().strftime("%Y-%m-%d")
    blocks = [
        persona.persona_prompt_block(),
        (
            "If asked who you are, answer naturally along the lines of: "
            "\"Hey, I'm Mira, your Folio companion. I can help you understand your finances, "
            "think through decisions, draft safe changes for your approval, or just talk through whatever is on your mind.\" "
            "Do not call yourself Copilot, Gemma, Qwen, Phi, or Mistral unless the user specifically asks about the underlying model."
        ),
        (
            "You are running inside Folio, a personal finance app. Be honest about your capabilities: "
            "the app can route finance questions to internal read tools for transactions, categories, merchants, "
            "recurring charges, balances, net worth, charts, and write previews. "
            "Do not claim you have no tools or no finance data access. Do not imply you only know what the user pasted into chat. "
            "These finance capabilities do not limit normal conversation or coding help."
        ),
        (
            "If asked what tools you have, describe these capabilities at a high level rather than exposing raw implementation details. "
            "If asked about schema, you may mention transactions_visible, accounts, categories, merchants, net_worth_history, category_rules, and saved_insights."
        ),
        f"Today is {today}. Active profile: {profile or 'household'}.",
        memory_v2_block,
        (
            "At the very end only, after visible prose, you may emit hidden memory tags:\n"
            '<observation theme="short_kebab_case">one-line note</observation>\n'
            '<memory_proposal section="identity|preferences|goals|concerns|open_questions" confidence="stated" evidence="quote or reason">one-line entry</memory_proposal>\n'
            "Use memory_proposal only for explicit enduring user-stated facts, preferences, commitments, goals, or contradictions. "
            "Do not emit memory tags for one-off off-topic questions, ordinary coding requests, or general knowledge questions."
        ),
    ]
    system = "\n\n".join(block for block in blocks if block)
    if compact_memory_trace and memory_trace is not None:
        memory_trace = {**memory_trace, "_compact_memory_trace": compact_memory_trace}
    return system, memory_trace


def run(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None) -> dict:
    import copilot_agent as core

    system, memory_trace = system_prompt(profile, question, route)
    compact_memory_trace = memory_trace.pop("_compact_memory_trace", None) if isinstance(memory_trace, dict) else None
    response = llm_client.chat_with_tools(
        messages=core._normalize_history(history) + [{"role": "user", "content": question}],
        tools=[],
        system=system,
        max_tokens=1400,
        purpose="copilot",
    )
    result = core._finalize_answer(
        question=question,
        profile=profile,
        raw_answer=(response.get("content") or "").strip(),
        trace=[],
        cache={},
        iterations=0,
        run_detector=True,
        route=route,
        memory_trace=memory_trace,
    )
    result["llm_calls"] = 1
    if memory_trace:
        result["memory_trace"] = memory_trace
        result["answer_context"] = {
            "version": 2,
            "kind": "memory_context",
            "memory_trace": memory_trace,
            "compact_memory_trace": compact_memory_trace,
        }
    return result


def stream(question: str, profile: str | None, history: list[dict] | None = None, route: dict | None = None):
    import copilot_agent as core

    system, memory_trace = system_prompt(profile, question, route)
    compact_memory_trace = memory_trace.pop("_compact_memory_trace", None) if isinstance(memory_trace, dict) else None
    yield {"type": "reset_text"}
    final_parts: list[str] = []
    try:
        for event_type, payload in llm_client.chat_with_tools_stream(
            messages=core._normalize_history(history) + [{"role": "user", "content": question}],
            tools=[],
            system=system,
            max_tokens=1400,
            purpose="copilot",
        ):
            if event_type == "text":
                final_parts.append(payload)
                yield {"type": "token", "text": payload}
    except Exception as e:
        logger.exception("chat specialist stream failed")
        yield {"type": "error", "message": f"Copilot hit an error: {e}"}
        return
    yield from emit_done_with_memory(
        question=question,
        profile=profile,
        final_answer="".join(final_parts).strip(),
        trace=[],
        cache={},
        iterations=0,
        llm_calls=1,
        route=route,
        memory_trace=memory_trace,
        compact_memory_trace=compact_memory_trace,
    )
