from __future__ import annotations

import logging

import llm_client

from .base import emit_done_with_memory

logger = logging.getLogger(__name__)


def system_prompt(profile: str | None) -> str:
    import copilot_agent as core
    from datetime import datetime
    from database import get_db
    import memory

    try:
        with get_db() as conn:
            memory_body = memory.render_markdown(profile, conn)
    except Exception:
        logger.debug("memory render failed for chat specialist", exc_info=True)
        memory_body = ""

    today = datetime.now().strftime("%Y-%m-%d")
    blocks = [
        "You're the user's close friend whose day job is senior financial advisor. Warm, direct, useful, and concise.",
        "You can chat normally about anything, including code, science, life, and general knowledge. Do not force every reply back to money. Usually answer in 2-5 sentences unless the user asks for code or detail.",
        (
            "You are running inside Folio, a personal finance app. Be honest about your capabilities: "
            "the app can route finance questions to internal read tools for transactions, categories, merchants, "
            "recurring charges, balances, net worth, charts, write previews, and read-only SQL when needed. "
            "Do not claim you have no tools or no finance data access. Do not imply you only know what the user pasted into chat. "
            "These finance capabilities do not limit normal conversation or coding help."
        ),
        (
            "If asked what tools you have, describe these capabilities at a high level rather than exposing raw implementation details. "
            "If asked about schema, you may mention transactions_visible, accounts, categories, merchants, net_worth_history, category_rules, and saved_insights."
        ),
        f"Today is {today}. Active profile: {profile or 'household'}.",
        (
            "Persistent memory about the user (read-only; use as background, do not restate verbatim):\n"
            f"{memory_body.rstrip()}"
        ) if memory_body.strip() else "",
        (
            "At the very end only, after visible prose, you may emit hidden memory tags:\n"
            '<observation theme="short_kebab_case">one-line note</observation>\n'
            '<memory_proposal section="identity|preferences|goals|concerns|open_questions" confidence="stated" evidence="quote or reason">one-line entry</memory_proposal>\n'
            "Use memory_proposal only for explicit enduring user-stated facts, preferences, commitments, goals, or contradictions. "
            "Do not emit memory tags for one-off off-topic questions, ordinary coding requests, or general knowledge questions."
        ),
    ]
    return "\n\n".join(block for block in blocks if block)


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    response = llm_client.chat_with_tools(
        messages=core._normalize_history(history) + [{"role": "user", "content": question}],
        tools=[],
        system=system_prompt(profile),
        max_tokens=600,
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
    )
    result["llm_calls"] = 1
    return result


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    yield {"type": "reset_text"}
    final_parts: list[str] = []
    try:
        for event_type, payload in llm_client.chat_with_tools_stream(
            messages=core._normalize_history(history) + [{"role": "user", "content": question}],
            tools=[],
            system=system_prompt(profile),
            max_tokens=600,
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
    )
