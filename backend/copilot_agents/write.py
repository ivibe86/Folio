from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import llm_client
from copilot_tools import execute_tool

from .base import emit_done_with_memory
from .base import tool_loop_result, tool_loop_stream
from .drilldown import _candidate_names, _load_categories, _load_merchants, _parse_jsonish, _resolve_name


def _candidate_payload(question: str, profile: str | None) -> dict[str, list[str]]:
    merchants = _load_merchants(profile)
    categories = _load_categories(profile)
    focused_merchants = _candidate_names(question, merchants, limit=40)
    focused_categories = _candidate_names(question, categories, limit=30)
    return {
        "merchants": focused_merchants or merchants[:60],
        "categories": focused_categories or categories[:40],
        "all_merchants": merchants,
        "all_categories": categories,
    }


def _resolve_candidate(subject_type: str, text: str | None, profile: str | None, candidates: dict[str, list[str]]) -> str | None:
    focused = {
        "merchants": candidates.get("merchants") or [],
        "categories": candidates.get("categories") or [],
    }
    resolved = _resolve_name(subject_type, text, profile, focused)
    if resolved:
        return resolved
    full = {
        "merchants": candidates.get("all_merchants") or [],
        "categories": candidates.get("all_categories") or [],
    }
    return _resolve_name(subject_type, text, profile, full)


def _structured_write_plan(question: str, profile: str | None, history: list[dict] | None = None) -> tuple[dict | None, int]:
    if not llm_client.is_available():
        return None, 0

    import copilot_agent as core

    candidates = _candidate_payload(question, profile)
    prompt = f"""Parse the latest user message for a Folio write-preview.

Return JSON only:
{{"operation":"bulk_recategorize|create_rule|rename_merchant|other","merchant":string|null,"category":string|null,"pattern":string|null,"old_name":string|null,"new_name":string|null,"confidence":number,"needs_clarification":boolean}}

Rules:
- Use bulk_recategorize when the user wants existing matching merchant transactions moved/reclassified/recategorized to a category.
- Use create_rule when the user wants future transactions/always categorize/auto-categorize matching text to a category.
- Use rename_merchant when the user wants merchant display text renamed/cleaned up.
- Pick merchant/category/old_name from candidates when possible. Preserve exact candidate spelling.
- category is the target category. new_name is the desired merchant display name.
- pattern is the matching text for a rule; use the named merchant/text if provided.
- If this is not a write/change request, use operation=other.
- If required fields are missing, set needs_clarification=true.

Merchant candidates: {json.dumps(candidates["merchants"][:80])}
Category candidates: {json.dumps(candidates["categories"][:60])}

Recent context:
{core._history_text(history, limit=4) or "(none)"}

Latest message: {question}
JSON:"""
    raw = llm_client.complete(prompt, max_tokens=220, purpose="copilot")
    parsed = _parse_jsonish(raw)
    if not parsed:
        return None, 1
    if parsed.get("operation") not in {"bulk_recategorize", "create_rule", "rename_merchant"}:
        return None, 1
    if parsed.get("needs_clarification"):
        return None, 1

    confidence = parsed.get("confidence")
    try:
        if float(confidence) < 0.55:
            return None, 1
    except (TypeError, ValueError):
        return None, 1

    operation = parsed["operation"]

    if operation == "bulk_recategorize":
        merchant = _resolve_candidate("merchant", parsed.get("merchant") or parsed.get("pattern"), profile, candidates)
        category = _resolve_candidate("category", parsed.get("category"), profile, candidates)
        if not merchant or not category:
            return None, 1
        return {"name": "preview_bulk_recategorize", "args": {"merchant": merchant, "category": category}}, 1

    if operation == "create_rule":
        category = _resolve_candidate("category", parsed.get("category"), profile, candidates)
        pattern = (parsed.get("pattern") or parsed.get("merchant") or "").strip()
        if not pattern and parsed.get("merchant"):
            pattern = str(parsed["merchant"]).strip()
        if not pattern or not category:
            return None, 1
        merchant = _resolve_candidate("merchant", pattern, profile, candidates)
        if merchant:
            pattern = merchant
        return {"name": "preview_create_rule", "args": {"pattern": pattern, "category": category}}, 1

    old_name = _resolve_candidate("merchant", parsed.get("old_name") or parsed.get("merchant"), profile, candidates)
    new_name = (parsed.get("new_name") or "").strip()
    if not old_name or not new_name:
        return None, 1
    return {"name": "preview_rename_merchant", "args": {"old_name": old_name, "new_name": new_name}}, 1


def _write_answer(tool_name: str, result: Any) -> str:
    if not isinstance(result, dict):
        return "I prepared the write preview. Review it before confirming."
    if result.get("error"):
        return f"I couldn't prepare that write preview: {result['error']}"
    if result.get("_write_preview"):
        summary = result.get("summary") or "Prepared write preview"
        rows = int(result.get("rows_affected") or 0)
        noun = "transaction" if rows == 1 else "transactions"
        if tool_name == "preview_create_rule":
            return f"Preview ready: {summary}. Confirm to create the rule."
        return f"Preview ready: {summary}. This would affect {rows} {noun}. Confirm to apply."
    note = result.get("note")
    if note:
        return str(note)
    count = result.get("count")
    if count == 0:
        return "I couldn't find matching transactions to change."
    return "I prepared the write preview. Review it before confirming."


def _pending_write(result: Any) -> dict | None:
    if not isinstance(result, dict) or not result.get("_write_preview"):
        return None
    return {
        "confirmation_id": result.get("confirmation_id"),
        "sql": result.get("sql"),
        "rows_affected": result.get("rows_affected"),
        "samples": result.get("samples", []),
        "preview_changes": result.get("preview_changes", []),
        "summary": result.get("summary"),
    }


def _direct_result(question: str, profile: str | None, history: list[dict] | None = None) -> dict | None:
    import copilot_agent as core

    plan, llm_calls = _structured_write_plan(question, profile, history)
    if not plan:
        return None
    cache: dict = {}
    start = datetime.now()
    result = execute_tool(plan["name"], plan.get("args") or {}, profile, cache=cache)
    duration_ms = int((datetime.now() - start).total_seconds() * 1000)
    trace = [{"name": plan["name"], "args": plan.get("args") or {}, "duration_ms": duration_ms}]
    answer = _write_answer(plan["name"], result)
    finalized = core._finalize_answer(
        question=question,
        profile=profile,
        raw_answer=answer,
        trace=trace,
        cache=cache,
        iterations=0,
        run_detector=True,
    )
    pending = _pending_write(result)
    if pending:
        finalized["pending_write"] = pending
    finalized["llm_calls"] = llm_calls
    return finalized


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    direct = _direct_result(question, profile, history)
    if direct:
        return direct

    return tool_loop_result(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.WRITE_TOOLS),
        system=core._build_system_prompt(profile, list(core.WRITE_TOOLS)),
    )


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    plan, llm_calls = _structured_write_plan(question, profile, history)
    if plan:
        cache: dict = {}
        trace: list[dict] = []
        yield {"type": "reset_text"}
        yield {"type": "tool_call", "name": plan["name"], "args": plan.get("args") or {}}
        start = datetime.now()
        result = execute_tool(plan["name"], plan.get("args") or {}, profile, cache=cache)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)
        trace.append({"name": plan["name"], "args": plan.get("args") or {}, "duration_ms": duration_ms})
        yield {"type": "tool_result", "name": plan["name"], "duration_ms": duration_ms}
        yield from emit_done_with_memory(
            question=question,
            profile=profile,
            final_answer=_write_answer(plan["name"], result),
            trace=trace,
            cache=cache,
            iterations=0,
            pending_write=_pending_write(result),
            llm_calls=llm_calls,
        )
        return

    yield from tool_loop_stream(
        question=question,
        profile=profile,
        history=history,
        selected_tools=list(core.WRITE_TOOLS),
        system=core._build_system_prompt(profile, list(core.WRITE_TOOLS)),
    )
