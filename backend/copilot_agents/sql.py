from __future__ import annotations

import re
from datetime import datetime

from database import get_db
from data_manager import explain_category_assignment

from .base import emit_done_with_memory, tool_loop_result, tool_loop_stream


SOURCE_LABELS = {
    "user": "a manual override",
    "user-rule": "a user-defined rule",
    "llm": "AI categorization",
    "rule": "a built-in rule",
    "system-rule": "a built-in rule",
    "fallback": "the fallback default",
    "teller": "the bank's own category",
    "enricher": "merchant enrichment",
    "merchant-memory": "merchant memory",
}


def _categorization_merchant(question: str) -> str | None:
    q = (question or "").strip()
    if not re.search(r"\bcategor(?:y|ized|ised|ization|isation)\b", q, re.I):
        return None
    patterns = [
        r"\bwhy\s+(?:is|was)\s+(.+?)\s+categor(?:ized|ised)\b",
        r"\bhow\s+(?:is|was)\s+(.+?)\s+categor(?:ized|ised)\b",
        r"\bwhy\s+(?:did|does)\s+(.+?)\s+(?:get\s+)?categor(?:ized|ised)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, q, re.I)
        if match:
            merchant = match.group(1).strip(" ?.,'\"")
            merchant = re.sub(r"^(?:my|the|a|an)\s+", "", merchant, flags=re.I).strip()
            return merchant or None
    return None


def _categorization_answer(merchant: str, profile: str | None) -> tuple[str, dict]:
    with get_db() as conn:
        data = explain_category_assignment(merchant, profile, conn)

    count = int(data.get("transaction_count") or 0)
    pattern = data.get("normalized_pattern") or merchant
    dominant_cat = data.get("dominant_category") or "an unknown category"
    dominant_src = data.get("dominant_source") or "unknown"
    source_label = SOURCE_LABELS.get(dominant_src, dominant_src)
    rule = data.get("rule")
    distribution = data.get("distribution") or []
    samples = data.get("samples") or []

    if count == 0:
        return f'I did not find transactions matching "{merchant}" (normalized as {pattern}).', data

    rule_detail = ""
    if rule:
        rule_detail = (
            f" There is an active {'user' if rule.get('source') == 'user' else 'built-in'} rule "
            f"for pattern {rule.get('pattern')} -> {rule.get('category')}."
        )

    breakdown = ""
    if distribution:
        top = []
        for row in distribution[:3]:
            cat = row.get("category") or "Uncategorized"
            src = SOURCE_LABELS.get(row.get("categorization_source"), row.get("categorization_source") or "unknown")
            top.append(f"{row.get('cnt')} as {cat} via {src}")
        breakdown = " Breakdown: " + "; ".join(top) + "."

    sample_detail = ""
    if samples:
        sample = samples[0]
        sample_detail = f" Most recent match: {sample.get('date')} - {sample.get('description')}."

    answer = (
        f'{merchant} is categorized mostly as {dominant_cat} across {count} matching '
        f"transaction{'s' if count != 1 else ''}. The dominant source is {source_label}."
        f"{rule_detail}{breakdown}{sample_detail}"
    )
    return answer, data


def _deterministic_categorization_debug(question: str, profile: str | None) -> tuple[str, list[dict], dict] | None:
    merchant = _categorization_merchant(question)
    if not merchant:
        return None

    start = datetime.now()
    answer, data = _categorization_answer(merchant, profile)
    duration_ms = int((datetime.now() - start).total_seconds() * 1000)
    trace = [{"name": "explain_category_assignment", "args": {"merchant": merchant}, "duration_ms": duration_ms}]
    return answer, trace, data


def run(question: str, profile: str | None, history: list[dict] | None = None) -> dict:
    import copilot_agent as core

    deterministic = _deterministic_categorization_debug(question, profile)
    if deterministic:
        answer, trace, data = deterministic
        result = core._finalize_answer(
            question=question,
            profile=profile,
            raw_answer=answer,
            trace=trace,
            cache={},
            iterations=0,
            run_detector=True,
        )
        result["data"] = data.get("samples") or []
        result["data_source"] = "explain_category_assignment"
        result["llm_calls"] = 0
        return result

    return tool_loop_result(
        question=question,
        profile=profile,
        history=history,
        selected_tools=["run_sql"],
        system=core._build_system_prompt(profile, ["run_sql"]),
    )


def stream(question: str, profile: str | None, history: list[dict] | None = None):
    import copilot_agent as core

    deterministic = _deterministic_categorization_debug(question, profile)
    if deterministic:
        answer, trace, data = deterministic
        yield {"type": "reset_text"}
        for call in trace:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            yield {"type": "tool_result", "name": call["name"], "duration_ms": call.get("duration_ms", 0)}
        yield from emit_done_with_memory(
            question=question,
            profile=profile,
            final_answer=answer,
            trace=trace,
            cache={},
            iterations=0,
            data=data.get("samples") or [],
            data_source="explain_category_assignment",
            llm_calls=0,
        )
        return

    yield from tool_loop_stream(
        question=question,
        profile=profile,
        history=history,
        selected_tools=["run_sql"],
        system=core._build_system_prompt(profile, ["run_sql"]),
    )
