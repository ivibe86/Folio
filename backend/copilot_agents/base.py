from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import llm_client
from copilot_tools import execute_tool

logger = logging.getLogger(__name__)


def emit_done_with_memory(
    *,
    question: str,
    profile: str | None,
    final_answer: str,
    trace: list[dict],
    cache: dict,
    iterations: int,
    pending_write: dict | None = None,
    pending_chart: dict | None = None,
    data: Any = None,
    data_source: str | None = None,
    llm_calls: int = 0,
):
    import copilot_agent as core

    display_rows, display_source = core._extract_display_data(trace, cache, profile)
    cleaned_answer, agent_props_raw, observations_logged, proposals_created = core._persist_agent_tags(
        raw_answer=final_answer,
        profile=profile,
    )
    answer_text = core._fallback_when_empty(cleaned_answer, bool(proposals_created))
    done_event = {
        "type": "done",
        "answer": answer_text,
        "data": data if data is not None else display_rows,
        "data_source": data_source or display_source,
        "tool_trace": trace,
        "iterations": iterations,
        "llm_calls": llm_calls,
        "memory_proposals": proposals_created,
        "memory_observations": observations_logged,
    }
    if pending_write:
        done_event["pending_write"] = pending_write
        if pending_write.get("samples"):
            done_event["data"] = pending_write["samples"]
            done_event["data_source"] = "write_preview"
    if pending_chart:
        done_event["chart"] = pending_chart
    yield done_event

    detector_props = core._persist_detector_signals(
        user_question=question,
        cleaned_answer=cleaned_answer or final_answer,
        profile=profile,
        agent_proposals_raw=agent_props_raw,
    )
    if detector_props:
        yield {"type": "memory_update", "memory_proposals": detector_props}


def tool_loop_result(
    *,
    question: str,
    profile: str | None,
    history: list[dict] | None,
    selected_tools: list[str],
    system: str,
    max_iterations: int = 2,
    run_detector: bool = True,
) -> dict:
    import copilot_agent as core

    messages: list[dict] = core._normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer = ""
    pending_chart: dict | None = None
    pending_write: dict | None = None

    for iteration in range(max_iterations + 1):
        force_final = iteration == max_iterations
        if force_final:
            messages.append({
                "role": "user",
                "content": "Iteration cap reached. Provide your best final answer now using what you already know. Do not call any more tools.",
            })
        response = llm_client.chat_with_tools(
            messages=messages,
            tools=selected_tools,
            system=system,
            max_tokens=1200,
            purpose="copilot",
        )
        content = response.get("content") or ""
        tool_calls = response.get("tool_calls") or []
        if not tool_calls or force_final:
            final_answer = content.strip()
            break
        messages.append({"role": "assistant", "content": content, "tool_calls": tool_calls})
        for call in tool_calls:
            start = datetime.now()
            result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            trace.append({"name": call["name"], "args": call.get("args") or {}, "duration_ms": duration_ms})
            if isinstance(result, dict) and result.get("_chart"):
                pending_chart = result
            if isinstance(result, dict) and result.get("_write_preview"):
                pending_write = {
                    "confirmation_id": result.get("confirmation_id"),
                    "sql": result.get("sql"),
                    "rows_affected": result.get("rows_affected"),
                    "samples": result.get("samples", []),
                    "preview_changes": result.get("preview_changes", []),
                    "summary": result.get("summary"),
                }
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": core._truncate_for_model(result)})

    result = core._finalize_answer(
        question=question,
        profile=profile,
        raw_answer=final_answer,
        trace=trace,
        cache=cache,
        iterations=iteration,
        run_detector=run_detector,
    )
    if pending_chart:
        result["chart"] = pending_chart
    if pending_write:
        result["pending_write"] = pending_write
    result["llm_calls"] = iteration + 1
    return result


def tool_loop_stream(
    *,
    question: str,
    profile: str | None,
    history: list[dict] | None,
    selected_tools: list[str],
    system: str,
    max_iterations: int = 2,
):
    import copilot_agent as core

    messages: list[dict] = core._normalize_history(history)
    messages.append({"role": "user", "content": question})
    cache: dict = {}
    trace: list[dict] = []
    final_answer_parts: list[str] = []
    pending_write: dict | None = None
    pending_chart: dict | None = None
    llm_calls = 0

    for iteration in range(max_iterations + 1):
        force_final = iteration == max_iterations
        if force_final:
            messages.append({
                "role": "user",
                "content": "Iteration cap reached. Provide your best final answer now using what you already know. Do not call any more tools.",
            })
        pending_tool_calls: list[dict] = []
        text_buffer: list[str] = []
        yield {"type": "reset_text"}
        llm_calls += 1
        try:
            for event_type, payload in llm_client.chat_with_tools_stream(
                messages=messages,
                tools=selected_tools,
                system=system,
                max_tokens=1200,
                purpose="copilot",
            ):
                if event_type == "text":
                    text_buffer.append(payload)
                    yield {"type": "token", "text": payload}
                elif event_type == "tool_call":
                    pending_tool_calls.append(payload)
        except Exception as e:
            logger.exception("dispatcher specialist stream failed at iteration %d", iteration)
            yield {"type": "error", "message": f"Copilot hit an error: {e}"}
            return

        content = "".join(text_buffer)
        if not pending_tool_calls or force_final:
            final_answer_parts.append(content)
            break

        yield {"type": "reset_text"}
        messages.append({"role": "assistant", "content": content, "tool_calls": pending_tool_calls})

        for call in pending_tool_calls:
            yield {"type": "tool_call", "name": call["name"], "args": call.get("args") or {}}
            start = datetime.now()
            result = execute_tool(call["name"], call.get("args") or {}, profile, cache=cache)
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            trace.append({"name": call["name"], "args": call.get("args") or {}, "duration_ms": duration_ms})
            yield {"type": "tool_result", "name": call["name"], "duration_ms": duration_ms}
            if isinstance(result, dict) and result.get("_write_preview"):
                pending_write = {
                    "confirmation_id": result.get("confirmation_id"),
                    "sql": result.get("sql"),
                    "rows_affected": result.get("rows_affected"),
                    "samples": result.get("samples", []),
                    "preview_changes": result.get("preview_changes", []),
                    "summary": result.get("summary"),
                }
            if isinstance(result, dict) and result.get("_chart"):
                pending_chart = {
                    "type": result.get("type"),
                    "title": result.get("title"),
                    "series_name": result.get("series_name"),
                    "labels": result.get("labels", []),
                    "values": result.get("values", []),
                    "unit": result.get("unit", "currency"),
                }
                yield {"type": "chart", "chart": pending_chart}
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": core._truncate_for_model(result)})

    raw_answer = "".join(final_answer_parts).strip()
    if raw_answer:
        final_answer = raw_answer
    elif pending_chart:
        final_answer = f"Done - here's the {pending_chart.get('title') or 'chart'}."
    else:
        final_answer = "I couldn't land on a confident answer from the available data."
    yield from emit_done_with_memory(
        question=question,
        profile=profile,
        final_answer=final_answer,
        trace=trace,
        cache=cache,
        iterations=iteration,
        pending_write=pending_write,
        pending_chart=pending_chart,
        llm_calls=llm_calls,
    )
