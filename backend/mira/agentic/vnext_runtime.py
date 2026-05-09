from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mira.agentic.schemas import AgentDecision, EvidencePacket, ValidationResult


_RUNTIME = "agentic_vnext"
_ANSWER = "Mira vNext selected a safe route, but tool execution and answer generation are not active in this experimental path yet."
_SHADOW_TRACE_LOCK = threading.Lock()
_MEMORY_TOOL_NAMES = {"manage_memory", "remember_user_context", "retrieve_relevant_memories", "list_mira_memories"}


def build_shadow_trace(
    *,
    question: str,
    profile: str | None,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
) -> dict[str, Any]:
    _ = profile
    return {
        "runtime": _RUNTIME,
        "phase": "skeleton",
        "status": "not_executed",
        "question_chars": len(question or ""),
        "history_turns": len(history or []),
        "forced_intent": forced_intent,
        "selected_tools": [],
        "llm_calls": 0,
        "legacy_router_used": False,
    }


def run_vnext_shadow(
    *,
    question: str,
    profile: str | None,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
    current_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        state = _prepare_vnext_turn(question=question, profile=profile, history=history, forced_intent=forced_intent)
        validation: ValidationResult = state["validation"]
        route: dict[str, Any] = state["route"]
        safe_to_execute, skipped_reason = _shadow_execution_policy(validation)
        evidence = EvidencePacket(question=question)
        if safe_to_execute:
            evidence = _execute_vnext_evidence(
                validation=validation,
                question=question,
                profile=profile,
            )
            answer_result = _answer_vnext_safely(
                question=question,
                route=route,
                validation=validation,
                evidence=evidence,
                history=history,
            )
        else:
            answer_result = _shadow_skipped_answer(skipped_reason)
        done = _done_event(route=route, validation=validation, evidence=evidence, answer_result=answer_result)
        payload = _shadow_payload(
            question=question,
            profile=profile,
            current_event=current_event,
            vnext_done=done,
            validation=validation,
            safe_to_execute=safe_to_execute,
            skipped_reason=skipped_reason,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
        )
    except Exception as exc:
        payload = {
            "runtime": _RUNTIME,
            "status": "error",
            "profile": profile or "household",
            "question": str(question or ""),
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
            "error": str(exc),
            "legacy_router_used": False,
        }
    _record_shadow_trace(payload)
    return payload


def run_vnext_result(
    question: str,
    profile: str | None,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
) -> dict[str, Any]:
    state = _prepare_vnext_turn(question=question, profile=profile, history=history, forced_intent=forced_intent)
    evidence = _execute_vnext_evidence(
        validation=state["validation"],
        question=question,
        profile=profile,
    )
    answer_result = _answer_vnext_safely(
        question=question,
        route=state["route"],
        validation=state["validation"],
        evidence=evidence,
        history=history,
    )
    done = _done_event(route=state["route"], validation=state["validation"], evidence=evidence, answer_result=answer_result)
    done.pop("type", None)
    return done


def run_vnext_stream(
    question: str,
    profile: str | None,
    history: list[dict] | None = None,
    forced_intent: str | None = None,
):
    yield {
        "type": "routing_started",
        "stage": "routing",
        "label": "Routing the request",
    }
    state = _prepare_vnext_turn(question=question, profile=profile, history=history, forced_intent=forced_intent)
    route = state["route"]
    validation = state["validation"]
    yield {"type": "route", **route}
    yield {
        "type": "controller",
        "act": route.get("controller_act"),
        "controller_act": route.get("controller_act"),
        "intent": route.get("intent"),
        "confidence": route.get("confidence"),
        "reason": "vnext_selector",
        "legacy_router_used": False,
        "mira_planner": _RUNTIME,
    }
    yield {
        "type": "action",
        "domain_action": route.get("domain_action"),
        "tool_plan": route.get("tool_plan") or [],
        "validation": route.get("validation"),
        "grounded_entities": route.get("grounded_entities") or [],
        "selected_tools": route.get("selected_tools") or [],
    }
    yield _progress_event(route)
    evidence = EvidencePacket(question=question)
    if _should_execute(validation):
        from mira.agentic.vnext_executor import chart_from_evidence, iter_execute_vnext_events

        for event in iter_execute_vnext_events(
            validation,
            question=question,
            profile=profile,
            cache={},
        ):
            if event.get("type") == "evidence":
                evidence = event["evidence"]
            else:
                yield event
        chart_payload = chart_from_evidence(evidence)
        if chart_payload:
            yield {"type": "chart", "chart": chart_payload}
    answer_result = None
    try:
        from mira.agentic.vnext_answerer import iter_answer_vnext_events

        for event in iter_answer_vnext_events(
            question=question,
            route=route,
            validation=validation,
            evidence=evidence,
            history=history,
        ):
            if event.get("type") == "_answer_result":
                answer_result = event.get("answer_result")
            else:
                yield event
    except Exception:
        answer_result = None
    if answer_result is None:
        answer_result = _answer_vnext_safely(
            question=question,
            route=route,
            validation=validation,
            evidence=evidence,
            history=history,
        )
    yield _done_event(route=route, validation=validation, evidence=evidence, answer_result=answer_result)


def _prepare_vnext_turn(
    *,
    question: str,
    profile: str | None,
    history: list[dict] | None,
    forced_intent: str | None,
) -> dict[str, Any]:
    selector = _run_selector_safely(question=question, history=history)
    validation = _validate_selector_safely(
        selector=selector,
        question=question,
        profile=profile,
        history=history,
    )
    route = _route_payload(
        question=question,
        history=history,
        forced_intent=forced_intent,
        selector=selector,
        validation=validation,
    )
    return {"selector": selector, "validation": validation, "route": route}


def _route_payload(
    *,
    question: str,
    history: list[dict] | None,
    forced_intent: str | None,
    selector: Any,
    validation: ValidationResult,
) -> dict[str, Any]:
    calls = list(getattr(selector, "calls", []) or [])
    selector_status = str(getattr(selector, "status", "") or "clarify")
    selector_decision = getattr(selector, "decision", {})
    if not isinstance(selector_decision, dict):
        selector_decision = {}
    controller_route = str(selector_decision.get("controller_route") or selector_decision.get("route") or "").strip()
    controller_intent = str(selector_decision.get("intent") or "").strip()
    selected_tools = [step.tool_name for step in validation.normalized_plan if step.tool_name and step.tool_name != "run_sql"]
    tool_plan = [step.to_dict() for step in validation.normalized_plan]
    intent = controller_intent or ("finance" if selected_tools and not _memory_only(selected_tools) else "chat")
    trace = {
        **(getattr(selector, "trace", {}) if isinstance(getattr(selector, "trace", {}), dict) else {}),
        "runtime": _RUNTIME,
        "phase": "validated_selector",
        "forced_intent": forced_intent,
        "validation_status": validation.status,
        "grounded_entity_count": len(validation.grounded_entities),
    }
    controller_act = _controller_act_for_status(validation.status, selected_tools)
    operation = _operation_for_status(selector_status, validation.status, selected_tools)
    return {
        "question": question,
        "intent": intent,
        "operation": operation,
        "controller_route": controller_route,
        "needs_folio_evidence": bool(selector_decision.get("needs_folio_evidence")) if selector_decision else bool(selected_tools),
        "uses_history": bool(history),
        "confidence": validation.decision.confidence,
        "needs_clarification": validation.status == "clarify",
        "clarification_question": validation.clarification_question,
        "pending_clarification": validation.pending_clarification,
        "controller_act": controller_act,
        "agent_decision": validation.decision.to_dict(),
        "tool_plan": tool_plan,
        "validation": validation.to_dict(),
        "grounded_entities": validation.grounded_entities,
        "selected_tools": selected_tools,
        "domain_action": {
            "name": "vnext_selector",
            "status": validation.status,
            "tool_plan": tool_plan,
            "blocked_reason": validation.blocked_reason,
            "clarification_question": validation.clarification_question,
            "pending_clarification": validation.pending_clarification,
        },
        "selector": {
            "status": selector_status,
            "decision": selector_decision,
            "calls": calls,
            "repair_used": bool(getattr(selector, "repair_used", False)),
            "family_detail_used": bool(getattr(selector, "family_detail_used", False)),
            "raw_response": str(getattr(selector, "raw", "") or ""),
        },
        "trace": trace,
        "llm_calls": int(getattr(selector, "llm_calls", 0) or 0),
        "mira_planner": _RUNTIME,
        "legacy_router_used": False,
    }


def _done_event(
    *,
    route: dict[str, Any],
    validation: ValidationResult,
    evidence: EvidencePacket,
    answer_result: Any,
) -> dict[str, Any]:
    from mira.agentic.vnext_executor import (
        chart_from_evidence,
        data_from_evidence,
        evidence_summary,
        pending_write_from_evidence,
        tool_trace_from_evidence,
    )

    trace = route.get("trace") if isinstance(route.get("trace"), dict) else {}
    trace = _done_trace(trace, validation=validation, evidence=evidence, answer_result=answer_result)
    selected_tools = route.get("selected_tools") if isinstance(route.get("selected_tools"), list) else []
    pending_write = pending_write_from_evidence(evidence)
    data, data_source = data_from_evidence(evidence, pending_write)
    chart_payload = chart_from_evidence(evidence)
    event = {
        "type": "done",
        "answer": getattr(answer_result, "answer", "") or _answer_for_route(route, validation, evidence),
        "data": data,
        "data_source": data_source,
        "tool_trace": tool_trace_from_evidence(evidence),
        "iterations": 0,
        "error": validation.blocked_reason if validation.status == "blocked" else None,
        "route": route,
        "intent": route.get("intent") or "chat",
        "agent_decision": validation.decision.to_dict(),
        "validation": validation.to_dict(),
        "evidence": evidence_summary(evidence),
        "provenance": evidence.provenance,
        "selected_tools": selected_tools,
        "grounded_entities": validation.grounded_entities,
        "pending_clarification": validation.pending_clarification,
        "answer_context": _answer_context_from_validation(validation, evidence),
        "trace": trace,
        "llm_calls": int(route.get("llm_calls") or 0) + int(getattr(answer_result, "llm_calls", 0) or 0),
        "legacy_router_used": False,
        "answer_guard": {
            "path": getattr(answer_result, "path", ""),
            "used_fallback": bool(getattr(answer_result, "used_fallback", False)),
            "error": getattr(answer_result, "error", ""),
        },
    }
    if pending_write:
        event["pending_write"] = pending_write
    if chart_payload:
        event["chart"] = chart_payload
    event["route"] = {**route, "trace": trace}
    return event


def _done_trace(
    trace: dict[str, Any],
    *,
    validation: ValidationResult,
    evidence: EvidencePacket,
    answer_result: Any,
) -> dict[str, Any]:
    executor_ms = 0.0
    for record in evidence.tool_results:
        try:
            executor_ms += float(record.get("ms") or 0)
        except (TypeError, ValueError):
            continue
    answer_path = getattr(answer_result, "path", "")
    return {
        **trace,
        "validation_status": validation.status,
        "grounded_entity_count": len(validation.grounded_entities),
        "executor_ms": round(executor_ms, 2),
        "tool_result_count": len(evidence.tool_results),
        "evidence_fact_count": len(evidence.facts),
        "evidence_row_count": len(evidence.rows),
        "evidence_chart_count": len(evidence.charts),
        "answer_path": answer_path,
        "answer_llm_calls": int(getattr(answer_result, "llm_calls", 0) or 0),
        "answer_used_fallback": bool(getattr(answer_result, "used_fallback", False)),
        "answer_max_tokens": int(getattr(answer_result, "max_tokens", 0) or 0),
    }


def _answer_vnext_safely(
    *,
    question: str,
    route: dict[str, Any],
    validation: ValidationResult,
    evidence: EvidencePacket,
    history: list[dict] | None = None,
) -> Any:
    try:
        from mira.agentic.vnext_answerer import VNextAnswerResult, answer_vnext

        return answer_vnext(
            question=question,
            route=route,
            validation=validation,
            evidence=evidence,
            history=history,
        )
    except Exception as exc:
        from mira.agentic.vnext_answerer import VNextAnswerResult

        return VNextAnswerResult(
            answer=_answer_for_route(route, validation, evidence),
            path="fallback",
            used_fallback=True,
            error=str(exc),
        )


def _shadow_execution_policy(validation: ValidationResult) -> tuple[bool, str]:
    if validation.status != "ready":
        return False, f"validation_{validation.status}"
    if not validation.normalized_plan:
        return True, ""
    selected = [step.tool_name for step in validation.normalized_plan]
    if any(str(name or "").startswith("preview_") or name == "preview_finance_change" for name in selected):
        return False, "preview_write_skipped"
    if any(name in {"manage_memory", "remember_user_context", "update_memory", "forget_memory"} for name in selected):
        return False, "write_tool_skipped"
    return True, ""


def _shadow_skipped_answer(reason: str) -> Any:
    from mira.agentic.vnext_answerer import VNextAnswerResult

    return VNextAnswerResult(
        answer="",
        path="shadow_skipped",
        used_fallback=False,
        error=reason,
    )


def _shadow_payload(
    *,
    question: str,
    profile: str | None,
    current_event: dict[str, Any] | None,
    vnext_done: dict[str, Any],
    validation: ValidationResult,
    safe_to_execute: bool,
    skipped_reason: str,
    latency_ms: float,
) -> dict[str, Any]:
    selected_tools = list(vnext_done.get("selected_tools") or [])
    current_tools = list((current_event or {}).get("selected_tools") or [])
    answer_guard = vnext_done.get("answer_guard") if isinstance(vnext_done.get("answer_guard"), dict) else {}
    trace = vnext_done.get("trace") if isinstance(vnext_done.get("trace"), dict) else {}
    payload = {
        "runtime": _RUNTIME,
        "status": "ok" if safe_to_execute else "skipped",
        "profile": profile or "household",
        "question": str(question or ""),
        "latency_ms": latency_ms,
        "safe_to_execute": safe_to_execute,
        "skipped_reason": skipped_reason,
        "selected_tools": selected_tools,
        "tool_args": [
            {"name": step.tool_name, "args": dict(step.args or {})}
            for step in validation.normalized_plan
        ],
        "validation_status": validation.status,
        "answer_path": answer_guard.get("path") or trace.get("answer_path") or "",
        "answer_used_fallback": bool(answer_guard.get("used_fallback")),
        "answer_error": answer_guard.get("error") or "",
        "llm_calls": vnext_done.get("llm_calls") or 0,
        "data_source": vnext_done.get("data_source"),
        "evidence": vnext_done.get("evidence") or {},
        "trace": trace,
        "mismatch_reason": _shadow_mismatch_reason(
            current_tools=current_tools,
            vnext_tools=selected_tools,
            current_answer=str((current_event or {}).get("answer") or ""),
            vnext_answer=str(vnext_done.get("answer") or ""),
            skipped_reason=skipped_reason,
        ),
        "legacy_router_used": False,
    }
    if vnext_done.get("pending_write"):
        payload["pending_write"] = {
            "rows_affected": (vnext_done.get("pending_write") or {}).get("rows_affected"),
            "preview_change_count": len((vnext_done.get("pending_write") or {}).get("preview_changes") or []),
        }
    return payload


def _shadow_mismatch_reason(
    *,
    current_tools: list[str],
    vnext_tools: list[str],
    current_answer: str,
    vnext_answer: str,
    skipped_reason: str,
) -> str:
    if skipped_reason:
        return skipped_reason
    if current_tools != vnext_tools:
        return "tool_selection_diff"
    if bool(current_answer.strip()) != bool(vnext_answer.strip()):
        return "answer_presence_diff"
    return ""


def _record_shadow_trace(payload: dict[str, Any]) -> None:
    trace_dir = _shadow_trace_dir()
    if trace_dir is None:
        return
    path = trace_dir / f"mira_vnext_shadow_{datetime.now().strftime('%Y%m%d')}.jsonl"
    try:
        with _SHADOW_TRACE_LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
    except Exception:
        return


def _shadow_trace_dir() -> Path | None:
    explicit = os.getenv("MIRA_VNEXT_SHADOW_TRACE_DIR", "").strip()
    if explicit:
        return Path(explicit).expanduser()
    if _env_flag("MIRA_VNEXT_SHADOW_TRACE"):
        return Path("benchmark_runs") / "mira_vnext_shadow_traces"
    return None


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _validate_selector_safely(
    *,
    selector: Any,
    question: str,
    profile: str | None,
    history: list[dict] | None,
) -> ValidationResult:
    try:
        from mira.agentic.vnext_validator import validate_selector_calls, validation_for_general_answer

        selector_status = str(getattr(selector, "status", "") or "")
        if selector_status == "general_answer":
            return validation_for_general_answer(question=question, history=history)
        if selector_status != "tool_calls":
            return _validation_failure(
                str(getattr(selector, "error", "") or "selector did not choose a valid route"),
                history=history,
            )
        return validate_selector_calls(
            list(getattr(selector, "calls", []) or []),
            question=question,
            profile=profile,
            history=history,
        )
    except Exception as exc:
        return _validation_failure(str(exc), history=history)


def _execute_vnext_evidence(
    *,
    validation: ValidationResult,
    question: str,
    profile: str | None,
) -> EvidencePacket:
    if not _should_execute(validation):
        return EvidencePacket(question=question)
    from mira.agentic.vnext_executor import execute_vnext_plan

    return execute_vnext_plan(
        validation,
        question=question,
        profile=profile,
        cache={},
    )


def _should_execute(validation: ValidationResult) -> bool:
    return validation.status == "ready" and bool(validation.normalized_plan)


def _validation_failure(error: str, *, history: list[dict] | None) -> ValidationResult:
    decision = AgentDecision(
        intent="chat",
        turn_kind="chat",
        tool_plan=[],
        confidence=0.0,
        uses_history=bool(history),
        reasoning_summary="vnext_validation_failure",
    )
    return ValidationResult(
        status="clarify",
        decision=decision,
        normalized_plan=[],
        clarification_question="I need one more detail to choose the right Folio tool.",
        blocked_reason=error,
    )


def _run_selector_safely(*, question: str, history: list[dict] | None) -> Any:
    try:
        from mira.agentic.vnext_selector import run_selector

        return run_selector(question=question, history=history)
    except Exception as exc:
        return _SelectorFailure(str(exc))


def _controller_act_for_status(status: str, selected_tools: list[str]) -> str:
    if selected_tools:
        return "execute_action"
    if status == "clarify":
        return "clarify"
    return "answer_direct"


def _operation_for_status(selector_status: str, validation_status: str, selected_tools: list[str]) -> str:
    if validation_status == "blocked":
        return "blocked"
    if validation_status == "clarify":
        return "clarify"
    if selected_tools:
        return "selector_tool_plan"
    if selector_status == "general_answer":
        return "general_answer"
    return "clarify"


def _memory_only(selected_tools: list[str]) -> bool:
    return bool(selected_tools) and all(name in _MEMORY_TOOL_NAMES for name in selected_tools)


def _progress_event(route: dict[str, Any]) -> dict[str, Any]:
    status = (route.get("selector") or {}).get("status") if isinstance(route.get("selector"), dict) else ""
    validation = route.get("validation") if isinstance(route.get("validation"), dict) else {}
    validation_status = validation.get("status")
    if validation_status == "blocked":
        stage = "blocked"
        label = "Stopping on a safety check"
    elif validation_status == "clarify":
        stage = "clarify"
        label = "Checking the vNext route"
    elif status == "tool_calls":
        stage = "action"
        label = "Selected Folio tools"
    elif status == "general_answer":
        stage = "model"
        label = "Selected general answer"
    else:
        stage = "clarify"
        label = "Checking the vNext route"
    return {
        "type": "progress",
        "stage": stage,
        "label": label,
        "intent": route.get("intent"),
        "operation": route.get("operation"),
        "selected_tools": route.get("selected_tools") or [],
        "domain_action_name": "vnext_selector",
        "domain_action_status": (route.get("domain_action") or {}).get("status") if isinstance(route.get("domain_action"), dict) else status,
    }


def _answer_for_route(route: dict[str, Any], validation: ValidationResult, evidence: EvidencePacket) -> str:
    status = (route.get("selector") or {}).get("status") if isinstance(route.get("selector"), dict) else ""
    selected_tools = route.get("selected_tools") if isinstance(route.get("selected_tools"), list) else []
    if validation.status == "clarify":
        return validation.clarification_question or "I need one more detail to choose the right Folio tool."
    if validation.status == "blocked":
        return validation.blocked_reason or "I could not safely run that request."
    if status == "tool_calls" and selected_tools and evidence.tool_results:
        return f"Mira vNext ran {', '.join(selected_tools)} and collected evidence. Final evidence-grounded answering comes next."
    if status == "tool_calls" and selected_tools:
        return f"Mira vNext selected {', '.join(selected_tools)}. Tool execution comes next."
    if status == "general_answer":
        return "Mira vNext routed this as a general answer. General answer generation comes next."
    return _ANSWER


def _answer_context_from_validation(validation: ValidationResult, evidence: EvidencePacket) -> dict | None:
    if validation.status == "clarify" and validation.pending_clarification:
        return {
            "version": 2,
            "kind": "finance_pending_clarification",
            "pending_clarification": validation.pending_clarification,
            "agentic": True,
            "runtime": _RUNTIME,
        }
    if validation.status != "ready" or not validation.normalized_plan:
        return None
    from mira.agentic.semantic_frames import primary_semantic_frame, semantic_frame_from_args

    subject_type = ""
    subject = ""
    ranges: list[str] = []
    tools = []
    frames: list[dict[str, Any]] = []
    for step in validation.normalized_plan:
        args = step.args if isinstance(step.args, dict) else {}
        tools.append({"id": step.step_id, "name": step.tool_name, "args": dict(args)})
        frame = semantic_frame_from_args(step.tool_name, args)
        if frame:
            frames.append(frame)
        if not subject:
            filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
            subject_type = str(args.get("entity_type") or args.get("subject_type") or "").strip()
            subject = str(args.get("entity") or args.get("subject") or args.get("merchant") or args.get("category") or filters.get("merchant") or filters.get("category") or "").strip()
            if not subject_type and args.get("merchant"):
                subject_type = "merchant"
            elif not subject_type and args.get("category"):
                subject_type = "category"
            elif not subject_type and filters.get("merchant"):
                subject_type = "merchant"
            elif not subject_type and filters.get("category"):
                subject_type = "category"
        for key in ("range", "range_a", "range_b"):
            value = str(args.get(key) or "").strip()
            if value and value not in ranges:
                ranges.append(value)
    current_frame = primary_semantic_frame(frames)
    return {
        "version": 2,
        "kind": "finance_answer_context",
        "subject_type": subject_type,
        "subject": subject,
        "ranges": ranges,
        "tools": tools,
        "conversation_frame": _conversation_frame_from_answer_context(
            current_frame=current_frame,
            subject_type=subject_type,
            subject=subject,
            ranges=ranges,
            tools=tools,
        ),
        "current_frame": current_frame,
        "current_frames": frames,
        "provenance_id": evidence.provenance.get("provenance_id") or evidence.provenance.get("id"),
        "agentic": True,
        "runtime": _RUNTIME,
    }


def _conversation_frame_from_answer_context(
    *,
    current_frame: dict[str, Any],
    subject_type: str,
    subject: str,
    ranges: list[str],
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(current_frame, dict) or not current_frame.get("tool"):
        return {}
    filters = current_frame.get("filters") if isinstance(current_frame.get("filters"), dict) else {}
    resolved_subject_type = subject_type
    resolved_subject = subject
    if not resolved_subject and filters.get("merchant"):
        resolved_subject_type = "merchant"
        resolved_subject = str(filters.get("merchant") or "")
    elif not resolved_subject and filters.get("category"):
        resolved_subject_type = "category"
        resolved_subject = str(filters.get("category") or "")
    elif not resolved_subject and filters.get("account"):
        resolved_subject_type = "account"
        resolved_subject = str(filters.get("account") or "")
    view = str(current_frame.get("view") or "").strip().lower()
    requested_output = "scalar_total" if view in {"entity_total", "period_total"} else "summary"
    source_step_id = ""
    for tool in tools:
        if str(tool.get("name") or "") != "make_chart":
            source_step_id = str(tool.get("id") or "")
            break
    return {
        "intent": "spend_total" if current_frame.get("tool") == "summarize_spending" else str(current_frame.get("tool") or ""),
        "tool": str(current_frame.get("tool") or ""),
        "view": str(current_frame.get("view") or ""),
        "subject": {
            "type": resolved_subject_type,
            "canonical": resolved_subject,
            "raw": resolved_subject,
        } if resolved_subject else {},
        "range": str(current_frame.get("range") or (ranges[0] if ranges else "") or ""),
        "requested_output": requested_output,
        "source_step_id": source_step_id,
        "payload": current_frame.get("payload") if isinstance(current_frame.get("payload"), dict) else {},
    }


class _SelectorFailure:
    def __init__(self, error: str):
        self.calls: list[dict[str, Any]] = []
        self.decision = {"error": error, "validation_errors": [error], "calls": []}
        self.raw = ""
        self.status = "clarify"
        self.error = error
        self.family_detail_used = False
        self.repair_used = False
        self.llm_calls = 0
        self.trace = {
            "runtime": _RUNTIME,
            "stage": "selector",
            "status": "clarify",
            "error": error,
            "llm_calls": 0,
        }


__all__ = [
    "build_shadow_trace",
    "run_vnext_shadow",
    "run_vnext_result",
    "run_vnext_stream",
]
