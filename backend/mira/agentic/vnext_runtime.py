from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from range_parser import has_explicit_time_scope, parse_range, words

from mira.agentic.intent_frame import ConversationFrame, MiraIntentFrame, MiraSubject, is_supported_time_token
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
    selector = _apply_pending_reply_selector(selector, history, question)
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
    selector_decision = _apply_pending_replies(selector_decision, history, question)
    intent_frame = selector_decision.get("intent_frame") if isinstance(selector_decision.get("intent_frame"), dict) else {}
    mira_conversation_frame = _merged_conversation_frame_from_decision(selector_decision, history, question=question)
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
        "intent_frame": intent_frame,
        "mira_conversation_frame": mira_conversation_frame.to_dict() if mira_conversation_frame else {},
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
            "intent_frame": intent_frame,
            "mira_conversation_frame": mira_conversation_frame.to_dict() if mira_conversation_frame else {},
            "intent_frame_source": selector_decision.get("intent_frame_source"),
            "intent_frame_error": selector_decision.get("intent_frame_error"),
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
        "error": _user_visible_error(validation) if validation.status == "blocked" else None,
        "route": route,
        "intent": route.get("intent") or "chat",
        "agent_decision": validation.decision.to_dict(),
        "validation": validation.to_dict(),
        "evidence": evidence_summary(evidence),
        "provenance": evidence.provenance,
        "selected_tools": selected_tools,
        "grounded_entities": validation.grounded_entities,
        "pending_clarification": validation.pending_clarification,
        "answer_context": _answer_context_from_validation(validation, evidence, route=route),
        "trace": trace,
        "llm_calls": int(route.get("llm_calls") or 0) + int(getattr(answer_result, "llm_calls", 0) or 0),
        "legacy_router_used": False,
        "answer_guard": {
            "path": getattr(answer_result, "path", ""),
            "used_fallback": bool(getattr(answer_result, "used_fallback", False)),
            "error": getattr(answer_result, "error", ""),
            "cache_hit": bool(getattr(answer_result, "cache_hit", False)),
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
        "answer_cache_hit": bool(getattr(answer_result, "cache_hit", False)),
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
        calls = _compiled_selector_calls(selector=selector, history=history, profile=profile, question=question)
        return validate_selector_calls(
            calls,
            question=question,
            profile=profile,
            history=history,
        )
    except Exception as exc:
        return _validation_failure(str(exc), history=history)


def _compiled_selector_calls(
    *,
    selector: Any,
    history: list[dict] | None,
    profile: str | None = None,
    question: str = "",
) -> list[dict[str, Any]]:
    selector_decision = getattr(selector, "decision", {})
    if not isinstance(selector_decision, dict):
        selector_decision = {}
    selector_decision = _apply_pending_replies(selector_decision, history, question)
    pending_error = str(selector_decision.get("pending_clarification_error") or "").strip()
    if pending_error:
        pending = selector_decision.get("pending_clarification") if isinstance(selector_decision.get("pending_clarification"), dict) else None
        return [_compiler_validation_error(pending_error, pending_clarification=pending)]
    frame = _merged_conversation_frame_from_decision(selector_decision, history, question=question)
    try:
        from mira.agentic.entity_grounder import ground_conversation_frame

        grounded = ground_conversation_frame(frame, profile=profile, source_text=question)
    except Exception as exc:
        selector_decision["entity_grounding_error"] = str(exc)
    else:
        selector_decision["entity_grounding"] = grounded.trace
        if grounded.entities:
            selector_decision["entity_grounding_entities"] = grounded.entities
        if grounded.frame is not None:
            frame = grounded.frame
            selector_decision["grounded_mira_conversation_frame"] = frame.to_dict()
        if grounded.status == "clarify":
            return [
                {
                    "id": "selector_call_1",
                    "name": "summarize_spending",
                    "args": {},
                    "validation_error": grounded.message,
                    "grounded_entities": grounded.entities,
                    "pending_clarification": grounded.pending_clarification,
                }
            ]
    frame = _transaction_evidence_frame_for_question(frame, question)
    if frame is not None:
        selector_decision["grounded_mira_conversation_frame"] = frame.to_dict()
    try:
        from mira.agentic.intent_compiler import compile_selector_decision

        compiled = compile_selector_decision(
            selector_decision,
            frame=frame,
            selector_calls=None,
        )
    except Exception as exc:
        selector_decision["intent_compiler_error"] = str(exc)
        return [_compiler_validation_error("I could not compile that Folio request safely.")]
    selector_decision["intent_compiler"] = compiled.trace
    selector_decision["intent_compiler_status"] = compiled.status
    if compiled.issue:
        selector_decision["intent_compiler_issue"] = compiled.issue
    if compiled.ok:
        selector_decision["compiled_calls"] = compiled.calls
        return compiled.calls
    selector_decision["compiler_fallback_removed"] = True
    return [_compiler_validation_error("I need one more detail to choose the right Folio view.")]


def _compiler_validation_error(
    message: str,
    *,
    pending_clarification: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": "selector_call_1",
        "name": "summarize_spending",
        "args": {},
        "validation_error": message,
        "pending_clarification": pending_clarification or {},
    }


def _apply_pending_reply_selector(selector: Any, history: list[dict] | None, question: str) -> Any:
    decision = getattr(selector, "decision", {})
    if not isinstance(decision, dict):
        return selector
    resolved = _apply_pending_replies(decision, history, question)
    if resolved is decision:
        return selector
    if resolved == decision:
        return selector
    force_tool_status = any(
        bool(resolved.get(key))
        for key in (
            "pending_amount_resolved",
            "pending_entity_resolved",
            "pending_clarification_error",
        )
    )
    if not force_tool_status:
        return selector
    return _SelectorOverride(selector, decision=resolved, status="tool_calls")


def _apply_pending_replies(
    selector_decision: dict[str, Any],
    history: list[dict] | None,
    question: str,
) -> dict[str, Any]:
    resolved = _apply_pending_entity_resolution_reply(selector_decision, history, question)
    return _apply_pending_amount_reply(resolved, history, question)


def _apply_pending_entity_resolution_reply(
    selector_decision: dict[str, Any],
    history: list[dict] | None,
    question: str,
) -> dict[str, Any]:
    pending = _latest_pending_entity_clarification(history)
    if not pending:
        return selector_decision

    resolution = _resolve_pending_entity_choice(pending, question)
    status = resolution.get("status")
    if status == "none":
        return selector_decision
    if status != "resolved":
        frame = _resume_frame_from_pending(pending)
        frame_payload = frame.to_dict() if frame else {}
        return {
            **selector_decision,
            "route": frame_payload.get("route") or "finance",
            "intent": frame_payload.get("intent") or "none",
            "subject": frame_payload.get("subject") or {"kind": "none"},
            "time": frame_payload.get("time") or "none",
            "time_a": frame_payload.get("time_a"),
            "time_b": frame_payload.get("time_b"),
            "output": frame_payload.get("output") or "status",
            "discourse_action": "clarification_reply",
            "answer": "",
            "intent_frame": frame_payload,
            "pending_clarification": pending,
            "pending_clarification_error": resolution.get("message") or _pending_entity_choice_message(pending),
        }

    frame = _resume_frame_from_pending(pending)
    option = resolution.get("option") if isinstance(resolution.get("option"), dict) else {}
    if frame is None or not option:
        return {
            **selector_decision,
            "route": "finance",
            "intent": "none",
            "subject": {"kind": "none"},
            "time": "none",
            "output": "status",
            "discourse_action": "clarification_reply",
            "answer": "",
            "pending_clarification": pending,
            "pending_clarification_error": _pending_entity_choice_message(pending),
        }

    label = _entity_option_display(option)
    selected_subject = MiraSubject(
        kind=str(option.get("type") or "").strip(),
        text=str(option.get("canonical") or label).strip() or None,
        canonical_id=str(option.get("canonical") or "").strip() or None,
        display_name=label or None,
        confidence=_float_or_none(option.get("confidence")),
    )
    resolved_frame = replace(
        frame,
        subject=selected_subject,
        pending_clarification={},
        evidence_stale=False,
        force_reground=False,
    )
    frame_payload = resolved_frame.to_dict()
    return {
        **selector_decision,
        "route": frame_payload.get("route") or "finance",
        "intent": frame_payload.get("intent") or "none",
        "subject": frame_payload.get("subject") or {"kind": selected_subject.kind, "text": selected_subject.text},
        "time": frame_payload.get("time") or "none",
        "time_a": frame_payload.get("time_a"),
        "time_b": frame_payload.get("time_b"),
        "output": frame_payload.get("output") or "status",
        "discourse_action": "clarification_reply",
        "answer": "",
        "intent_frame": frame_payload,
        "grounded_mira_conversation_frame": frame_payload,
        "pending_entity_resolved": True,
        "pending_entity_resolution": {
            "kind": selected_subject.kind,
            "canonical": selected_subject.canonical_id,
            "label": selected_subject.display_name,
            "matched_by": resolution.get("matched_by"),
        },
    }


def _latest_pending_entity_clarification(history: list[dict] | None) -> dict[str, Any]:
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        if not answer_context:
            continue
        pending = answer_context.get("pending_clarification") if isinstance(answer_context.get("pending_clarification"), dict) else {}
        if pending.get("kind") == "entity_resolution":
            return pending
        return {}
    return {}


def _resume_frame_from_pending(pending: dict[str, Any]) -> ConversationFrame | None:
    payload = pending.get("resume_frame") if isinstance(pending.get("resume_frame"), dict) else {}
    if not payload:
        return None
    try:
        return ConversationFrame.from_dict(payload)
    except ValueError:
        return None


def _resolve_pending_entity_choice(pending: dict[str, Any], question: str) -> dict[str, Any]:
    options = _pending_entity_options(pending)
    choice = _normalized_choice(question)
    if not choice:
        return {"status": "none"}
    if choice in {"all", "both", "everything"}:
        return {
            "status": "unsupported",
            "message": "I can only use one match here. Please pick one option, like `category`, `merchant`, or `1`.",
        }

    by_exact: dict[str, list[dict[str, Any]]] = {}
    for option in options:
        for candidate in _option_match_values(option):
            by_exact.setdefault(candidate, []).append(option)
    if choice in by_exact:
        matches = _unique_options(by_exact[choice])
        if len(matches) == 1:
            return {"status": "resolved", "option": matches[0], "matched_by": "exact"}
        return {"status": "ambiguous", "message": _pending_entity_choice_message(pending)}

    ordinal = _choice_ordinal(choice)
    if ordinal is not None:
        if 0 <= ordinal < len(options):
            return {"status": "resolved", "option": options[ordinal], "matched_by": "ordinal"}
        return {"status": "ambiguous", "message": _pending_entity_choice_message(pending)}

    type_choice = _choice_entity_type(choice)
    if type_choice:
        matches = [option for option in options if str(option.get("type") or "").strip() == type_choice]
        if len(matches) == 1:
            return {"status": "resolved", "option": matches[0], "matched_by": "type"}
        return {"status": "ambiguous", "message": _pending_entity_choice_message(pending)}

    if _looks_like_short_clarification_reply(choice):
        return {"status": "ambiguous", "message": _pending_entity_choice_message(pending)}
    return {"status": "none"}


def _pending_entity_options(pending: dict[str, Any]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for option in pending.get("options") or []:
        if not isinstance(option, dict):
            continue
        entity_type = str(option.get("type") or "").strip()
        if entity_type not in {"account", "category", "merchant"}:
            continue
        canonical = str(option.get("canonical") or option.get("id") or option.get("label") or "").strip()
        if not canonical:
            continue
        options.append(option)
    return options


def _option_match_values(option: dict[str, Any]) -> set[str]:
    values = {
        _normalized_choice(option.get("id")),
        _normalized_choice(option.get("canonical")),
        _normalized_choice(option.get("label")),
        _normalized_choice(_entity_option_display(option)),
    }
    entity_type = str(option.get("type") or "").strip()
    canonical = str(option.get("canonical") or "").strip()
    label = _entity_option_display(option)
    if entity_type and canonical:
        values.add(_normalized_choice(f"{canonical} {entity_type}"))
    if entity_type and label:
        values.add(_normalized_choice(f"{label} {entity_type}"))
    return {value for value in values if value}


def _entity_option_display(option: dict[str, Any]) -> str:
    label = str(option.get("label") or "").strip()
    entity_type = str(option.get("type") or "").strip()
    if label and entity_type and label.lower().endswith(f" {entity_type}".lower()):
        return label[: -len(entity_type)].strip()
    return label or str(option.get("canonical") or option.get("id") or "").strip()


def _unique_options(options: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for option in options:
        key = str(option.get("id") or f"{option.get('type')}:{option.get('canonical')}").strip().lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(option)
    return unique


def _normalized_choice(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    for char in "`'\".,!?()[]{}":
        text = text.replace(char, " ")
    return " ".join(text.replace("_", " ").split())


def _choice_ordinal(choice: str) -> int | None:
    ordinal_words = {
        "1": 0,
        "one": 0,
        "first": 0,
        "2": 1,
        "two": 1,
        "second": 1,
        "3": 2,
        "three": 2,
        "third": 2,
        "4": 3,
        "four": 3,
        "fourth": 3,
        "5": 4,
        "five": 4,
        "fifth": 4,
    }
    filler = {"the", "option", "choice", "number", "one", "use", "pick", "select"}
    tokens = choice.split()
    non_filler_ordinals = {
        token
        for token in tokens
        if token in ordinal_words and token not in {"one"}
    }
    found = [
        ordinal_words[token]
        for token in tokens
        if token in ordinal_words and (token != "one" or not non_filler_ordinals)
    ]
    if len(found) != 1:
        return None
    if any(token not in ordinal_words and token not in filler for token in tokens):
        return None
    return found[0]


def _choice_entity_type(choice: str) -> str:
    filler = {"the", "one", "option", "choice", "use", "pick", "select"}
    tokens = choice.split()
    entity_types = [token for token in tokens if token in {"account", "category", "merchant"}]
    if len(entity_types) != 1:
        return ""
    if any(token not in filler and token not in {"account", "category", "merchant"} for token in tokens):
        return ""
    return entity_types[0]


def _looks_like_short_clarification_reply(choice: str) -> bool:
    tokens = choice.split()
    return 0 < len(tokens) <= 4


def _pending_entity_choice_message(pending: dict[str, Any]) -> str:
    options = _pending_entity_options(pending)
    labels = [str(option.get("label") or _entity_option_display(option) or "").strip() for option in options]
    labels = [label for label in labels if label]
    if labels:
        return "Please pick one match: " + ", ".join(labels[:5]) + "."
    raw = str(pending.get("raw") or "that").strip()
    return f"I still need the exact merchant, category, or account for `{raw}`."


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _apply_pending_amount_reply(
    selector_decision: dict[str, Any],
    history: list[dict] | None,
    question: str,
) -> dict[str, Any]:
    pending = _latest_pending_amount_clarification(history)
    amount = _standalone_amount_reply(question)
    if not pending or amount is None:
        return selector_decision

    prior_frame = _latest_mira_conversation_frame(history)
    if prior_frame is None or prior_frame.intent != "affordability":
        return selector_decision

    frame_payload = prior_frame.to_dict()
    frame_payload.update(
        {
            "route": "finance",
            "intent": "affordability",
            "subject": {"kind": "none"},
            "output": "status",
            "discourse_action": "clarification_reply",
            "answer": "",
        }
    )
    details = selector_decision.get("details") if isinstance(selector_decision.get("details"), dict) else {}
    details = dict(details)
    details["amount"] = amount
    if pending.get("purpose") not in (None, "", [], {}) and details.get("purpose") in (None, "", [], {}):
        details["purpose"] = pending.get("purpose")

    return {
        **selector_decision,
        "route": "finance",
        "intent": "affordability",
        "subject": {"kind": "none", "text": None},
        "time": frame_payload.get("time") or "none",
        "time_a": frame_payload.get("time_a"),
        "time_b": frame_payload.get("time_b"),
        "output": "status",
        "discourse_action": "clarification_reply",
        "answer": "",
        "details": details,
        "intent_frame": frame_payload,
        "pending_amount_resolved": True,
    }


def _latest_pending_amount_clarification(history: list[dict] | None) -> dict[str, Any]:
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        pending = answer_context.get("pending_clarification") if isinstance(answer_context.get("pending_clarification"), dict) else {}
        if pending.get("kind") == "missing_slot" and pending.get("slot") == "amount":
            return pending
    return {}


def _standalone_amount_reply(question: str) -> float | None:
    text = " ".join(str(question or "").replace(",", "").split())
    if not text:
        return None
    cleaned = text[1:] if text.startswith("$") else text
    if cleaned.count(".") > 1:
        return None
    try:
        amount = float(cleaned)
    except ValueError:
        return None
    if amount <= 0:
        return None
    return amount


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
        return _user_visible_error(validation)
    if validation.status == "blocked":
        return _user_visible_error(validation)
    if status == "tool_calls" and selected_tools and evidence.tool_results:
        return f"Mira vNext ran {', '.join(selected_tools)} and collected evidence. Final evidence-grounded answering comes next."
    if status == "tool_calls" and selected_tools:
        return f"Mira vNext selected {', '.join(selected_tools)}. Tool execution comes next."
    if status == "general_answer":
        return "Mira vNext routed this as a general answer. General answer generation comes next."
    return _ANSWER


def _user_visible_error(validation: ValidationResult) -> str:
    try:
        from mira.agentic.vnext_answerer import safe_validation_answer

        answer = safe_validation_answer(validation)
        if answer:
            return answer
    except Exception:
        pass
    if validation.status == "clarify":
        return "I need one more detail to choose the right Folio tool."
    return "I could not safely run that request."


def _merged_conversation_frame_from_decision(
    selector_decision: dict[str, Any],
    history: list[dict] | None,
    *,
    question: str = "",
) -> ConversationFrame | None:
    if not isinstance(selector_decision, dict):
        return None
    grounded_payload = selector_decision.get("grounded_mira_conversation_frame")
    if isinstance(grounded_payload, dict) and grounded_payload:
        try:
            return ConversationFrame.from_dict(grounded_payload)
        except ValueError:
            pass
    intent_frame = _intent_frame_from_decision(selector_decision)
    if intent_frame is None:
        return None
    prior = _latest_mira_conversation_frame(history)
    if prior is not None:
        intent_frame = _contextual_finance_frame(intent_frame, prior)
    try:
        frame = ConversationFrame.merge(prior, intent_frame)
    except ValueError:
        frame = ConversationFrame.from_intent_frame(intent_frame)
    frame = _repair_subject_only_followup_range(
        frame=frame,
        prior=prior,
        intent_frame=intent_frame,
        question=question,
    )
    return _repair_explicit_time_from_question(frame=frame, intent_frame=intent_frame, question=question)


def _contextual_finance_frame(intent_frame: MiraIntentFrame, prior: ConversationFrame) -> MiraIntentFrame:
    if intent_frame.route != "chat" or prior.route not in {"finance", "write_preview", "memory"}:
        return intent_frame
    has_context_slot = (
        not intent_frame.subject.is_empty
        or intent_frame.time != "none"
        or intent_frame.output != "none"
        or intent_frame.intent != "none"
    )
    if not has_context_slot:
        return intent_frame
    payload = intent_frame.to_dict()
    payload["route"] = prior.route
    if payload.get("intent") == "none":
        payload["intent"] = prior.intent
    if payload.get("output") == "none":
        payload["output"] = prior.output
    payload["discourse_action"] = "follow_up"
    payload["answer"] = ""
    try:
        return MiraIntentFrame.from_dict(payload)
    except ValueError:
        return intent_frame


def _repair_subject_only_followup_range(
    *,
    frame: ConversationFrame,
    prior: ConversationFrame | None,
    intent_frame: MiraIntentFrame,
    question: str,
) -> ConversationFrame:
    if prior is None:
        return frame
    if intent_frame.time not in {"month_before_prior", "next_month_after_prior"}:
        return frame
    if intent_frame.subject.is_empty:
        return frame
    if has_explicit_time_scope(question):
        return frame
    return replace(frame, time=prior.time, time_a=prior.time_a, time_b=prior.time_b)


def _repair_explicit_time_from_question(
    *,
    frame: ConversationFrame,
    intent_frame: MiraIntentFrame,
    question: str,
) -> ConversationFrame:
    parsed = parse_range(question)
    token = str(parsed.token or "").strip()
    if parsed.explicit and parsed.unsupported_reason:
        return replace(frame, time="custom", time_a=None, time_b=None)
    if not parsed.explicit or not token:
        return frame
    if intent_frame.time in {"month_before_prior", "next_month_after_prior"}:
        return frame
    current_range = _range_token_from_frame(frame)
    if current_range == token:
        return frame
    should_repair = intent_frame.time in {"none", "custom"} or frame.time in {"none", "custom"}
    if not should_repair and token not in {"current_month", "this_month"}:
        should_repair = True
    if not should_repair:
        return frame
    repaired = _frame_time_from_range_token(token)
    if repaired is None:
        return replace(frame, time="custom", time_a=None, time_b=None)
    time_value, time_a, time_b = repaired
    return replace(frame, time=time_value, time_a=time_a, time_b=time_b)


def _frame_time_from_range_token(token: str) -> tuple[str, str | None, str | None] | None:
    value = str(token or "").strip().lower()
    aliases = {
        "all": "all_time",
        "current": "this_month",
        "current_month": "this_month",
        "prior": "last_month",
        "prior_month": "last_month",
        "previous_month": "last_month",
    }
    value = aliases.get(value, value)
    if len(value) == 7 and value[4] == "-":
        try:
            int(value[:4])
            month = int(value[5:7])
        except ValueError:
            return None
        if 1 <= month <= 12:
            return "custom", f"{value}-01", None
        return None
    if is_supported_time_token(value):
        return value, None, None
    return None


def _range_token_from_frame(frame: ConversationFrame) -> str:
    token = str(frame.time or "").strip().lower()
    if token == "custom" and frame.time_a:
        month = str(frame.time_a)[:7]
        if len(month) == 7 and month[4] == "-":
            return month
        return ""
    aliases = {
        "all_time": "all",
        "this_month": "current_month",
    }
    return aliases.get(token, token)


def _transaction_evidence_frame_for_question(frame: ConversationFrame | None, question: str) -> ConversationFrame | None:
    if frame is None or frame.route != "finance":
        return frame
    if frame.intent not in {"spending_total", "spending_breakdown"}:
        return frame
    if frame.subject.is_empty:
        return frame
    token_set = set(words(question))
    if not ({"why", "when"} & token_set):
        return frame
    return replace(frame, intent="spending_explain", output="table")


def _latest_mira_conversation_frame(history: list[dict] | None) -> ConversationFrame | None:
    for turn in reversed(history or []):
        if not isinstance(turn, dict):
            continue
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else {}
        if not answer_context:
            continue
        try:
            frame = ConversationFrame.from_answer_context(answer_context)
        except ValueError:
            frame = None
        if frame:
            return frame
        legacy = answer_context.get("conversation_frame") if isinstance(answer_context.get("conversation_frame"), dict) else {}
        intent = _intent_frame_from_legacy_conversation_frame(legacy, fallback={})
        if intent:
            return ConversationFrame.from_intent_frame(intent)
    return None


def _intent_frame_from_decision(selector_decision: dict[str, Any]) -> MiraIntentFrame | None:
    compiled = selector_decision.get("compiled_conversation_frame")
    if isinstance(compiled, dict) and compiled:
        frame = _intent_frame_from_legacy_conversation_frame(compiled, fallback=selector_decision)
        if frame:
            return frame
    payload = selector_decision.get("intent_frame") if isinstance(selector_decision.get("intent_frame"), dict) else {}
    if not payload:
        payload = selector_decision
    try:
        return MiraIntentFrame.from_dict(payload)
    except ValueError:
        return None


def _conversation_frame_from_route(route: dict[str, Any]) -> ConversationFrame | None:
    payload = route.get("mira_conversation_frame") if isinstance(route.get("mira_conversation_frame"), dict) else {}
    if not payload:
        return None
    try:
        return ConversationFrame.from_dict(payload)
    except ValueError:
        return None


def _answer_context_from_validation(
    validation: ValidationResult,
    evidence: EvidencePacket,
    route: dict[str, Any] | None = None,
) -> dict | None:
    route = route if isinstance(route, dict) else {}
    route_frame = _conversation_frame_from_route(route)
    if validation.status == "clarify" and validation.pending_clarification:
        context = {
            "version": 2,
            "kind": "finance_pending_clarification",
            "pending_clarification": validation.pending_clarification,
            "agentic": True,
            "runtime": _RUNTIME,
        }
        if route_frame:
            context["mira_conversation_frame"] = route_frame.to_dict()
        return context
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
    legacy_conversation_frame = _conversation_frame_from_answer_context(
        current_frame=current_frame,
        subject_type=subject_type,
        subject=subject,
        ranges=ranges,
        tools=tools,
    )
    mira_conversation_frame = _mira_conversation_frame_from_answer_context(
        route_frame=route_frame,
        legacy_frame=legacy_conversation_frame,
        tools=tools,
        evidence=evidence,
    )
    return {
        "version": 2,
        "kind": "finance_answer_context",
        "subject_type": subject_type,
        "subject": subject,
        "ranges": ranges,
        "tools": tools,
        "mira_conversation_frame": mira_conversation_frame.to_dict() if mira_conversation_frame else {},
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


def _mira_conversation_frame_from_answer_context(
    *,
    route_frame: ConversationFrame | None,
    legacy_frame: dict[str, Any],
    tools: list[dict[str, Any]],
    evidence: EvidencePacket,
) -> ConversationFrame | None:
    frame = route_frame
    if frame is None:
        intent = _intent_frame_from_legacy_conversation_frame(legacy_frame, fallback={})
        frame = ConversationFrame.from_intent_frame(intent) if intent else None
    if frame is None:
        return None

    payload = frame.to_dict()
    legacy_subject = _subject_from_legacy_conversation_frame(legacy_frame)
    current_subject = payload.get("subject") if isinstance(payload.get("subject"), dict) else {}
    if legacy_subject and not str(current_subject.get("text") or current_subject.get("canonical_id") or "").strip():
        payload["subject"] = legacy_subject.to_dict()

    legacy_range = str(legacy_frame.get("range") or "").strip()
    if legacy_range and payload.get("time") == "none":
        time_value, time_a, time_b = _time_from_legacy_range(legacy_range)
        payload["time"] = time_value
        payload["time_a"] = time_a
        payload["time_b"] = time_b

    source_step_id = str(legacy_frame.get("source_step_id") or "").strip()
    if not source_step_id:
        for tool in tools:
            if str(tool.get("name") or "") != "make_chart":
                source_step_id = str(tool.get("id") or "")
                break
    if source_step_id:
        payload["last_evidence_step_id"] = source_step_id

    backend_tool = _backend_tool_from_evidence(evidence)
    if backend_tool:
        payload["last_backend_tool"] = backend_tool

    try:
        return ConversationFrame.from_dict(payload)
    except ValueError:
        return frame


def _backend_tool_from_evidence(evidence: EvidencePacket) -> str:
    for record in evidence.tool_results:
        if not isinstance(record, dict):
            continue
        name = str(record.get("execution_tool_name") or record.get("tool_name") or record.get("tool") or "").strip()
        if name and name != "plot_chart":
            return name
    return ""


def _intent_frame_from_legacy_conversation_frame(
    legacy_frame: dict[str, Any],
    *,
    fallback: dict[str, Any],
) -> MiraIntentFrame | None:
    if not isinstance(legacy_frame, dict) or not legacy_frame:
        return None
    tool = str(legacy_frame.get("tool") or "").strip()
    view = str(legacy_frame.get("view") or "").strip()
    intent = _intent_from_legacy_tool_view(tool=tool, view=view, raw_intent=str(legacy_frame.get("intent") or fallback.get("intent") or ""))
    subject = _subject_from_legacy_conversation_frame(legacy_frame)
    time_value, time_a, time_b = _time_from_legacy_range(str(legacy_frame.get("range") or "").strip())
    output = _output_from_legacy_frame(legacy_frame)
    discourse_action = _discourse_action_from_selector_fallback(fallback)
    try:
        return MiraIntentFrame.from_dict(
            {
                "route": "finance" if tool else str(fallback.get("route") or "finance"),
                "intent": intent,
                "subject": subject.to_dict() if subject else {"kind": "none"},
                "time": time_value,
                "time_a": time_a,
                "time_b": time_b,
                "output": output,
                "chart_type": "line" if output == "chart" else None,
                "discourse_action": discourse_action,
            }
        )
    except ValueError:
        return None


def _intent_from_legacy_tool_view(*, tool: str, view: str, raw_intent: str) -> str:
    aliases = {
        "spend_total": "spending_total",
        "spending": "spending_total",
        "budget": "budget_status",
        "net_worth": "net_worth_trend",
    }
    raw = aliases.get(str(raw_intent or "").strip().lower(), str(raw_intent or "").strip().lower())
    if raw in {
        "affordability",
        "budget_plan",
        "budget_status",
        "cashflow_forecast",
        "cashflow_shortfall",
        "data_health",
        "enrichment_quality",
        "explain_metric",
        "explain_transaction",
        "finance_priorities",
        "finance_snapshot",
        "low_confidence_transactions",
        "memory_op",
        "net_worth_balance",
        "net_worth_delta",
        "net_worth_trend",
        "none",
        "recurring_changes",
        "recurring_summary",
        "savings_capacity",
        "spending_breakdown",
        "spending_compare",
        "spending_explain",
        "spending_top",
        "spending_total",
        "spending_trend",
        "transaction_lookup",
        "write_preview",
    }:
        return raw
    tool = str(tool or "").strip()
    view = str(view or "").strip()
    if tool == "summarize_spending":
        return {
            "top": "spending_top",
            "breakdown": "spending_breakdown",
            "trend": "spending_trend",
            "compare": "spending_compare",
        }.get(view, "spending_total")
    if tool == "query_transactions":
        return "transaction_lookup"
    if tool == "review_budget":
        return "savings_capacity" if view == "savings_capacity" else "budget_status"
    if tool == "review_cashflow":
        return "cashflow_shortfall" if view == "shortfall" else "cashflow_forecast"
    if tool == "review_recurring":
        return "recurring_changes" if view == "changes" else "recurring_summary"
    if tool == "review_net_worth":
        return {"balances": "net_worth_balance", "delta": "net_worth_delta"}.get(view, "net_worth_trend")
    if tool == "review_data_quality":
        return {
            "enrichment_summary": "enrichment_quality",
            "low_confidence": "low_confidence_transactions",
            "explain_transaction": "explain_transaction",
        }.get(view, "data_health")
    if tool == "check_affordability":
        return "affordability"
    if tool == "preview_finance_change":
        return "write_preview"
    return "none"


def _subject_from_legacy_conversation_frame(legacy_frame: dict[str, Any]) -> MiraSubject | None:
    subject = legacy_frame.get("subject") if isinstance(legacy_frame.get("subject"), dict) else {}
    kind = str(subject.get("type") or subject.get("kind") or subject.get("type_hint") or "").strip().lower()
    text = str(subject.get("canonical") or subject.get("text") or subject.get("raw") or "").strip()
    if not kind or kind not in {"merchant", "category", "account", "transaction", "metric", "net_worth", "self", "unknown"}:
        filters = legacy_frame.get("filters") if isinstance(legacy_frame.get("filters"), dict) else {}
        for candidate in ("merchant", "category", "account"):
            value = str(filters.get(candidate) or "").strip()
            if value:
                kind = candidate
                text = value
                break
    if not kind and not text:
        return None
    return MiraSubject(kind=kind or "unknown", text=text or None, canonical_id=text or None, display_name=text or None)


def _time_from_legacy_range(range_value: str) -> tuple[str, str | None, str | None]:
    token = str(range_value or "").strip().lower()
    aliases = {
        "": "none",
        "all": "all_time",
        "current": "this_month",
        "current_month": "this_month",
        "prior_month": "last_month",
        "previous_month": "last_month",
    }
    token = aliases.get(token, token)
    if token in {
        "all_time",
        "last_30d",
        "last_365d",
        "last_3_months",
        "last_6_months",
        "last_7d",
        "last_90d",
        "last_month",
        "last_week",
        "last_year",
        "month_before_prior",
        "next_month_after_prior",
        "none",
        "this_month",
        "this_week",
        "ytd",
    }:
        return token, None, None
    if len(token) == 7 and token[4] == "-":
        year, month = token.split("-", 1)
        if year.isdigit() and month.isdigit() and 1 <= int(month) <= 12:
            return "custom", f"{token}-01", None
    return "custom" if token else "none", None, None


def _output_from_legacy_frame(legacy_frame: dict[str, Any]) -> str:
    requested = str(legacy_frame.get("requested_output") or "").strip().lower()
    return {
        "scalar_total": "scalar",
        "summary": "status",
        "rows": "table",
    }.get(requested, "chart" if str(legacy_frame.get("tool") or "") == "make_chart" else "status")


def _discourse_action_from_selector_fallback(fallback: dict[str, Any]) -> str:
    intent_frame = fallback.get("intent_frame") if isinstance(fallback.get("intent_frame"), dict) else {}
    action = str(intent_frame.get("discourse_action") or "").strip().lower()
    if action in {"clarification_reply", "clear", "correction", "follow_up", "new", "refine"}:
        return action
    patch = fallback.get("frame_patch") if isinstance(fallback.get("frame_patch"), dict) else {}
    frame_action = str(patch.get("frame_action") or "").strip().lower()
    if frame_action == "clarification_reply":
        return "clarification_reply"
    if frame_action == "patch_prior":
        return "follow_up"
    return "new"


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


class _SelectorOverride:
    def __init__(self, selector: Any, *, decision: dict[str, Any], status: str):
        self.calls = list(getattr(selector, "calls", []) or [])
        self.decision = decision
        self.raw = str(getattr(selector, "raw", "") or "")
        self.status = status
        self.error = str(getattr(selector, "error", "") or "")
        self.family_detail_used = bool(getattr(selector, "family_detail_used", False))
        self.repair_used = bool(getattr(selector, "repair_used", False))
        self.llm_calls = int(getattr(selector, "llm_calls", 0) or 0)
        trace = getattr(selector, "trace", {})
        self.trace = {**(trace if isinstance(trace, dict) else {}), "pending_reply_override": True}


__all__ = [
    "build_shadow_trace",
    "run_vnext_shadow",
    "run_vnext_result",
    "run_vnext_stream",
]
