from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from typing import Any

from copilot_agents.dialogue import (
    DialogueAct,
    ContextTurnAct,
    interpret_context_turn as _interpret_context_turn,
    interpret_dialogue_reply as _interpret_dialogue_reply,
)
from mira.turn_state import (
    TurnInterpretation,
    interpret_turn as _interpret_turn_state,
)


CONTROLLER_ACTS = {
    "answer_direct",
    "clarify",
    "ground_entity",
    "execute_action",
    "plan",
    "explain_provenance",
    "cancel",
    "new_task",
}
CONTROLLER_INTENTS = {"chat", "finance", "write", "chart", "overview", "memory"}
_STATE_TTL_SECONDS = 30 * 60

_ACTIVE_CLARIFICATIONS: dict[str, dict[str, Any]] = {}
_LAST_ANSWER_CONTEXTS: dict[str, dict[str, Any]] = {}
_TASK_FRAMES: dict[str, dict[str, Any]] = {}

TASK_FRAME_ACTIONS = {
    "spend_total",
    "transactions",
    "chart",
    "compare",
    "budget_status",
    "explain_metric",
    "write_preview",
    "chat",
}
TASK_FRAME_TURN_ACTS = {
    "answer",
    "ask_clarification",
    "replace_subject",
    "replace_subject_type",
    "replace_range",
    "replace_action",
    "explain_provenance",
    "cancel",
    "reset_context",
    "chat",
}
SLOT_SOURCE_VALUES = {"explicit_user", "prior_context", "controller", "resolver", "default"}


@dataclass(frozen=True)
class ControllerAct:
    act: str
    intent: str
    slots: dict[str, Any] = field(default_factory=dict)
    uses_prior_context: bool = False
    confidence: float = 0.0
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        act = self.act if self.act in CONTROLLER_ACTS else "answer_direct"
        intent = self.intent if self.intent in CONTROLLER_INTENTS else "chat"
        return {
            "act": act,
            "intent": intent,
            "slots": copy.deepcopy(self.slots or {}),
            "uses_prior_context": bool(self.uses_prior_context),
            "confidence": _coerce_confidence(self.confidence),
            "reason": str(self.reason or ""),
        }


def make_controller_act(
    act: str,
    intent: str,
    *,
    slots: dict[str, Any] | None = None,
    uses_prior_context: bool = False,
    confidence: float = 0.0,
    reason: str = "",
) -> ControllerAct:
    return ControllerAct(
        act=act if act in CONTROLLER_ACTS else "answer_direct",
        intent=intent if intent in CONTROLLER_INTENTS else "chat",
        slots=slots or {},
        uses_prior_context=uses_prior_context,
        confidence=_coerce_confidence(confidence),
        reason=reason,
    )


def interpret_dialogue_reply(reply: str, state: dict[str, Any], last_assistant: str = "") -> DialogueAct:
    return _interpret_dialogue_reply(reply, state, last_assistant)


def interpret_context_turn(reply: str, answer_context: dict[str, Any], recent_history: str = "") -> ContextTurnAct:
    return _interpret_context_turn(reply, answer_context, recent_history)


def interpret_turn_state(
    reply: str,
    *,
    answer_context: dict[str, Any] | None = None,
    has_active_dialogue: bool = False,
    now: Any = None,
) -> TurnInterpretation:
    return _interpret_turn_state(
        reply,
        answer_context=answer_context,
        has_active_dialogue=has_active_dialogue,
        now=now,
    )


def active_clarification_from_history(
    profile: str | None,
    history: list[dict] | None,
) -> tuple[dict[str, Any] | None, str, str]:
    """Return the authoritative active clarification state for this turn.

    The server store wins when it matches the latest assistant turn. The
    frontend-carried dialogue_state remains as a compatibility fallback for
    older clients, page refreshes, and direct test calls.
    """
    last_content, frontend_state, _ = _last_assistant_payload(history)
    key = _profile_key(profile)
    stored = _ACTIVE_CLARIFICATIONS.get(key)
    if stored and _is_fresh(stored) and _assistant_text_matches(stored.get("last_assistant"), last_content):
        state = stored.get("state")
        if isinstance(state, dict):
            return copy.deepcopy(state), last_content, "server"
    if isinstance(frontend_state, dict) and frontend_state.get("kind") == "merchant_clarification":
        return copy.deepcopy(frontend_state), last_content, "frontend"
    return None, "", ""


def answer_context_from_history(profile: str | None, history: list[dict] | None) -> dict[str, Any] | None:
    last_content, _, frontend_context = _last_assistant_payload(history)
    key = _profile_key(profile)
    stored = _LAST_ANSWER_CONTEXTS.get(key)
    if stored and _is_fresh(stored) and _assistant_text_matches(stored.get("last_assistant"), last_content):
        context = stored.get("context")
        if isinstance(context, dict):
            return copy.deepcopy(context)
    if isinstance(frontend_context, dict) and frontend_context.get("kind") == "finance_answer_context":
        return copy.deepcopy(frontend_context)
    return answer_context_from_task_frame(profile, history)


def task_frame_from_history(profile: str | None, history: list[dict] | None = None) -> dict[str, Any] | None:
    last_content, _, _ = _last_assistant_payload(history)
    key = _profile_key(profile)
    stored = _TASK_FRAMES.get(key)
    if not stored or not _is_fresh(stored):
        return None
    latest_frame = stored.get("frame") if isinstance(stored.get("frame"), dict) else None
    if not latest_frame:
        return None
    if last_content and not _assistant_text_matches(stored.get("last_assistant"), last_content):
        return None
    return copy.deepcopy(latest_frame)


def answer_context_from_task_frame(profile: str | None, history: list[dict] | None = None) -> dict[str, Any] | None:
    frame = task_frame_from_history(profile, history)
    if not frame:
        return None
    subject_type = str(frame.get("active_subject_type") or "")
    subject = str(frame.get("active_subject") or "")
    if subject_type not in {"merchant", "category"} or not subject:
        return None
    action = str(frame.get("active_action") or "spend_total")
    range_token = str(frame.get("active_range") or "current_month")
    tool_plan = frame.get("last_tool_plan") if isinstance(frame.get("last_tool_plan"), list) else []
    tools = [
        {"name": str(step.get("name") or ""), "args": copy.deepcopy(step.get("args") or {})}
        for step in tool_plan
        if isinstance(step, dict) and step.get("name")
    ]
    if not tools:
        tool_name = "get_merchant_spend" if subject_type == "merchant" else "get_category_spend"
        arg_name = "merchant" if subject_type == "merchant" else "category"
        tools = [{"name": tool_name, "args": {arg_name: subject, "range": range_token}}]
    context = {
        "version": 1,
        "kind": "finance_answer_context",
        "subject_type": subject_type,
        "subject": subject,
        "intent": _intent_for_frame_action(action),
        "operation": action,
        "tool_name": tools[0]["name"] if tools else None,
        "ranges": [range_token] if range_token else [],
        "tools": tools[:4],
    }
    if frame.get("last_provenance_id"):
        context["provenance_id"] = frame.get("last_provenance_id")
    if frame.get("last_domain_action"):
        context["provenance_action"] = frame.get("last_domain_action")
    return context


def record_active_clarification(profile: str | None, state: dict[str, Any] | None, last_assistant: str) -> None:
    if not isinstance(state, dict) or state.get("kind") not in {"merchant_clarification", "entity_type_clarification"}:
        return
    _ACTIVE_CLARIFICATIONS[_profile_key(profile)] = {
        "state": copy.deepcopy(state),
        "last_assistant": _compact(last_assistant),
        "ts": time.time(),
    }


def clear_active_clarification(profile: str | None = None) -> None:
    if profile is None:
        _ACTIVE_CLARIFICATIONS.clear()
        return
    _ACTIVE_CLARIFICATIONS.pop(_profile_key(profile), None)


def record_answer_context(profile: str | None, last_assistant: str, context: dict[str, Any] | None) -> None:
    if not isinstance(context, dict) or context.get("kind") != "finance_answer_context":
        return
    _LAST_ANSWER_CONTEXTS[_profile_key(profile)] = {
        "context": copy.deepcopy(context),
        "last_assistant": _compact(last_assistant),
        "ts": time.time(),
    }
    clear_active_clarification(profile)


def clear_answer_context(profile: str | None = None) -> None:
    if profile is None:
        _LAST_ANSWER_CONTEXTS.clear()
        return
    _LAST_ANSWER_CONTEXTS.pop(_profile_key(profile), None)


def clear_conversation_state(profile: str | None = None) -> None:
    clear_active_clarification(profile)
    clear_answer_context(profile)
    clear_task_frame(profile)
    try:
        from mira import provenance

        provenance.clear(profile)
    except Exception:
        pass


def clear_task_frame(profile: str | None = None) -> None:
    if profile is None:
        _TASK_FRAMES.clear()
        return
    _TASK_FRAMES.pop(_profile_key(profile), None)


def preview_task_frame_for_route(profile: str | None, route: dict[str, Any]) -> dict[str, Any] | None:
    existing = task_frame_from_history(profile)
    frame = _task_frame_from_route(profile, route, existing=existing, result=None)
    return frame


def record_task_frame(
    profile: str | None,
    route: dict[str, Any] | None,
    result: dict[str, Any] | None = None,
    last_assistant: str = "",
) -> dict[str, Any] | None:
    route = route or {}
    result = result or {}
    turn_act = _task_frame_turn_act(route)
    if turn_act in {"chat", "cancel", "reset_context"}:
        if turn_act in {"chat", "cancel", "reset_context"} and route.get("operation") != "explain_grounding":
            clear_task_frame(profile)
        return None
    existing = task_frame_from_history(profile)
    frame = _task_frame_from_route(profile, route, existing=existing, result=result)
    if not frame:
        return existing
    _TASK_FRAMES[_profile_key(profile)] = {
        "frame": copy.deepcopy(frame),
        "last_assistant": _compact(last_assistant),
        "ts": time.time(),
    }
    return copy.deepcopy(frame)


def controller_act_for_dialogue(dialogue_act: DialogueAct, state: dict[str, Any]) -> ControllerAct:
    slots = {
        "subject_slot": state.get("subject_slot") or "merchant",
        "current_candidates": list(state.get("current_candidates") or []),
        "rejected_candidates": list(state.get("rejected_candidates") or []),
        "selected_candidate": dialogue_act.selected_candidate,
        "correction_text": dialogue_act.correction_text,
    }
    if dialogue_act.act in {"confirm", "select_candidate"}:
        return make_controller_act(
            "execute_action",
            "finance",
            slots=slots,
            uses_prior_context=True,
            confidence=dialogue_act.confidence,
            reason=dialogue_act.reason or dialogue_act.act,
        )
    if dialogue_act.act in {"reject", "reject_and_correct"}:
        return make_controller_act(
            "ground_entity",
            "finance",
            slots=slots,
            uses_prior_context=True,
            confidence=dialogue_act.confidence,
            reason=dialogue_act.reason or dialogue_act.act,
        )
    if dialogue_act.act == "new_task":
        return make_controller_act(
            "new_task",
            "finance",
            slots=slots,
            uses_prior_context=False,
            confidence=dialogue_act.confidence,
            reason=dialogue_act.reason or "fresh task",
        )
    if dialogue_act.act == "ask_provenance":
        return make_controller_act(
            "explain_provenance",
            "chat",
            slots=slots,
            uses_prior_context=True,
            confidence=dialogue_act.confidence,
            reason=dialogue_act.reason or "provenance question",
        )
    if dialogue_act.act == "cancel":
        return make_controller_act(
            "cancel",
            "chat",
            slots=slots,
            uses_prior_context=True,
            confidence=dialogue_act.confidence,
            reason=dialogue_act.reason or "cancel clarification",
        )
    return make_controller_act(
        "clarify",
        "finance",
        slots=slots,
        uses_prior_context=True,
        confidence=dialogue_act.confidence,
        reason=dialogue_act.reason or "unclear clarification reply",
    )


def ensure_controller_act(route: dict[str, Any]) -> dict[str, Any]:
    current = route.get("controller_act")
    if isinstance(current, ControllerAct):
        route["controller_act"] = current.as_dict()
        return route
    if isinstance(current, dict) and current.get("act") in CONTROLLER_ACTS:
        route["controller_act"] = make_controller_act(
            str(current.get("act") or "answer_direct"),
            str(current.get("intent") or "chat"),
            slots=current.get("slots") if isinstance(current.get("slots"), dict) else {},
            uses_prior_context=bool(current.get("uses_prior_context")),
            confidence=_coerce_confidence(current.get("confidence")),
            reason=str(current.get("reason") or ""),
        ).as_dict()
        return route
    route["controller_act"] = controller_act_for_route(route).as_dict()
    return route


def controller_act_for_route(route: dict[str, Any]) -> ControllerAct:
    intent = str(route.get("intent") or "chat")
    operation = str(route.get("operation") or intent)
    slots = {
        "operation": operation,
        "tool_name": route.get("tool_name"),
        "args": copy.deepcopy(route.get("args") if isinstance(route.get("args"), dict) else {}),
    }
    if route.get("needs_clarification"):
        clarify_intent = _controller_intent_for_route(intent)
        if isinstance(route.get("dialogue_state"), dict):
            clarify_intent = "finance"
        return make_controller_act(
            "clarify",
            clarify_intent,
            slots=slots,
            uses_prior_context=bool(route.get("uses_history")),
            confidence=_coerce_confidence(route.get("confidence", 1.0)),
            reason=str(route.get("shortcut") or operation or "needs clarification"),
        )
    if operation == "explain_grounding":
        return make_controller_act(
            "explain_provenance",
            "chat",
            slots=slots,
            uses_prior_context=bool(route.get("uses_history")),
            confidence=_coerce_confidence(route.get("confidence", 1.0)),
            reason=str(route.get("shortcut") or "provenance question"),
        )
    if operation in {"context_acknowledge", "local_only_provider", "chat"} or intent == "chat":
        return make_controller_act(
            "answer_direct",
            "chat",
            slots=slots,
            uses_prior_context=bool(route.get("uses_history")),
            confidence=_coerce_confidence(route.get("confidence", 1.0)),
            reason=str(route.get("shortcut") or operation or "direct answer"),
        )
    if operation == "dialogue_cancelled":
        return make_controller_act(
            "cancel",
            "chat",
            slots=slots,
            uses_prior_context=bool(route.get("uses_history")),
            confidence=_coerce_confidence(route.get("confidence", 1.0)),
            reason="cancel clarification",
        )
    if intent == "plan":
        return make_controller_act(
            "plan",
            "finance",
            slots=slots,
            uses_prior_context=bool(route.get("uses_history")),
            confidence=_coerce_confidence(route.get("confidence", 1.0)),
            reason=str(route.get("shortcut") or operation or "plan"),
        )
    return make_controller_act(
        "execute_action",
        _controller_intent_for_route(intent),
        slots=slots,
        uses_prior_context=bool(route.get("uses_history")),
        confidence=_coerce_confidence(route.get("confidence", 1.0)),
        reason=str(route.get("shortcut") or operation or "execute route"),
    )


def finalize_route(route: dict[str, Any], profile: str | None) -> dict[str, Any]:
    route = ensure_controller_act(route)
    turn = route.get("turn_interpretation") if isinstance(route.get("turn_interpretation"), dict) else {}
    if turn.get("reset_finance_context") and route.get("operation") not in {"explain_grounding"}:
        clear_task_frame(profile)
        clear_answer_context(profile)
    if route.get("needs_clarification") and isinstance(route.get("dialogue_state"), dict):
        record_active_clarification(profile, route.get("dialogue_state"), route.get("clarification_question") or "")
    elif (route.get("controller_act") or {}).get("act") in {"execute_action", "plan", "new_task", "cancel"}:
        clear_active_clarification(profile)
    return route


def _task_frame_from_route(
    profile: str | None,
    route: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    result: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(route, dict):
        return None
    action = route.get("domain_action") if isinstance(route.get("domain_action"), dict) else {}
    status = str(action.get("status") or "")
    if status not in {"ready", "clarify"}:
        return None
    turn_act = _task_frame_turn_act(route)
    if turn_act in {"chat", "cancel", "reset_context"}:
        return None

    existing = copy.deepcopy(existing or {})
    validated = action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}
    grounded = action.get("grounded_entities") if isinstance(action.get("grounded_entities"), list) else []
    first_entity = grounded[0] if grounded and isinstance(grounded[0], dict) else {}
    active_subject_type = _first_non_empty(
        validated.get("subject_type"),
        first_entity.get("entity_type"),
        "merchant" if validated.get("merchant") else None,
        "category" if validated.get("category") else None,
        existing.get("active_subject_type"),
    )
    active_subject = _first_non_empty(
        validated.get("subject"),
        validated.get("merchant"),
        validated.get("category"),
        first_entity.get("display_name"),
        first_entity.get("value"),
        existing.get("active_subject"),
    )
    active_range = _first_non_empty(
        validated.get("range"),
        validated.get("range_a"),
        _range_from_tool_plan(action.get("tool_plan")),
        existing.get("active_range"),
        "current_month",
    )
    active_action = _frame_action_for_domain_action(str(action.get("name") or ""), route)
    tool_plan = copy.deepcopy(action.get("tool_plan") if isinstance(action.get("tool_plan"), list) else [])
    provenance = result.get("provenance") if isinstance(result, dict) and isinstance(result.get("provenance"), dict) else {}
    rejected = list(existing.get("rejected_interpretations") or [])
    if status == "clarify":
        rejected.append({
            "turn": route.get("turn_interpretation"),
            "reason": action.get("reason") or route.get("shortcut"),
            "clarification_question": action.get("clarification_question") or route.get("clarification_question"),
        })
    rejected = rejected[-8:]

    slot_sources = _slot_sources(route, first_entity, existing)
    confidence = _slot_confidence(route, first_entity, existing)
    frame = {
        "version": 1,
        "active_subject": active_subject,
        "active_subject_type": active_subject_type,
        "active_range": active_range,
        "active_action": active_action,
        "last_domain_action": action.get("name"),
        "last_tool_plan": tool_plan,
        "last_provenance_id": provenance.get("id") or existing.get("last_provenance_id"),
        "rejected_interpretations": rejected,
        "slot_sources": slot_sources,
        "confidence": confidence,
        "updated_at": time.time(),
        "ttl_seconds": _STATE_TTL_SECONDS,
        "session_id": _profile_key(profile),
        "window_id": None,
        "last_turn_act": turn_act,
    }
    return frame


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _range_from_tool_plan(tool_plan: Any) -> str | None:
    if not isinstance(tool_plan, list):
        return None
    for step in tool_plan:
        args = step.get("args") if isinstance(step, dict) and isinstance(step.get("args"), dict) else {}
        for key in ("range", "range_a", "month"):
            value = args.get(key)
            if value not in (None, "", []):
                return str(value)
    return None


def _frame_action_for_domain_action(action_name: str, route: dict[str, Any]) -> str:
    operation = str(route.get("operation") or "")
    if action_name == "SpendTotal":
        return "spend_total"
    if action_name == "TransactionSearch":
        return "transactions"
    if action_name in {"MonthlyTrend", "NetWorthTrend"}:
        return "chart"
    if action_name == "CompareSpend":
        return "compare"
    if action_name == "BudgetStatus":
        return "budget_status"
    if action_name == "WritePreview":
        return "write_preview"
    if action_name == "ExplainLastAnswer":
        return "explain_metric"
    if operation in TASK_FRAME_ACTIONS:
        return operation
    return "chat"


def _intent_for_frame_action(action: str) -> str:
    if action == "transactions":
        return "transactions"
    if action == "chart":
        return "chart"
    if action in {"compare", "budget_status", "explain_metric"}:
        return "plan"
    if action == "write_preview":
        return "write"
    if action == "spend_total":
        return "spending"
    return "chat"


def _task_frame_turn_act(route: dict[str, Any]) -> str:
    if route.get("needs_clarification") or (route.get("domain_action") or {}).get("status") == "clarify":
        return "ask_clarification"
    operation = str(route.get("operation") or "")
    intent = str(route.get("intent") or "")
    if operation == "explain_grounding":
        return "explain_provenance"
    if operation == "dialogue_cancelled":
        return "cancel"
    if intent == "chat" or operation in {"chat", "context_acknowledge", "local_only_provider"}:
        return "chat"
    turn = route.get("turn_interpretation") if isinstance(route.get("turn_interpretation"), dict) else {}
    kind = str(turn.get("turn_kind") or "")
    if kind == "followup_subject_shift":
        return "replace_subject"
    if kind == "followup_range_shift" or (kind == "correction" and turn.get("replace_range_text")):
        return "replace_range"
    if kind == "followup_action_shift":
        return "replace_action"
    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    if args.get("subject_type") or "subject_type" in args:
        return "replace_subject_type" if route.get("uses_history") else "answer"
    return "answer"


def _slot_sources(route: dict[str, Any], entity: dict[str, Any], existing: dict[str, Any]) -> dict[str, str]:
    turn = route.get("turn_interpretation") if isinstance(route.get("turn_interpretation"), dict) else {}
    sources = dict(existing.get("slot_sources") or {})
    subject_source = "prior_context" if route.get("uses_history") else "explicit_user"
    if entity.get("source") == "resolver" or entity.get("grounded"):
        subject_source = "resolver"
    if _task_frame_turn_act(route) in {"replace_subject", "replace_subject_type"}:
        subject_source = "resolver" if entity else "controller"
    range_source = "prior_context" if route.get("uses_history") else "default"
    if (route.get("args") or {}).get("range") or turn.get("replace_range_text"):
        range_source = "explicit_user" if not route.get("uses_history") else "controller"
    action_source = "prior_context" if route.get("uses_history") else "explicit_user"
    if turn.get("replace_action"):
        action_source = "controller"
    sources.update({
        "active_subject": _valid_slot_source(subject_source),
        "active_subject_type": _valid_slot_source(subject_source),
        "active_range": _valid_slot_source(range_source),
        "active_action": _valid_slot_source(action_source),
    })
    return sources


def _slot_confidence(route: dict[str, Any], entity: dict[str, Any], existing: dict[str, Any]) -> dict[str, float]:
    confidence = dict(existing.get("confidence") or {})
    route_confidence = _coerce_confidence(route.get("confidence", 1.0))
    entity_confidence = _coerce_confidence(entity.get("confidence", route_confidence)) if entity else route_confidence
    confidence.update({
        "active_subject": entity_confidence,
        "active_subject_type": entity_confidence,
        "active_range": 1.0 if (route.get("args") or {}).get("range") else _coerce_confidence(confidence.get("active_range", route_confidence)),
        "active_action": route_confidence,
    })
    return confidence


def _valid_slot_source(value: str) -> str:
    return value if value in SLOT_SOURCE_VALUES else "controller"


def _controller_intent_for_route(intent: str) -> str:
    if intent == "write":
        return "write"
    if intent == "chart":
        return "chart"
    if intent == "overview":
        return "overview"
    if intent in {"spending", "transactions", "drilldown", "plan"}:
        return "finance"
    return "chat"


def _last_assistant_payload(history: list[dict] | None) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
    if not history:
        return "", None, None
    for turn in reversed(history):
        if turn.get("role") != "assistant":
            continue
        content = turn.get("content") or ""
        dialogue_state = turn.get("dialogue_state") if isinstance(turn.get("dialogue_state"), dict) else None
        answer_context = turn.get("answer_context") if isinstance(turn.get("answer_context"), dict) else None
        return content, dialogue_state, answer_context
    return "", None, None


def _assistant_text_matches(stored: Any, latest: str) -> bool:
    stored_text = _compact(str(stored or ""))
    latest_text = _compact(latest)
    return bool(stored_text and latest_text and stored_text == latest_text)


def _compact(text: str) -> str:
    return " ".join(str(text or "").split())


def _profile_key(profile: str | None) -> str:
    return profile or "household"


def _is_fresh(entry: dict[str, Any]) -> bool:
    try:
        return (time.time() - float(entry.get("ts") or 0)) <= _STATE_TTL_SECONDS
    except (TypeError, ValueError):
        return False


def _coerce_confidence(value: Any) -> float:
    try:
        return max(0.0, min(float(value), 1.0))
    except (TypeError, ValueError):
        return 0.0
