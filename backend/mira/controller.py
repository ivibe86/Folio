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
    return None


def record_active_clarification(profile: str | None, state: dict[str, Any] | None, last_assistant: str) -> None:
    if not isinstance(state, dict) or state.get("kind") != "merchant_clarification":
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
    try:
        from mira import provenance

        provenance.clear(profile)
    except Exception:
        pass


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
    if route.get("needs_clarification") and isinstance(route.get("dialogue_state"), dict):
        record_active_clarification(profile, route.get("dialogue_state"), route.get("clarification_question") or "")
    elif (route.get("controller_act") or {}).get("act") in {"execute_action", "plan", "new_task", "cancel"}:
        clear_active_clarification(profile)
    return route


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
