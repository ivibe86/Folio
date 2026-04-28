from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import llm_client
from range_parser import contains, words


VALID_DIALOGUE_ACTS = {
    "confirm",
    "select_candidate",
    "reject",
    "reject_and_correct",
    "new_task",
    "ask_provenance",
    "cancel",
    "unclear",
}
VALID_CONTEXT_TURN_ACTS = {
    "same_subject_spend",
    "same_subject_transactions",
    "same_subject_plan",
    "same_subject_chart",
    "explain_grounding",
    "answer_directly",
    "new_task",
    "unclear",
}

_NEGATIVE_TOKENS = {"no", "nope", "nah", "wrong", "different", "other"}
_CANCEL_TOKENS = {"cancel", "stop", "nevermind", "forget"}
_CONFIRM_TOKENS = {"yes", "yeah", "yep", "correct", "right", "sure", "ok", "okay"}
_CORRECTION_STOP_TOKENS = {
    "a",
    "an",
    "actually",
    "another",
    "bro",
    "different",
    "dont",
    "do",
    "i",
    "it",
    "man",
    "merchant",
    "nah",
    "no",
    "nope",
    "not",
    "one",
    "other",
    "that",
    "the",
    "this",
    "use",
    "wrong",
}
_TASK_TOKENS = {
    "budget",
    "category",
    "chart",
    "compare",
    "goal",
    "groceries",
    "how",
    "merchant",
    "move",
    "plot",
    "rename",
    "show",
    "spend",
    "spending",
    "spent",
    "transaction",
    "transactions",
}


@dataclass(frozen=True)
class DialogueAct:
    act: str
    selected_candidate: str = ""
    correction_text: str = ""
    rejected_candidates: tuple[str, ...] = ()
    confidence: float = 0.0
    reason: str = ""


@dataclass(frozen=True)
class ContextTurnAct:
    act: str
    confidence: float = 0.0
    reason: str = ""


def _jsonish(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coerce_confidence(value: Any) -> float:
    try:
        return max(0.0, min(float(value), 1.0))
    except (TypeError, ValueError):
        return 0.0


def _clean_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    cleaned: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return tuple(cleaned)


def _candidate_tokens(name: str) -> set[str]:
    return {
        token for token in words(name)
        if token not in {"and", "the", "of", "for", "at", "to", "in", "on", "my"}
    }


def _candidate_from_reply(reply: str, candidates: list[str]) -> str:
    tokens = words(reply)
    token_set = set(tokens)
    if not candidates:
        return ""

    ordinal_map = {
        "first": 0,
        "1": 0,
        "second": 1,
        "2": 1,
        "third": 2,
        "3": 2,
    }
    for token in tokens:
        idx = ordinal_map.get(token)
        if idx is not None and idx < len(candidates):
            return candidates[idx]

    compact_reply = "".join(tokens)
    exact_matches: list[str] = []
    token_matches: list[str] = []
    for candidate in candidates:
        candidate_tokens = _candidate_tokens(candidate)
        compact_candidate = "".join(words(candidate))
        if compact_candidate and compact_candidate in compact_reply:
            exact_matches.append(candidate)
            continue
        if candidate_tokens and candidate_tokens <= token_set:
            exact_matches.append(candidate)
            continue
        if candidate_tokens and token_set & candidate_tokens:
            token_matches.append(candidate)

    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(token_matches) == 1 and not (token_set & _NEGATIVE_TOKENS):
        return token_matches[0]
    return ""


def _looks_like_grounding_question(reply: str) -> bool:
    tokens = words(reply)
    token_set = set(tokens)
    if (
        contains(tokens, ("how", "did", "you", "get", "that"))
        or contains(tokens, ("where", "did", "that", "come", "from"))
        or contains(tokens, ("where", "did", "you", "get", "that"))
    ):
        return True
    asks_how = "how" in token_set or contains(tokens, ("where", "did"))
    info_terms = {"information", "info", "numbers", "number", "data", "source", "sources", "transactions", "proof"}
    get_terms = {"get", "got", "find", "derive", "calculate", "computed", "answer"}
    return asks_how and bool(token_set & info_terms) and bool(token_set & get_terms)


def _looks_like_new_task(reply: str) -> bool:
    token_set = set(words(reply))
    if token_set & _NEGATIVE_TOKENS:
        return False
    return bool(token_set & _TASK_TOKENS)


def _correction_text(reply: str, candidates: list[str]) -> str:
    candidate_token_set: set[str] = set()
    for candidate in candidates:
        candidate_token_set.update(_candidate_tokens(candidate))
    kept = [
        token for token in words(reply)
        if token not in _CORRECTION_STOP_TOKENS and token not in candidate_token_set
    ]
    return " ".join(kept).strip()


def _fallback_dialogue_act(reply: str, state: dict[str, Any]) -> DialogueAct:
    tokens = words(reply)
    token_set = set(tokens)
    candidates = [str(item) for item in (state.get("current_candidates") or []) if str(item or "").strip()]

    if _looks_like_grounding_question(reply):
        return DialogueAct("ask_provenance", confidence=0.9, reason="grounding question")
    if token_set & _CANCEL_TOKENS:
        return DialogueAct("cancel", confidence=0.9, reason="cancel request")

    selected = _candidate_from_reply(reply, candidates)
    has_negative = bool(token_set & _NEGATIVE_TOKENS or contains(tokens, ("not", "that")))
    if has_negative:
        correction = _correction_text(reply, candidates)
        if correction:
            return DialogueAct(
                "reject_and_correct",
                correction_text=correction,
                rejected_candidates=tuple(candidates),
                confidence=0.75,
                reason="negative reply with replacement clue",
            )
        return DialogueAct(
            "reject",
            rejected_candidates=tuple(candidates),
            confidence=0.75,
            reason="negative reply",
        )

    if selected:
        return DialogueAct("select_candidate", selected_candidate=selected, confidence=0.8, reason="candidate mention")

    if token_set <= _CONFIRM_TOKENS and len(candidates) == 1:
        return DialogueAct("confirm", selected_candidate=candidates[0], confidence=0.8, reason="single-candidate confirmation")

    if not candidates and tokens and not _looks_like_new_task(reply):
        return DialogueAct("reject_and_correct", correction_text=reply.strip(), confidence=0.65, reason="merchant clue")

    if _looks_like_new_task(reply):
        return DialogueAct("new_task", confidence=0.7, reason="fresh task wording")

    return DialogueAct("unclear", confidence=0.3, reason="ambiguous clarification reply")


def _act_from_json(parsed: dict[str, Any], candidates: list[str]) -> DialogueAct | None:
    act = str(parsed.get("act") or "").strip().lower()
    if act not in VALID_DIALOGUE_ACTS:
        return None
    selected = str(parsed.get("selected_candidate") or "").strip()
    if selected and selected not in candidates:
        selected = ""
    correction = str(parsed.get("correction_text") or "").strip()
    rejected = _clean_list(parsed.get("rejected_candidates"))
    return DialogueAct(
        act=act,
        selected_candidate=selected,
        correction_text=correction,
        rejected_candidates=rejected,
        confidence=_coerce_confidence(parsed.get("confidence", 0.0)),
        reason=str(parsed.get("reason") or "").strip(),
    )


def _context_act_from_json(parsed: dict[str, Any]) -> ContextTurnAct | None:
    act = str(parsed.get("act") or "").strip().lower()
    if act not in VALID_CONTEXT_TURN_ACTS:
        return None
    return ContextTurnAct(
        act=act,
        confidence=_coerce_confidence(parsed.get("confidence", 0.0)),
        reason=str(parsed.get("reason") or "").strip(),
    )


def interpret_dialogue_reply(reply: str, state: dict[str, Any], last_assistant: str = "") -> DialogueAct:
    candidates = [str(item) for item in (state.get("current_candidates") or []) if str(item or "").strip()]
    try:
        if llm_client.is_available():
            prompt = f"""You interpret a user's reply to an active Mira clarification.
Return JSON only:
{{"act":"confirm|select_candidate|reject|reject_and_correct|new_task|ask_provenance|cancel|unclear","selected_candidate":string,"correction_text":string,"rejected_candidates":[string],"confidence":number,"reason":"short"}}

Rules:
- Interpret the user's conversational act in context; do not answer the finance question.
- If the user rejects the current candidate and gives a new merchant clue, use act=reject_and_correct and put only the new clue in correction_text.
- If the user rejects without a new clue, use act=reject.
- If the user asks how the prior answer was derived, use act=ask_provenance.
- If the user changes to a fresh task, use act=new_task.
- selected_candidate must be copied exactly from current_candidates, or be empty.
- Do not invent merchants.

Dialogue state:
{json.dumps(state, ensure_ascii=True)}

Last assistant clarification:
{last_assistant}

Latest user reply:
{reply}

JSON:"""
            parsed = _jsonish(llm_client.complete(prompt, max_tokens=180, purpose="controller"))
            act = _act_from_json(parsed, candidates)
            if act is not None and act.act != "unclear":
                return act
    except Exception:
        pass
    return _fallback_dialogue_act(reply, state)


def interpret_context_turn(reply: str, answer_context: dict[str, Any], recent_history: str = "") -> ContextTurnAct:
    """Interpret an ambiguous follow-up to a completed grounded answer.

    This is intentionally tiny: it decides the kind of turn, not the finance
    arguments. The caller still grounds subjects/ranges through deterministic
    code and tool contracts.
    """
    try:
        if llm_client.is_available():
            prompt = f"""You interpret a user's latest reply after Mira already gave a grounded finance answer.
Return JSON only:
{{"act":"same_subject_spend|same_subject_transactions|same_subject_plan|same_subject_chart|explain_grounding|answer_directly|new_task|unclear","confidence":number,"reason":"short"}}

Rules:
- Pick same_subject_spend when the user asks to rerun the prior subject for a different period or says "again" about the same total.
- Pick same_subject_transactions when the user asks for details, rows, charges, purchases, or transactions behind the prior answer.
- Pick same_subject_plan when the user asks to compare the prior subject to an average, usual amount, pace, projection, or another period.
- Pick same_subject_chart when the user asks to chart/plot/graph/visualize the prior subject.
- Pick explain_grounding when the user asks where numbers came from or how the answer was computed.
- Pick answer_directly for acknowledgements or negative feedback that should not call a finance tool.
- Pick new_task only when the latest reply is clearly unrelated to the prior answer.
- Do not invent merchants, categories, ranges, or numbers.

Prior answer context:
{json.dumps(answer_context, ensure_ascii=True, default=str)}

Recent context:
{recent_history or "(none)"}

Latest user reply:
{reply}

JSON:"""
            parsed = _jsonish(llm_client.complete(prompt, max_tokens=120, purpose="controller"))
            act = _context_act_from_json(parsed)
            if act is not None and act.act != "unclear" and act.confidence >= 0.7:
                return act
    except Exception:
        pass
    return ContextTurnAct("unclear", confidence=0.0, reason="no confident context turn act")
