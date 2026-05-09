from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from typing import Any


MEMORY_TYPES = {
    "preference",
    "goal",
    "constraint",
    "stressor",
    "commitment",
    "rejected_advice",
    "coaching_state",
    "identity_fact",
    "tone_preference",
}
SENSITIVITIES = {"low", "medium", "high"}
EXACT_FINANCE_INTENTS = {"spending", "transactions", "chart", "drilldown"}
EXACT_FINANCE_ACTIONS = {
    "SpendTotal",
    "TransactionSearch",
    "MonthlyTrend",
    "NetWorthTrend",
    "CompareSpend",
    "ExplainLastAnswer",
}
EXACT_FINANCE_OPERATIONS = {
    "category_total",
    "merchant_total",
    "list_transactions",
    "find_transactions",
    "current_vs_previous",
    "current_vs_average",
    "monthly_spending_chart",
    "net_worth_chart",
    "explain_grounding",
}
EXACT_FINANCE_TOOLS = {
    "get_merchant_spend",
    "get_category_spend",
    "get_transactions",
    "get_transactions_for_merchant",
    "find_transactions",
    "get_monthly_spending_trend",
    "get_net_worth_trend",
    "compare_periods",
}
MEMORY_MANAGEMENT_OPERATIONS = {
    "remember_user_context",
    "retrieve_relevant_memories",
    "update_memory",
    "forget_memory",
    "list_mira_memories",
}
MEMORY_RETRIEVAL_TYPES = {
    "affordability_coaching": ("goal", "constraint", "commitment", "tone_preference", "stressor"),
    "goal_followup": ("goal", "commitment", "constraint"),
    "casual_persona": ("tone_preference", "preference"),
    "memory_management": tuple(sorted(MEMORY_TYPES)),
}
MEMORY_RETRIEVAL_CAPS = {
    "affordability_coaching": 3,
    "goal_followup": 3,
    "casual_persona": 2,
    "memory_management": 12,
    "exact_finance": 0,
    "none": 0,
}

_STOPWORDS = {
    "a", "about", "am", "and", "are", "at", "be", "can", "doing", "for",
    "from", "how", "i", "im", "in", "is", "it", "keep", "me", "my", "of",
    "on", "or", "remember", "save", "saved", "saving", "that", "the", "this",
    "to", "trying", "user", "want", "wants", "what", "with", "you",
}
_SENSITIVE_TERMS = {
    "anxious", "anxiety", "debt", "medical", "health", "mental", "job", "income",
    "layoff", "laid", "family", "support", "rent", "eviction", "stress", "stressed",
}
_DERIVABLE_FINANCE_RE = re.compile(
    r"\b(?:spent|spend|paid|charged|transaction|transactions|balance|net worth|costco|merchant)\b",
    re.I,
)
_AMOUNT_RE = re.compile(r"\$[\d,]+(?:\.\d{2})?")
_EXACT_FINANCE_QUESTION_RE = re.compile(
    r"\b(?:how much did i spend|how much have i spent|show(?: me)? transactions?|"
    r"list transactions?|latest transaction|find transactions?|compare .+ (?:vs|versus) .+|"
    r"chart|plot|graph|net worth)\b",
    re.I,
)
_AFFORDABILITY_RE = re.compile(
    r"\b(?:can i afford|afford another|afford to|should i buy|should i spend|"
    r"can i spend|help me spend less|spend less|budget advice|financial advice|"
    r"what should i do|coach(?:ing)?|advice)\b",
    re.I,
)
_GOAL_FOLLOWUP_RE = re.compile(
    r"\b(?:goal|goals|on track|track for|pacing|pace|how am i doing|still on track)\b",
    re.I,
)
_CASUAL_PERSONA_RE = re.compile(
    r"\b(?:talk to me|tone|joke|jokes|roast|tease|serious|short answers?|concise|"
    r"like i asked|hey mira|hi mira|hey girl|hello mira)\b",
    re.I,
)
_STYLE_TOPIC_TERMS = {
    "answer", "answers", "reply", "replies", "tone", "style", "serious",
    "short", "concise", "brief", "joke", "jokes", "roast", "tease",
}
_DOMAIN_TOPIC_TERMS = {
    "budget", "coffee", "debt", "dining", "family", "groceries", "health",
    "house", "income", "job", "medical", "rent", "subscriptions", "support",
}
_TOPIC_HINT_STOPWORDS = _STOPWORDS | {
    "advice", "afford", "another", "budget", "buy", "cap", "compare",
    "current", "did", "doing", "find", "goal", "goals", "help", "last",
    "less", "list", "month", "monthly", "much", "show", "should", "spend",
    "spending", "spent", "still", "track", "under", "versus", "vs", "week",
    "year",
}


def remember_user_context(
    *,
    conn: sqlite3.Connection,
    profile: str | None,
    text: str,
    memory_type: str | None = None,
    topic: str | None = None,
    source_summary: str = "",
    source_conversation_id: int | None = None,
    source_turn_id: str | None = None,
    pinned: bool = False,
    expires_at: str | None = None,
    consent: str = "explicit",
) -> dict[str, Any]:
    candidate = extract_memory_candidate(text, memory_type=memory_type, topic=topic)
    if not candidate:
        return {"saved": False, "reason": "No durable user memory detected."}
    memory_id = create_memory(
        conn=conn,
        profile=profile,
        source_conversation_id=source_conversation_id,
        source_turn_id=source_turn_id,
        source_summary=source_summary,
        pinned=pinned,
        expires_at=expires_at,
        consent=consent,
        **candidate,
    )
    memory = get_memory(conn, memory_id, profile)
    return {"saved": True, "memory": memory, "memory_trace": trace_for_memories([memory] if memory else [], allowed=True, reason="explicit_memory_write")}


def extract_memory_candidate(text: str, memory_type: str | None = None, topic: str | None = None) -> dict[str, Any] | None:
    original = " ".join((text or "").strip().split())
    if not original:
        return None
    lowered = _normalize_for_match(original)
    explicit = bool(re.search(r"\b(?:remember(?: that)?|keep in mind|save this|save that)\b", lowered))
    body = re.sub(r"^(?:please\s+)?(?:remember(?: that)?|keep in mind(?: that)?|save this|save that)\s+", "", original, flags=re.I).strip(" .")

    inferred_type = memory_type if memory_type in MEMORY_TYPES else None
    if not inferred_type:
        inferred_type = _infer_memory_type(lowered)
    if not inferred_type:
        return None

    if _looks_like_rejected_finance_fact(lowered, inferred_type, explicit):
        return None
    if inferred_type == "stressor" and not _first_person_stated(lowered):
        return None

    normalized = _normalize_memory_text(body or original, inferred_type)
    if not normalized:
        return None
    inferred_topic = (topic or _infer_topic(lowered, normalized)).strip().lower()
    sensitivity = _infer_sensitivity(lowered, inferred_type, inferred_topic)
    confidence = 1.0 if explicit else 0.86
    return {
        "memory_type": inferred_type,
        "topic": inferred_topic,
        "normalized_text": normalized,
        "original_text": original,
        "sensitivity": sensitivity,
        "confidence": confidence,
    }


def create_memory(
    *,
    conn: sqlite3.Connection,
    profile: str | None,
    memory_type: str,
    topic: str,
    normalized_text: str,
    original_text: str = "",
    source_summary: str = "",
    sensitivity: str = "low",
    confidence: float = 1.0,
    source_conversation_id: int | None = None,
    source_turn_id: str | None = None,
    pinned: bool = False,
    expires_at: str | None = None,
    consent: str = "explicit",
    metadata: dict[str, Any] | None = None,
) -> int:
    if memory_type not in MEMORY_TYPES:
        raise ValueError(f"memory_type must be one of {sorted(MEMORY_TYPES)}")
    if sensitivity not in SENSITIVITIES:
        raise ValueError("sensitivity must be low, medium, or high")
    text = " ".join((normalized_text or "").strip().split())
    if not text:
        raise ValueError("normalized_text is required")
    existing = _find_duplicate(conn, profile, memory_type, text)
    if existing:
        return existing
    cursor = conn.execute(
        """
        INSERT INTO mira_memories (
            profile_id, scope, memory_type, topic, normalized_text, original_text,
            source_summary, sensitivity, confidence, source_conversation_id,
            source_turn_id, pinned, expires_at, consent, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            profile,
            "household" if profile in (None, "household") else "profile",
            memory_type,
            (topic or "").strip().lower(),
            text,
            original_text or text,
            source_summary,
            sensitivity,
            max(0.0, min(float(confidence), 1.0)),
            source_conversation_id,
            source_turn_id,
            1 if pinned else 0,
            expires_at,
            consent or "explicit",
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    memory_id = int(cursor.lastrowid)
    _log_event(conn, memory_id, profile, "created", after=get_memory(conn, memory_id, profile), source_turn_id=source_turn_id)
    return memory_id


def list_memories(
    conn: sqlite3.Connection,
    profile: str | None,
    *,
    include_inactive: bool = False,
    include_expired: bool = False,
    memory_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = ["(? IS NULL OR profile_id = ? OR profile_id IS NULL OR scope = 'household')"]
    params: list[Any] = [profile, profile]
    if not include_inactive:
        where.append("status = 'active'")
    if not include_expired:
        where.append("(pinned = 1 OR expires_at IS NULL OR expires_at > datetime('now'))")
    if memory_type:
        where.append("memory_type = ?")
        params.append(memory_type)
    params.append(max(1, min(int(limit), 500)))
    rows = conn.execute(
        f"""
        SELECT *
        FROM mira_memories
        WHERE {' AND '.join(where)}
        ORDER BY pinned DESC, updated_at DESC, id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [_public_memory(dict(row)) for row in rows]


def get_memory(conn: sqlite3.Connection, memory_id: int, profile: str | None) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM mira_memories
        WHERE id = ? AND (? IS NULL OR profile_id = ? OR profile_id IS NULL OR scope = 'household')
        """,
        (memory_id, profile, profile),
    ).fetchone()
    return _public_memory(dict(row)) if row else None


def update_memory(
    *,
    conn: sqlite3.Connection,
    profile: str | None,
    memory_id: int,
    normalized_text: str | None = None,
    memory_type: str | None = None,
    topic: str | None = None,
    sensitivity: str | None = None,
    confidence: float | None = None,
    pinned: bool | None = None,
    expires_at: str | None = None,
    status: str | None = None,
    source_turn_id: str | None = None,
) -> dict[str, Any] | None:
    before = get_memory(conn, memory_id, profile)
    if not before:
        return None
    updates: list[str] = ["updated_at = datetime('now')"]
    params: list[Any] = []
    if normalized_text is not None:
        updates.append("normalized_text = ?")
        params.append(" ".join(normalized_text.strip().split()))
    if memory_type is not None:
        if memory_type not in MEMORY_TYPES:
            raise ValueError(f"memory_type must be one of {sorted(MEMORY_TYPES)}")
        updates.append("memory_type = ?")
        params.append(memory_type)
    if topic is not None:
        updates.append("topic = ?")
        params.append(topic.strip().lower())
    if sensitivity is not None:
        if sensitivity not in SENSITIVITIES:
            raise ValueError("sensitivity must be low, medium, or high")
        updates.append("sensitivity = ?")
        params.append(sensitivity)
    if confidence is not None:
        updates.append("confidence = ?")
        params.append(max(0.0, min(float(confidence), 1.0)))
    if pinned is not None:
        updates.append("pinned = ?")
        params.append(1 if pinned else 0)
    if expires_at is not None:
        updates.append("expires_at = ?")
        params.append(expires_at or None)
    if status is not None:
        if status not in {"active", "superseded", "deleted", "rejected"}:
            raise ValueError("status must be active, superseded, deleted, or rejected")
        updates.append("status = ?")
        params.append(status)
    params.extend([memory_id, profile, profile])
    conn.execute(
        f"""
        UPDATE mira_memories
        SET {', '.join(updates)}
        WHERE id = ? AND (? IS NULL OR profile_id = ? OR profile_id IS NULL OR scope = 'household')
        """,
        params,
    )
    after = get_memory(conn, memory_id, profile)
    _log_event(conn, memory_id, profile, "updated", before=before, after=after, source_turn_id=source_turn_id)
    return after


def forget_memory(
    *,
    conn: sqlite3.Connection,
    profile: str | None,
    memory_id: int | None = None,
    topic: str | None = None,
    text: str | None = None,
    source_turn_id: str | None = None,
) -> dict[str, Any]:
    candidates = list_memories(conn, profile, limit=200)
    target: dict[str, Any] | None = None
    if memory_id is not None:
        target = get_memory(conn, memory_id, profile)
    elif topic or text:
        query = " ".join([topic or "", text or ""]).strip()
        ranked = _rank_memories(candidates, query)
        target = ranked[0] if ranked else None
    else:
        return {"forgot": False, "reason": "Which memory should I remove?"}
    if not target:
        return {"forgot": False, "reason": "No matching active memory found."}
    before = target
    conn.execute(
        """
        UPDATE mira_memories
        SET status = 'deleted', updated_at = datetime('now')
        WHERE id = ?
        """,
        (target["id"],),
    )
    after = get_memory(conn, int(target["id"]), profile)
    _log_event(conn, int(target["id"]), profile, "deleted", before=before, after=after, source_turn_id=source_turn_id)
    return {"forgot": True, "memory": before}


def retrieve_relevant_memories(
    *,
    conn: sqlite3.Connection,
    profile: str | None,
    question: str,
    route: dict[str, Any] | None = None,
    limit: int = 5,
    include_expired: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    retrieval = classify_memory_retrieval_intent(question, route, force=force)
    allowed = bool(retrieval.get("allowed"))
    reason = str(retrieval.get("reason") or "")
    if not allowed:
        trace = trace_for_memories(
            [],
            allowed=False,
            reason=reason,
            intent=str(retrieval.get("intent") or "none"),
            candidate_count=0,
            allowed_types=retrieval.get("allowed_types") or [],
            topic_hints=retrieval.get("topic_hints") or [],
        )
        packet = compact_memory_packet(
            [],
            question=question,
            route=route,
            allowed=False,
            reason=reason,
            excluded_count=0,
            retrieval=retrieval,
        )
        return {"memories": [], "memory_trace": trace, "compact_memory": packet, "compact_memory_trace": packet}

    explicit = str(retrieval.get("intent") or "") == "memory_management"
    memories = list_memories(
        conn,
        profile,
        include_expired=bool(include_expired and explicit),
        limit=200,
    )
    retrieval = {**retrieval, "candidate_count": len(memories)}
    ranked, excluded_reasons = _rank_memory_candidates(memories, question, retrieval)
    requested_limit = max(1, min(int(limit or 1), 12))
    intent_cap = max(1, min(int(retrieval.get("max_items") or requested_limit), 12))
    selected = ranked[: min(requested_limit, intent_cap)]
    excluded_count = max(0, len(memories) - len(selected))
    if len(ranked) > len(selected):
        excluded_reasons["not_selected_after_cap"] = excluded_reasons.get("not_selected_after_cap", 0) + (len(ranked) - len(selected))
    if selected:
        ids = [int(item["id"]) for item in selected]
        conn.execute(
            f"UPDATE mira_memories SET last_used_at = datetime('now') WHERE id IN ({','.join('?' for _ in ids)})",
            ids,
        )
    trace = trace_for_memories(
        selected,
        allowed=True,
        reason=reason,
        intent=str(retrieval.get("intent") or "none"),
        excluded_count=excluded_count,
        candidate_count=len(memories),
        excluded_reasons=excluded_reasons,
        allowed_types=retrieval.get("allowed_types") or [],
        topic_hints=retrieval.get("topic_hints") or [],
    )
    packet = compact_memory_packet(
        selected,
        question=question,
        route=route,
        allowed=True,
        reason=reason,
        excluded_count=excluded_count,
        retrieval=retrieval,
    )
    return {"memories": selected, "memory_trace": trace, "compact_memory": packet, "compact_memory_trace": packet}


def classify_memory_retrieval_intent(
    question: str,
    route: dict[str, Any] | None = None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    route = route or {}
    q = " ".join((question or "").strip().split())
    lowered = _normalize_for_match(q)
    topic_hints = _topic_hints(q, route)

    def result(intent: str, allowed: bool, reason: str, allowed_types: tuple[str, ...] | list[str] = ()) -> dict[str, Any]:
        types = list(allowed_types or MEMORY_RETRIEVAL_TYPES.get(intent, ()))
        return {
            "intent": intent,
            "allowed": bool(allowed),
            "reason": reason,
            "allowed_types": types,
            "topic_hints": topic_hints,
            "max_items": int(MEMORY_RETRIEVAL_CAPS.get(intent, 0)),
        }

    if force or _route_is_memory_management(route) or _looks_like_explicit_memory_request(lowered):
        return result("memory_management", True, "explicit_memory_request")
    if _route_is_exact_finance(route) or _looks_like_exact_finance_question(lowered):
        return result("exact_finance", False, "exact_finance_query")
    if _route_is_affordability(route) or _AFFORDABILITY_RE.search(lowered):
        return result("affordability_coaching", True, "affordability question")
    if _route_is_goal_followup(route) or _GOAL_FOLLOWUP_RE.search(lowered):
        return result("goal_followup", True, "goal follow-up")
    if _route_is_casual_chat(route) and _CASUAL_PERSONA_RE.search(lowered):
        return result("casual_persona", True, "casual/persona preference")
    return result("none", False, "memory_not_relevant")


def retrieval_allowed(question: str, route: dict[str, Any] | None = None, *, force: bool = False) -> tuple[bool, str]:
    classified = classify_memory_retrieval_intent(question, route, force=force)
    return bool(classified.get("allowed")), str(classified.get("reason") or "")


def _route_action(route: dict[str, Any] | None) -> dict[str, Any]:
    action = (route or {}).get("domain_action") if isinstance(route, dict) else None
    return action if isinstance(action, dict) else {}


def _route_is_memory_management(route: dict[str, Any] | None) -> bool:
    route = route or {}
    action = _route_action(route)
    intent = str(route.get("intent") or "").lower()
    operation = str(route.get("operation") or "").lower()
    tool_name = str(route.get("tool_name") or "").lower()
    return (
        intent == "memory"
        or str(action.get("name") or "") == "Memory"
        or operation in MEMORY_MANAGEMENT_OPERATIONS
        or tool_name in MEMORY_MANAGEMENT_OPERATIONS
    )


def _route_is_exact_finance(route: dict[str, Any] | None) -> bool:
    route = route or {}
    action = _route_action(route)
    action_name = str(action.get("name") or "")
    intent = str(route.get("intent") or "").lower()
    operation = str(route.get("operation") or "").lower()
    tool_name = str(route.get("tool_name") or "").lower()
    return (
        intent in EXACT_FINANCE_INTENTS
        or action_name in EXACT_FINANCE_ACTIONS
        or operation in EXACT_FINANCE_OPERATIONS
        or tool_name in EXACT_FINANCE_TOOLS
    )


def _route_is_affordability(route: dict[str, Any] | None) -> bool:
    route = route or {}
    action = _route_action(route)
    return (
        str(action.get("name") or "") == "Affordability"
        or str(route.get("operation") or "").lower() == "affordability"
        or str(route.get("tool_name") or "").lower() == "check_affordability"
    )


def _route_is_goal_followup(route: dict[str, Any] | None) -> bool:
    route = route or {}
    action = _route_action(route)
    operation = str(route.get("operation") or "").lower()
    return str(action.get("name") or "") == "BudgetStatus" or operation in {"on_track", "budget_status"}


def _route_is_casual_chat(route: dict[str, Any] | None) -> bool:
    route = route or {}
    action = _route_action(route)
    intent = str(route.get("intent") or "").lower()
    return intent in {"", "chat"} or str(action.get("name") or "") == "GeneralChat"


def _looks_like_explicit_memory_request(lowered: str) -> bool:
    return bool(
        re.search(
            r"\b(?:remember(?: that)?|forget(?: that| this)?|what do you remember|"
            r"what do you know about me|list(?: my)? memories|show(?: my)? memories|"
            r"update my .+memory|change my .+memory|memory|memories)\b",
            lowered or "",
        )
    )


def _looks_like_exact_finance_question(lowered: str) -> bool:
    if _EXACT_FINANCE_QUESTION_RE.search(lowered or ""):
        return True
    tokens = set(_tokens(lowered or ""))
    if {"how", "much"} <= tokens and tokens & {"spent", "spend", "paid"}:
        return True
    if tokens & {"transactions", "transaction"} and tokens & {"show", "list", "find"}:
        return True
    return False


def _topic_hints(question: str, route: dict[str, Any] | None) -> list[str]:
    hints: list[str] = []

    def add(value: Any) -> None:
        text = " ".join(str(value or "").strip().lower().split())
        if not text:
            return
        for candidate in (text, *_tokens(text)):
            normalized = _canonical_topic(candidate)
            if normalized and not normalized.isdigit() and normalized not in hints and normalized not in _TOPIC_HINT_STOPWORDS:
                hints.append(normalized)

    args = (route or {}).get("args") if isinstance((route or {}).get("args"), dict) else {}
    for key in ("category", "merchant", "subject", "purpose"):
        add(args.get(key))
    action = _route_action(route)
    slots = action.get("validated_slots") if isinstance(action.get("validated_slots"), dict) else {}
    for key in ("category", "merchant", "subject", "purpose"):
        add(slots.get(key))

    lowered = _normalize_for_match(question)
    for token in _tokens(lowered):
        canonical = _canonical_topic(token)
        if canonical and not canonical.isdigit() and canonical not in hints and canonical not in _TOPIC_HINT_STOPWORDS:
            hints.append(canonical)
    for match in re.finditer(r"\b(?:about|on|for|toward|towards)\s+([a-z0-9 &'-]{2,40})", lowered):
        phrase = " ".join(token for token in _tokens(match.group(1)) if not token.isdigit() and token not in _TOPIC_HINT_STOPWORDS)
        if phrase:
            add(phrase)
    return hints[:8]


def _canonical_topic(value: str) -> str:
    token = " ".join(str(value or "").lower().split()).strip(" .,:;!?")
    if not token:
        return ""
    aliases = {
        "restaurants": "dining",
        "restaurant": "dining",
        "food": "dining",
        "food dining": "dining",
        "food & dining": "dining",
        "home": "house",
        "housing": "house",
        "mortgage": "house",
        "downpayment": "house",
        "loan": "debt",
        "loans": "debt",
        "credit": "debt",
        "cards": "debt",
        "card": "debt",
        "grocery": "groceries",
        "summary": "summaries",
        "answer": "answers",
        "reply": "answers",
        "replies": "answers",
        "jokes": "joke",
        "roasts": "roast",
    }
    compact = re.sub(r"[^a-z0-9]+", " ", token).strip()
    return aliases.get(token) or aliases.get(compact) or compact


def trace_for_memories(
    memories: list[dict[str, Any]],
    *,
    allowed: bool,
    reason: str,
    intent: str = "",
    excluded_count: int = 0,
    candidate_count: int = 0,
    excluded_reasons: dict[str, int] | None = None,
    allowed_types: list[str] | tuple[str, ...] | None = None,
    topic_hints: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    items = []
    for item in memories:
        items.append(
            {
                "id": item.get("id"),
                "type": item.get("memory_type"),
                "memory_type": item.get("memory_type"),
                "topic": item.get("topic"),
                "sensitivity": item.get("sensitivity"),
                "confidence": item.get("confidence"),
                "pinned": bool(item.get("pinned")),
                "status": item.get("status"),
            }
        )
    return {
        "version": 2,
        "allowed": bool(allowed),
        "used": bool(items),
        "intent": intent,
        "reason": reason,
        "used_count": len(items),
        "excluded_count": max(0, int(excluded_count or 0)),
        "candidate_count": max(0, int(candidate_count or len(memories) or 0)),
        "excluded_reasons": dict(excluded_reasons or {}),
        "allowed_types": list(allowed_types or []),
        "topic_hints": list(topic_hints or []),
        "used_memory_ids": [item["id"] for item in items if item.get("id") is not None],
        "sensitive_used": any(item.get("sensitivity") == "high" for item in items),
        "items": items,
    }


def context_block(memories: list[dict[str, Any]]) -> str:
    packet = compact_memory_packet(memories, question="", route=None, allowed=bool(memories), reason="prompt_context")
    return context_block_from_packet(packet)


def context_block_from_packet(packet: dict[str, Any] | None) -> str:
    if not isinstance(packet, dict) or not packet.get("items"):
        return ""
    return "Compact relevant Mira memory packet:\n" + json.dumps(packet, ensure_ascii=True, sort_keys=True)


def compact_memory_packet(
    memories: list[dict[str, Any]],
    *,
    question: str,
    route: dict[str, Any] | None,
    allowed: bool,
    reason: str,
    excluded_count: int = 0,
    retrieval: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in memories[:12]:
        items.append(
            {
                "id": str(item.get("id")),
                "type": item.get("memory_type"),
                "topic": item.get("topic") or "general",
                "summary": _memory_summary(item),
                "confidence": _confidence_label(item.get("confidence")),
                "sensitivity": _packet_sensitivity(item.get("sensitivity")),
            }
        )
    return {
        "version": 1,
        "used": bool(allowed and items),
        "allowed": bool(allowed),
        "intent": str((retrieval or {}).get("intent") or _memory_intent(route)),
        "reason": reason,
        "items": items,
        "excluded_count": max(0, int(excluded_count or 0)),
        "candidate_count": max(0, int((retrieval or {}).get("candidate_count") or len(memories) or 0)),
        "allowed_types": list((retrieval or {}).get("allowed_types") or []),
        "topic_hints": list((retrieval or {}).get("topic_hints") or []),
        "sensitive_used": any(item.get("sensitivity") == "high" for item in memories),
    }


def affordability_constraint_context(
    memories: list[dict[str, Any]],
    *,
    category: str,
    amount: float,
) -> dict[str, Any]:
    """Return compact affordability memory context; raw memory text stays internal."""
    conflicts: list[str] = []
    used: list[dict[str, Any]] = []
    category_lower = (category or "").lower()
    category_topics = {_canonical_topic(category_lower), *(_canonical_topic(token) for token in _tokens(category_lower))}
    category_topics = {token for token in category_topics if token}
    for memory in memories:
        memory_type = str(memory.get("memory_type") or "")
        if memory_type not in {"goal", "constraint", "commitment"}:
            continue
        summary = _memory_summary(memory)
        used.append(
            {
                "id": memory.get("id"),
                "type": memory_type,
                "topic": memory.get("topic") or "general",
                "summary": summary,
                "confidence": _confidence_label(memory.get("confidence")),
                "sensitivity": _packet_sensitivity(memory.get("sensitivity")),
            }
        )
        text = str(memory.get("normalized_text") or memory.get("original_text") or "")
        cap = _amount_from_text(text)
        if category_topics and category_topics & _memory_topic_tokens(memory) and cap is not None and float(amount or 0) > cap:
            conflicts.append(f"it conflicts with your saved {category} cap context")
        elif any(token in text.lower() for token in ("save", "saving", "debt", "house", "emergency")):
            if float(amount or 0) >= 100:
                conflicts.append("it works against a saved goal or constraint")
    return {"used_memories": used, "conflicts": conflicts}


def parse_memory_command(question: str) -> dict[str, Any] | None:
    q = " ".join((question or "").strip().split())
    lowered = _normalize_for_match(q)
    if not q:
        return None
    if re.search(r"\b(?:what do you remember about me|what do you know about me|list(?: my)? memories|show(?: my)? memories)\b", lowered):
        return {"operation": "list_mira_memories", "args": {}}
    m = re.search(r"\bforget(?: that)?\s+(.+)$", q, re.I)
    if m and m.group(1).strip(" .").lower() not in {"that", "this"}:
        return {"operation": "forget_memory", "args": {"text": m.group(1).strip(" .")}}
    if re.search(r"\b(?:forget that|forget this|dont remember this|don't remember this|delete that memory|remove that memory)\b", lowered):
        return {"operation": "forget_memory", "args": {}}
    if re.search(r"\b(?:that'?s not true anymore|not true anymore)\b", q, re.I):
        return {"operation": "forget_memory", "args": {"text": q}}
    m = re.search(r"\b(?:update my|change my)\b(.+)$", q, re.I)
    if m:
        return {"operation": "update_memory", "args": {"text": m.group(1).strip(" ."), "original": q}}
    if re.search(r"\b(?:remember(?: that)?|keep in mind|save this|save that|i prefer|i'm trying|i am trying|i want|i'm anxious|i am anxious|don't joke|dont joke|don't roast|dont roast)\b", lowered):
        return {"operation": "remember_user_context", "args": {"text": q}}
    return None


def answer_for_memory_tool(operation: str, result: dict[str, Any]) -> str:
    if operation == "list_mira_memories":
        items = result.get("items") or result.get("memories") or []
        if not items:
            return "I don't have any Mira memories saved for you yet."
        lines = ["Here's what I have saved:"]
        for item in items[:12]:
            lines.append(f"- {item.get('normalized_text')}")
        return "\n".join(lines)
    if operation == "forget_memory":
        if result.get("forgot"):
            return "Got it. I removed that memory."
        return result.get("reason") or "I couldn't find a matching memory to remove."
    if operation == "update_memory":
        if result.get("updated"):
            memory = result.get("memory") or {}
            return f"Updated: {memory.get('normalized_text')}"
        return result.get("reason") or "I couldn't find a matching memory to update."
    if result.get("saved"):
        return "Got it. I'll keep that in mind."
    return result.get("reason") or "I did not save that as memory."


def _find_duplicate(conn: sqlite3.Connection, profile: str | None, memory_type: str, normalized_text: str) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM mira_memories
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL OR scope = 'household')
          AND memory_type = ?
          AND LOWER(normalized_text) = LOWER(?)
          AND status = 'active'
        LIMIT 1
        """,
        (profile, profile, memory_type, normalized_text),
    ).fetchone()
    return int(row["id"]) if row else None


def _infer_memory_type(lowered: str) -> str | None:
    if re.search(r"\b(?:don't|dont|do not|never)\s+(?:joke|roast|tease)\b", lowered):
        return "tone_preference"
    if re.search(r"\b(?:prefer|preference|like)\b.*\b(?:short|concise|brief|serious|tone|answers?|replies|no jokes?)\b", lowered):
        return "tone_preference"
    if re.search(r"\b(?:prefer|preference|like|dislike|hate)\b", lowered):
        return "preference"
    if re.search(r"\b(?:anxious|worried|stressed|stress|concerned)\b", lowered):
        return "stressor"
    if re.search(r"\b(?:under|below|cap|limit|keep .+ under|avoid|must|need to)\b", lowered):
        return "constraint"
    if re.search(r"\b(?:trying to|working on|committed to)\b", lowered):
        return "commitment"
    if re.search(r"\b(?:goal|save for|saving for|want to save|i want)\b", lowered):
        return "goal"
    if re.search(r"\b(?:i am|i'm|my job|my family|i work)\b", lowered):
        return "identity_fact"
    return None


def _normalize_memory_text(text: str, memory_type: str) -> str:
    cleaned = " ".join((text or "").strip(" .").split())
    if not cleaned:
        return ""
    lowered = _normalize_for_match(cleaned)
    cleaned = re.sub(r"^(?:that\s+)?", "", cleaned, flags=re.I).strip()
    if memory_type == "tone_preference" and re.search(r"\b(?:don't|dont|do not|never)\s+(?:joke|roast|tease)\b", lowered):
        topic = _topic_after_about(cleaned) or _infer_topic(lowered, cleaned)
        verb = "jokes" if "joke" in lowered else "roasts"
        return f"User does not want {verb} about {topic}."
    first_person = _third_person_memory_text(cleaned)
    if first_person:
        return first_person.rstrip(".") + "."
    return cleaned[0].upper() + cleaned[1:] + "."


def _third_person_memory_text(text: str) -> str:
    cleaned = " ".join((text or "").strip().split())
    patterns = (
        (r"^i\s+want\s+to\s+", "User wants to "),
        (r"^i\s+prefer\s+", "User prefers "),
        (r"^i\s+like\s+", "User likes "),
        (r"^i\s+dislike\s+", "User dislikes "),
        (r"^i\s+hate\s+", "User hates "),
        (r"^i(?:'m| am)\s+trying\s+to\s+", "User is trying to "),
        (r"^i(?:'m| am)\s+working\s+on\s+", "User is working on "),
        (r"^i(?:'m| am)\s+committed\s+to\s+", "User is committed to "),
        (r"^i(?:'m| am)\s+anxious\s+about\s+", "User is anxious about "),
        (r"^i(?:'m| am)\s+worried\s+about\s+", "User is worried about "),
        (r"^i(?:'m| am)\s+stressed\s+about\s+", "User is stressed about "),
        (r"^we\s+want\s+to\s+", "User's household wants to "),
        (r"^we(?:'re| are)\s+trying\s+to\s+", "User's household is trying to "),
        (r"^our\s+", "User's household "),
        (r"^my\s+", "User's "),
    )
    for pattern, replacement in patterns:
        if re.search(pattern, cleaned, re.I):
            return re.sub(pattern, replacement, cleaned, count=1, flags=re.I)
    if cleaned.lower().startswith(("i ", "i'm ", "i am ", "we ")):
        return "User " + cleaned[0].lower() + cleaned[1:]
    return ""


def _looks_like_rejected_finance_fact(lowered: str, memory_type: str, explicit: bool) -> bool:
    if memory_type in {"constraint", "goal"} and re.search(r"\b(?:under|below|cap|limit|save|saving)\b", lowered):
        return False
    if explicit and not re.search(r"\b(?:spent|paid|transaction|balance|net worth)\b", lowered):
        return False
    if _DERIVABLE_FINANCE_RE.search(lowered) and _AMOUNT_RE.search(lowered):
        return True
    if re.search(r"\b(?:how much|latest transaction|last transaction)\b", lowered):
        return True
    return False


def _first_person_stated(lowered: str) -> bool:
    return bool(re.search(r"\b(?:i|im|i'm|i am|my|we|our)\b", lowered))


def _infer_sensitivity(lowered: str, memory_type: str, topic: str) -> str:
    if memory_type == "stressor":
        return "high" if set(_tokens(lowered)) & _SENSITIVE_TERMS else "medium"
    if topic in _SENSITIVE_TERMS or set(_tokens(lowered)) & _SENSITIVE_TERMS:
        return "high"
    if memory_type in {"constraint", "rejected_advice", "coaching_state"}:
        return "medium"
    return "low"


def _infer_topic(lowered: str, normalized: str) -> str:
    about = _topic_after_about(normalized)
    if about:
        return about.lower()
    tokens = [token for token in _tokens(lowered or normalized) if token not in _STOPWORDS]
    for token in tokens:
        if token in {"dining", "debt", "coffee", "house", "summaries", "summary", "budget", "rent"}:
            return "weekly summaries" if token in {"summaries", "summary"} else token
    return tokens[-1] if tokens else "general"


def _topic_after_about(text: str) -> str:
    match = re.search(r"\babout\s+(.+?)(?:[.!?]|$)", text, re.I)
    if not match:
        return ""
    return " ".join(match.group(1).strip().split()[:4]).strip(" .")


def _rank_memories(memories: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    retrieval = {
        "intent": "memory_management",
        "allowed_types": list(MEMORY_TYPES),
        "topic_hints": _topic_hints(query, None),
        "max_items": 12,
    }
    ranked, _excluded = _rank_memory_candidates(memories, query, retrieval)
    return ranked


def _rank_memory_candidates(
    memories: list[dict[str, Any]],
    query: str,
    retrieval: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    query_tokens = {token for token in _tokens(query) if token not in _STOPWORDS}
    topic_hints = set(str(item).lower() for item in retrieval.get("topic_hints") or [] if item)
    allowed_types = set(str(item) for item in retrieval.get("allowed_types") or [])
    intent = str(retrieval.get("intent") or "none")
    excluded: dict[str, int] = {}
    ranked: list[tuple[float, int, dict[str, Any]]] = []

    def reject(reason: str) -> None:
        excluded[reason] = excluded.get(reason, 0) + 1

    for item in memories:
        memory_type = str(item.get("memory_type") or "")
        if allowed_types and memory_type not in allowed_types:
            reject("type_not_allowed")
            continue
        if str(item.get("status") or "active") != "active":
            reject("inactive")
            continue
        if _memory_is_expired(item) and not item.get("pinned") and intent != "memory_management":
            reject("expired")
            continue
        if item.get("sensitivity") == "high" and not _sensitive_memory_relevant(item, query_tokens, topic_hints, intent):
            reject("sensitive_not_relevant")
            continue
        if intent != "memory_management" and not _memory_topic_relevant(item, topic_hints, query_tokens, intent):
            reject("topic_mismatch")
            continue

        score = _memory_relevance_score(item, query_tokens, topic_hints, intent)
        if score <= 0 and intent != "memory_management":
            reject("no_relevance")
            continue
        ranked.append((score, int(item.get("id") or 0), item))

    ranked.sort(key=lambda pair: (pair[0], pair[1]), reverse=True)
    return [item for _score, _id, item in ranked], excluded


def _memory_relevance_score(
    item: dict[str, Any],
    query_tokens: set[str],
    topic_hints: set[str],
    intent: str,
) -> float:
    memory_type = str(item.get("memory_type") or "")
    topic_tokens = _memory_topic_tokens(item)
    text_tokens = {
        token for token in _tokens(f"{item.get('topic') or ''} {item.get('normalized_text') or ''}")
        if token not in _STOPWORDS
    }
    overlap = len(query_tokens & text_tokens)
    topic_overlap = len(topic_hints & topic_tokens)
    score = float(overlap)
    if topic_overlap:
        score += 5.0 + topic_overlap
    if str(item.get("topic") or "").lower() in topic_hints:
        score += 3.0
    if memory_type in {"goal", "constraint", "commitment"}:
        if intent in {"affordability_coaching", "goal_followup"}:
            score += 2.0
        if query_tokens & {"afford", "budget", "goal", "goals", "track", "pace", "saving", "save", "spend"}:
            score += 1.5
    if memory_type == "stressor":
        if query_tokens & {"advice", "debt", "anxious", "worried", "stress", "stressed"}:
            score += 2.5
    if memory_type in {"tone_preference", "preference"}:
        if _style_memory_relevant(item, query_tokens, topic_hints, intent):
            score += 3.0
        elif query_tokens & {"joke", "roast", "tone", "serious", "short", "concise", "summaries"}:
            score += 1.5
    if item.get("pinned"):
        score += 0.5
    try:
        score += max(0.0, min(float(item.get("confidence") or 0), 1.0))
    except (TypeError, ValueError):
        pass
    if _memory_is_expired(item):
        score -= 0.25
    if intent == "memory_management" and score <= 0:
        score = 0.1
    return score


def _memory_topic_relevant(
    item: dict[str, Any],
    topic_hints: set[str],
    query_tokens: set[str],
    intent: str,
) -> bool:
    memory_type = str(item.get("memory_type") or "")
    memory_topics = _memory_topic_tokens(item)
    if memory_topics & topic_hints:
        return True
    if memory_type in {"tone_preference", "preference"}:
        return _style_memory_relevant(item, query_tokens, topic_hints, intent)
    return bool(memory_topics & query_tokens & _DOMAIN_TOPIC_TERMS)


def _memory_topic_tokens(item: dict[str, Any]) -> set[str]:
    topic = str(item.get("topic") or "").lower()
    tokens = {_canonical_topic(topic)} if topic and _canonical_topic(topic) not in _TOPIC_HINT_STOPWORDS else set()
    tokens.update(_canonical_topic(token) for token in _tokens(topic) if _canonical_topic(token) not in _TOPIC_HINT_STOPWORDS)
    text = str(item.get("normalized_text") or "")
    for token in _tokens(text):
        canonical = _canonical_topic(token)
        if canonical in _DOMAIN_TOPIC_TERMS or canonical in _STYLE_TOPIC_TERMS:
            tokens.add(canonical)
    return {token for token in tokens if token}


def _style_memory_relevant(
    item: dict[str, Any],
    query_tokens: set[str],
    topic_hints: set[str],
    intent: str,
) -> bool:
    memory_type = str(item.get("memory_type") or "")
    if memory_type not in {"tone_preference", "preference"}:
        return False
    tokens = _memory_topic_tokens(item)
    text_tokens = set(_tokens(str(item.get("normalized_text") or "")))
    domain_topics = tokens & _DOMAIN_TOPIC_TERMS
    query_topics = (query_tokens | topic_hints) & _DOMAIN_TOPIC_TERMS
    style_request = bool((query_tokens | topic_hints) & _STYLE_TOPIC_TERMS)
    style_overlap = bool((tokens | text_tokens) & _STYLE_TOPIC_TERMS)
    if domain_topics:
        return bool(domain_topics & query_topics) and (style_overlap or style_request or intent != "casual_persona")
    if query_tokens & _STYLE_TOPIC_TERMS:
        return style_overlap or bool((tokens | text_tokens) & query_tokens)
    if topic_hints & _STYLE_TOPIC_TERMS:
        return style_overlap
    return intent == "casual_persona" and style_overlap


def _sensitive_memory_relevant(
    item: dict[str, Any],
    query_tokens: set[str],
    topic_hints: set[str],
    intent: str,
) -> bool:
    tokens = _memory_topic_tokens(item)
    sensitive_tokens = tokens & _SENSITIVE_TERMS
    if sensitive_tokens and (sensitive_tokens & (query_tokens | topic_hints)):
        return True
    if tokens & topic_hints:
        return True
    return bool(query_tokens & {"debt", "anxious", "worried", "stress", "stressed"} and tokens & query_tokens)


def _memory_is_expired(item: dict[str, Any]) -> bool:
    raw = str(item.get("expires_at") or "").strip()
    if not raw:
        return False
    try:
        expires = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    now = datetime.now(expires.tzinfo) if expires.tzinfo else datetime.now()
    return expires <= now


def _memory_intent(route: dict[str, Any] | None) -> str:
    route = route or {}
    operation = str(route.get("operation") or "").strip()
    intent = str(route.get("intent") or "").strip()
    if operation:
        return operation
    return intent or "unknown"


def _packet_sensitivity(value: Any) -> str:
    raw = str(value or "low").lower()
    if raw == "high":
        return "sensitive"
    if raw == "medium":
        return "caution"
    return "normal"


def _confidence_label(value: Any) -> str:
    try:
        confidence = float(value or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence >= 0.9:
        return "high"
    if confidence >= 0.7:
        return "medium"
    return "low"


def _memory_summary(item: dict[str, Any]) -> str:
    text = " ".join(str(item.get("normalized_text") or "").strip().split())
    memory_type = str(item.get("memory_type") or "memory")
    topic = str(item.get("topic") or "general").strip()
    if not text:
        return f"{memory_type.replace('_', ' ').title()} about {topic}."

    summary = text.rstrip(".")
    summary = re.sub(r"^User\s+", "", summary, flags=re.I)
    summary = re.sub(r"^does not want\s+", "Does not want ", summary, flags=re.I)
    summary = re.sub(r"^wants\s+", "Wants ", summary, flags=re.I)
    summary = re.sub(r"^is\s+", "Is ", summary, flags=re.I)
    summary = re.sub(r"^prefers\s+", "Prefers ", summary, flags=re.I)
    summary = re.sub(r"^user\s+", "", summary, flags=re.I)
    if summary and summary[0].islower():
        summary = summary[0].upper() + summary[1:]
    if not summary:
        summary = f"{memory_type.replace('_', ' ').title()} about {topic}"
    return summary[:220].rstrip(" ,;:") + "."


def _amount_from_text(text: str) -> float | None:
    match = _AMOUNT_RE.search(text or "")
    if not match:
        return None
    try:
        return float(match.group(0).replace("$", "").replace(",", ""))
    except ValueError:
        return None


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _normalize_for_match(text))


def _normalize_for_match(text: str) -> str:
    return (
        (text or "")
        .lower()
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def _public_memory(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    try:
        row["metadata"] = json.loads(row.get("metadata_json") or "{}")
    except Exception:
        row["metadata"] = {}
    row.pop("metadata_json", None)
    row["pinned"] = bool(row.get("pinned"))
    try:
        row["confidence"] = float(row.get("confidence") or 0)
    except (TypeError, ValueError):
        row["confidence"] = 0.0
    return row


def _log_event(
    conn: sqlite3.Connection,
    memory_id: int,
    profile: str | None,
    event_type: str,
    *,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    reason: str = "",
    source_turn_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO mira_memory_events (memory_id, profile_id, event_type, before_json, after_json, reason, source_turn_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            memory_id,
            profile,
            event_type,
            json.dumps(before or {}, sort_keys=True, default=str),
            json.dumps(after or {}, sort_keys=True, default=str),
            reason,
            source_turn_id,
        ),
    )
