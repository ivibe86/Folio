from __future__ import annotations

import json
import re
import sqlite3
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

_STOPWORDS = {
    "a", "about", "and", "are", "at", "be", "can", "for", "from", "how", "i",
    "im", "in", "is", "it", "me", "my", "of", "on", "or", "that", "the",
    "this", "to", "what", "with", "you",
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
    allowed, reason = retrieval_allowed(question, route, force=force)
    if not allowed:
        trace = trace_for_memories([], allowed=False, reason=reason, intent=_memory_intent(route))
        packet = compact_memory_packet([], question=question, route=route, allowed=False, reason=reason, excluded_count=0)
        return {"memories": [], "memory_trace": trace, "compact_memory": packet, "compact_memory_trace": packet}
    memories = list_memories(conn, profile, include_expired=include_expired, limit=200)
    ranked = _rank_memories(memories, question)
    selected = ranked[: max(1, min(int(limit), 12))]
    excluded_count = max(0, len(memories) - len(selected))
    if selected:
        ids = [int(item["id"]) for item in selected]
        conn.execute(
            f"UPDATE mira_memories SET last_used_at = datetime('now') WHERE id IN ({','.join('?' for _ in ids)})",
            ids,
        )
    trace = trace_for_memories(selected, allowed=True, reason=reason, intent=_memory_intent(route), excluded_count=excluded_count)
    packet = compact_memory_packet(selected, question=question, route=route, allowed=True, reason=reason, excluded_count=excluded_count)
    return {"memories": selected, "memory_trace": trace, "compact_memory": packet, "compact_memory_trace": packet}


def retrieval_allowed(question: str, route: dict[str, Any] | None = None, *, force: bool = False) -> tuple[bool, str]:
    if force:
        return True, "explicit_memory_request"
    route = route or {}
    action = route.get("domain_action") if isinstance(route.get("domain_action"), dict) else {}
    action_name = str(action.get("name") or "")
    intent = str(route.get("intent") or "").lower()
    operation = str(route.get("operation") or "").lower()
    tool_name = str(route.get("tool_name") or "").lower()
    if intent in EXACT_FINANCE_INTENTS or action_name in EXACT_FINANCE_ACTIONS or operation in EXACT_FINANCE_OPERATIONS:
        return False, "exact_finance_query"
    if tool_name in {"get_merchant_spend", "get_category_spend", "get_transactions", "find_transactions", "get_monthly_spending_trend", "get_net_worth_trend"}:
        return False, "exact_finance_tool"
    tokens = set(_tokens(question))
    if tokens & {"remember", "forget", "memories", "memory"}:
        return True, "explicit_memory_request"
    if intent == "plan" or operation in {"on_track", "current_vs_average", "budget_status", "subject_analysis"}:
        return True, "coaching_or_budget_guidance"
    if tokens & {"advice", "advise", "afford", "should", "budget", "goal", "goals", "anxious", "worried", "debt", "plan", "coaching", "help"}:
        return True, "coaching_or_emotional_context"
    return False, "memory_not_relevant"


def trace_for_memories(
    memories: list[dict[str, Any]],
    *,
    allowed: bool,
    reason: str,
    intent: str = "",
    excluded_count: int = 0,
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
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in memories[:12]:
        items.append(
            {
                "id": str(item.get("id")),
                "type": item.get("memory_type"),
                "topic": item.get("topic") or "general",
                "summary": _memory_summary(item),
                "confidence": item.get("confidence"),
                "sensitivity": _packet_sensitivity(item.get("sensitivity")),
            }
        )
    return {
        "version": 1,
        "used": bool(allowed and items),
        "allowed": bool(allowed),
        "intent": _memory_intent(route),
        "items": items,
        "excluded_count": max(0, int(excluded_count or 0)),
        "reason": reason,
        "sensitive_used": any(item.get("sensitivity") == "high" for item in memories),
    }


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
    if cleaned.lower().startswith(("i ", "i'm ", "i am ", "my ", "we ", "our ")):
        return "User " + cleaned[0].lower() + cleaned[1:] + "."
    return cleaned[0].upper() + cleaned[1:] + "."


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
    query_tokens = set(_tokens(query))
    ranked: list[tuple[float, dict[str, Any]]] = []
    for item in memories:
        text = f"{item.get('topic') or ''} {item.get('normalized_text') or ''}"
        tokens = set(_tokens(text))
        overlap = len(query_tokens & tokens)
        type_bonus = 0.0
        if item.get("memory_type") in {"tone_preference", "preference"} and query_tokens & {"joke", "jokes", "roast", "tone", "summary", "summaries"}:
            type_bonus += 2.0
        if item.get("memory_type") in {"goal", "constraint", "commitment"} and query_tokens & {"afford", "budget", "goal", "save", "saving", "dining", "spend"}:
            type_bonus += 1.5
        if item.get("memory_type") == "stressor" and query_tokens & {"advice", "debt", "anxious", "worried", "stress"}:
            type_bonus += 2.0
        pin_bonus = 0.5 if item.get("pinned") else 0.0
        score = overlap + type_bonus + pin_bonus
        if score > 0:
            ranked.append((score, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in ranked]


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
