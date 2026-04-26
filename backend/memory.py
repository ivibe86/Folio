"""
Personal memory file for Copilot.

Persistent, user-specific store of identity, stated preferences, goals,
recurring concerns, and open questions. Read into the agent system prompt
every turn so the model walks in already knowing the user.

Two distinct operations:
- READ: every turn, free, via render_markdown()
- WRITE: rare, gated, via insert_entry() / supersede_entry() / delete_entry()
"""

from __future__ import annotations

import re as _re
import sqlite3
from typing import Iterable

from log_config import get_logger

logger = get_logger(__name__)

SECTIONS: tuple[tuple[str, str], ...] = (
    ("identity", "Identity"),
    ("preferences", "Stated preferences"),
    ("goals", "Goals & commitments"),
    ("concerns", "Recurring concerns"),
    ("open_questions", "Open questions"),
)
_SECTION_KEYS = {key for key, _ in SECTIONS}
_CONFIDENCE_VALUES = {"stated", "saved", "inferred"}


def list_active_entries(profile: str | None, conn: sqlite3.Connection) -> list[dict]:
    """Return non-superseded, non-expired entries for a profile, ordered by section then created_at."""
    expire_inferred_entries(conn)
    rows = conn.execute(
        """
        SELECT id, profile_id, section, body, confidence, evidence, theme, created_at
        FROM memory_entries
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
          AND superseded_at IS NULL
        ORDER BY section, created_at ASC
        """,
        (profile, profile),
    ).fetchall()
    return [dict(r) for r in rows]


def expire_inferred_entries(conn: sqlite3.Connection) -> int:
    """
    Lazy decay: any inferred entry past its expires_at gets superseded_at set to its
    expiry timestamp. Runs on every read; cheap thanks to the partial index.
    Returns the number of entries that were retired this call.
    """
    cursor = conn.execute(
        """
        UPDATE memory_entries
        SET superseded_at = expires_at
        WHERE superseded_at IS NULL
          AND expires_at IS NOT NULL
          AND expires_at <= datetime('now')
        """,
    )
    return cursor.rowcount or 0


def insert_entry(
    *,
    profile: str | None,
    section: str,
    body: str,
    confidence: str = "stated",
    evidence: str = "",
    theme: str | None = None,
    expires_at: str | None = None,
    conn: sqlite3.Connection,
) -> int:
    """Insert a new active entry. Returns the new row id."""
    if section not in _SECTION_KEYS:
        raise ValueError(f"section must be one of {sorted(_SECTION_KEYS)}")
    if confidence not in _CONFIDENCE_VALUES:
        raise ValueError(f"confidence must be one of {sorted(_CONFIDENCE_VALUES)}")
    body = body.strip()
    if not body:
        raise ValueError("body is required")

    cursor = conn.execute(
        """
        INSERT INTO memory_entries (profile_id, section, body, confidence, evidence, theme, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (profile, section, body, confidence, evidence.strip(), theme, expires_at),
    )
    return cursor.lastrowid


def find_active_entry_id(
    *,
    profile: str | None,
    section: str,
    body: str,
    conn: sqlite3.Connection,
) -> int | None:
    """Return an active entry id with the same normalized body/section, if one exists."""
    key = _re.sub(r"\s+", " ", (body or "").strip().lower())
    if not key:
        return None
    rows = conn.execute(
        """
        SELECT id, body
        FROM memory_entries
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
          AND section = ?
          AND superseded_at IS NULL
        """,
        (profile, profile, section),
    ).fetchall()
    for row in rows:
        row_key = _re.sub(r"\s+", " ", (row["body"] or "").strip().lower())
        if row_key == key:
            return int(row["id"])
    return None


def supersede_entry(
    *,
    old_id: int,
    profile: str | None,
    new_body: str,
    new_confidence: str = "stated",
    new_evidence: str = "",
    new_expires_at: str | None = None,
    conn: sqlite3.Connection,
) -> int:
    """Mark old entry as superseded and insert a new entry in the same section."""
    old = conn.execute(
        "SELECT section FROM memory_entries WHERE id = ? AND superseded_at IS NULL",
        (old_id,),
    ).fetchone()
    if old is None:
        raise ValueError(f"no active memory entry with id {old_id}")
    new_id = insert_entry(
        profile=profile,
        section=old["section"],
        body=new_body,
        confidence=new_confidence,
        evidence=new_evidence,
        expires_at=new_expires_at,
        conn=conn,
    )
    conn.execute(
        "UPDATE memory_entries SET superseded_at = datetime('now'), superseded_by = ? WHERE id = ?",
        (new_id, old_id),
    )
    return new_id


def delete_entry(*, entry_id: int, profile: str | None, conn: sqlite3.Connection) -> bool:
    """Hard delete a single entry. Returns True if a row was removed."""
    cursor = conn.execute(
        "DELETE FROM memory_entries WHERE id = ? AND (? IS NULL OR profile_id = ? OR profile_id IS NULL)",
        (entry_id, profile, profile),
    )
    return cursor.rowcount > 0


def list_changelog(profile: str | None, conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    """Return recent supersede events for the changelog tail of the markdown view."""
    rows = conn.execute(
        """
        SELECT old.id AS old_id, old.body AS old_body, old.section AS section,
               old.superseded_at AS superseded_at, new.body AS new_body
        FROM memory_entries old
        LEFT JOIN memory_entries new ON old.superseded_by = new.id
        WHERE (? IS NULL OR old.profile_id = ? OR old.profile_id IS NULL)
          AND old.superseded_at IS NOT NULL
        ORDER BY old.superseded_at DESC
        LIMIT ?
        """,
        (profile, profile, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def render_markdown(profile: str | None, conn: sqlite3.Connection) -> str:
    """
    Render the active memory as an about_user.md document.
    Returns empty string if no entries exist.
    """
    entries = list_active_entries(profile, conn)
    if not entries:
        return ""

    by_section: dict[str, list[dict]] = {key: [] for key, _ in SECTIONS}
    for entry in entries:
        by_section.setdefault(entry["section"], []).append(entry)

    lines: list[str] = []
    for key, label in SECTIONS:
        items = by_section.get(key) or []
        if not items:
            continue
        lines.append(f"## {label}")
        for item in items:
            lines.append(_format_entry(item))
        lines.append("")

    changelog = list_changelog(profile, conn)
    if changelog:
        lines.append("---")
        lines.append("## Changelog")
        for ev in changelog:
            when = (ev.get("superseded_at") or "")[:10]
            old_body = (ev.get("old_body") or "").strip().splitlines()[0]
            new_body = (ev.get("new_body") or "").strip().splitlines()[0] if ev.get("new_body") else None
            if new_body:
                lines.append(f"- {when}: superseded \"{_truncate(old_body, 80)}\" → \"{_truncate(new_body, 80)}\"")
            else:
                lines.append(f"- {when}: removed \"{_truncate(old_body, 80)}\"")

    return "\n".join(lines).rstrip() + "\n"


def _format_entry(entry: dict) -> str:
    body = (entry.get("body") or "").strip()
    confidence = entry.get("confidence") or "stated"
    evidence = (entry.get("evidence") or "").strip()
    created_at = (entry.get("created_at") or "")[:10]

    line = f"- {body}"
    meta_bits: list[str] = []
    if confidence != "stated":
        meta_bits.append(confidence)
    if evidence:
        meta_bits.append(evidence)
    elif created_at:
        meta_bits.append(created_at)
    if meta_bits:
        line += f"\n  ↳ {' · '.join(meta_bits)}"
    return line


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ──────────────────────────────────────────────────────────────────────────────
# OBSERVATIONS  (Layer 1 — cheap streaming notes; promoted to entries on threshold)
# ──────────────────────────────────────────────────────────────────────────────

OBSERVATION_THRESHOLD = 3
OBSERVATION_WINDOW_DAYS = 30
OBSERVATION_RETENTION_DAYS = 90
INFERRED_ENTRY_TTL_DAYS = 90


def log_observation(
    *,
    profile: str | None,
    theme: str,
    note: str,
    source_conversation_id: int | None = None,
    conn: sqlite3.Connection,
) -> int:
    """Append a one-line observation. Truncates the log to OBSERVATION_RETENTION_DAYS in passing."""
    theme = (theme or "").strip().lower()
    note = (note or "").strip()
    if not theme or not note:
        raise ValueError("theme and note are required")
    cursor = conn.execute(
        """
        INSERT INTO memory_observations (profile_id, theme, note, source_conversation_id)
        VALUES (?, ?, ?, ?)
        """,
        (profile, theme, note, source_conversation_id),
    )
    conn.execute(
        f"DELETE FROM memory_observations WHERE created_at < datetime('now', '-{OBSERVATION_RETENTION_DAYS} days')"
    )
    return cursor.lastrowid


def count_recent_observations(
    *, profile: str | None, theme: str, conn: sqlite3.Connection,
    window_days: int = OBSERVATION_WINDOW_DAYS,
) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM memory_observations
        WHERE (? IS NULL OR profile_id = ?)
          AND theme = ?
          AND created_at >= datetime('now', '-{int(window_days)} days')
        """,
        (profile, profile, theme),
    ).fetchone()
    return int(row[0]) if row else 0


def list_recent_observations(
    *, profile: str | None, theme: str, conn: sqlite3.Connection, limit: int = 5,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, theme, note, created_at
        FROM memory_observations
        WHERE (? IS NULL OR profile_id = ?)
          AND theme = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (profile, profile, theme, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def has_active_entry_for_theme(*, profile: str | None, theme: str, conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM memory_entries
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
          AND theme = ?
          AND superseded_at IS NULL
        LIMIT 1
        """,
        (profile, profile, theme),
    ).fetchone()
    return row is not None


def _has_pending_proposal_for_theme(*, profile: str | None, theme: str, conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM memory_proposals
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
          AND theme = ?
          AND status = 'pending'
        LIMIT 1
        """,
        (profile, profile, theme),
    ).fetchone()
    return row is not None


def maybe_promote_observation(
    *,
    profile: str | None,
    theme: str,
    section: str,
    body: str,
    source_conversation_id: int | None,
    conn: sqlite3.Connection,
) -> int | None:
    """
    If a theme has crossed OBSERVATION_THRESHOLD recent observations and has no
    active entry or pending proposal yet, create an inferred-confidence proposal.
    Returns the new proposal id, or None if not promoted.
    """
    if has_active_entry_for_theme(profile=profile, theme=theme, conn=conn):
        return None
    if _has_pending_proposal_for_theme(profile=profile, theme=theme, conn=conn):
        return None
    if count_recent_observations(profile=profile, theme=theme, conn=conn) < OBSERVATION_THRESHOLD:
        return None

    recent = list_recent_observations(profile=profile, theme=theme, conn=conn, limit=3)
    evidence_bits = [r["note"] for r in recent if r.get("note")]
    evidence = f"earned from {len(recent)} observations: " + " · ".join(evidence_bits[:3])

    return create_proposal(
        profile=profile,
        section=section,
        body=body,
        confidence="inferred",
        evidence=evidence,
        theme=theme,
        source="observation_threshold",
        source_conversation_id=source_conversation_id,
        conn=conn,
    )


# ──────────────────────────────────────────────────────────────────────────────
# PROPOSALS  (Layer 2 — pending writes the user reviews before they enter the file)
# ──────────────────────────────────────────────────────────────────────────────

_PROPOSAL_SOURCES = {"agent", "observation_threshold", "consolidation", "save_to_memory"}

_DURABLE_USER_PATTERNS = [
    r"\buser\s+(?:prefers|likes|dislikes|hates|wants|needs|is trying|is working|does(?:n't| not) want)\b",
    r"\b(?:prefers|likes|dislikes|hates|wants|needs|trying to|working on|goal is|commit(?:ted)? to)\b",
    r"\b(?:remember that|remember this|save this|call me|my preference|i prefer|i like|i dislike|i hate|i want|i need|i'm trying|i am trying)\b",
    r"\b(?:tone|style|non[- ]judgmental|direct|concise|detailed|friend|best friend|judge|judgment)\b",
]

_TRANSIENT_OR_DERIVABLE_PATTERNS = [
    r"\bhow much\b",
    r"\b(?:spent|spend|paid|charges?|transactions?|categorized|categorised|category|merchant|net worth|balance|chart|graph|plot)\b",
    r"\b(?:move all|recategorize|reclassify|rename|create rule|preview|confirm to|write preview)\b",
    r"\b(?:explained|ready to|need .*to write|spool lineage|python script|bfs script|casual conversation)\b",
    r"\b(?:transaction data|account balances|spending categories|merchant details|saved insights)\b",
    r"\$[\d,]+(?:\.\d{2})?",
    r"\b\d+\s+(?:transactions?|months?|days?)\b",
]


def _looks_like_durable_user_memory(section: str, body: str, evidence: str = "", source: str = "agent") -> bool:
    """
    Gate memory proposals before they enter the review queue.

    Memory is for durable user context: preferences, tone, goals, commitments,
    identity, recurring concerns, and open questions about the user. It is not a
    transcript, not a task log, and not a place for facts we can re-query from
    Folio's database.
    """
    section = (section or "").strip().lower()
    body = (body or "").strip()
    if not body or section not in _SECTION_KEYS:
        return False

    text = f"{body} {evidence or ''}".lower()
    durable = any(_re.search(pattern, text) for pattern in _DURABLE_USER_PATTERNS)
    transient = any(_re.search(pattern, text) for pattern in _TRANSIENT_OR_DERIVABLE_PATTERNS)

    if section == "identity":
        return durable and not transient
    if section == "preferences":
        if transient and not _re.search(r"\b(?:tone|style|direct|concise|detailed|non[- ]judgmental|judge|judgment|talked to|answers?|responses?)\b", text):
            return False
        return durable
    if section == "goals":
        return durable
    if section == "concerns":
        # Concerns must be emotional/behavioral or recurring, not just "spent $X".
        if transient and not _re.search(r"\b(?:worried|concerned|anxious|trying|wants|goal|cap|limit|avoid|cut back|reduce)\b", text):
            return False
        return durable
    if section == "open_questions":
        # Avoid saving ordinary task blockers like "need data structure to write a script".
        return durable and not transient
    return False


def _proposal_duplicate_key(row: dict) -> tuple[str, str]:
    body = _re.sub(r"\s+", " ", (row.get("body") or "").strip().lower())
    return ((row.get("section") or "").strip().lower(), body)


def create_proposal(
    *,
    profile: str | None,
    section: str,
    body: str,
    confidence: str = "inferred",
    evidence: str = "",
    theme: str | None = None,
    supersedes_id: int | None = None,
    source: str = "agent",
    source_conversation_id: int | None = None,
    conn: sqlite3.Connection,
) -> int:
    if section not in _SECTION_KEYS:
        raise ValueError(f"section must be one of {sorted(_SECTION_KEYS)}")
    if confidence not in _CONFIDENCE_VALUES:
        raise ValueError(f"confidence must be one of {sorted(_CONFIDENCE_VALUES)}")
    if source not in _PROPOSAL_SOURCES:
        raise ValueError(f"source must be one of {sorted(_PROPOSAL_SOURCES)}")
    body = body.strip()
    if not body:
        raise ValueError("body is required")
    if source != "consolidation" and not _looks_like_durable_user_memory(section, body, evidence, source):
        raise ValueError("proposal is not durable user memory")

    cursor = conn.execute(
        """
        INSERT INTO memory_proposals
            (profile_id, section, body, confidence, evidence, theme, supersedes_id, source, source_conversation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (profile, section, body, confidence, evidence.strip(), theme, supersedes_id, source, source_conversation_id),
    )
    return cursor.lastrowid


def list_pending_proposals(profile: str | None, conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, profile_id, section, body, confidence, evidence, theme,
               supersedes_id, source, source_conversation_id, created_at
        FROM memory_proposals
        WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
          AND status = 'pending'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (profile, profile, limit),
    ).fetchall()
    items: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        item = dict(row)
        if item.get("source") != "consolidation" and not _looks_like_durable_user_memory(
            item.get("section") or "",
            item.get("body") or "",
            item.get("evidence") or "",
            item.get("source") or "agent",
        ):
            continue
        key = _proposal_duplicate_key(item)
        if key in seen:
            continue
        seen.add(key)
        items.append(item)
    return items


def get_proposal(proposal_id: int, conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT * FROM memory_proposals WHERE id = ?", (proposal_id,)
    ).fetchone()
    return dict(row) if row else None


def accept_proposal(
    *,
    proposal_id: int,
    profile: str | None,
    conn: sqlite3.Connection,
    body_override: str | None = None,
    section_override: str | None = None,
) -> int:
    """
    Materialize a pending proposal into a memory entry. Honors supersedes_id if set.
    Returns the new memory_entries.id. Optional overrides let the user edit before saving.
    """
    proposal = get_proposal(proposal_id, conn)
    if proposal is None:
        raise ValueError(f"no proposal with id {proposal_id}")
    if proposal["status"] != "pending":
        raise ValueError(f"proposal {proposal_id} is already {proposal['status']}")

    section = section_override or proposal["section"]
    body = (body_override or proposal["body"]).strip()
    confidence = proposal["confidence"]
    expires_at = None
    if confidence == "inferred":
        expires_at = _iso_in_days(INFERRED_ENTRY_TTL_DAYS)

    if proposal.get("supersedes_id"):
        new_id = supersede_entry(
            old_id=proposal["supersedes_id"],
            profile=profile,
            new_body=body,
            new_confidence=confidence,
            new_evidence=proposal.get("evidence") or "",
            new_expires_at=expires_at,
            conn=conn,
        )
    else:
        existing_id = find_active_entry_id(profile=profile, section=section, body=body, conn=conn)
        if existing_id:
            new_id = existing_id
        else:
            new_id = insert_entry(
                profile=profile,
                section=section,
                body=body,
                confidence=confidence,
                evidence=proposal.get("evidence") or "",
                theme=proposal.get("theme"),
                expires_at=expires_at,
                conn=conn,
            )

    conn.execute(
        "UPDATE memory_proposals SET status = 'accepted', resolved_at = datetime('now') WHERE id = ?",
        (proposal_id,),
    )
    return new_id


def reject_proposal(*, proposal_id: int, conn: sqlite3.Connection) -> bool:
    cursor = conn.execute(
        "UPDATE memory_proposals SET status = 'rejected', resolved_at = datetime('now') "
        "WHERE id = ? AND status = 'pending'",
        (proposal_id,),
    )
    return cursor.rowcount > 0


def run_consolidation(*, profile: str | None, conn: sqlite3.Connection) -> list[dict]:
    """
    On-demand lint pass: ask the LLM to identify duplicates, contradictions, and
    stale entries in the user's active memory, and emit them as supersede/remove
    proposals the user reviews.

    Returns the list of proposal rows created (empty if nothing to consolidate
    or LLM unavailable).
    """
    entries = list_active_entries(profile, conn)
    if len(entries) < 2:
        return []

    try:
        import llm_client
        if not llm_client.is_available():
            return []
    except Exception:
        return []

    payload_lines = [
        f"#{e['id']} [{e['section']}/{e['confidence']}] {e['body']}" + (f"  (evidence: {e['evidence']})" if e.get('evidence') else "")
        for e in entries
    ]

    # Compute a rough budget pressure to bias the prompt toward shedding entries when full.
    rendered = render_markdown(profile, conn)
    token_estimate = max(1, len(rendered) // 4)
    budget = 4000
    pressure_note = ""
    if token_estimate > budget:
        pressure_note = (
            f"\n\nFILE IS OVER BUDGET ({token_estimate} tokens vs {budget} cap). "
            "Aggressively merge or remove entries. Prioritize removing inferred entries that "
            "haven't been confirmed by stated entries."
        )
    elif token_estimate > int(budget * 0.7):
        pressure_note = (
            f"\n\nFile size approaching budget ({token_estimate} / {budget} tokens). "
            "Lean toward consolidation when judgment calls go either way."
        )

    prompt = (
        "You're reviewing a personal memory file for duplicates, contradictions, and stale entries.\n"
        "Each entry has an id like #42, a section, and a confidence level.\n"
        + pressure_note + "\n\n"
        "Active entries:\n" + "\n".join(payload_lines) + "\n\n"
        "Return a JSON array of consolidation actions. Each action has:\n"
        '  {"action": "supersede", "old_id": <int>, "new_body": "<replacement text>", "reason": "<short reason>"}\n'
        '  {"action": "remove", "old_id": <int>, "reason": "<short reason>"}\n\n'
        "Only emit actions where the case is clear. Return [] if nothing needs consolidating.\n"
        "Return ONLY the JSON array, no markdown, no commentary.\n\nJSON:"
    )

    try:
        raw = llm_client.complete(prompt, max_tokens=600, purpose="copilot")
    except Exception:
        logger.exception("consolidation LLM call failed")
        return []

    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = _re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    try:
        import json as _json
        actions = _json.loads(raw)
    except Exception:
        logger.debug("consolidation LLM returned non-JSON: %r", raw[:200])
        return []
    if not isinstance(actions, list):
        return []

    by_id = {e["id"]: e for e in entries}
    created: list[dict] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        kind = (action.get("action") or "").lower()
        old_id = action.get("old_id")
        if not isinstance(old_id, int) or old_id not in by_id:
            continue
        old = by_id[old_id]
        reason = (action.get("reason") or "").strip() or "consolidation suggestion"

        if kind == "supersede":
            new_body = (action.get("new_body") or "").strip()
            if not new_body or new_body == old["body"]:
                continue
            try:
                pid = create_proposal(
                    profile=profile,
                    section=old["section"],
                    body=new_body,
                    confidence=old["confidence"],
                    evidence=f"consolidation: {reason}",
                    supersedes_id=old_id,
                    source="consolidation",
                    conn=conn,
                )
                created.append(get_proposal(pid, conn) or {})
            except Exception:
                logger.debug("consolidation supersede proposal failed", exc_info=True)
        elif kind == "remove":
            # Model an "empty supersede" — UI will render as a delete suggestion.
            try:
                pid = create_proposal(
                    profile=profile,
                    section=old["section"],
                    body=f"[remove] {old['body']}",
                    confidence=old["confidence"],
                    evidence=f"consolidation: {reason}",
                    supersedes_id=old_id,
                    source="consolidation",
                    conn=conn,
                )
                created.append(get_proposal(pid, conn) or {})
            except Exception:
                logger.debug("consolidation remove proposal failed", exc_info=True)

    return created


def _iso_in_days(days: int) -> str:
    """ISO 8601 timestamp `days` from now, used for expires_at."""
    row = sqlite3.connect(":memory:").execute(
        f"SELECT datetime('now', '+{int(days)} days')"
    ).fetchone()
    return row[0]


# ──────────────────────────────────────────────────────────────────────────────
# RENDER FOR AGENT  (same content, distinguished from user view if needed later)
# ──────────────────────────────────────────────────────────────────────────────

def render_for_agent(profile: str | None, conn: sqlite3.Connection) -> str:
    """Today identical to render_markdown. Reserved as a hook for agent-only annotations."""
    return render_markdown(profile, conn)


# ──────────────────────────────────────────────────────────────────────────────
# TAKEAWAY EXTRACTION  (used by the Save to memory button)
# ──────────────────────────────────────────────────────────────────────────────

_TAKEAWAY_PROMPT = """You're extracting a single durable takeaway from a Copilot conversation turn so it can live in the user's persistent memory file. The file already records numbers and trends derivable from their bank data — those are NOT memorable.

A good takeaway is:
- ABOUT the user: their stated preference, goal, commitment, identity, recurring concern, or open question
- One short line (under 120 characters)
- Still useful months later
- Not just a number or fact you could re-derive from the database
- Not a task log of what the assistant did
- Not a one-off coding/general-knowledge question
- Not a write preview, chart result, transaction total, category total, merchant total, balance, or capability explanation

Allowed sections:
- identity            — who they are (job, family role, life stage)
- preferences         — how they want to be talked to or what they don't want
- goals               — commitments and targets they've stated
- concerns            — patterns or worries they keep returning to
- open_questions      — things you don't yet know about them but should ask

Conversation turn:
USER: {question}
ASSISTANT: {answer}

Return ONLY a single-line JSON object. No markdown, no commentary.
If a memorable takeaway exists:
  {{"section": "<one of the allowed sections>", "body": "<short statement about the user>", "evidence": "<brief why-this-matters or quote>"}}
If nothing memorable can be extracted (the turn was a lookup, computation, or routine answer):
  {{"section": null}}

JSON:"""


_SIGNAL_DETECTOR_PROMPT = """You're scanning a Copilot conversation turn for things worth saving to the user's persistent memory file. The file is the LLM's working notes about WHO the user is — not a transcript and not a fact dump.

Save a signal ONLY if the USER (not the assistant) did one of these in this turn:
- Stated a COMMITMENT or goal: "I'm trying to X", "I want to Y", "going to Z by next month"
- Stated a PREFERENCE about how they want to be talked to or what they like/dislike
- Shared an ENDURING FACT about themselves not derivable from bank data: job, family, life situation, health, plans
- Explicitly asked you to remember something about them
- Asserted something that CONTRADICTS what the file already says about them (you don't see the file here — flag it conservatively)

Do NOT save:
- Lookups ("how much did I spend on X")
- Any transaction/category/merchant/balance/net-worth numbers
- Write previews or categorization actions ("move Netflix to Entertainment", "create a rule")
- Assistant capabilities or schema/tool explanations
- Coding/general-knowledge tasks unless the user states an enduring preference or identity
- One-off questions that don't reveal anything new about the user
- The assistant's framings or analyses (those are derivable; only what the USER asserted matters)
- Vague reactions ("interesting", "ok", "thanks")

Conversation turn:
USER: {question}
ASSISTANT: {answer}

Return ONLY a JSON array. Up to 2 signals max. Each element:
  {{"section": "identity|preferences|goals|concerns|open_questions", "body": "<one short line about the user>", "confidence": "stated", "evidence": "<short quote from the user>"}}

If nothing memorable, return [].
Return ONLY the JSON array, no markdown, no commentary.

JSON:"""


def detect_memory_signals(question: str, answer: str) -> list[dict]:
    """
    Dedicated post-turn LLM call: scans a single turn for memory-worthy signals.
    Returns a list of proposal dicts (possibly empty). Never raises.

    This is the reliability backbone — runs every turn, doesn't depend on the
    conversational agent remembering to emit tags.
    """
    if not (question and answer):
        return []
    try:
        import llm_client
        if not llm_client.is_available():
            return []
        prompt = _SIGNAL_DETECTOR_PROMPT.format(
            question=question.strip()[:2000],
            answer=answer.strip()[:4000],
        )
        raw = llm_client.complete(prompt, max_tokens=400, purpose="copilot")
    except Exception:
        logger.exception("memory signal detector failed")
        return []

    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = _re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    try:
        import json as _json
        items = _json.loads(raw)
    except Exception:
        logger.debug("memory detector returned non-JSON: %r", raw[:200])
        return []
    if not isinstance(items, list):
        return []

    cleaned: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        section = (item.get("section") or "").strip().lower()
        body = (item.get("body") or "").strip()
        if not body or section not in _SECTION_KEYS:
            continue
        confidence = (item.get("confidence") or "stated").strip().lower()
        if confidence not in _CONFIDENCE_VALUES:
            confidence = "stated"
        if not _looks_like_durable_user_memory(section, body, item.get("evidence") or "", source="agent"):
            continue
        cleaned.append({
            "section": section,
            "body": body,
            "confidence": confidence,
            "evidence": (item.get("evidence") or "").strip(),
        })
    return cleaned[:2]


def extract_takeaway(question: str, answer: str) -> dict | None:
    """
    Ask the LLM to distill a Q&A pair into a memory entry. Returns a dict
    {section, body, evidence} or None if nothing memorable / on any failure.
    """
    if not (question and answer):
        return None
    try:
        import llm_client
        if not llm_client.is_available():
            return None
        prompt = _TAKEAWAY_PROMPT.format(
            question=question.strip()[:2000],
            answer=answer.strip()[:4000],
        )
        raw = llm_client.complete(prompt, max_tokens=200, purpose="copilot")
    except Exception:
        logger.exception("takeaway extraction failed")
        return None

    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = _re.sub(r"```[a-z]*\n?", "", raw).strip("`\n ")
    try:
        import json as _json
        parsed = _json.loads(raw)
    except Exception:
        logger.debug("takeaway LLM returned non-JSON: %r", raw[:200])
        return None

    if not isinstance(parsed, dict):
        return None
    section = (parsed.get("section") or "").strip().lower()
    if not section or section not in _SECTION_KEYS:
        return None
    body = (parsed.get("body") or "").strip()
    if not body:
        return None
    evidence = (parsed.get("evidence") or "").strip()
    if not _looks_like_durable_user_memory(section, body, evidence, source="save_to_memory"):
        return None
    return {
        "section": section,
        "body": body,
        "evidence": evidence,
    }


# ──────────────────────────────────────────────────────────────────────────────
# OUTPUT PARSING  (extract <observation> / <memory_proposal> tags from agent reply)
# ──────────────────────────────────────────────────────────────────────────────

import re as _re

_OBS_RE = _re.compile(
    r"<observation\s+theme\s*=\s*\"([^\"]+)\"\s*>(.*?)</observation>",
    _re.IGNORECASE | _re.DOTALL,
)
_PROP_RE = _re.compile(
    r"<memory_proposal\s+([^>]+?)>(.*?)</memory_proposal>",
    _re.IGNORECASE | _re.DOTALL,
)
_ATTR_RE = _re.compile(r'(\w+)\s*=\s*"([^"]*)"')


def parse_agent_memory_tags(answer: str) -> tuple[str, list[dict], list[dict]]:
    """
    Strip <observation> and <memory_proposal> tags from the agent's reply.
    Returns (cleaned_answer, observations, proposals).

    observations: [{"theme": str, "note": str}]
    proposals:    [{"section": str, "body": str, "confidence": str, "evidence": str, "theme": str|None}]
    """
    if not answer:
        return answer or "", [], []

    observations: list[dict] = []
    for match in _OBS_RE.finditer(answer):
        theme = match.group(1).strip().lower()
        note = match.group(2).strip()
        if theme and note:
            observations.append({"theme": theme, "note": note})

    proposals: list[dict] = []
    for match in _PROP_RE.finditer(answer):
        attrs = dict(_ATTR_RE.findall(match.group(1)))
        body = match.group(2).strip()
        section = (attrs.get("section") or "").strip().lower()
        if not body or section not in _SECTION_KEYS:
            continue
        confidence = (attrs.get("confidence") or "stated").strip().lower()
        if confidence not in _CONFIDENCE_VALUES:
            confidence = "stated"
        evidence = (attrs.get("evidence") or "").strip()
        if not _looks_like_durable_user_memory(section, body, evidence, source="agent"):
            continue
        proposals.append({
            "section": section,
            "body": body,
            "confidence": confidence,
            "evidence": evidence,
            "theme": (attrs.get("theme") or "").strip().lower() or None,
        })

    cleaned = _OBS_RE.sub("", answer)
    cleaned = _PROP_RE.sub("", cleaned)
    cleaned = _re.sub(r"<observation\b[^>]*/>", "", cleaned, flags=_re.IGNORECASE | _re.DOTALL)
    cleaned = _re.sub(r"<memory_proposal\b[^>]*/>", "", cleaned, flags=_re.IGNORECASE | _re.DOTALL)
    # Some models emit literal '/n' (slash n) instead of '\n' when trying to write
    # newlines. Treat them as the newlines they meant.
    cleaned = cleaned.replace("/n", "\n")
    cleaned = _re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, observations, proposals
