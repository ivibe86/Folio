from __future__ import annotations

import json
import secrets
from typing import Any

from database import get_db


PENDING_OPERATION_TTL_SECONDS = 300
ALLOWED_OPERATIONS = {
    "bulk_recategorize",
    "create_rule",
    "rename_merchant",
    "confirm_recurring_obligation",
    "dismiss_recurring_obligation",
    "cancel_recurring",
    "restore_recurring",
    "set_budget",
    "create_goal",
    "update_goal_target",
    "mark_goal_funded",
    "set_transaction_note",
    "set_transaction_tags",
    "mark_reviewed",
    "bulk_mark_reviewed",
    "update_manual_account_balance",
    "split_transaction",
}


def _scope_profile(profile: str | None) -> str:
    return profile if profile and profile != "household" else "household"


def _json_dump(payload: dict | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=True, sort_keys=True, default=str)


def _json_load(raw: str | None) -> dict:
    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def store_pending_operation(
    operation: str,
    params: dict,
    profile: str | None,
    preview: dict | None = None,
    *,
    conn=None,
    ttl_seconds: int = PENDING_OPERATION_TTL_SECONDS,
) -> str:
    operation = (operation or "").strip()
    if operation not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported pending operation: {operation}")
    nonce = secrets.token_urlsafe(24)
    scoped_profile = _scope_profile(profile)

    def _store(c):
        c.execute(
            """
            DELETE FROM pending_operations
             WHERE consumed_at IS NOT NULL
                OR datetime(expires_at) <= datetime('now')
            """
        )
        c.execute(
            """
            INSERT INTO pending_operations
                (nonce, profile_id, operation, params_json, preview_json, expires_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', ? || ' seconds'))
            """,
            (nonce, scoped_profile, operation, _json_dump(params), _json_dump(preview), int(ttl_seconds)),
        )

    if conn is not None:
        _store(conn)
    else:
        with get_db() as c:
            _store(c)
    return nonce


def retrieve_pending_operation(
    nonce: str,
    profile: str | None = None,
    *,
    conn=None,
) -> tuple[dict | None, str | None]:
    nonce = (nonce or "").strip()
    if not nonce:
        return None, "confirmation_not_found"

    def _retrieve(c) -> tuple[dict | None, str | None]:
        row = c.execute(
            """
            SELECT nonce, profile_id, operation, params_json, preview_json,
                   created_at, expires_at, consumed_at
              FROM pending_operations
             WHERE nonce = ?
            """,
            (nonce,),
        ).fetchone()
        if row is None:
            return None, "confirmation_not_found"
        entry_profile = row["profile_id"] or "household"
        requested_profile = _scope_profile(profile)
        if entry_profile != requested_profile:
            return None, "profile_mismatch"
        if row["consumed_at"]:
            return None, "confirmation_consumed"
        expired = c.execute(
            "SELECT datetime(?) <= datetime('now')",
            (row["expires_at"],),
        ).fetchone()[0]
        if expired:
            c.execute(
                "UPDATE pending_operations SET consumed_at = datetime('now') WHERE nonce = ?",
                (nonce,),
            )
            return None, "confirmation_expired"
        c.execute(
            "UPDATE pending_operations SET consumed_at = datetime('now') WHERE nonce = ?",
            (nonce,),
        )
        return {
            "nonce": row["nonce"],
            "profile": entry_profile,
            "operation": row["operation"],
            "params": _json_load(row["params_json"]),
            "preview": _json_load(row["preview_json"]),
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
        }, None

    if conn is not None:
        return _retrieve(conn)
    with get_db() as c:
        return _retrieve(c)


def pending_error_message(code: str | None) -> str:
    if code == "confirmation_expired":
        return "Preview expired. Ask Mira to prepare it again."
    if code == "profile_mismatch":
        return "This preview belongs to a different profile. Switch back or ask Mira to prepare it again."
    if code == "confirmation_consumed":
        return "This preview was already used. Ask Mira to prepare it again."
    return "Confirmation not found. Ask Mira to prepare the preview again."
