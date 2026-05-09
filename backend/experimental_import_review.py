from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


REPO_ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT_ROOT = REPO_ROOT / "experiments" / "ml_categorization_hook"
if EXPERIMENT_ROOT.exists() and str(EXPERIMENT_ROOT) not in sys.path:
    sys.path.insert(0, str(EXPERIMENT_ROOT))

EXPERIMENT_WARNING = "Experimental staging-only import review. No production transactions are created or modified."

try:
    from ml_categorization_hook.import_export import export_training_from_staging_db
    from ml_categorization_hook.import_staging import (
        DEFAULT_STAGING_DB,
        apply_review_decision,
        best_suggestions,
        group_counts,
        latest_review_decisions,
        load_staged_transactions,
        open_staging_db,
    )
    from ml_categorization_hook.import_suggest import run_import_suggestions

    EXPERIMENT_IMPORT_ERROR: str | None = None
except Exception as exc:  # pragma: no cover - exercised only by packaged backend without experiments.
    DEFAULT_STAGING_DB = EXPERIMENT_ROOT / "artifacts" / "import_staging" / "private_imports.db"
    EXPERIMENT_IMPORT_ERROR = str(exc)


router = APIRouter(prefix="/api/experiments/import-review", tags=["experiments"])


class ReviewDecisionRequest(BaseModel):
    staged_transaction_id: int
    review_action: str
    review_category: str | None = None
    review_notes: str | None = None


class BulkReviewDecisionRequest(BaseModel):
    group_key: str | None = None
    staged_transaction_ids: list[int] | None = None
    review_action: str
    review_category: str | None = None
    review_notes: str | None = None
    max_rows: int | None = 250


class SuggestRequest(BaseModel):
    threshold: float | None = 0.80
    use_existing_model: bool | None = False
    use_folio_history: bool | None = False
    artifact_dir: str | None = None


class ExportTrainingRequest(BaseModel):
    artifact_dir: str | None = None


def _configured_staging_db() -> Path:
    configured = os.getenv("FOLIO_IMPORT_STAGING_DB", "").strip()
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else REPO_ROOT / path
    return Path(DEFAULT_STAGING_DB)


def _configured_path(env_name: str) -> Path | None:
    configured = os.getenv(env_name, "").strip()
    if not configured:
        return None
    path = Path(configured)
    return path if path.is_absolute() else REPO_ROOT / path


def _artifact_dir(configured: str | None, default_name: str) -> Path:
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else REPO_ROOT / path
    return EXPERIMENT_ROOT / "artifacts" / default_name


def _ensure_experiment_available() -> None:
    if EXPERIMENT_IMPORT_ERROR:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Historical import review experiment is not available in this backend package.",
                "error": EXPERIMENT_IMPORT_ERROR,
            },
        )


def _missing_response(staging_db: Path) -> dict[str, Any]:
    return {
        "staging_db": str(staging_db),
        "exists": False,
        "staged_row_count": 0,
        "suggestion_count": 0,
        "reviewed_count": 0,
        "accepted_count": 0,
        "overridden_count": 0,
        "ignored_count": 0,
        "unreviewed_count": 0,
        "distinct_group_count": 0,
        "warning": EXPERIMENT_WARNING,
        "message": "Staging DB not found.",
    }


def _ensure_staging_db_exists(staging_db: Path) -> None:
    if not staging_db.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "message": "Import staging DB not found.",
                "staging_db": str(staging_db),
                "warning": EXPERIMENT_WARNING,
            },
        )


def _review_payload(decision: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "review_action": (decision or {}).get("review_action"),
        "review_category": (decision or {}).get("review_category"),
        "review_notes": (decision or {}).get("review_notes"),
        "reviewed_at": (decision or {}).get("reviewed_at"),
    }


def _suggestion_payload(suggestion: dict[str, Any] | None) -> dict[str, Any] | None:
    if not suggestion:
        return None
    return {
        "suggested_category": suggestion.get("suggested_category"),
        "confidence": suggestion.get("confidence"),
        "alternatives": suggestion.get("alternatives") or [],
        "reason": suggestion.get("reason"),
        "strategy": suggestion.get("strategy"),
        "evidence": suggestion.get("evidence") or {},
    }


def _recommended_bulk_action(row: dict[str, Any], suggestion: dict[str, Any] | None, group_count: int) -> str:
    if suggestion and group_count > 1 and float(suggestion.get("confidence") or 0) >= 0.8:
        return "accept_or_apply_to_group"
    if suggestion:
        return "accept_suggestion"
    if group_count > 1:
        return "review_group"
    return "manual_review"


def _row_payload(
    row: dict[str, Any],
    suggestion: dict[str, Any] | None,
    decision: dict[str, Any] | None,
    count_by_group: dict[str, int],
) -> dict[str, Any]:
    group_key = str(row.get("group_key") or "")
    group_count = count_by_group.get(group_key, 1)
    return {
        "staged_transaction_id": row.get("id"),
        "date": row.get("posted_date"),
        "amount": row.get("amount"),
        "description": row.get("description"),
        "raw_description": row.get("raw_description"),
        "account_hint": row.get("account_hint"),
        "transaction_type": row.get("transaction_type"),
        "external_category": row.get("external_category"),
        "group_key": group_key,
        "group_count": group_count,
        "suggestion": _suggestion_payload(suggestion),
        "review": _review_payload(decision),
        "recommended_bulk_action": _recommended_bulk_action(row, suggestion, group_count),
    }


def _load_review_rows(staging_db: Path) -> list[dict[str, Any]]:
    with open_staging_db(staging_db) as conn:
        rows = load_staged_transactions(conn)
        suggestions = best_suggestions(conn)
        decisions = latest_review_decisions(conn)
        counts = group_counts(conn)
    return [_row_payload(row, suggestions.get(int(row["id"])), decisions.get(int(row["id"])), counts) for row in rows]


def _matches_status(row: dict[str, Any], status: str) -> bool:
    action = str((row.get("review") or {}).get("review_action") or "")
    has_suggestion = bool(row.get("suggestion"))
    review_status_actions = {
        "accepted": "accept",
        "overridden": "override",
        "ignored": "ignore",
    }
    if status == "all":
        return True
    if status == "needs_review":
        return not action
    if status == "suggested":
        return has_suggestion and not action
    return action == review_status_actions.get(status, status)


@router.get("/status")
def import_review_status():
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    if not staging_db.exists():
        return _missing_response(staging_db)

    with open_staging_db(staging_db) as conn:
        staged_count = int(conn.execute("SELECT COUNT(*) FROM staged_transactions").fetchone()[0])
        suggestion_count = int(
            conn.execute("SELECT COUNT(DISTINCT staged_transaction_id) FROM staged_suggestions").fetchone()[0]
        )
        group_count = int(conn.execute("SELECT COUNT(DISTINCT group_key) FROM staged_transactions").fetchone()[0])
        decisions = latest_review_decisions(conn)

    action_counts = {"accept": 0, "override": 0, "ignore": 0}
    for decision in decisions.values():
        action = str(decision.get("review_action") or "")
        if action in action_counts:
            action_counts[action] += 1
    reviewed_count = sum(action_counts.values())
    return {
        "staging_db": str(staging_db),
        "exists": True,
        "staged_row_count": staged_count,
        "suggestion_count": suggestion_count,
        "reviewed_count": reviewed_count,
        "accepted_count": action_counts["accept"],
        "overridden_count": action_counts["override"],
        "ignored_count": action_counts["ignore"],
        "unreviewed_count": max(0, staged_count - reviewed_count),
        "distinct_group_count": group_count,
        "warning": EXPERIMENT_WARNING,
    }


@router.get("/rows")
def import_review_rows(
    status: str = Query("needs_review", pattern="^(all|needs_review|suggested|accepted|overridden|ignored)$"),
    group_key: str | None = Query(None),
    suggested_category: str | None = Query(None),
    review_category: str | None = Query(None),
    q: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    if not staging_db.exists():
        return {
            "staging_db": str(staging_db),
            "rows": [],
            "total_count": 0,
            "limit": limit,
            "offset": offset,
            "warning": EXPERIMENT_WARNING,
            "message": "Staging DB not found.",
        }

    query = str(q or "").strip().lower()
    filtered = []
    for row in _load_review_rows(staging_db):
        if not _matches_status(row, status):
            continue
        if group_key and row.get("group_key") != group_key:
            continue
        suggestion = row.get("suggestion") or {}
        review = row.get("review") or {}
        if suggested_category and suggestion.get("suggested_category") != suggested_category:
            continue
        if review_category and review.get("review_category") != review_category:
            continue
        if query:
            haystack = " ".join(
                str(value or "")
                for value in (row.get("description"), row.get("raw_description"), row.get("group_key"))
            ).lower()
            if query not in haystack:
                continue
        filtered.append(row)

    return {
        "staging_db": str(staging_db),
        "rows": filtered[offset : offset + limit],
        "total_count": len(filtered),
        "limit": limit,
        "offset": offset,
        "warning": EXPERIMENT_WARNING,
    }


@router.post("/decision")
def import_review_decision(body: ReviewDecisionRequest):
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    _ensure_staging_db_exists(staging_db)

    try:
        with open_staging_db(staging_db) as conn:
            decision = apply_review_decision(
                conn,
                staged_transaction_id=body.staged_transaction_id,
                review_action=body.review_action,
                review_category=body.review_category,
                review_notes=body.review_notes,
            )
            conn.commit()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "status": "updated",
        "staging_db": str(staging_db),
        "staged_transaction_id": body.staged_transaction_id,
        "review": _review_payload(decision),
        "warning": EXPERIMENT_WARNING,
    }


@router.post("/bulk-decision")
def import_review_bulk_decision(body: BulkReviewDecisionRequest):
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    _ensure_staging_db_exists(staging_db)

    ids = [int(item) for item in (body.staged_transaction_ids or []) if item is not None]
    group_key = str(body.group_key or "").strip()
    if not ids and not group_key:
        raise HTTPException(status_code=400, detail="Provide group_key or staged_transaction_ids.")

    max_rows = max(1, int(body.max_rows or 250))
    try:
        with open_staging_db(staging_db) as conn:
            if group_key:
                group_ids = [
                    int(row["id"])
                    for row in conn.execute(
                        "SELECT id FROM staged_transactions WHERE group_key = ? ORDER BY id",
                        (group_key,),
                    ).fetchall()
                ]
                group_id_set = set(group_ids)
                ids = group_ids if not ids else [item for item in ids if item in group_id_set]
            ids = list(dict.fromkeys(ids))
            if len(ids) > max_rows:
                raise ValueError(f"Bulk decision matched {len(ids)} rows, above max_rows={max_rows}")
            decisions = []
            for tx_id in ids:
                decisions.append(
                    apply_review_decision(
                        conn,
                        staged_transaction_id=tx_id,
                        review_action=body.review_action,
                        review_category=body.review_category,
                        review_notes=body.review_notes,
                    )
                )
            conn.commit()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "status": "updated",
        "staging_db": str(staging_db),
        "updated_count": len(ids),
        "staged_transaction_ids": ids,
        "reviews": [_review_payload(decision) for decision in decisions],
        "warning": EXPERIMENT_WARNING,
    }


@router.post("/suggest")
def import_review_suggest(body: SuggestRequest):
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    _ensure_staging_db_exists(staging_db)

    model_dir = _configured_path("FOLIO_IMPORT_MODEL_ARTIFACT_DIR") if body.use_existing_model else None
    if body.use_existing_model and not model_dir:
        raise HTTPException(status_code=400, detail="Set FOLIO_IMPORT_MODEL_ARTIFACT_DIR to use an existing model.")

    history_db = None
    if body.use_folio_history:
        history_db = _configured_path("FOLIO_IMPORT_HISTORY_DB") or REPO_ROOT / "data" / "Folio.db"

    try:
        summary = run_import_suggestions(
            staging_db=staging_db,
            artifact_dir=_artifact_dir(body.artifact_dir, "import_review_suggest"),
            model_artifact_dir=model_dir,
            db_file=history_db,
            threshold=float(body.threshold if body.threshold is not None else 0.80),
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {**summary, "warning": EXPERIMENT_WARNING}


@router.post("/export-training")
def import_review_export_training(body: ExportTrainingRequest):
    _ensure_experiment_available()
    staging_db = _configured_staging_db()
    _ensure_staging_db_exists(staging_db)

    try:
        summary = export_training_from_staging_db(
            staging_db=staging_db,
            artifact_dir=_artifact_dir(body.artifact_dir, "import_review_training"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "path": summary.get("output_path"),
        "output_path": summary.get("output_path"),
        "exported_row_count": summary.get("exported_rows", 0),
        "skipped_row_count": summary.get("skipped_rows", 0),
        "summary": summary,
        "warning": EXPERIMENT_WARNING,
    }
