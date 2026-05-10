"""
main.py
FastAPI backend for Folio personal finance tracker.
"""

from pathlib import Path as FilePath
from datetime import date, datetime, timedelta
import csv
import io
import json

from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, ValidationError
from auth import verify_api_key, rate_limit_middleware
import bank
from bank import validate_teller_config, close_all_clients
import os

from log_config import get_logger, setup_logging
from sync_status import start_sync, finish_sync, get_sync_status, update_phase

# Ensure logging is configured before anything else
setup_logging()

logger = get_logger(__name__)

DEMO_MODE = os.getenv("DEMO_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
_receipt_flag = os.getenv("RECEIPT_INTELLIGENCE_ENABLED")
RECEIPT_INTELLIGENCE_ENABLED = (
    (_receipt_flag.strip().lower() in {"1", "true", "yes", "on"})
    if _receipt_flag is not None
    else not DEMO_MODE
)

from data_manager import (
    get_data, fetch_fresh_data, fetch_simplefin_data,
    update_transaction_category,
    bulk_mark_transactions_reviewed,
    add_category, deactivate_category, get_categories, get_categories_meta, get_category_rules,
    get_accounts_filtered, get_transactions_paginated,
    get_summary_data, get_monthly_analytics_data,
    get_category_analytics_data, get_merchant_insights_data,
    get_net_worth_series_data, get_dashboard_bundle_data,
    update_category_parent, update_category_rule,
    get_copilot_conversations, clear_copilot_conversations, delete_copilot_conversation, get_data_browser_rows,
    log_copilot_conversation, prepare_copilot_history_record, prune_copilot_conversations,
    get_category_budgets, update_category_budget,
    get_goals, upsert_goal, delete_goal,
    get_review_queue_data,
    get_merchant_directory, update_merchant_directory_entry,
    update_transaction_excluded, update_transaction_metadata,
    get_transaction_splits, replace_transaction_splits,
    create_manual_account, update_manual_account, deactivate_manual_account,
    get_data_health_summary,
    get_scheduled_transactions_data,
    get_cash_flow_forecast_data,
    create_month_explanation,
    get_investments_summary_data,
    upsert_investment_holding,
    delete_investment_holding,
    get_backup_status_data,
    create_backup_export_data,
    get_transactions_for_merchant,
    get_category_rule_impact,
    explain_category_assignment, find_merchants_missing_category,
    bulk_recategorize_preview, preview_rule_creation,
    rename_merchant_variants, repair_polluted_merchant_categories,
)
from categorizer import get_active_categories
from categorization_backends import resolve_categorization_backend
from database import init_db, get_db, get_db_session, close_thread_local_connection
from local_llm import (
    get_catalog_response,
    get_status_response,
    update_settings as update_local_llm_settings,
    get_frontend_flags,
    install_model as install_local_llm_model,
    schedule_prewarm_selected_model,
)
from experimental_import_review import router as experimental_import_review_router


def schedule_prewarm_chat_prompt(*args, **kwargs) -> bool:
    return False

# CORS origins from env, with dev defaults
_cors_origins = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://localhost:3000",
).split(",")

from fastapi import APIRouter

# ── Health check (no auth — used by Docker healthcheck) ──
# This is a separate mini-app mounted BEFORE the main app processes
# global dependencies. FastAPI's sub-application mounting ensures
# the health endpoint is completely independent of the main app's auth.
_health_app = FastAPI(title="Health", openapi_url=None)


@_health_app.get("/health")
async def health_check():
    """Health check endpoint for Docker. No auth required."""
    return {"status": "ok"}


app = FastAPI(
    title="Folio API",
    version="3.1.0",
    dependencies=[Depends(verify_api_key)],
)

# Mount health as a sub-application — bypasses all main app middleware and deps
app.mount("/healthz", _health_app)
app.include_router(experimental_import_review_router)

if os.getenv("MERCURY_MIRA_EXPERIMENT", "").strip().lower() in {"1", "true", "yes", "on"}:
    from mira.mercury_adapter import router as mercury_mira_router
    app.include_router(mercury_mira_router)


# Also keep a convenience redirect so /health works too
@app.get("/health", include_in_schema=False, dependencies=[])
async def health_redirect():
    """
    Convenience health endpoint on the main app.
    NOTE: FastAPI's dependencies=[] at decorator level does NOT override
    app-level global deps. For truly unauthenticated health checks,
    Docker should use /healthz/health. This endpoint exists only for
    manual testing by developers who include the API key header.
    """
    return {"status": "ok"}


@app.on_event("startup")
def startup():
    if not DEMO_MODE:
        validate_teller_config()
    init_db()
    # These were previously auto-called at database.py import time.
    # Moved here for explicit, single-point initialization.
    from database import sync_subscription_seeds, sync_enrichment_cache_from_seeds
    sync_subscription_seeds()
    sync_enrichment_cache_from_seeds()
    # Repair legacy non-spending misclassifications before the UI reads totals.
    from data_manager import (
        repair_non_spending_transaction_categories,
        repair_polluted_merchant_categories,
        repair_cc_income_misclassifications,
        reclassify_transfers,
    )
    repair_non_spending_transaction_categories()
    repair_polluted_merchant_categories()
    repair_cc_income_misclassifications()
    reclassify_transfers()
    schedule_prewarm_selected_model("controller")
    schedule_prewarm_selected_model("copilot")
    schedule_prewarm_chat_prompt()


@app.on_event("shutdown")
def shutdown():
    """Close any remaining thread-local DB connections and Teller clients on server shutdown."""
    close_thread_local_connection()
    close_all_clients()


# Rate limiting middleware (must be added before CORS)
app.middleware("http")(rate_limit_middleware)

# [FIX M1] Trusted Host — blocks DNS rebinding attacks
# Only requests with Host header matching these values are accepted
_trusted_hosts = os.getenv(
    "TRUSTED_HOSTS", "*" if DEMO_MODE else "localhost,127.0.0.1,backend"
).split(",")
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=[h.strip() for h in _trusted_hosts],
)

# [FIX M3] CORS — restricted methods and headers (configurable origins via env)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins],
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "X-API-Key"],
)

# Categories excluded from spending calculations
TRANSFER_CATEGORIES = {"Savings Transfer", "Personal Transfer", "Credit Card Payment"}
DIRECT_CASHFLOW_CATEGORIES = {"Cash Withdrawal", "Cash Deposit", "Investment Transfer"}
NON_SPENDING_CATEGORIES = TRANSFER_CATEGORIES | DIRECT_CASHFLOW_CATEGORIES | {"Income", "Credits & Refunds"}

# ── Profile helpers ──────────────────────────────────────────────
def _normalize_profile_whitespace(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _canonicalize_profile_id(value: str | None) -> str:
    return _normalize_profile_whitespace(value).lower()


def _titleize_profile_name(value: str | None) -> str:
    normalized = _normalize_profile_whitespace(value)
    return normalized.title() if normalized else ""


def _invalidate_copilot_cache() -> None:
    try:
        import copilot_cache
        copilot_cache.invalidate_all()
    except Exception:
        logger.debug("Copilot cache invalidation skipped", exc_info=True)


def _display_name_from_profile_id(profile_id: str) -> str:
    return _titleize_profile_name(profile_id) or "Primary"


def _load_profile_rows(conn) -> list[dict]:
    rows = conn.execute(
        """SELECT id, display_name
           FROM profiles
           WHERE TRIM(COALESCE(id, '')) != ''
             AND LOWER(TRIM(id)) != 'household'
           ORDER BY LOWER(COALESCE(display_name, id)), LOWER(id)"""
    ).fetchall()
    return [dict(r) for r in rows]


def _get_profile_list(conn) -> list[dict]:
    rows = _load_profile_rows(conn)
    profiles = [
        {
            "id": row["id"],
            "name": (row.get("display_name") or "").strip() or _display_name_from_profile_id(row["id"]),
        }
        for row in rows
    ]
    if len(profiles) > 1:
        profiles.append({"id": "household", "name": "Household"})
    return profiles


def _load_valid_profiles(conn) -> set[str]:
    return {row["id"] for row in _load_profile_rows(conn)}


def _ensure_profile(profile_value: str, display_name: str | None = None, conn=None) -> dict:
    canonical_id = _canonicalize_profile_id(profile_value)
    if not canonical_id:
        raise ValueError("Profile is required.")
    if canonical_id == "household":
        raise ValueError("'household' is reserved and cannot be used as a profile.")

    desired_display = _titleize_profile_name(display_name) or _display_name_from_profile_id(canonical_id)

    def _upsert(target_conn):
        existing = target_conn.execute(
            "SELECT id, display_name FROM profiles WHERE id = ?",
            (canonical_id,),
        ).fetchone()
        if existing:
            current_display = (existing["display_name"] or "").strip()
            if not current_display or current_display.lower() == canonical_id:
                target_conn.execute(
                    "UPDATE profiles SET display_name = ? WHERE id = ?",
                    (desired_display, canonical_id),
                )
                current_display = desired_display
            return {
                "id": canonical_id,
                "display_name": current_display or desired_display,
                "created": False,
            }

        target_conn.execute(
            """INSERT INTO profiles (id, display_name, is_default)
               VALUES (?, ?, ?)""",
            (canonical_id, desired_display, 1 if canonical_id == "primary" else 0),
        )
        return {"id": canonical_id, "display_name": desired_display, "created": True}

    if conn is not None:
        return _upsert(conn)

    with get_db() as target_conn:
        return _upsert(target_conn)


def _filter_by_profile(items: list[dict], profile: str | None) -> list[dict]:
    """
    Filter a list of dicts (transactions or accounts) by profile.
    - None or 'household' → return all
    - specific name → filter to that profile only
    """
    if not profile or profile == "household":
        return items
    return [item for item in items if item.get("profile") == profile]


# [FIX M4] Profile validation — reject unknown profile names
_VALID_PROFILES: set[str] | None = None


def validate_profile(profile: str | None = Query(None)) -> str | None:
    global _VALID_PROFILES
    normalized = _canonicalize_profile_id(profile)
    if not normalized:
        return None
    if normalized == "household":
        return "household"
    if _VALID_PROFILES is None:
        with get_db() as conn:
            _VALID_PROFILES = _load_valid_profiles(conn) | {"household"}
    if normalized not in _VALID_PROFILES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown profile: '{profile}'. Valid profiles: {sorted(_VALID_PROFILES)}",
        )
    return normalized


def _invalidate_profile_cache():
    """Reset cached profile validation results after profile-affecting changes."""
    global _VALID_PROFILES
    _VALID_PROFILES = None


def _require_live_mode(detail: str = "This action is disabled in demo mode.") -> None:
    if DEMO_MODE:
        raise HTTPException(status_code=403, detail=detail)


def _require_receipts_enabled() -> None:
    if not RECEIPT_INTELLIGENCE_ENABLED:
        raise HTTPException(status_code=404, detail="Receipt intelligence is disabled.")


def _mira_agentic_runtime_payload() -> dict:
    return {
        "miraAgenticEnabled": True,
        "miraAgenticRuntime": "vnext",
    }


def _categorization_status_payload(*, preload_distilbert: bool = False) -> dict:
    backend = resolve_categorization_backend()
    payload = {
        "backend": backend,
        "localLlmCategorization": backend == "local_llm",
        "distilbertCategorization": backend == "distilbert",
        "rulesOnlyCategorization": backend == "rules_only",
    }
    if backend == "distilbert":
        try:
            from distilbert_categorizer import get_runtime_status

            payload["distilbert"] = get_runtime_status(preload=preload_distilbert)
        except Exception as exc:
            payload["distilbert"] = {
                "available": False,
                "warnings": [str(exc)],
            }
    return payload


def _app_config_payload(db=None) -> dict:
    payload = {
        "demoMode": DEMO_MODE,
        "bankLinkingEnabled": not DEMO_MODE,
        "manualSyncEnabled": not DEMO_MODE,
        "demoPersistence": "ephemeral" if DEMO_MODE else "persistent",
        "receiptIntelligenceEnabled": RECEIPT_INTELLIGENCE_ENABLED,
        "categorization": _categorization_status_payload(preload_distilbert=False),
        **_mira_agentic_runtime_payload(),
    }
    try:
        payload.update(get_frontend_flags(db))
    except Exception as exc:
        logger.debug("Failed to load local LLM frontend flags: %s", exc)
    return payload


# ── Models ──


class CategoryUpdate(BaseModel):
    category: str
    one_off: bool = False


class TransactionExcludeUpdate(BaseModel):
    is_excluded: bool


class CopilotRequest(BaseModel):
    question: str
    history: list[dict] | None = None


class SaveInsightRequest(BaseModel):
    question: str
    answer: str
    kind: str = "insight"
    source_conversation_id: int | None = None


class MonthExplanationRequest(BaseModel):
    month: str
    use_llm: bool = True


class MemoryEntryCreate(BaseModel):
    section: str
    body: str
    confidence: str = "stated"
    evidence: str = ""


class MemoryEntryUpdate(BaseModel):
    body: str
    evidence: str | None = None


class MemoryProposalAccept(BaseModel):
    body: str | None = None
    section: str | None = None


class MiraMemoryUpdate(BaseModel):
    normalized_text: str | None = None
    memory_type: str | None = None
    topic: str | None = None
    sensitivity: str | None = None
    confidence: float | None = None
    pinned: bool | None = None
    expires_at: str | None = None
    status: str | None = None


class LocalLlmSettingsUpdate(BaseModel):
    llm_provider: str | None = None
    preset: str | None = None
    categorize_model: str | None = None
    controller_model: str | None = None
    copilot_model: str | None = None
    categorize_batch_size: int | None = None
    inter_batch_delay_ms: int | None = None
    low_power_mode: bool | None = None
    expert_mode: bool | None = None


class LocalLlmInstallRequest(BaseModel):
    model: str


class ReceiptItemUpdateRequest(BaseModel):
    items: list[dict]
    store_name: str | None = None
    receipt_date: str | None = None


# ── Helper Functions ──


def _is_expense(tx: dict) -> bool:
    """True if transaction is a real spending expense."""
    amount = float(tx.get("amount", 0))
    cat = tx.get("category", "Other")
    return amount < 0 and cat not in NON_SPENDING_CATEGORIES


def _is_income(tx: dict) -> bool:
    """True if transaction is income."""
    amount = float(tx.get("amount", 0))
    cat = tx.get("category", "Other")
    return cat == "Income" and amount > 0


def _is_refund(tx: dict) -> bool:
    """
    True if transaction is a refund.
    Positive amount, not income, not a transfer/savings.
    """
    amount = float(tx.get("amount", 0))
    cat = tx.get("category", "Other")
    return amount > 0 and (cat == "Credits & Refunds" or cat not in NON_SPENDING_CATEGORIES)


def _is_savings(tx: dict) -> bool:
    """True if transaction is a savings transfer."""
    return tx.get("category") == "Savings Transfer"


# ── Routes ──


@app.get("/api/profiles")
def profiles(db=Depends(get_db_session)):
    """Return available profile names for the frontend toggle."""
    return _get_profile_list(db)


@app.get("/api/app-config")
def app_config(db=Depends(get_db_session)):
    """Frontend-safe runtime flags for demo/public deployments."""
    return _app_config_payload(db)


@app.get("/api/local-llm/catalog")
def local_llm_catalog(db=Depends(get_db_session)):
    return get_catalog_response(db)


@app.get("/api/categorization/status")
def categorization_status():
    return _categorization_status_payload(preload_distilbert=False)


@app.get("/api/local-llm/status")
def local_llm_status(db=Depends(get_db_session)):
    schedule_prewarm_selected_model("controller", db)
    schedule_prewarm_selected_model("copilot", db)
    schedule_prewarm_chat_prompt()
    return get_status_response(db)


@app.patch("/api/local-llm/settings")
def patch_local_llm_settings(body: LocalLlmSettingsUpdate, db=Depends(get_db_session)):
    payload = {}
    if body.llm_provider is not None:
        payload["llm_provider"] = body.llm_provider
    if body.preset is not None:
        payload["local_ai_profile"] = body.preset
    if body.categorize_model is not None:
        payload["categorize_model"] = body.categorize_model
    if body.controller_model is not None:
        payload["controller_model"] = body.controller_model
    if body.copilot_model is not None:
        payload["copilot_model"] = body.copilot_model
    if body.categorize_batch_size is not None:
        payload["categorize_batch_size"] = body.categorize_batch_size
    if body.inter_batch_delay_ms is not None:
        payload["inter_batch_delay_ms"] = body.inter_batch_delay_ms
    if body.low_power_mode is not None:
        payload["low_power_mode"] = body.low_power_mode
    if body.expert_mode is not None:
        payload["expert_mode"] = body.expert_mode

    try:
        update_local_llm_settings(db, payload)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if body.controller_model is not None or body.llm_provider is not None:
        schedule_prewarm_selected_model("controller", db, force=True)
    if body.copilot_model is not None or body.llm_provider is not None:
        schedule_prewarm_selected_model("copilot", db, force=True)
        schedule_prewarm_chat_prompt(force=True)

    return {
        "status": get_status_response(db),
        "config": _app_config_payload(db),
    }


@app.post("/api/local-llm/install")
def post_local_llm_install(body: LocalLlmInstallRequest, db=Depends(get_db_session)):
    try:
        result = install_local_llm_model(body.model, db)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        **result,
        "config": _app_config_payload(db),
    }


@app.get("/api/accounts")
def accounts(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_accounts_filtered(profile=profile, conn=db)


@app.get("/api/transactions")
def transactions(
    month: str | None = Query(None, description="YYYY-MM"),
    category: str | None = Query(None),
    account: str | None = Query(None),
    search: str | None = Query(None),
    reviewed: bool | None = Query(None),
    profile: str | None = Depends(validate_profile),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db=Depends(get_db_session),
):
    return get_transactions_paginated(
        month=month,
        category=category,
        account=account,
        search=search,
        reviewed=reviewed,
        profile=profile,
        limit=limit,
        offset=offset,
        conn=db,
    )


@app.get("/api/transactions/review-queue")
def transaction_review_queue(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_review_queue_data(profile=profile, conn=db)


@app.post("/api/transactions/bulk-review")
def bulk_review_transactions(
    month: str | None = Query(None, description="YYYY-MM"),
    category: str | None = Query(None),
    account: str | None = Query(None),
    search: str | None = Query(None),
    reviewed: bool | None = Query(None),
    target_reviewed: bool = Query(True),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    return {
        "status": "updated",
        **bulk_mark_transactions_reviewed(
            month=month,
            category=category,
            account=account,
            search=search,
            reviewed=reviewed,
            target_reviewed=target_reviewed,
            profile=profile,
            conn=db,
        ),
    }


@app.get("/api/transactions/export")
def export_transactions(
    month: str | None = Query(None, description="YYYY-MM"),
    category: str | None = Query(None),
    account: str | None = Query(None),
    search: str | None = Query(None),
    reviewed: bool | None = Query(None),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    rows = []
    offset = 0
    total_count = None
    while True:
        result = get_transactions_paginated(
            month=month,
            category=category,
            account=account,
            search=search,
            reviewed=reviewed,
            profile=profile,
            limit=1000,
            offset=offset,
            conn=db,
        )
        page = result.get("data", [])
        rows.extend(page)
        total_count = result.get("total_count", len(rows))
        if not page or len(rows) >= total_count:
            break
        offset += len(page)
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "date", "description", "merchant_display_name", "account_name",
            "amount", "category", "reviewed", "notes", "tags", "profile",
        ],
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({
            **row,
            "reviewed": "yes" if row.get("reviewed") else "no",
            "tags": ", ".join(row.get("tags") or []),
        })
    suffix = month or datetime.utcnow().strftime("%Y-%m-%d")
    return {
        "filename": f"folio-transactions-{suffix}.csv",
        "csv": output.getvalue(),
        "row_count": len(rows),
        "total_count": total_count if total_count is not None else len(rows),
    }


@app.post("/api/receipts/parse")
async def parse_receipt(
    file: UploadFile = File(...),
    profile: str | None = Query(None),
    db=Depends(get_db_session),
):
    _require_receipts_enabled()
    validated_profile = validate_profile(profile)
    content_type = (file.content_type or "").lower()
    if content_type and not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Upload a receipt image.")
    image_bytes = await file.read()
    if not image_bytes:
        raise HTTPException(status_code=400, detail="Receipt image is empty.")
    if len(image_bytes) > 12 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Receipt image must be under 12 MB.")

    try:
        from receipts import create_draft_receipt, parse_receipt_image
        parsed, parser_model = await run_in_threadpool(parse_receipt_image, image_bytes, file.content_type)
        return create_draft_receipt(db, validated_profile, parsed, parser_model)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Receipt parsing failed")
        raise HTTPException(status_code=502, detail=f"Receipt parsing failed: {exc}")


@app.get("/api/receipts")
def receipt_list(
    status: str | None = Query(None),
    limit: int = Query(12, ge=1, le=50),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    _require_receipts_enabled()
    from receipts import list_receipts
    statuses = [part.strip().lower() for part in (status or "").split(",") if part.strip()]
    invalid = [part for part in statuses if part not in {"draft", "approved", "discarded"}]
    if invalid:
        raise HTTPException(status_code=400, detail="Receipt status must be draft, approved, or discarded.")
    return list_receipts(db, profile, statuses or None, limit)


@app.get("/api/receipts/comparisons")
def receipt_comparisons(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    _require_receipts_enabled()
    from receipts import get_comparisons
    return get_comparisons(db, profile)


@app.get("/api/receipts/{receipt_id}")
def receipt_detail(receipt_id: int, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    _require_receipts_enabled()
    from receipts import get_receipt
    try:
        return get_receipt(db, receipt_id, profile)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.patch("/api/receipts/{receipt_id}/items")
def patch_receipt_items(
    receipt_id: int,
    body: ReceiptItemUpdateRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    _require_receipts_enabled()
    from receipts import ReceiptDraftMetadataUpdate, ReceiptItemUpdate, update_receipt_items
    try:
        items = [ReceiptItemUpdate.model_validate(item) for item in body.items]
        metadata = ReceiptDraftMetadataUpdate.model_validate({
            "store_name": body.store_name,
            "receipt_date": body.receipt_date,
        })
        return update_receipt_items(db, receipt_id, profile, items, metadata)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/receipts/{receipt_id}/approve")
def approve_receipt(receipt_id: int, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    _require_receipts_enabled()
    from receipts import set_receipt_status
    try:
        return set_receipt_status(db, receipt_id, profile, "approved")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/receipts/{receipt_id}/discard")
def discard_receipt(receipt_id: int, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    _require_receipts_enabled()
    from receipts import set_receipt_status
    try:
        return set_receipt_status(db, receipt_id, profile, "discarded")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.patch("/api/transactions/{tx_id}/category")
def update_category(tx_id: str, body: CategoryUpdate):
    active_cats = get_active_categories()
    # Allow new categories — they'll be auto-created
    # Only reject empty strings
    if not body.category or not body.category.strip():
        raise HTTPException(
            status_code=400,
            detail="Category cannot be empty.",
        )
    result = update_transaction_category(tx_id, body.category, one_off=body.one_off)
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
    _invalidate_copilot_cache()

    response = {"status": "updated", "tx_id": tx_id, "category": body.category}

    if isinstance(result, dict):
        response["retroactive_count"] = result.get("retroactive_count", 0)
        # Enhancement 7: Pass through subscription prompt signal
        if result.get("subscription_prompt"):
            response["subscription_prompt"] = True
            response["merchant"] = result.get("merchant", "")
            response["amount"] = result.get("amount", 0.0)
            response["transaction_id"] = result.get("transaction_id", tx_id)

    return response


@app.patch("/api/transactions/{tx_id}/exclude")
def update_transaction_exclusion(tx_id: str, body: TransactionExcludeUpdate, db=Depends(get_db_session)):
    result = update_transaction_excluded(tx_id=tx_id, is_excluded=body.is_excluded, conn=db)
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
    _invalidate_copilot_cache()
    return {"status": "updated", "transaction": result}


@app.get("/api/categories")
def categories():
    return get_active_categories()


class NewCategory(BaseModel):
    name: str


class CategoryDeactivateBody(BaseModel):
    replacement_category: str | None = None


@app.post("/api/categories")
def create_category(body: NewCategory):
    """Add a new user-defined category."""
    if not body.name or not body.name.strip():
        raise HTTPException(status_code=400, detail="Category name cannot be empty.")
    success = add_category(body.name.strip())
    if not success:
        raise HTTPException(status_code=409, detail="Category already exists.")
    _invalidate_copilot_cache()
    return {"status": "created", "category": body.name.strip()}


@app.delete("/api/categories/{category_name}")
def delete_category(category_name: str, body: CategoryDeactivateBody | None = None, db=Depends(get_db_session)):
    """Soft-delete a user-defined category, optionally moving references first."""
    try:
        result = deactivate_category(
            category_name,
            replacement_category=body.replacement_category if body else None,
            conn=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not result:
        raise HTTPException(status_code=404, detail="Category not found.")
    _invalidate_copilot_cache()
    return {"status": "deleted", **result}


class ExpenseTypeUpdate(BaseModel):
    expense_type: str


class CategoryParentUpdate(BaseModel):
    parent_category: str | None = None


@app.patch("/api/categories/{category_name}/expense-type")
def update_expense_type(category_name: str, body: ExpenseTypeUpdate, db=Depends(get_db_session)):
    """Update a category's expense_type classification (fixed/variable)."""
    if body.expense_type not in ("fixed", "variable"):
        raise HTTPException(
            status_code=400,
            detail="expense_type must be 'fixed' or 'variable'.",
        )
    row = db.execute(
        "SELECT name, expense_type FROM categories WHERE name = ? AND is_active = 1",
        (category_name,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Category not found.")
    # Don't allow toggling non_expense categories
    if row[1] == "non_expense":
        raise HTTPException(
            status_code=400,
            detail="Cannot change expense type of transfer/income categories.",
        )
    db.execute(
        "UPDATE categories SET expense_type = ?, expense_type_source = 'user' WHERE name = ?",
        (body.expense_type, category_name),
    )
    _invalidate_copilot_cache()
    return {
        "status": "updated",
        "category": category_name,
        "expense_type": body.expense_type,
    }


@app.get("/api/categories/meta")
def categories_meta():
    return get_categories_meta()


@app.patch("/api/categories/{category_name}/parent")
def update_category_parent_endpoint(category_name: str, body: CategoryParentUpdate, db=Depends(get_db_session)):
    try:
        result = update_category_parent(category_name, body.parent_category, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not result:
        raise HTTPException(status_code=404, detail="Category not found.")
    _invalidate_copilot_cache()

    return {"status": "updated", "category": result}


@app.get("/api/category-rules")
def list_category_rules(source: str | None = Query(None)):
    """List category rules, optionally filtered by source ('user' or 'system')."""
    return get_category_rules(source)


class CategoryRuleUpdate(BaseModel):
    category: str | None = None
    priority: int | None = None
    is_active: bool | None = None


@app.patch("/api/category-rules/{rule_id}")
def update_category_rule_endpoint(rule_id: int, body: CategoryRuleUpdate, db=Depends(get_db_session)):
    try:
        result = update_category_rule(
            rule_id=rule_id,
            category=body.category,
            priority=body.priority,
            is_active=body.is_active,
            conn=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not result:
        raise HTTPException(status_code=404, detail="Rule not found.")

    _invalidate_copilot_cache()
    return {"status": "updated", "rule": result}


@app.get("/api/category-rules/{rule_id}/impact")
def category_rule_impact(
    rule_id: int,
    profile: str | None = Depends(validate_profile),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_session),
):
    result = get_category_rule_impact(rule_id=rule_id, profile=profile, limit=limit, conn=db)
    if not result:
        raise HTTPException(status_code=404, detail="Rule not found.")
    return result


@app.get("/api/analytics/monthly")
def monthly_analytics(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_monthly_analytics_data(profile=profile, conn=db)


@app.get("/api/analytics/categories")
def category_analytics(month: str | None = Query(None), profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_category_analytics_data(month=month, profile=profile, conn=db)

@app.get("/api/summary")
def summary(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_summary_data(profile=profile, conn=db)

@app.get("/api/merchants")
def merchant_insights(month: str | None = Query(None), profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    """Merchant-level spending breakdown from Trove-enriched data."""
    return get_merchant_insights_data(month=month, profile=profile, conn=db)


@app.get("/api/analytics/recurring")
def get_recurring_transactions(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    """
    Return recurring / subscription data from the recurring obligation model.
    Detection runs incrementally after each sync (see data_manager.fetch_fresh_data)
    and still maintain legacy merchant subscription fields as a compatibility cache.
    Use POST /api/subscriptions/redetect for a manual full refresh.

    Response includes items, events, and dismissed arrays for the frontend bundle.
    """
    from data_manager import get_recurring_from_db

    try:
        result = get_recurring_from_db(profile=profile, conn=db)
        # If no items stored yet (first load before any sync), fall back to live detection
        if not result["items"] and result["active_count"] == 0:
            from recurring import RecurringDetector, write_detection_results_to_db
            data = get_data()
            txns = data["transactions"]
            txns = _filter_by_profile(txns, profile)
            if txns:
                detector = RecurringDetector(get_db_conn=get_db)
                detection = detector.detect(transactions=txns, profile=profile, generate_events=True)
                write_detection_results_to_db(
                    get_db_conn=get_db,
                    items=detection["items"],
                    events=detection.get("events", []),
                    profile=profile,
                )
                result = get_recurring_from_db(profile=profile, conn=db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Subscription user feedback ───────────────────────────────────

class SubscriptionConfirm(BaseModel):
    merchant: str
    pattern: str | None = None
    frequency_hint: str = "monthly"
    category: str = "Subscriptions"


class SubscriptionDismiss(BaseModel):
    merchant: str
    pattern: str | None = None


@app.post("/api/subscriptions/confirm")
def confirm_subscription(body: SubscriptionConfirm, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    User confirms a detected recurring charge as a subscription.
    Creates a user-sourced seed in the subscription_seeds table.
    """
    from recurring_obligations import canonical_key as _recurring_canonical_key, record_feedback as _record_feedback

    pattern = (body.pattern or body.merchant).upper().strip()
    if not pattern:
        raise HTTPException(status_code=400, detail="Merchant or pattern required.")

    created_by = profile or "household"

    existing = db.execute(
        """SELECT id FROM subscription_seeds
           WHERE pattern = ? AND source = 'user' AND created_by = ?""",
        (pattern, created_by),
    ).fetchone()

    if existing:
        db.execute(
            """UPDATE subscription_seeds
               SET name = ?, frequency_hint = ?, category = ?, is_active = 1
               WHERE id = ?""",
            (body.merchant, body.frequency_hint, body.category, existing[0]),
        )
    else:
        db.execute(
            """INSERT INTO subscription_seeds
               (name, pattern, frequency_hint, category, source, created_by)
               VALUES (?, ?, ?, ?, 'user', ?)""",
            (body.merchant, pattern, body.frequency_hint, body.category, created_by),
        )

    merchant_key = _recurring_canonical_key(body.merchant)
    if merchant_key:
        db.execute(
            """UPDATE recurring_obligations
               SET state = 'confirmed',
                   source = CASE WHEN source = 'user' THEN source ELSE 'user_confirmed' END,
                   confidence_score = MAX(confidence_score, 100),
                   confidence_label = 'user',
                   last_user_action_at = datetime('now'),
                   updated_at = datetime('now')
               WHERE profile_id = ? AND merchant_key = ?""",
            (created_by, merchant_key),
        )
        _record_feedback(
            db,
            merchant=body.merchant,
            profile_id=created_by,
            feedback_type="confirmed",
            scope="merchant",
            payload={
                "pattern": pattern,
                "frequency_hint": body.frequency_hint,
                "category": body.category,
            },
        )

    return {"status": "confirmed", "merchant": body.merchant, "pattern": pattern}


@app.post("/api/subscriptions/dismiss")
def dismiss_subscription(body: SubscriptionDismiss, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    User dismisses a false positive — marks the pattern as inactive for this user.
    Also records in dismissed_recurring table for Enhancement 2.
    If it's a system seed, we create a user-level suppression entry.
    """
    from recurring_obligations import record_feedback as _record_feedback

    pattern = (body.pattern or body.merchant).upper().strip()
    if not pattern:
        raise HTTPException(status_code=400, detail="Merchant or pattern required.")

    created_by = profile or "household"

    existing = db.execute(
        """SELECT id FROM subscription_seeds
           WHERE pattern = ? AND source = 'user' AND created_by = ?""",
        (pattern, created_by),
    ).fetchone()

    if existing:
        db.execute(
            "UPDATE subscription_seeds SET is_active = 0 WHERE id = ?",
            (existing[0],),
        )
    else:
        db.execute(
            """INSERT INTO subscription_seeds
               (name, pattern, frequency_hint, category, source, created_by, is_active)
               VALUES (?, ?, 'monthly', 'Dismissed', 'user', ?, 0)""",
            (body.merchant, pattern, created_by),
        )

    # Also record in dismissed_recurring table (Enhancement 2)
    db.execute(
        """INSERT OR IGNORE INTO dismissed_recurring
           (merchant_name, profile_id)
           VALUES (?, ?)""",
        (body.merchant, created_by),
    )
    _record_feedback(
        db,
        merchant=body.merchant,
        profile_id=created_by,
        feedback_type="dismissed",
        scope="merchant",
        payload={"pattern": pattern},
    )

    return {"status": "dismissed", "merchant": body.merchant, "pattern": pattern}


# ── Subscription management (Enhancements 1-4, 6) ────────────────────────

class SubscriptionDeclare(BaseModel):
    merchant: str
    amount: float
    frequency: str = "monthly"
    category: str = "Subscriptions"
    expected_day: int | None = None
    profile: str | None = None


class SubscriptionAmountReviewDismiss(BaseModel):
    merchant: str
    suggested_amount: float
    latest_date: str
    profile: str | None = None


@app.post("/api/subscriptions/declare")
def declare_subscription_endpoint(body: SubscriptionDeclare, db=Depends(get_db_session)):
    """
    User explicitly declares a transaction as a recurring subscription.
    Layer 0 — always appears in results with confidence = 'user'.
    """
    from data_manager import declare_subscription

    if not body.merchant or not body.merchant.strip():
        raise HTTPException(status_code=400, detail="Merchant name required.")
    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive.")
    if body.frequency not in ("monthly", "quarterly", "semi_annual", "annual"):
        raise HTTPException(status_code=400, detail="Frequency must be monthly, quarterly, semi_annual, or annual.")
    if body.expected_day is not None and not (1 <= int(body.expected_day) <= 31):
        raise HTTPException(status_code=400, detail="Expected day must be between 1 and 31.")

    profile = body.profile or "household"
    result = declare_subscription(
        merchant=body.merchant.strip(),
        amount=body.amount,
        frequency=body.frequency,
        profile=profile,
        category=body.category or "Subscriptions",
        expected_day=body.expected_day,
    )
    return {"status": "ok", "message": "Subscription declared", "subscription": result}


@app.post("/api/subscriptions/amount-review/dismiss")
def dismiss_subscription_amount_review(body: SubscriptionAmountReviewDismiss, db=Depends(get_db_session)):
    """
    Suppress the current expected-amount suggestion for a user-declared recurring bill.
    A newer latest charge or materially different suggested amount can surface again.
    """
    if not body.merchant or not body.merchant.strip():
        raise HTTPException(status_code=400, detail="Merchant name required.")
    if body.suggested_amount <= 0:
        raise HTTPException(status_code=400, detail="Suggested amount must be positive.")
    if not body.latest_date or not body.latest_date.strip():
        raise HTTPException(status_code=400, detail="Latest date required.")

    from recurring_obligations import canonical_key as _recurring_canonical_key, record_feedback as _record_feedback

    profile_id = body.profile or "household"
    merchant = body.merchant.strip()
    result = db.execute(
        """UPDATE user_declared_subscriptions
           SET amount_review_dismissed_amount = ?,
               amount_review_dismissed_latest_date = ?,
               amount_review_dismissed_at = datetime('now'),
               updated_at = datetime('now')
           WHERE profile_id = ?
             AND is_active = 1
             AND (merchant_name = ? OR UPPER(merchant_name) = ?)""",
        (body.suggested_amount, body.latest_date.strip(), profile_id, merchant, merchant.upper()),
    )
    merchant_key = _recurring_canonical_key(merchant)
    v2_exists = db.execute(
        """SELECT 1
           FROM recurring_obligations
           WHERE profile_id = ? AND merchant_key = ?
           LIMIT 1""",
        (profile_id, merchant_key),
    ).fetchone()
    if result.rowcount == 0 and not v2_exists:
        raise HTTPException(status_code=404, detail="User-declared subscription not found.")
    _record_feedback(
        db,
        merchant=merchant,
        profile_id=profile_id,
        feedback_type="amount_review_dismissed",
        scope="exact_candidate",
        payload={
            "suggested_amount": body.suggested_amount,
            "latest_date": body.latest_date.strip(),
        },
    )
    return {"status": "dismissed", "merchant": merchant, "suggested_amount": body.suggested_amount}


@app.post("/api/subscriptions/{merchant}/cancel")
def cancel_subscription_endpoint(merchant: str, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    User confirms an inactive subscription has been cancelled.
    Zombie detection will flag new charges from this merchant.
    """
    from data_manager import cancel_subscription

    if not merchant or not merchant.strip():
        raise HTTPException(status_code=400, detail="Merchant name required.")

    result = cancel_subscription(merchant=merchant.strip(), profile=profile)
    return result


@app.post("/api/subscriptions/{merchant}/restore")
def restore_subscription_endpoint(merchant: str, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    Restore a previously dismissed subscription.
    Removes the merchant from the dismissed_recurring table.
    """
    from data_manager import restore_subscription
    import urllib.parse

    decoded_merchant = urllib.parse.unquote(merchant).strip()
    if not decoded_merchant:
        raise HTTPException(status_code=400, detail="Merchant name required.")

    profile_id = profile or "household"

    # Also re-activate seed if it was suppressed
    existing = db.execute(
        """SELECT id FROM subscription_seeds
           WHERE pattern = ? AND source = 'user' AND created_by = ? AND is_active = 0""",
        (decoded_merchant.upper(), profile_id),
    ).fetchone()
    if existing:
        db.execute(
            "UPDATE subscription_seeds SET is_active = 1 WHERE id = ?",
            (existing[0],),
        )

    success = restore_subscription(merchant=decoded_merchant, profile=profile)
    if not success:
        raise HTTPException(status_code=404, detail="Merchant not found in dismissed list.")
    return {"status": "ok", "message": "Subscription restored"}


@app.get("/api/subscriptions/dismissed")
def list_dismissed_subscriptions(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    """Return all dismissed subscription items for a profile."""
    from data_manager import get_dismissed_subscriptions
    items = get_dismissed_subscriptions(profile=profile, conn=db)
    return {"items": items}


@app.get("/api/subscriptions/events")
def list_subscription_events(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    """Return subscription events (alerts) for a profile."""
    from data_manager import get_subscription_events
    return get_subscription_events(profile=profile, conn=db)


class MarkEventsRead(BaseModel):
    event_ids: list[int]


@app.post("/api/subscriptions/events/mark-read")
def mark_events_read_endpoint(body: MarkEventsRead):
    """Mark subscription events as read."""
    from data_manager import mark_events_read
    if not body.event_ids:
        raise HTTPException(status_code=400, detail="event_ids required.")
    count = mark_events_read(body.event_ids)
    return {"status": "ok", "updated": count}


@app.post("/api/subscriptions/redetect")
def redetect_subscriptions(profile: str | None = Depends(validate_profile)):
    """
    Trigger a full re-detection of recurring subscriptions.
    Scans all transactions and updates the merchants table.
    """
    from data_manager import trigger_full_redetection
    try:
        result = trigger_full_redetection(profile=profile)
        status = result.get("status") or "ok"
        if status == "already_running":
            return {
                "status": "already_running",
                "items_detected": 0,
                "events_generated": 0,
            }
        return {
            "status": "ok",
            "items_detected": len(result.get("items", [])),
            "events_generated": len(result.get("events", [])),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sync")
def sync(profile: str | None = Query(None)):
    _require_live_mode("Manual sync is disabled in demo mode.")
    # Currently syncs all profiles. Profile param reserved for future selective sync.
    job_id = start_sync("manual-sync", phase="starting", detail="Starting manual sync")
    try:
        data = fetch_fresh_data(sync_job_id=job_id)
        finish_sync(job_id, status="completed")
        _invalidate_copilot_cache()
        return {
            "status": "synced",
            "accounts": len(data["accounts"]),
            "transactions": len(data["transactions"]),
            "last_updated": data["last_updated"],
        }
    except Exception as exc:
        finish_sync(job_id, status="failed", error=str(exc))
        raise


@app.get("/api/sync-status")
def sync_status():
    return get_sync_status()


@app.get("/api/data-health")
def data_health(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_data_health_summary(profile=profile, conn=db)


@app.get("/api/scheduled-transactions")
def scheduled_transactions(
    days: int = Query(45, ge=1, le=180),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    return get_scheduled_transactions_data(days=days, profile=profile, conn=db)


@app.post("/api/analytics/explain-month")
def explain_month(
    body: MonthExplanationRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    try:
        return create_month_explanation(body.month, profile=profile, use_llm=body.use_llm, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/analytics/cash-flow-forecast")
def cash_flow_forecast(
    days: int = Query(90, ge=7, le=180),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    return get_cash_flow_forecast_data(days=days, profile=profile, conn=db)


class CopilotConfirm(BaseModel):
    """Client sends back the confirmation_id, NOT raw SQL."""
    question: str
    confirmation_id: str


@app.post("/api/copilot/ask")
async def copilot_ask(body: CopilotRequest, profile: str | None = Query(None)):
    """Compatibility wrapper for non-streaming clients.

    The product UI uses /api/copilot/ask/stream. Keep this path aligned with
    the same dispatcher/runtime so old callers do not hit retired routers.
    """
    from copilot import ask_copilot
    validated_profile = validate_profile(profile)
    result = ask_copilot(question=body.question, profile=validated_profile, history=body.history)
    return result


@app.post("/api/copilot/ask/stream")
async def copilot_ask_stream(body: CopilotRequest, profile: str | None = Query(None)):
    """Streaming variant of /api/copilot/ask — emits Server-Sent Events so the
    UI can render tool progress and the agent's final answer incrementally."""
    import json as _json
    from copilot_agent import run_agent_stream

    validated_profile = validate_profile(profile)

    def event_stream():
        final_event: dict | None = None
        try:
            for event in run_agent_stream(
                question=body.question,
                profile=validated_profile,
                history=body.history,
            ):
                if event.get("type") == "done":
                    final_event = event
                yield f"data: {_json.dumps(event, default=str)}\n\n"
            if final_event:
                try:
                    tool_trace = final_event.get("tool_trace") or []
                    pending_write = final_event.get("pending_write") or {}
                    route = final_event.get("route") or {}
                    route_intent = route.get("intent") or final_event.get("intent")
                    operation = "write_preview" if pending_write else (route_intent or "read")
                    generated_sql = ""
                    rows_affected = final_event.get("rows_affected")
                    if rows_affected is None:
                        rows_affected = len(final_event.get("data") or []) if isinstance(final_event.get("data"), list) else 0
                    record = prepare_copilot_history_record(
                        profile=validated_profile,
                        question=body.question,
                        generated_sql=generated_sql,
                        result=_json.dumps({"route": route, "tool_trace": tool_trace}, default=str),
                        answer=final_event.get("answer") or "",
                        operation=operation,
                        rows_affected=rows_affected,
                        route=route,
                    )
                    log_copilot_conversation(**record)
                    prune_copilot_conversations(profile=validated_profile)
                except Exception:
                    logger.exception("Failed to persist Copilot conversation history")
        except Exception as e:
            yield f"data: {_json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/copilot/confirm")
async def copilot_confirm(body: CopilotConfirm, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    Confirm and execute a write operation previewed by the copilot.
    Client sends a confirmation_id that references a server-stored structured operation.
    The client can no longer supply arbitrary SQL, and Mira no longer stores SQL for previews.
    """
    from data_manager import execute_pending_write_operation
    from pending_operations import pending_error_message, retrieve_pending_operation

    validated_profile = validate_profile(profile)
    pending, code = retrieve_pending_operation(body.confirmation_id, validated_profile, conn=db)
    if pending is None:
        status = 410 if code in {"confirmation_expired", "confirmation_consumed"} else 404
        raise HTTPException(
            status_code=status,
            detail={"code": code or "confirmation_not_found", "message": pending_error_message(code)},
        )

    try:
        result = execute_pending_write_operation(
            pending["operation"],
            pending.get("params") or {},
            validated_profile,
            conn=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"code": "write_rejected", "message": str(exc)})
    from copilot import _log_conversation
    _log_conversation(
        validated_profile,
        body.question,
        "",
        json.dumps({"operation": pending["operation"], "params": pending.get("params") or {}}, default=str),
        result.get("answer") or "",
        "write_executed",
        int(result.get("rows_affected") or 0),
    )
    _invalidate_copilot_cache()
    return result


@app.get("/api/copilot/history")
def copilot_history(
    profile: str | None = Depends(validate_profile),
    limit: int = Query(40, ge=1, le=200),
    db=Depends(get_db_session),
):
    return {"items": get_copilot_conversations(limit=limit, profile=profile, conn=db)}


@app.delete("/api/copilot/history")
def clear_copilot_history(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    deleted = clear_copilot_conversations(profile=profile, conn=db)
    return {"cleared": deleted}


@app.delete("/api/copilot/history/{conversation_id}")
def delete_copilot_history_item(
    conversation_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    deleted = delete_copilot_conversation(conversation_id, profile=profile, conn=db)
    if not deleted:
        raise HTTPException(status_code=404, detail="Copilot history item not found.")
    return {"deleted": deleted}


@app.post("/api/copilot/insights")
def save_insight(
    body: SaveInsightRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Save to memory: extract a durable takeaway from the Q&A pair via LLM and append
    to the persistent memory file as a 'saved' entry. Returns the new entry, or
    {saved: false, reason} if nothing memorable was found.
    """
    import memory as _mem

    question = body.question.strip()
    answer = body.answer.strip()
    if not question or not answer:
        raise HTTPException(status_code=400, detail="question and answer are required")

    takeaway = _mem.extract_takeaway(question, answer)
    if not takeaway:
        return {
            "saved": False,
            "reason": "No durable takeaway detected — this turn was a lookup or routine answer.",
        }

    new_id = _mem.insert_entry(
        profile=profile,
        section=takeaway["section"],
        body=takeaway["body"],
        confidence="saved",
        evidence=takeaway.get("evidence", ""),
        conn=db,
    )
    db.commit()
    row = db.execute(
        "SELECT id, profile_id, section, body, confidence, evidence, theme, created_at "
        "FROM memory_entries WHERE id = ?",
        (new_id,),
    ).fetchone()
    _invalidate_copilot_cache()
    return {"saved": True, "entry": dict(row)}


@app.get("/api/copilot/insights")
def list_insights(
    profile: str | None = Depends(validate_profile),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_session),
):
    rows = db.execute(
        """
        SELECT id, profile_id, question, answer, kind, pinned, source_conversation_id, created_at
        FROM saved_insights
        WHERE (? IS NULL OR profile_id = ?)
        ORDER BY pinned DESC, created_at DESC
        LIMIT ?
        """,
        (profile, profile, limit),
    ).fetchall()
    return {"items": [dict(r) for r in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# PERSISTENT MEMORY (about_user.md)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/memory/entries")
def memory_list_entries(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    return {"items": _mem.list_active_entries(profile, db), "sections": [
        {"key": k, "label": label} for k, label in _mem.SECTIONS
    ]}


@app.post("/api/memory/entries")
def memory_create_entry(
    body: MemoryEntryCreate,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    try:
        new_id = _mem.insert_entry(
            profile=profile,
            section=body.section,
            body=body.body,
            confidence=body.confidence,
            evidence=body.evidence,
            conn=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    db.commit()
    row = db.execute(
        "SELECT id, profile_id, section, body, confidence, evidence, theme, created_at "
        "FROM memory_entries WHERE id = ?",
        (new_id,),
    ).fetchone()
    _invalidate_copilot_cache()
    return dict(row)


@app.patch("/api/memory/entries/{entry_id}")
def memory_update_entry(
    entry_id: int,
    body: MemoryEntryUpdate,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    try:
        new_id = _mem.supersede_entry(
            old_id=entry_id,
            profile=profile,
            new_body=body.body,
            new_evidence=body.evidence or "",
            conn=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    db.commit()
    row = db.execute(
        "SELECT id, profile_id, section, body, confidence, evidence, theme, created_at "
        "FROM memory_entries WHERE id = ?",
        (new_id,),
    ).fetchone()
    _invalidate_copilot_cache()
    return dict(row)


@app.delete("/api/memory/entries/{entry_id}")
def memory_delete_entry(
    entry_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    removed = _mem.delete_entry(entry_id=entry_id, profile=profile, conn=db)
    if not removed:
        raise HTTPException(status_code=404, detail="entry not found")
    db.commit()
    _invalidate_copilot_cache()
    return {"deleted": True, "id": entry_id}


@app.get("/api/memory/markdown")
def memory_markdown(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """Return the rendered about_user.md text plus a token-budget estimate."""
    import memory as _mem
    text = _mem.render_markdown(profile, db)
    char_count = len(text)
    # Crude token estimate: ~4 chars per token. Good enough for a budget gauge.
    token_estimate = max(1, char_count // 4) if text else 0
    return {
        "markdown": text,
        "token_estimate": token_estimate,
        "char_count": char_count,
        "budget": 4000,
    }


@app.get("/api/memory/proposals")
def memory_list_proposals(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    return {"items": _mem.list_pending_proposals(profile, db)}


@app.post("/api/memory/proposals/{proposal_id}/accept")
def memory_accept_proposal(
    proposal_id: int,
    body: MemoryProposalAccept | None = None,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    try:
        new_id = _mem.accept_proposal(
            proposal_id=proposal_id,
            profile=profile,
            conn=db,
            body_override=(body.body if body else None),
            section_override=(body.section if body else None),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    db.commit()
    row = db.execute(
        "SELECT id, profile_id, section, body, confidence, evidence, theme, created_at "
        "FROM memory_entries WHERE id = ?",
        (new_id,),
    ).fetchone()
    _invalidate_copilot_cache()
    return {"accepted": True, "entry": dict(row)}


@app.post("/api/memory/proposals/{proposal_id}/reject")
def memory_reject_proposal(
    proposal_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    import memory as _mem
    rejected = _mem.reject_proposal(proposal_id=proposal_id, conn=db)
    if not rejected:
        raise HTTPException(status_code=404, detail="proposal not found or already resolved")
    db.commit()
    _invalidate_copilot_cache()
    return {"rejected": True, "id": proposal_id}


@app.post("/api/memory/consolidate")
def memory_consolidate(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """On-demand lint pass — proposes supersedes/merges/removals as proposals the user reviews."""
    import memory as _mem
    proposals = _mem.run_consolidation(profile=profile, conn=db)
    db.commit()
    _invalidate_copilot_cache()
    return {"proposals_created": len(proposals), "items": proposals}


# ══════════════════════════════════════════════════════════════════════════════
# MIRA MEMORY V2
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/mira/memories")
def mira_memory_list(
    profile: str | None = Depends(validate_profile),
    include_inactive: bool = Query(False),
    include_expired: bool = Query(False),
    memory_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db=Depends(get_db_session),
):
    from mira import memory_v2

    return {
        "items": memory_v2.list_memories(
            db,
            profile,
            include_inactive=include_inactive,
            include_expired=include_expired,
            memory_type=memory_type,
            limit=limit,
        )
    }


@app.patch("/api/mira/memories/{memory_id}")
def mira_memory_update(
    memory_id: int,
    body: MiraMemoryUpdate,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    from mira import memory_v2

    try:
        updated = memory_v2.update_memory(
            conn=db,
            profile=profile,
            memory_id=memory_id,
            normalized_text=body.normalized_text,
            memory_type=body.memory_type,
            topic=body.topic,
            sensitivity=body.sensitivity,
            confidence=body.confidence,
            pinned=body.pinned,
            expires_at=body.expires_at,
            status=body.status,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not updated:
        raise HTTPException(status_code=404, detail="memory not found")
    db.commit()
    _invalidate_copilot_cache()
    return updated


@app.delete("/api/mira/memories/{memory_id}")
def mira_memory_delete(
    memory_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    from mira import memory_v2

    result = memory_v2.forget_memory(conn=db, profile=profile, memory_id=memory_id)
    if not result.get("forgot"):
        raise HTTPException(status_code=404, detail=result.get("reason") or "memory not found")
    db.commit()
    _invalidate_copilot_cache()
    return {"deleted": True, "id": memory_id}


@app.get("/api/copilot/explain-category")
def copilot_explain_category(
    merchant: str = Query(..., description="Merchant name or description fragment"),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Deterministic tool: explain why a merchant is categorized the way it is.
    Backed by real transaction + rule data — no LLM.
    """
    data = explain_category_assignment(merchant_query=merchant, profile=profile, conn=db)
    count = data["transaction_count"]
    dominant_cat = data["dominant_category"] or "an unknown category"
    dominant_src = data["dominant_source"] or "unknown"
    pattern = data["normalized_pattern"]
    rule = data["rule"]

    source_label = {
        "user": "a manual override",
        "user-rule": "a user-defined rule",
        "llm": "AI categorization",
        "rule": "a built-in rule",
        "fallback": "the fallback default",
        "teller": "the bank's own category",
        "enricher": "merchant enrichment",
        "merchant-memory": "merchant memory",
    }.get(dominant_src, dominant_src)

    if count == 0:
        answer = f'No transactions found matching "{merchant}" (normalized: {pattern}).'
    else:
        rule_detail = ""
        if rule:
            rule_detail = (
                f" A {'user' if rule['source'] == 'user' else 'built-in'} rule exists "
                f"for pattern **{rule['pattern']}** (priority {rule['priority']})."
            )
        answer = (
            f'**{merchant}** is categorized as **{dominant_cat}** '
            f"across {count} transaction{'s' if count != 1 else ''}. "
            f"Assigned by {source_label}.{rule_detail}"
        )

    return {
        "answer": answer,
        "operation": "read",
        "distribution": data["distribution"],
        "samples": data["samples"],
        "rule": rule,
        "transaction_count": count,
    }


@app.get("/api/copilot/merchants-missing-category")
def copilot_merchants_missing_category(
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Deterministic tool: find merchant patterns with uncategorized transactions.
    """
    items = find_merchants_missing_category(profile=profile, conn=db)
    total_tx = sum(item["transaction_count"] for item in items)
    if not items:
        answer = "No uncategorized transactions found. Your data looks clean!"
    else:
        patterns = ", ".join(item["pattern"] for item in items[:5])
        more = f" and {len(items) - 5} more" if len(items) > 5 else ""
        answer = (
            f"Found **{len(items)}** merchant pattern{'s' if len(items) != 1 else ''} "
            f"with {total_tx} uncategorized transaction{'s' if total_tx != 1 else ''}: "
            f"{patterns}{more}."
        )
    return {"answer": answer, "operation": "read", "items": items}


class BulkRecategorizePreviewRequest(BaseModel):
    merchant_query: str
    new_category: str


@app.post("/api/copilot/bulk-recategorize-preview")
def copilot_bulk_recategorize_preview(
    body: BulkRecategorizePreviewRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Deterministic tool: preview moving all transactions for a merchant to a new category.
    Returns a confirmation_id so the existing /copilot/confirm route can execute it.
    """
    from pending_operations import store_pending_operation

    data = bulk_recategorize_preview(
        merchant_query=body.merchant_query,
        new_category=body.new_category,
        profile=profile,
        conn=db,
    )
    count = data["count"]

    if count == 0:
        return {
            "answer": (
                f'No transactions found for "{body.merchant_query}" that aren\'t already '
                f'categorized as **{body.new_category}**.'
            ),
            "operation": "read",
            "count": 0,
            "preview_changes": [],
            "needs_confirmation": False,
        }

    pending = data["pending_operation"]
    confirmation_id = store_pending_operation(
        pending["operation"],
        pending["params"],
        profile,
        {"rows_affected": count, "samples": data["samples"]},
        conn=db,
    )

    preview_changes = [
        {"column": "category", "raw_value": body.new_category, "new_value": body.new_category}
    ]

    answer = (
        f"Found **{count}** {body.merchant_query} transaction{'s' if count != 1 else ''} "
        f"to move to **{body.new_category}**. Confirm to apply."
    )

    return {
        "answer": answer,
        "operation": "write_preview",
        "count": count,
        "samples": data["samples"],
        "preview_changes": preview_changes,
        "confirmation_id": confirmation_id,
        "needs_confirmation": True,
        "rows_affected": count,
    }


class PreviewRuleRequest(BaseModel):
    pattern: str
    category: str


@app.post("/api/copilot/preview-rule")
def copilot_preview_rule(
    body: PreviewRuleRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Deterministic tool: preview creating a new user category rule.
    Returns a confirmation_id so the existing /copilot/confirm route can execute the INSERT.
    """
    from pending_operations import store_pending_operation

    data = preview_rule_creation(
        raw_pattern=body.pattern,
        category=body.category,
        profile=profile,
        conn=db,
    )
    count = data["count"]
    existing = data["existing_rule"]
    pattern = data["pattern"]

    pending = data["pending_operation"]
    confirmation_id = store_pending_operation(
        pending["operation"],
        pending["params"],
        profile,
        {"rows_affected": count, "samples": data["samples"]},
        conn=db,
    )

    preview_changes = [
        {"column": "rule", "raw_value": f"{pattern} → {body.category}", "new_value": body.category}
    ]

    if existing:
        existing_note = (
            f" Note: a rule for **{pattern}** already exists "
            f"(currently → {existing['category']}) — this will replace it."
        )
    else:
        existing_note = ""

    answer = (
        f"Creating rule **{pattern}** → **{body.category}** will apply to "
        f"**{count}** existing transaction{'s' if count != 1 else ''} "
        f"and all future matches.{existing_note} Confirm to create."
    )

    return {
        "answer": answer,
        "operation": "write_preview",
        "count": count,
        "samples": data["samples"],
        "preview_changes": preview_changes,
        "confirmation_id": confirmation_id,
        "needs_confirmation": True,
        "rows_affected": count,
        "existing_rule": existing,
    }


class RenameMerchantRequest(BaseModel):
    old_name: str
    new_name: str


@app.post("/api/copilot/rename-merchant-preview")
def copilot_rename_merchant_preview(
    body: RenameMerchantRequest,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Deterministic tool: preview renaming a merchant across all matching transactions.
    Returns a confirmation_id so the existing /copilot/confirm route can execute both UPDATEs.
    """
    from pending_operations import store_pending_operation

    data = rename_merchant_variants(
        old_pattern=body.old_name,
        new_name=body.new_name,
        profile=profile,
        conn=db,
    )
    count = data["count"]

    if count == 0:
        return {
            "answer": f'No transactions found matching "{body.old_name}".',
            "operation": "read",
            "count": 0,
            "preview_changes": [],
            "needs_confirmation": False,
        }

    pending = data["pending_operation"]
    confirmation_id = store_pending_operation(
        pending["operation"],
        pending["params"],
        profile,
        {"rows_affected": count, "samples": data["samples"]},
        conn=db,
    )
    preview_changes = [
        {"column": "merchant_name", "raw_value": body.new_name, "new_value": body.new_name}
    ]

    return {
        "answer": (
            f"Found **{count}** transaction{'s' if count != 1 else ''} for "
            f"**{body.old_name}** to rename to **{body.new_name}**. Confirm to apply."
        ),
        "operation": "write_preview",
        "count": count,
        "samples": data["samples"],
        "preview_changes": preview_changes,
        "confirmation_id": confirmation_id,
        "needs_confirmation": True,
        "rows_affected": count,
    }


@app.get("/api/copilot/data-browser")
def copilot_data_browser(
    table: str = Query(..., description="Safe allowlisted table name"),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=250),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    try:
        rows = get_data_browser_rows(table=table, profile=profile, search=search, limit=limit, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"table": table, "items": rows}


@app.get("/api/budgets")
def budgets(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return {"items": get_category_budgets(profile=profile, conn=db)}


class BudgetUpdate(BaseModel):
    amount: float | None = None
    rollover_mode: str | None = None
    rollover_balance: float | None = None


@app.patch("/api/budgets/{category_name}")
def update_budget_endpoint(
    category_name: str,
    body: BudgetUpdate,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    try:
        result = update_category_budget(
            category=category_name,
            amount=body.amount,
            profile=profile,
            conn=db,
            rollover_mode=body.rollover_mode,
            rollover_balance=body.rollover_balance,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "updated", "budget": result}


class GoalPayload(BaseModel):
    id: int | None = None
    name: str
    goal_type: str = "custom"
    target_amount: float = 0
    current_amount: float = 0
    target_date: str | None = None
    linked_category: str | None = None
    linked_account_id: str | None = None


@app.get("/api/goals")
def goals(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return {"items": get_goals(profile=profile, conn=db)}


@app.post("/api/goals")
def create_goal(body: GoalPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    try:
        goal = upsert_goal(body.model_dump(), profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "created", "goal": goal}


@app.patch("/api/goals/{goal_id}")
def update_goal(goal_id: int, body: GoalPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    payload = body.model_dump()
    payload["id"] = goal_id
    try:
        goal = upsert_goal(payload, profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "updated", "goal": goal}


@app.delete("/api/goals/{goal_id}")
def remove_goal(goal_id: int, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    if not delete_goal(goal_id, profile=profile, conn=db):
        raise HTTPException(status_code=404, detail="Goal not found.")
    return {"status": "deleted"}


class TransactionMetadataUpdate(BaseModel):
    notes: str | None = None
    tags: list[str] | None = None
    reviewed: bool | None = None


@app.patch("/api/transactions/{tx_id}/metadata")
def update_transaction_metadata_endpoint(tx_id: str, body: TransactionMetadataUpdate, db=Depends(get_db_session)):
    result = update_transaction_metadata(
        tx_id,
        notes=body.notes,
        tags=body.tags,
        reviewed=body.reviewed,
        conn=db,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found.")
    return {"status": "updated", "transaction": result}


class TransactionSplitItem(BaseModel):
    category: str
    amount: float
    notes: str | None = ""
    tags: list[str] | None = None


class TransactionSplitsUpdate(BaseModel):
    splits: list[TransactionSplitItem]


@app.get("/api/transactions/{tx_id}/splits")
def transaction_splits(tx_id: str, db=Depends(get_db_session)):
    return {"items": get_transaction_splits(tx_id, conn=db)}


@app.patch("/api/transactions/{tx_id}/splits")
def update_transaction_splits_endpoint(tx_id: str, body: TransactionSplitsUpdate, db=Depends(get_db_session)):
    result = replace_transaction_splits(
        tx_id,
        [item.model_dump() for item in body.splits],
        conn=db,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found.")
    return {"status": "updated", **result}


class ManualAccountPayload(BaseModel):
    name: str
    account_type: str = "depository"
    account_subtype: str = "manual"
    balance: float = 0
    notes: str | None = ""


class InvestmentHoldingPayload(BaseModel):
    account_id: str | None = None
    symbol: str | None = ""
    name: str
    asset_class: str = "stock"
    quantity: float = 0
    cost_basis: float = 0
    current_price: float = 0
    manual_value: float | None = None
    target_percent: float | None = None
    notes: str | None = ""
    price_as_of: str | None = None


@app.post("/api/manual-accounts")
def create_manual_account_endpoint(body: ManualAccountPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    try:
        account = create_manual_account(body.model_dump(), profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "created", "account": account}


@app.patch("/api/manual-accounts/{account_id}")
def update_manual_account_endpoint(account_id: str, body: ManualAccountPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    try:
        account = update_manual_account(account_id, body.model_dump(), profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not account:
        raise HTTPException(status_code=404, detail="Manual account not found.")
    return {"status": "updated", "account": account}


@app.delete("/api/manual-accounts/{account_id}")
def delete_manual_account_endpoint(account_id: str, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    if not deactivate_manual_account(account_id, profile=profile, conn=db):
        raise HTTPException(status_code=404, detail="Manual account not found.")
    return {"status": "deleted"}


@app.get("/api/investments")
def investments_endpoint(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_investments_summary_data(profile=profile, conn=db)


@app.post("/api/investments/holdings")
def create_investment_holding_endpoint(body: InvestmentHoldingPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    try:
        holding = upsert_investment_holding(body.model_dump(), profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "created", "holding": holding}


@app.patch("/api/investments/holdings/{holding_id}")
def update_investment_holding_endpoint(holding_id: int, body: InvestmentHoldingPayload, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    try:
        holding = upsert_investment_holding(body.model_dump(), holding_id=holding_id, profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not holding:
        raise HTTPException(status_code=404, detail="Holding not found.")
    return {"status": "updated", "holding": holding}


@app.delete("/api/investments/holdings/{holding_id}")
def delete_investment_holding_endpoint(holding_id: int, profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    if not delete_investment_holding(holding_id, profile=profile, conn=db):
        raise HTTPException(status_code=404, detail="Holding not found.")
    return {"status": "deleted"}


@app.get("/api/backup/status")
def backup_status_endpoint(profile: str | None = Depends(validate_profile), db=Depends(get_db_session)):
    return get_backup_status_data(profile=profile, conn=db)


@app.get("/api/backup/export")
def backup_export_endpoint(
    include_credentials: bool = Query(False),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    data = create_backup_export_data(profile=profile, include_credentials=include_credentials, conn=db)
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    filename = f"folio-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/merchant-directory")
def merchant_directory(
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=250),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    items = get_merchant_directory(profile=profile, search=search, limit=limit, conn=db)
    return {"items": items}


@app.get("/api/merchant-directory/{merchant_key}/transactions")
def merchant_directory_transactions(
    merchant_key: str,
    profile_id: str | None = Query(None),
    profile: str | None = Depends(validate_profile),
    limit: int = Query(25, ge=1, le=100),
    db=Depends(get_db_session),
):
    effective_profile = profile_id or (profile if profile and profile != "household" else None)
    items = get_transactions_for_merchant(
        merchant_key=merchant_key,
        profile_id=effective_profile,
        limit=limit,
        conn=db,
    )
    return {"items": items}


class MerchantDirectoryUpdate(BaseModel):
    profile_id: str
    clean_name: str | None = None
    category: str | None = None
    domain: str | None = None
    industry: str | None = None


@app.patch("/api/merchant-directory/{merchant_key}")
def update_merchant_directory_endpoint(
    merchant_key: str,
    body: MerchantDirectoryUpdate,
    db=Depends(get_db_session),
):
    try:
        result = update_merchant_directory_entry(
            merchant_key=merchant_key,
            profile_id=body.profile_id,
            clean_name=body.clean_name,
            category=body.category,
            domain=body.domain,
            industry=body.industry,
            conn=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not result:
        raise HTTPException(status_code=404, detail="Merchant not found.")

    return {"status": "updated", "merchant": result}


@app.get("/api/dashboard-bundle")
def dashboard_bundle(
    nw_interval: str = Query("biweekly", description="weekly or biweekly"),
    as_of: str | None = Query(None, description="Local YYYY-MM-DD date used for dashboard planning metrics"),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Single-request dashboard loader.
    Returns summary, accounts, monthly analytics, category analytics,
    and net-worth time series — all using SQL-level aggregation.
    Replaces 5 separate API calls.
    """
    def _load_bundle():
        bundle = get_dashboard_bundle_data(nw_interval=nw_interval, profile=profile, conn=db, as_of=as_of)
        return {**bundle, "config": _app_config_payload(db)}

    try:
        import copilot_cache

        fingerprint = copilot_cache.db_fingerprint(db, profile)
        return copilot_cache.get_or_set(
            "dashboard_bundle",
            copilot_cache.make_key(nw_interval, profile or "household", as_of or date.today().isoformat(), fingerprint),
            _load_bundle,
        )
    except Exception:
        return _load_bundle()


@app.get("/api/proactive-insights")
def proactive_insights_endpoint(
    include_dismissed: bool = Query(False),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    from proactive_insights import list_insights

    return {"items": list_insights(profile=profile, include_dismissed=include_dismissed, conn=db, generate=True)}


@app.post("/api/proactive-insights/{insight_id}/dismiss")
def dismiss_proactive_insight_endpoint(
    insight_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    from proactive_insights import dismiss_insight

    if not dismiss_insight(insight_id, profile=profile, conn=db):
        raise HTTPException(status_code=404, detail="Insight not found.")
    return {"status": "dismissed", "id": insight_id}


@app.post("/api/proactive-insights/{insight_id}/restore")
def restore_proactive_insight_endpoint(
    insight_id: int,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    from proactive_insights import restore_insight

    if not restore_insight(insight_id, profile=profile, conn=db):
        raise HTTPException(status_code=404, detail="Insight not found.")
    return {"status": "active", "id": insight_id}


@app.get("/api/analytics/net-worth-series")
def net_worth_series(
    interval: str = Query("weekly", description="weekly or biweekly"),
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Compute a running net-worth time series from transaction history.
    Returns one data point per week (or bi-week), preserving intra-month
    volatility that the monthly endpoint destroys.
    """
    return get_net_worth_series_data(interval=interval, profile=profile, conn=db)

# ══════════════════════════════════════════════════════════════════════════════
# TELLER CONNECT ENROLLMENT
# ══════════════════════════════════════════════════════════════════════════════


class EnrollRequest(BaseModel):
    accessToken: str
    institutionName: str = ""
    enrollmentId: str | None = None


@app.get("/api/teller-config")
def teller_config():
    """
    Return the Teller application ID and environment so the frontend
    can initialize Teller Connect without hardcoding secrets.
    """
    if DEMO_MODE:
        return {
            **_app_config_payload(),
            "enabled": False,
            "applicationId": "",
            "environment": "sandbox",
        }

    app_id = os.getenv("TELLER_APPLICATION_ID", "")
    env = os.getenv("TELLER_ENVIRONMENT", "sandbox")
    if not app_id:
        raise HTTPException(
            status_code=503,
            detail="TELLER_APPLICATION_ID not configured on the server.",
        )
    return {
        **_app_config_payload(),
        "enabled": True,
        "applicationId": app_id,
        "environment": env,
    }


@app.post("/api/enroll")
def enroll_account(req: EnrollRequest):
    _require_live_mode("Bank enrollment is disabled in demo mode.")
    """
    Handle a new Teller Connect enrollment.

    1. Validate the token by fetching accounts from Teller.
    2. Attempt to resolve the owner's name via the Identity API.
    3. Persist the token in the encrypted token store.
    4. Hot-reload the in-memory token/profile registries.
    5. Trigger a data sync for the new accounts.
    """
    from bank import (
        get_accounts_for_token,
        get_identity,
        reload_tokens_and_profiles,
    )
    from token_store import save_token

    # 1. Validate — can we actually use this token?
    accounts = get_accounts_for_token(req.accessToken)
    if not accounts:
        raise HTTPException(
            status_code=400,
            detail="Could not fetch accounts with the provided token. It may be invalid or expired.",
        )

    # 2. Resolve identity
    first_account_id = accounts[0].get("id", "")
    identity = {"first_name": "", "last_name": "", "full_name": ""}
    if first_account_id:
        identity = get_identity(req.accessToken, first_account_id)

    # Determine profile name: prefer identity first name, fall back to "primary"
    profile_name = (
        identity["first_name"].lower().strip()
        if identity["first_name"]
        else "primary"
    )

    # Sanitize: if the name looks like a company or is too long, fall back
    if len(profile_name) > 20 or " " in profile_name:
        profile_name = "primary"
        
    # 3. Persist
    profile_record = _ensure_profile(
        profile_name,
        display_name=identity["first_name"] or profile_name,
    )
    was_new = save_token(
        profile=profile_record["id"],
        token=req.accessToken,
        institution=req.institutionName,
        owner_name=identity["full_name"],
        enrollment_id=req.enrollmentId,
    )

    # 4. Hot-reload
    reload_tokens_and_profiles()
    _invalidate_profile_cache()

    # 5. Sync the new accounts into the transaction database
    sync_result = {"accounts": 0, "transactions": 0}
    job_id = start_sync("enrollment", phase="starting", detail="Starting account enrollment sync")
    try:
        data = fetch_fresh_data(sync_job_id=job_id)
        sync_result = {
            "accounts": len(data.get("accounts", [])),
            "transactions": len(data.get("transactions", [])),
        }
        finish_sync(job_id, status="completed")
    except Exception as e:
        finish_sync(job_id, status="failed", error=str(e))
        logger.warning("Post-enrollment sync failed (non-fatal): %s", e)

    institution = req.institutionName or accounts[0].get("institution", {}).get("name", "Unknown")

    return {
        "status": "enrolled" if was_new else "already_exists",
        "profile": profile_record["id"],
        "institution": institution,
        "owner": identity["full_name"],
        "accounts_found": len(accounts),
        "synced": sync_result,
    }


@app.get("/api/enrollments")
def list_enrollments():
    """Return all active Teller Connect enrollments (metadata only, no tokens)."""
    if DEMO_MODE:
        return []
    from token_store import load_all_enrollments
    return load_all_enrollments()


class DeactivateEnrollment(BaseModel):
    id: int


@app.post("/api/enrollments/deactivate")
def deactivate_enrollment(body: DeactivateEnrollment):
    """Soft-delete an enrollment. The token will no longer be used on next reload."""
    _require_live_mode("Bank enrollment changes are disabled in demo mode.")
    from token_store import deactivate_token
    from bank import reload_tokens_and_profiles

    success = deactivate_token(body.id)
    if not success:
        raise HTTPException(status_code=404, detail="Enrollment not found or already inactive.")

    reload_tokens_and_profiles()
    _invalidate_profile_cache()

    return {"status": "deactivated", "id": body.id}


# ── Provider Migration ────────────────────────────────────────────────────────

@app.get("/api/migration/status")
def migration_status(db=Depends(get_db_session)):
    """
    Lightweight check: do both Teller and SimpleFIN have active data?
    Returns {needs_migration, overlap_days, simplefin_window_start}.
    Used by the dashboard to decide whether to show the migration banner.
    """
    if DEMO_MODE:
        return {"needs_migration": False, "overlap_days": 0, "simplefin_window_start": None}

    teller_count = db.execute(
        "SELECT COUNT(*) FROM enrolled_tokens WHERE is_active = 1"
    ).fetchone()[0]

    sf_count = db.execute(
        "SELECT COUNT(*) FROM simplefin_connections WHERE is_active = 1"
    ).fetchone()[0]

    if not teller_count or not sf_count:
        return {"needs_migration": False, "overlap_days": 0, "simplefin_window_start": None}

    sf_start = db.execute(
        "SELECT MIN(date) FROM transactions WHERE id LIKE 'sf_%' AND is_excluded = 0"
    ).fetchone()[0]

    teller_end = db.execute(
        "SELECT MAX(date) FROM transactions WHERE id NOT LIKE 'sf_%' AND is_excluded = 0"
    ).fetchone()[0]

    if not sf_start or not teller_end:
        return {"needs_migration": False, "overlap_days": 0, "simplefin_window_start": sf_start}

    from datetime import date as _date
    try:
        d1 = _date.fromisoformat(sf_start)
        d2 = _date.fromisoformat(teller_end)
        overlap_days = max(0, (d2 - d1).days)
    except ValueError:
        overlap_days = 0

    return {
        "needs_migration": overlap_days > 0,
        "overlap_days": overlap_days,
        "simplefin_window_start": sf_start,
    }


@app.get("/api/migration/preview")
def migration_preview(db=Depends(get_db_session)):
    _require_live_mode("Provider migration is disabled in demo mode.")
    from migration import analyze_migration
    return analyze_migration(db)


class MigrationExecuteRequest(BaseModel):
    mappings: list[dict]  # [{"teller_account_id": "...", "sf_account_id": "..." | None}]
    deactivate_teller: bool = True


@app.post("/api/migration/execute")
def migration_execute(
    req: MigrationExecuteRequest,
    db=Depends(get_db_session),
):
    _require_live_mode("Provider migration is disabled in demo mode.")
    from migration import execute_migration
    try:
        result = execute_migration(req.mappings, req.deactivate_teller, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _invalidate_profile_cache()
    return result


# ── SimpleFIN Bridge ─────────────────────────────────────────────────────────

class SimpleFINClaimRequest(BaseModel):
    setupToken: str
    profile: str
    displayName: str = ""


@app.post("/api/simplefin/claim")
def simplefin_claim(req: SimpleFINClaimRequest, background_tasks: BackgroundTasks):
    _require_live_mode("SimpleFIN connection is disabled in demo mode.")
    """
    Exchange a SimpleFIN Setup Token for an Access URL.

    1. base64-decode → claim URL → POST to get permanent Access URL.
    2. Encrypt and store in simplefin_connections table.
    3. Kick off initial sync in the background (LLM categorization can take
       30-120 s — running it synchronously causes the frontend proxy to timeout).
    """
    import simplefin

    requested_profile = req.profile or "primary"
    canonical_profile = _canonicalize_profile_id(requested_profile)
    if not canonical_profile:
        raise HTTPException(status_code=400, detail="Profile is required.")
    if canonical_profile == "household":
        raise HTTPException(status_code=400, detail="'household' is reserved and cannot be used as a profile.")

    # 1. Claim
    try:
        access_url = simplefin.claim_setup_token(req.setupToken)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    profile_record = _ensure_profile(canonical_profile, display_name=requested_profile)

    # 2. Store
    try:
        conn_id = simplefin.save_connection(
            profile=profile_record["id"],
            access_url=access_url,
            display_name=req.displayName,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    _invalidate_profile_cache()

    # 3. Initial sync runs in the background so this endpoint returns immediately
    job_id = start_sync("simplefin", phase="queued", detail="Queued SimpleFIN initial sync")

    def _bg_sync():
        try:
            update_phase(job_id, "starting", "Starting SimpleFIN initial sync")
            fetch_simplefin_data(sync_job_id=job_id)
            finish_sync(job_id, status="completed")
        except Exception as e:
            finish_sync(job_id, status="failed", error=str(e))
            logger.warning("Post-claim SimpleFIN background sync failed: %s", e)

    background_tasks.add_task(_bg_sync)

    return {
        "status": "connected",
        "connection_id": conn_id,
        "profile": profile_record["id"],
        "displayName": req.displayName,
        "syncing": True,
    }


@app.get("/api/simplefin/connections")
def simplefin_connections():
    """Return all active SimpleFIN connections (metadata only, no access URLs)."""
    if DEMO_MODE:
        return []
    import simplefin
    return simplefin.load_all_connections()


class SimpleFINDeactivate(BaseModel):
    id: int


@app.post("/api/simplefin/connections/deactivate")
def simplefin_deactivate(body: SimpleFINDeactivate):
    """Soft-delete a SimpleFIN connection."""
    _require_live_mode("SimpleFIN connection changes are disabled in demo mode.")
    import simplefin

    success = simplefin.deactivate_connection(body.id)
    if not success:
        raise HTTPException(status_code=404, detail="Connection not found or already inactive.")

    _invalidate_profile_cache()
    return {"status": "deactivated", "id": body.id}


@app.post("/api/simplefin/sync")
def simplefin_sync():
    """Trigger a SimpleFIN-only sync (does not touch Teller)."""
    _require_live_mode("SimpleFIN sync is disabled in demo mode.")
    job_id = start_sync("simplefin", phase="starting", detail="Starting SimpleFIN sync")
    try:
        data = fetch_simplefin_data(sync_job_id=job_id)
        finish_sync(job_id, status="completed")
        _invalidate_copilot_cache()
        return {
            "status": "synced",
            "accounts": len(data.get("accounts", [])),
            "transactions": len(data.get("transactions", [])),
            "last_updated": data.get("last_updated"),
        }
    except Exception as exc:
        finish_sync(job_id, status="failed", error=str(exc))
        raise
