"""
main.py
FastAPI backend for Folio personal finance tracker.
"""

from pathlib import Path as FilePath
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
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

from data_manager import (
    get_data, fetch_fresh_data, fetch_simplefin_data,
    update_transaction_category,
    add_category, get_categories, get_categories_meta, get_category_rules,
    get_accounts_filtered, get_transactions_paginated,
    get_summary_data, get_monthly_analytics_data,
    get_category_analytics_data, get_merchant_insights_data,
    get_net_worth_series_data, get_dashboard_bundle_data,
    update_category_parent, update_category_rule,
    get_copilot_conversations, get_data_browser_rows,
    get_category_budgets, update_category_budget,
    get_merchant_directory, update_merchant_directory_entry,
    update_transaction_excluded, get_transactions_for_merchant,
    get_category_rule_impact,
    explain_category_assignment, find_merchants_missing_category,
    bulk_recategorize_preview, preview_rule_creation,
    rename_merchant_variants, repair_polluted_merchant_categories,
)
from categorizer import get_active_categories
from database import init_db, get_db, get_db_session, close_thread_local_connection
from local_llm import (
    get_catalog_response,
    get_status_response,
    update_settings as update_local_llm_settings,
    get_frontend_flags,
    install_model as install_local_llm_model,
    schedule_prewarm_selected_model,
)

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
    schedule_prewarm_selected_model("copilot")


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
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["Content-Type", "X-API-Key"],
)

# Categories excluded from spending calculations
TRANSFER_CATEGORIES = {"Savings Transfer", "Personal Transfer", "Credit Card Payment"}
NON_SPENDING_CATEGORIES = TRANSFER_CATEGORIES | {"Income"}

# ── Profile helpers ──────────────────────────────────────────────
def _normalize_profile_whitespace(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _canonicalize_profile_id(value: str | None) -> str:
    return _normalize_profile_whitespace(value).lower()


def _titleize_profile_name(value: str | None) -> str:
    normalized = _normalize_profile_whitespace(value)
    return normalized.title() if normalized else ""


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


def _app_config_payload(db=None) -> dict:
    payload = {
        "demoMode": DEMO_MODE,
        "bankLinkingEnabled": not DEMO_MODE,
        "manualSyncEnabled": not DEMO_MODE,
        "demoPersistence": "ephemeral" if DEMO_MODE else "persistent",
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


class LocalLlmSettingsUpdate(BaseModel):
    llm_provider: str | None = None
    preset: str | None = None
    categorize_model: str | None = None
    copilot_model: str | None = None
    categorize_batch_size: int | None = None
    inter_batch_delay_ms: int | None = None
    low_power_mode: bool | None = None
    expert_mode: bool | None = None


class LocalLlmInstallRequest(BaseModel):
    model: str


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
    return amount > 0 and cat not in NON_SPENDING_CATEGORIES


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


@app.get("/api/local-llm/status")
def local_llm_status(db=Depends(get_db_session)):
    schedule_prewarm_selected_model("copilot", db)
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
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if body.copilot_model is not None or body.llm_provider is not None:
        schedule_prewarm_selected_model("copilot", db, force=True)

    return {
        "status": get_status_response(db),
        "config": _app_config_payload(db),
    }


@app.post("/api/local-llm/install")
def post_local_llm_install(body: LocalLlmInstallRequest, db=Depends(get_db_session)):
    try:
        result = install_local_llm_model(body.model, db)
    except ValueError as exc:
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
        profile=profile,
        limit=limit,
        offset=offset,
        conn=db,
    )


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
    return {"status": "updated", "transaction": result}


@app.get("/api/categories")
def categories():
    return get_active_categories()


class NewCategory(BaseModel):
    name: str


@app.post("/api/categories")
def create_category(body: NewCategory):
    """Add a new user-defined category."""
    if not body.name or not body.name.strip():
        raise HTTPException(status_code=400, detail="Category name cannot be empty.")
    success = add_category(body.name.strip())
    if not success:
        raise HTTPException(status_code=409, detail="Category already exists.")
    return {"status": "created", "category": body.name.strip()}


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
    Return stored recurring / subscription data from the merchants table.
    Detection runs incrementally after each sync (see data_manager.fetch_fresh_data).
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

    return {"status": "confirmed", "merchant": body.merchant, "pattern": pattern}


@app.post("/api/subscriptions/dismiss")
def dismiss_subscription(body: SubscriptionDismiss, profile: str | None = Query(None), db=Depends(get_db_session)):
    """
    User dismisses a false positive — marks the pattern as inactive for this user.
    Also records in dismissed_recurring table for Enhancement 2.
    If it's a system seed, we create a user-level suppression entry.
    """
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

    return {"status": "dismissed", "merchant": body.merchant, "pattern": pattern}


# ── Subscription management (Enhancements 1-4, 6) ────────────────────────

class SubscriptionDeclare(BaseModel):
    merchant: str
    amount: float
    frequency: str = "monthly"
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

    profile = body.profile or "household"
    result = declare_subscription(
        merchant=body.merchant.strip(),
        amount=body.amount,
        frequency=body.frequency,
        profile=profile,
    )
    return {"status": "ok", "message": "Subscription declared", "subscription": result}


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


class CopilotConfirm(BaseModel):
    """Client sends back the confirmation_id, NOT raw SQL."""
    question: str
    confirmation_id: str


@app.post("/api/copilot/ask")
async def copilot_ask(body: CopilotRequest, profile: str | None = Query(None)):
    """NLP copilot — translates questions to SQL and returns answers."""
    from copilot import ask_copilot
    result = ask_copilot(question=body.question, profile=profile)
    return result


@app.post("/api/copilot/confirm")
async def copilot_confirm(body: CopilotConfirm, profile: str | None = Query(None)):
    """
    Confirm and execute a write operation previewed by the copilot.
    [FIX M2] Client sends a confirmation_id that references server-stored SQL.
    The client can no longer supply arbitrary SQL.
    """
    from copilot import ask_copilot, retrieve_pending_sql

    pending = retrieve_pending_sql(body.confirmation_id)
    if pending is None:
        raise HTTPException(
            status_code=404,
            detail="Confirmation expired or not found. Please re-ask the question.",
        )

    result = ask_copilot(
        question=body.question,
        profile=profile,
        confirm_write=True,
        pending_sql=pending["sql"],
    )
    return result


@app.get("/api/copilot/history")
def copilot_history(
    profile: str | None = Depends(validate_profile),
    limit: int = Query(40, ge=1, le=200),
    db=Depends(get_db_session),
):
    return {"items": get_copilot_conversations(limit=limit, profile=profile, conn=db)}


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
    from copilot import store_pending_sql

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

    confirmation_id = store_pending_sql(data["update_sql"], profile)

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
    from copilot import store_pending_sql

    data = preview_rule_creation(
        raw_pattern=body.pattern,
        category=body.category,
        profile=profile,
        conn=db,
    )
    count = data["count"]
    existing = data["existing_rule"]
    pattern = data["pattern"]

    confirmation_id = store_pending_sql(data["insert_sql"], profile)

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
    from copilot import store_pending_sql

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

    confirmation_id = store_pending_sql(data["update_sql"], profile)
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


@app.patch("/api/budgets/{category_name}")
def update_budget_endpoint(
    category_name: str,
    body: BudgetUpdate,
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    try:
        result = update_category_budget(category=category_name, amount=body.amount, profile=profile, conn=db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "updated", "budget": result}


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
    profile: str | None = Depends(validate_profile),
    db=Depends(get_db_session),
):
    """
    Single-request dashboard loader.
    Returns summary, accounts, monthly analytics, category analytics,
    and net-worth time series — all using SQL-level aggregation.
    Replaces 5 separate API calls.
    """
    return get_dashboard_bundle_data(nw_interval=nw_interval, profile=profile, conn=db)


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
        return {
            "status": "synced",
            "accounts": len(data.get("accounts", [])),
            "transactions": len(data.get("transactions", [])),
            "last_updated": data.get("last_updated"),
        }
    except Exception as exc:
        finish_sync(job_id, status="failed", error=str(exc))
        raise
