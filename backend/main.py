"""
main.py
FastAPI backend for Folio personal finance tracker.
"""

from pathlib import Path as FilePath
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
from auth import verify_api_key, rate_limit_middleware
import bank
from bank import validate_teller_config, close_all_clients
import os

from log_config import get_logger, setup_logging

# Ensure logging is configured before anything else
setup_logging()

logger = get_logger(__name__)

from data_manager import (
    get_data, fetch_fresh_data, update_transaction_category,
    add_category, get_categories, get_category_rules,
    get_accounts_filtered, get_transactions_paginated,
    get_summary_data, get_monthly_analytics_data,
    get_category_analytics_data, get_merchant_insights_data,
    get_net_worth_series_data, get_dashboard_bundle_data,
)
from categorizer import get_active_categories
from database import init_db, get_db, get_db_session, close_thread_local_connection

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
    validate_teller_config()
    init_db()
    # These were previously auto-called at database.py import time.
    # Moved here for explicit, single-point initialization.
    from database import sync_subscription_seeds, sync_enrichment_cache_from_seeds
    sync_subscription_seeds()
    sync_enrichment_cache_from_seeds()


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
    "TRUSTED_HOSTS", "localhost,127.0.0.1,backend"
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
def _get_profile_list() -> list[dict]:
    names = sorted(bank.PROFILES.keys())  # ← always reads current
    profiles = [{"id": n, "name": n.title()} for n in names]
    if len(names) > 1:
        profiles.append({"id": "household", "name": "Household"})
    return profiles


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
    if not profile or profile == "household":
        return profile
    if _VALID_PROFILES is None:
        _VALID_PROFILES = set(bank.PROFILES.keys()) | {"household"}
    if profile not in _VALID_PROFILES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown profile: '{profile}'. Valid profiles: {sorted(_VALID_PROFILES)}",
        )
    return profile


def _invalidate_profile_cache():
    """Reset the valid profiles cache after a new enrollment."""
    global _VALID_PROFILES
    _VALID_PROFILES = None


# ── Models ──


class CategoryUpdate(BaseModel):
    category: str


class CopilotRequest(BaseModel):
    question: str


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
def profiles():
    """Return available profile names for the frontend toggle."""
    return _get_profile_list()


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
    result = update_transaction_category(tx_id, body.category)
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")

    response = {"status": "updated", "tx_id": tx_id, "category": body.category}

    # Enhancement 7: Pass through subscription prompt signal
    if isinstance(result, dict):
        if result.get("subscription_prompt"):
            response["subscription_prompt"] = True
            response["merchant"] = result.get("merchant", "")
            response["amount"] = result.get("amount", 0.0)
            response["transaction_id"] = result.get("transaction_id", tx_id)

    return response


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


@app.get("/api/category-rules")
def list_category_rules(source: str | None = Query(None)):
    """List category rules, optionally filtered by source ('user' or 'system')."""
    return get_category_rules(source)


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
    # Currently syncs all profiles. Profile param reserved for future selective sync.
    data = fetch_fresh_data()
    return {
        "status": "synced",
        "accounts": len(data["accounts"]),
        "transactions": len(data["transactions"]),
        "last_updated": data["last_updated"],
    }


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
    app_id = os.getenv("TELLER_APPLICATION_ID", "")
    env = os.getenv("TELLER_ENVIRONMENT", "sandbox")
    if not app_id:
        raise HTTPException(
            status_code=503,
            detail="TELLER_APPLICATION_ID not configured on the server.",
        )
    return {"applicationId": app_id, "environment": env}


@app.post("/api/enroll")
def enroll_account(req: EnrollRequest):
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
    was_new = save_token(
        profile=profile_name,
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
    try:
        data = fetch_fresh_data()
        sync_result = {
            "accounts": len(data.get("accounts", [])),
            "transactions": len(data.get("transactions", [])),
        }
    except Exception as e:
        logger.warning("Post-enrollment sync failed (non-fatal): %s", e)

    institution = req.institutionName or accounts[0].get("institution", {}).get("name", "Unknown")

    return {
        "status": "enrolled" if was_new else "already_exists",
        "profile": profile_name,
        "institution": institution,
        "owner": identity["full_name"],
        "accounts_found": len(accounts),
        "synced": sync_result,
    }


@app.get("/api/enrollments")
def list_enrollments():
    """Return all active Teller Connect enrollments (metadata only, no tokens)."""
    from token_store import load_all_enrollments
    return load_all_enrollments()


class DeactivateEnrollment(BaseModel):
    id: int


@app.post("/api/enrollments/deactivate")
def deactivate_enrollment(body: DeactivateEnrollment):
    """Soft-delete an enrollment. The token will no longer be used on next reload."""
    from token_store import deactivate_token
    from bank import reload_tokens_and_profiles

    success = deactivate_token(body.id)
    if not success:
        raise HTTPException(status_code=404, detail="Enrollment not found or already inactive.")

    reload_tokens_and_profiles()
    _invalidate_profile_cache()

    return {"status": "deactivated", "id": body.id}