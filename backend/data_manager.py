"""
data_manager.py
Handles data fetching, syncing from Teller, and persistence via SQLite.
Replaces the old JSON cache with database operations.
"""

import re
import os
import threading
from datetime import datetime
from dotenv import load_dotenv
import bank
from bank import get_all_accounts_by_profile, get_transactions, get_balances
from categorizer import categorize_transactions
from database import get_db, dicts_from_rows, _extract_merchant_pattern
from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

_lock = threading.Lock()

def _escape_like(pattern: str) -> str:
    """Escape SQL LIKE wildcards in a pattern to prevent unintended matches."""
    return pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

EMPTY_DATA = {
    "last_updated": None,
    "accounts": [],
    "transactions": [],
}

# Categories excluded from spending calculations (duplicated from main.py for
# SQL-level filtering — keep in sync with main.py constants).
TRANSFER_CATEGORIES = ("Savings Transfer", "Personal Transfer", "Credit Card Payment")
NON_SPENDING_CATEGORIES = TRANSFER_CATEGORIES + ("Income",)


# ══════════════════════════════════════════════════════════════════════════════
# READ OPERATIONS — TARGETED QUERIES (preferred)
# ══════════════════════════════════════════════════════════════════════════════

def get_accounts_filtered(profile: str | None = None, conn=None) -> list[dict]:
    """Fetch accounts with optional profile filter, pushed into SQL."""
    def _query(c):
        sql = """SELECT id, account_name as name, account_subtype as type,
                        CASE WHEN account_type IN ('credit', 'loan') THEN 1 ELSE 0 END as is_credit,
                        account_type,
                        current_balance as balance, currency, profile_id as profile
                 FROM accounts WHERE is_active = 1"""
        params = []
        if profile and profile != "household":
            sql += " AND profile_id = ?"
            params.append(profile)
        rows = c.execute(sql, params).fetchall()
        accounts = dicts_from_rows(rows)
        for acct in accounts:
            acct["is_credit"] = bool(acct.get("is_credit", 0))
        return accounts

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_transactions_paginated(
    month: str | None = None,
    category: str | None = None,
    account: str | None = None,
    search: str | None = None,
    profile: str | None = None,
    limit: int = 100,
    offset: int = 0,
    conn=None,
) -> dict:
    """
    Fetch transactions with all filters pushed into SQL WHERE clauses.
    Returns {"data": [...], "total_count": int, "limit": int, "offset": int}.
    """
    def _query(c):
        where_clauses = []
        params = []

        if profile and profile != "household":
            where_clauses.append("profile_id = ?")
            params.append(profile)
        if month:
            where_clauses.append("date LIKE ?")
            params.append(month + "%")
        if category:
            where_clauses.append("category = ?")
            params.append(category)
        if account:
            where_clauses.append("account_name = ?")
            params.append(account)
        if search:
            escaped = _escape_like(search.upper())
            where_clauses.append("UPPER(description) LIKE ? ESCAPE '\\'")
            params.append(f"%{escaped}%")

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Count query
        count_sql = f"SELECT COUNT(*) FROM transactions{where_sql}"
        total_count = c.execute(count_sql, params).fetchone()[0]

        # Data query with pagination
        data_sql = f"""SELECT id as original_id, profile_id as profile, date, description,
                              raw_description, amount, category, categorization_source,
                              confidence, transaction_type as type,
                              counterparty_name, counterparty_type, teller_category,
                              account_name, account_type, merchant_name, merchant_domain,
                              merchant_industry, merchant_city, merchant_state,
                              enriched, is_excluded
                       FROM transactions{where_sql}
                       ORDER BY date DESC
                       LIMIT ? OFFSET ?"""
        data_params = params + [limit, offset]
        rows = c.execute(data_sql, data_params).fetchall()
        transactions = dicts_from_rows(rows)
        for tx in transactions:
            tx["enriched"] = bool(tx.get("enriched", 0))
            tx["is_excluded"] = bool(tx.get("is_excluded", 0))

        return {
            "data": transactions,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        }

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_summary_data(profile: str | None = None, conn=None) -> dict:
    """Compute summary statistics using SQL-level aggregation."""
    def _query(c):
        profile_clause = ""
        profile_params = []
        if profile and profile != "household":
            profile_clause = " AND profile_id = ?"
            profile_params = [profile]

        # Build the non-spending categories placeholders
        non_spending_ph = ",".join("?" * len(NON_SPENDING_CATEGORIES))
        transfer_ph = ",".join("?" * len(TRANSFER_CATEGORIES))

        # Income: category='Income' AND amount > 0
        income = c.execute(
            f"SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE category = 'Income' AND amount > 0{profile_clause}",
            profile_params,
        ).fetchone()[0]

        # Expenses: amount < 0 AND category NOT IN non_spending
        expenses = c.execute(
            f"SELECT COALESCE(SUM(ABS(amount)), 0) FROM transactions WHERE amount < 0 AND category NOT IN ({non_spending_ph}){profile_clause}",
            list(NON_SPENDING_CATEGORIES) + profile_params,
        ).fetchone()[0]

        # Refunds: amount > 0 AND category NOT IN non_spending AND NOT income
        refunds = c.execute(
            f"SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE amount > 0 AND category NOT IN ({non_spending_ph}) AND category != 'Income'{profile_clause}",
            list(NON_SPENDING_CATEGORIES) + profile_params,
        ).fetchone()[0]

        # Savings: category = 'Savings Transfer'
        savings = c.execute(
            f"SELECT COALESCE(SUM(ABS(amount)), 0) FROM transactions WHERE category = 'Savings Transfer'{profile_clause}",
            profile_params,
        ).fetchone()[0]

        # Transaction counts
        tx_count = c.execute(
            f"SELECT COUNT(*) FROM transactions WHERE 1=1{profile_clause}",
            profile_params,
        ).fetchone()[0]

        enriched_count = c.execute(
            f"SELECT COUNT(*) FROM transactions WHERE enriched = 1{profile_clause}",
            profile_params,
        ).fetchone()[0]

        # Account balances
        acct_profile_clause = ""
        acct_params = []
        if profile and profile != "household":
            acct_profile_clause = " AND profile_id = ?"
            acct_params = [profile]

        total_assets = c.execute(
            f"SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('depository', 'investment') AND is_active = 1{acct_profile_clause}",
            acct_params,
        ).fetchone()[0]

        total_owed = c.execute(
            f"SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('credit', 'loan') AND is_active = 1{acct_profile_clause}",
            acct_params,
        ).fetchone()[0]

        # Last updated
        last_row = c.execute("SELECT MAX(last_synced_at) FROM accounts").fetchone()
        last_updated = last_row[0] if last_row and last_row[0] else None

        net_spending = expenses - refunds
        return {
            "income": round(income, 2),
            "expenses": round(expenses, 2),
            "refunds": round(refunds, 2),
            "net_spending": round(net_spending, 2),
            "savings": round(savings, 2),
            "net_flow": round(income - net_spending, 2),
            "savings_rate": round(savings / income * 100, 1) if income > 0 else 0,
            "total_assets": round(total_assets, 2),
            "total_owed": round(total_owed, 2),
            "net_worth": round(total_assets - total_owed, 2),
            "last_updated": last_updated,
            "transaction_count": tx_count,
            "enriched_count": enriched_count,
        }

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_monthly_analytics_data(profile: str | None = None, conn=None) -> list[dict]:
    """
    Compute monthly income/expense/refund/savings aggregation using SQL GROUP BY.
    """
    def _query(c):
        profile_clause = ""
        profile_params = []
        if profile and profile != "household":
            profile_clause = " AND profile_id = ?"
            profile_params = [profile]

        non_spending_ph = ",".join("?" * len(NON_SPENDING_CATEGORIES))

        # One query using conditional aggregation
        sql = f"""
            SELECT
                SUBSTR(date, 1, 7) as month,
                COALESCE(SUM(CASE WHEN category = 'Income' AND amount > 0 THEN amount ELSE 0 END), 0) as income,
                COALESCE(SUM(CASE WHEN amount < 0 AND category NOT IN ({non_spending_ph}) THEN ABS(amount) ELSE 0 END), 0) as expenses,
                COALESCE(SUM(CASE WHEN amount > 0 AND category NOT IN ({non_spending_ph}) AND category != 'Income' THEN amount ELSE 0 END), 0) as refunds,
                COALESCE(SUM(CASE WHEN category = 'Savings Transfer' THEN ABS(amount) ELSE 0 END), 0) as savings
            FROM transactions
            WHERE LENGTH(date) >= 7{profile_clause}
            GROUP BY SUBSTR(date, 1, 7)
            ORDER BY month ASC
        """
        # Parameters: non_spending used twice (expenses and refunds), plus profile
        params = list(NON_SPENDING_CATEGORIES) + list(NON_SPENDING_CATEGORIES) + profile_params
        rows = c.execute(sql, params).fetchall()

        result = []
        for row in rows:
            inc = row[1]
            exp = row[2]
            ref = row[3]
            sav = row[4]
            result.append({
                "month": row[0],
                "income": round(inc, 2),
                "expenses": round(exp, 2),
                "refunds": round(ref, 2),
                "savings": round(sav, 2),
                "net": round(inc - exp + ref, 2),
            })
        return result

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_category_analytics_data(month: str | None = None, profile: str | None = None, conn=None) -> list[dict]:
    """
    Compute per-category spending breakdown using SQL GROUP BY.
    """
    def _query(c):
        where_clauses = []
        params = []

        if profile and profile != "household":
            where_clauses.append("profile_id = ?")
            params.append(profile)
        if month:
            where_clauses.append("date LIKE ?")
            params.append(month + "%")

        where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

        non_spending_ph = ",".join("?" * len(NON_SPENDING_CATEGORIES))

        # Gross expenses by category
        expense_sql = f"""
            SELECT category, SUM(ABS(amount)) as total
            FROM transactions
            WHERE amount < 0 AND category NOT IN ({non_spending_ph}){where_sql}
            GROUP BY category
        """
        expense_params = list(NON_SPENDING_CATEGORIES) + params
        expense_rows = c.execute(expense_sql, expense_params).fetchall()
        expense_by_cat = {row[0]: row[1] for row in expense_rows}

        # Refunds by category
        refund_sql = f"""
            SELECT category, SUM(amount) as total
            FROM transactions
            WHERE amount > 0 AND category NOT IN ({non_spending_ph}) AND category != 'Income'{where_sql}
            GROUP BY category
        """
        refund_params = list(NON_SPENDING_CATEGORIES) + params
        refund_rows = c.execute(refund_sql, refund_params).fetchall()
        refund_by_cat = {row[0]: row[1] for row in refund_rows}

        # Compute net per category
        all_cats = set(list(expense_by_cat.keys()) + list(refund_by_cat.keys()))
        net_cat = {}
        for cat in all_cats:
            gross = expense_by_cat.get(cat, 0)
            refs = refund_by_cat.get(cat, 0)
            net_val = gross - refs
            if net_val > 0:
                net_cat[cat] = net_val

        # Load expense_type mapping
        et_rows = c.execute("SELECT name, expense_type FROM categories WHERE is_active = 1").fetchall()
        expense_type_map = {row[0]: row[1] for row in et_rows}

        total = sum(net_cat.values())
        result = []
        for cat, amt in sorted(net_cat.items(), key=lambda x: -x[1]):
            result.append({
                "category": cat,
                "total": round(amt, 2),
                "gross": round(expense_by_cat.get(cat, 0), 2),
                "refunds": round(refund_by_cat.get(cat, 0), 2),
                "percent": round(amt / total * 100, 1) if total > 0 else 0,
                "expense_type": expense_type_map.get(cat, "variable"),
            })
        return result

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_merchant_insights_data(month: str | None = None, profile: str | None = None, conn=None) -> list[dict]:
    """Merchant-level spending breakdown using SQL aggregation."""
    def _query(c):
        where_clauses = ["enriched = 1", "merchant_name != ''"]
        params = []

        non_spending_ph = ",".join("?" * len(NON_SPENDING_CATEGORIES))
        where_clauses.append(f"amount < 0 AND category NOT IN ({non_spending_ph})")
        params.extend(NON_SPENDING_CATEGORIES)

        if profile and profile != "household":
            where_clauses.append("profile_id = ?")
            params.append(profile)
        if month:
            where_clauses.append("date LIKE ?")
            params.append(month + "%")

        where_sql = " WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT merchant_name, merchant_domain, merchant_industry,
                   merchant_city, merchant_state,
                   SUM(ABS(amount)) as total_spent,
                   COUNT(*) as transaction_count
            FROM transactions
            {where_sql}
            GROUP BY merchant_name
            ORDER BY total_spent DESC
        """
        rows = c.execute(sql, params).fetchall()
        result = []
        for row in rows:
            result.append({
                "name": row[0],
                "domain": row[1] or "",
                "industry": row[2] or "",
                "city": row[3] or "",
                "state": row[4] or "",
                "total_spent": round(row[5], 2),
                "transaction_count": row[6],
            })
        return result

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_net_worth_series_data(interval: str = "weekly", profile: str | None = None, conn=None) -> list[dict]:
    """
    Compute a running net-worth time series from transaction history.
    Uses SQL to fetch daily net changes, then builds the series in Python
    (cumulative day-by-day logic doesn't benefit from SQL window functions in SQLite).
    """
    from datetime import date as dt_date, timedelta
    from collections import defaultdict

    def _query(c):
        # Current total balances
        acct_profile_clause = ""
        acct_params = []
        if profile and profile != "household":
            acct_profile_clause = " AND profile_id = ?"
            acct_params = [profile]

        total_assets = c.execute(
            f"SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('depository', 'investment') AND is_active = 1{acct_profile_clause}",
            acct_params,
        ).fetchone()[0]
        total_owed = c.execute(
            f"SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('credit', 'loan') AND is_active = 1{acct_profile_clause}",
            acct_params,
        ).fetchone()[0]
        current_net_worth = total_assets - total_owed

        # Get daily net changes via SQL aggregation
        tx_profile_clause = ""
        tx_params = []
        if profile and profile != "household":
            tx_profile_clause = " AND profile_id = ?"
            tx_params = [profile]

        daily_rows = c.execute(
            f"""SELECT SUBSTR(date, 1, 10) as day, SUM(amount) as net
                FROM transactions
                WHERE LENGTH(date) >= 10{tx_profile_clause}
                GROUP BY SUBSTR(date, 1, 10)
                ORDER BY day ASC""",
            tx_params,
        ).fetchall()

        if not daily_rows:
            return []

        daily_net = {row[0]: row[1] for row in daily_rows}
        first_date = dt_date.fromisoformat(daily_rows[0][0])
        last_date = dt_date.fromisoformat(daily_rows[-1][0])

        # Build cumulative
        all_days = []
        d = first_date
        while d <= last_date:
            all_days.append(d)
            d += timedelta(days=1)

        cumulative = {}
        running = 0.0
        for day in all_days:
            running += daily_net.get(day.isoformat(), 0.0)
            cumulative[day] = running

        total_cumulative = cumulative.get(last_date, 0.0)
        starting_nw = current_net_worth - total_cumulative

        step_days = 7 if interval == "weekly" else 14
        series = []
        d = first_date
        while d <= last_date:
            nw = starting_nw + cumulative.get(d, 0.0)
            series.append({"date": d.isoformat(), "value": round(nw, 2)})
            d += timedelta(days=step_days)

        if series and series[-1]["date"] != last_date.isoformat():
            nw = starting_nw + cumulative.get(last_date, 0.0)
            series.append({"date": last_date.isoformat(), "value": round(nw, 2)})

        return series

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_dashboard_bundle_data(
    nw_interval: str = "biweekly",
    profile: str | None = None,
    conn=None,
) -> dict:
    """
    Single-request dashboard data using SQL-level aggregation for summary,
    monthly, and category analytics. Net-worth series uses the dedicated helper.
    """
    def _query(c):
        summary_out = get_summary_data(profile=profile, conn=c)
        accounts_out = get_accounts_filtered(profile=profile, conn=c)
        monthly_sorted = get_monthly_analytics_data(profile=profile, conn=c)
        categories_out = get_category_analytics_data(profile=profile, conn=c)
        nw_series = get_net_worth_series_data(interval=nw_interval, profile=profile, conn=c)

        return {
            "summary": summary_out,
            "accounts": accounts_out,
            "monthly": monthly_sorted,
            "categories": categories_out,
            "netWorthSeries": nw_series,
        }

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)

# ══════════════════════════════════════════════════════════════════════════════
# RECURRING / SUBSCRIPTION QUERY OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def get_recurring_from_db(profile: str | None = None, conn=None) -> dict:
    """
    Read stored recurring subscription data from the merchants table.
    Returns the same shape as RecurringDetector.detect() so the frontend
    migration is minimal. Includes events and dismissed items in the bundle.
    """
    import json as _json

    def _query(c):
        profile_clause = ""
        profile_params = []
        if profile and profile != "household":
            profile_clause = " AND profile_id = ?"
            profile_params = [profile]

        # Active + inactive subscriptions
        rows = c.execute(
            f"""SELECT merchant_key, clean_name, logo_url, domain, category,
                       industry, subscription_frequency, subscription_amount,
                       subscription_status, last_charge_date, next_expected_date,
                       charge_count, total_spent, cancelled_by_user, cancelled_at,
                       source
                FROM merchants
                WHERE is_subscription = 1{profile_clause}
                ORDER BY
                    CASE subscription_status
                        WHEN 'active' THEN 0
                        WHEN 'inactive' THEN 1
                        ELSE 2
                    END,
                    subscription_amount DESC""",
            profile_params,
        ).fetchall()

        # Dismissed items
        dismissed_rows = c.execute(
            f"""SELECT merchant_name, dismissed_at
                FROM dismissed_recurring
                WHERE 1=1{profile_clause.replace('profile_id', 'profile_id')}""",
            profile_params,
        ).fetchall()

        # Events (recent, last 50)
        event_rows = c.execute(
            f"""SELECT id, event_type, merchant_name, detail, created_at, is_read
                FROM subscription_events
                WHERE 1=1{profile_clause.replace('profile_id', 'profile_id')}
                ORDER BY created_at DESC
                LIMIT 50""",
            profile_params,
        ).fetchall()

        # User-declared subscriptions (to merge in)
        user_decl_rows = c.execute(
            f"""SELECT merchant_name, amount, frequency
                FROM user_declared_subscriptions
                WHERE is_active = 1{profile_clause.replace('profile_id', 'profile_id')}""",
            profile_params,
        ).fetchall()

        dismissed_set = {row[0] for row in dismissed_rows}

        items = []
        active_count = 0
        inactive_count = 0
        cancelled_count = 0
        total_monthly = 0.0
        total_annual = 0.0

        seen_merchants = set()

        # Add user-declared first (Layer 0 — highest priority)
        for row in user_decl_rows:
            merchant_name = row[0]
            if merchant_name in dismissed_set:
                continue
            merchant_key_upper = merchant_name.upper().strip()
            seen_merchants.add(merchant_key_upper)

            amt = row[1]
            freq = row[2]
            annual = _annualize_amount(amt, freq)

            items.append({
                "merchant": merchant_name,
                "clean_name": merchant_name,
                "logo_url": None,
                "category": "Subscriptions",
                "frequency": freq,
                "amount": round(amt, 2),
                "annual_cost": round(annual, 2),
                "status": "active",
                "confidence": "user",
                "last_charge": None,
                "next_expected": None,
                "charge_count": 0,
                "total_spent": 0,
                "price_change": None,
                "matched_by": "user",
                "cancelled": False,
            })
            active_count += 1
            total_annual += annual
            total_monthly += annual / 12

        # Add detection-based subscriptions
        for row in rows:
            merchant_key = row[0]
            if merchant_key in dismissed_set:
                continue
            if merchant_key in seen_merchants:
                # User declaration wins — update with transaction data
                for item in items:
                    if item["merchant"].upper().strip() == merchant_key:
                        item["last_charge"] = row[9]
                        item["next_expected"] = row[10]
                        item["charge_count"] = row[11] or 0
                        item["total_spent"] = round(row[12] or 0, 2)
                        item["clean_name"] = row[1] or item["clean_name"]
                        item["logo_url"] = row[2]
                        break
                continue

            seen_merchants.add(merchant_key)
            status = row[8] or "inactive"
            cancelled = bool(row[13])
            amt = row[7] or 0
            freq = row[6] or "monthly"
            annual = _annualize_amount(amt, freq)

            if cancelled:
                display_status = "cancelled"
            else:
                display_status = status

            items.append({
                "merchant": row[1] or merchant_key,
                "clean_name": row[1] or merchant_key,
                "logo_url": row[2],
                "category": row[4] or "Subscriptions",
                "frequency": freq,
                "amount": round(amt, 2),
                "annual_cost": round(annual, 2),
                "status": display_status,
                "confidence": "high" if row[15] == "seed" else "medium",
                "last_charge": row[9],
                "next_expected": row[10],
                "charge_count": row[11] or 0,
                "total_spent": round(row[12] or 0, 2),
                "price_change": None,
                "matched_by": row[15] or "algorithm",
                "cancelled": cancelled,
            })

            if status == "active" and not cancelled:
                active_count += 1
                total_annual += annual
                total_monthly += annual / 12
            elif cancelled:
                cancelled_count += 1
            else:
                inactive_count += 1

        # Build events list
        events = []
        unread_count = 0
        for erow in event_rows:
            try:
                detail = _json.loads(erow[3]) if erow[3] else {}
            except Exception:
                detail = {}
            events.append({
                "id": erow[0],
                "event_type": erow[1],
                "merchant_name": erow[2],
                "detail": detail,
                "created_at": erow[4],
                "is_read": bool(erow[5]),
            })
            if not erow[5]:
                unread_count += 1

        # Build dismissed list
        dismissed = [
            {"merchant": row[0], "dismissed_at": row[1]}
            for row in dismissed_rows
        ]

        return {
            "items": items,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "cancelled_count": cancelled_count,
            "dismissed_count": len(dismissed),
            "total_monthly": round(total_monthly, 2),
            "total_annual": round(total_annual, 2),
            "events": events,
            "unread_event_count": unread_count,
            "dismissed": dismissed,
        }

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def _annualize_amount(amount: float, frequency: str) -> float:
    """Convert per-period amount to annual cost."""
    multipliers = {"monthly": 12, "quarterly": 4, "semi_annual": 2, "annual": 1}
    return amount * multipliers.get(frequency, 12)


def get_dismissed_subscriptions(profile: str | None = None, conn=None) -> list[dict]:
    """Return dismissed subscription items for a profile."""
    def _query(c):
        if profile and profile != "household":
            rows = c.execute(
                "SELECT merchant_name, dismissed_at FROM dismissed_recurring WHERE profile_id = ?",
                (profile,),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT merchant_name, dismissed_at FROM dismissed_recurring"
            ).fetchall()
        return [{"merchant": row[0], "dismissed_at": row[1]} for row in rows]

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def get_subscription_events(profile: str | None = None, conn=None) -> dict:
    """Return subscription events and unread count."""
    import json as _json

    def _query(c):
        params = []
        clause = ""
        if profile and profile != "household":
            clause = " WHERE profile_id = ?"
            params = [profile]

        rows = c.execute(
            f"""SELECT id, event_type, merchant_name, detail, created_at, is_read
                FROM subscription_events{clause}
                ORDER BY created_at DESC
                LIMIT 100""",
            params,
        ).fetchall()

        events = []
        unread_count = 0
        for row in rows:
            try:
                detail = _json.loads(row[3]) if row[3] else {}
            except Exception:
                detail = {}
            events.append({
                "id": row[0],
                "event_type": row[1],
                "merchant_name": row[2],
                "detail": detail,
                "created_at": row[4],
                "is_read": bool(row[5]),
            })
            if not row[5]:
                unread_count += 1

        return {"events": events, "unread_count": unread_count}

    if conn is not None:
        return _query(conn)
    with get_db() as c:
        return _query(c)


def declare_subscription(merchant: str, amount: float, frequency: str, profile: str | None = None) -> dict:
    """
    Store a user-declared subscription.
    Writes to user_declared_subscriptions and merchants tables.
    """
    profile_id = profile or "household"

    with get_db() as conn:
        conn.execute(
            """INSERT INTO user_declared_subscriptions
               (merchant_name, amount, frequency, profile_id)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(merchant_name, profile_id) DO UPDATE SET
                   amount = excluded.amount,
                   frequency = excluded.frequency,
                   is_active = 1,
                   updated_at = datetime('now')""",
            (merchant, amount, frequency, profile_id),
        )

        # Also upsert merchants table
        merchant_key = merchant.upper().strip()
        from recurring import _annualize
        annual = _annualize(amount, frequency)
        conn.execute(
            """INSERT INTO merchants
               (merchant_key, clean_name, category, source, is_subscription,
                subscription_frequency, subscription_amount, subscription_status,
                profile_id)
               VALUES (?, ?, 'Subscriptions', 'user', 1, ?, ?, 'active', ?)
               ON CONFLICT(merchant_key, profile_id) DO UPDATE SET
                   clean_name = excluded.clean_name,
                   is_subscription = 1,
                   subscription_frequency = excluded.subscription_frequency,
                   subscription_amount = excluded.subscription_amount,
                   subscription_status = 'active',
                   source = 'user',
                   cancelled_by_user = 0,
                   cancelled_at = NULL,
                   updated_at = datetime('now')""",
            (merchant_key, merchant, frequency, amount, profile_id),
        )

        # Fetch the record to return
        row = conn.execute(
            """SELECT merchant_name, amount, frequency, profile_id, created_at
               FROM user_declared_subscriptions
               WHERE merchant_name = ? AND profile_id = ?""",
            (merchant, profile_id),
        ).fetchone()

    return {
        "merchant": row[0],
        "amount": row[1],
        "frequency": row[2],
        "profile_id": row[3],
        "created_at": row[4],
        "annual_cost": round(_annualize(row[1], row[2]), 2),
    }


def cancel_subscription(merchant: str, profile: str | None = None) -> dict:
    """Mark a subscription as cancelled by the user."""
    from datetime import datetime as _dt
    profile_id = profile or "household"
    now = _dt.now().isoformat()

    with get_db() as conn:
        merchant_key = merchant.upper().strip()
        conn.execute(
            """UPDATE merchants
               SET cancelled_at = ?, cancelled_by_user = 1,
                   subscription_status = 'cancelled',
                   updated_at = datetime('now')
               WHERE merchant_key = ? AND profile_id = ?""",
            (now, merchant_key, profile_id),
        )

    return {"status": "ok", "cancelled_at": now}


def restore_subscription(merchant: str, profile: str | None = None) -> bool:
    """Remove a merchant from the dismissed_recurring table."""
    profile_id = profile or "household"

    with get_db() as conn:
        result = conn.execute(
            """DELETE FROM dismissed_recurring
               WHERE merchant_name = ? AND profile_id = ?""",
            (merchant, profile_id),
        )
        return result.rowcount > 0


def mark_events_read(event_ids: list[int]) -> int:
    """Mark subscription events as read. Returns count of updated rows."""
    if not event_ids:
        return 0

    with get_db() as conn:
        placeholders = ",".join("?" * len(event_ids))
        result = conn.execute(
            f"UPDATE subscription_events SET is_read = 1 WHERE id IN ({placeholders})",
            event_ids,
        )
        return result.rowcount


def trigger_full_redetection(profile: str | None = None) -> dict:
    """
    Run full recurring detection across all transactions and persist results.
    Used for manual refresh via POST /api/subscriptions/redetect.
    """
    from recurring import RecurringDetector, write_detection_results_to_db

    data = get_data()
    txns = data["transactions"]

    if profile and profile != "household":
        txns = [t for t in txns if t.get("profile") == profile]

    detector = RecurringDetector(get_db_conn=get_db)
    result = detector.detect(
        transactions=txns,
        profile=profile,
        generate_events=True,
    )

    write_detection_results_to_db(
        get_db_conn=get_db,
        items=result["items"],
        events=result.get("events", []),
        profile=profile,
    )

    return result


# ══════════════════════════════════════════════════════════════════════════════
# READ OPERATIONS — FULL DATASET (legacy, use sparingly)
# ══════════════════════════════════════════════════════════════════════════════

def get_data(force_refresh: bool = False) -> dict:
    """
    Returns ALL data from SQLite — loads every transaction and account into memory.

    ⚠️  DEPRECATED FOR GENERAL USE — prefer targeted query functions above.

    This function exists ONLY for endpoints that genuinely need the full
    transaction list in memory, specifically:
      - /api/analytics/recurring — the recurring-detection algorithm needs
        to group, sort, and iterate over all expense transactions with
        complex cross-transaction analysis (interval detection, amount
        consistency, merchant grouping, seed matching) that cannot be
        efficiently expressed as SQL queries.

    All other endpoints should use the targeted query functions
    (get_transactions_paginated, get_summary_data, get_monthly_analytics_data,
    get_category_analytics_data, get_merchant_insights_data, etc.) which push
    filters and aggregation into SQL.
    """
    if force_refresh:
        return fetch_fresh_data(incremental=True)

    with get_db() as conn:
        # Check if we have any data
        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        if count == 0:
            return EMPTY_DATA

        # Accounts
        acct_rows = conn.execute(
            """SELECT id, account_name as name, account_subtype as type,
                      CASE WHEN account_type IN ('credit', 'loan') THEN 1 ELSE 0 END as is_credit,
                      account_type,
                      current_balance as balance, currency, profile_id as profile
               FROM accounts WHERE is_active = 1"""
        ).fetchall()
        accounts = dicts_from_rows(acct_rows)

        # Transactions
        tx_rows = conn.execute(
            """SELECT id as original_id, profile_id as profile, date, description,
                      raw_description, amount, category, categorization_source,
                      confidence, transaction_type as type,
                      counterparty_name, counterparty_type, teller_category,
                      account_name, account_type, merchant_name, merchant_domain,
                      merchant_industry, merchant_city, merchant_state,
                      enriched, is_excluded
               FROM transactions
               ORDER BY date DESC"""
        ).fetchall()
        transactions = dicts_from_rows(tx_rows)

        # Convert sqlite integer booleans to Python bools for enriched
        for tx in transactions:
            tx["enriched"] = bool(tx.get("enriched", 0))
            tx["is_excluded"] = bool(tx.get("is_excluded", 0))

        # Convert account is_credit to bool
        for acct in accounts:
            acct["is_credit"] = bool(acct.get("is_credit", 0))

        # Last updated: most recent sync timestamp from accounts
        last_row = conn.execute(
            "SELECT MAX(last_synced_at) FROM accounts"
        ).fetchone()
        last_updated = last_row[0] if last_row and last_row[0] else None

        return {
            "last_updated": last_updated,
            "accounts": accounts,
            "transactions": transactions,
        }


def get_cached_tx_ids() -> set:
    """Get all existing transaction IDs from the database."""
    with get_db() as conn:
        rows = conn.execute("SELECT id FROM transactions").fetchall()
        return {row[0] for row in rows}


# ══════════════════════════════════════════════════════════════════════════════
# SYNC (Teller → SQLite)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fresh_data(incremental: bool = True) -> dict:
    """
    Fetch from Teller API and write to SQLite.
    Called ONLY by /api/sync.
    """
    with _lock:
        cached_ids = get_cached_tx_ids() if incremental else set()

        if cached_ids:
            logger.info("Database has %d existing transactions.", len(cached_ids))
        else:
            logger.info("No cached transactions — full fetch.")

        logger.info("Fetching accounts from Teller...")
        accounts = get_all_accounts_by_profile()

        now = datetime.now().isoformat()

        with get_db() as conn:
            # Ensure profiles exist
            for profile_name in bank.PROFILES.keys():
                conn.execute(
                    "INSERT OR IGNORE INTO profiles (id, display_name) VALUES (?, ?)",
                    (profile_name, profile_name.title()),
                )

            # Ensure the virtual 'household' profile exists (aggregate of all profiles)
            conn.execute(
                "INSERT OR IGNORE INTO profiles (id, display_name) VALUES ('household', 'Household')",
            )

            all_new_transactions = []

            for account in accounts:
                token = account["access_token"]
                name = account["name"]
                subtype = account.get("subtype", "")
                profile = account.get("profile", "primary")
                institution = account.get("institution", {}).get("name", "")
                logger.info("[%s] Fetching transactions for %s...", profile, name)

                # Ensure this account's profile exists regardless
                conn.execute(
                    "INSERT OR IGNORE INTO profiles (id, display_name) VALUES (?, ?)",
                    (profile, profile.title()),
                )
                
                all_txns = get_transactions(account["id"], token)
                balances = get_balances(account["id"], token)

                new_txns = [t for t in all_txns if t.get("id") not in cached_ids]

                for t in new_txns:
                    t["account_name"] = name
                    t["account_type"] = subtype
                    t["profile"] = profile

                # Upsert account
                # Teller account types: depository, credit, loan, investment
                # Map to our internal types for net worth classification
                teller_type = account.get("type", "depository")
                is_credit = subtype in ("credit_card", "credit") or teller_type == "credit"
                is_loan = teller_type == "loan"
                is_investment = teller_type == "investment"

                # Balance selection: credit/loan use ledger, others use available
                if is_credit or is_loan:
                    balance = balances.get("ledger") or balances.get("available")
                else:
                    balance = balances.get("available") or balances.get("ledger")

                # Determine internal account_type
                if is_credit:
                    internal_type = "credit"
                elif is_loan:
                    internal_type = "loan"
                elif is_investment:
                    internal_type = "investment"
                else:
                    internal_type = "depository"

                conn.execute(
                    """INSERT OR REPLACE INTO accounts
                       (id, profile_id, institution_name, account_name,
                        account_type, account_subtype, current_balance,
                        currency, last_synced_at, is_active)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                    (
                        account["id"],
                        profile,
                        institution,
                        name,
                        internal_type,
                        subtype,
                        float(balance) if balance else 0.0,
                        account.get("currency", "USD"),
                        now,
                    ),
                )

                if new_txns:
                    logger.info(
                        "    Found %d new (skipping %d cached)",
                        len(new_txns), len(all_txns) - len(new_txns),
                    )
                    categorized = categorize_transactions(new_txns)
                    all_new_transactions.extend(categorized)
                else:
                    logger.info("    No new transactions for %s", name)

            # Insert new transactions
            for tx in all_new_transactions:
                _insert_transaction(conn, tx)

            # Snapshot net worth
            _snapshot_net_worth(conn, now)

        # Incremental recurring detection on newly synced transactions
        if all_new_transactions:
            try:
                from recurring import RecurringDetector, write_detection_results_to_db
                from database import _extract_merchant_pattern as _emp

                # Collect merchant keys from new transactions
                new_merchant_keys = set()
                for tx in all_new_transactions:
                    merchant = (tx.get("merchant_name") or "").upper().strip()
                    if merchant:
                        new_merchant_keys.add(merchant)
                    desc = tx.get("description", "")
                    pattern = _emp(desc)
                    if pattern:
                        new_merchant_keys.add(pattern)

                if new_merchant_keys:
                    logger.info(
                        "Running incremental recurring detection for %d merchant keys...",
                        len(new_merchant_keys),
                    )

                    # Need full transaction set for affected groups
                    all_data = get_data()
                    all_txns = all_data["transactions"]

                    # Detect per profile
                    profiles_seen = {tx.get("profile", "primary") for tx in all_new_transactions}
                    for prof in profiles_seen:
                        prof_txns = [t for t in all_txns if t.get("profile") == prof]
                        detector = RecurringDetector(get_db_conn=get_db)
                        result = detector.detect(
                            transactions=prof_txns,
                            profile=prof,
                            merchant_keys=new_merchant_keys,
                            generate_events=True,
                        )
                        write_detection_results_to_db(
                            get_db_conn=get_db,
                            items=result["items"],
                            events=result.get("events", []),
                            profile=prof,
                        )

                    logger.info("Incremental recurring detection complete.")

            except Exception as e:
                logger.warning("Incremental recurring detection failed (non-fatal): %s", e)

        total_count_row = None
        with get_db() as conn:
            total_count_row = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()

        total_count = total_count_row[0] if total_count_row else 0
        logger.info("Sync complete: %d new, %d total.", len(all_new_transactions), total_count)

        return get_data()


def _insert_transaction(conn, tx: dict):
    """Insert a single categorized transaction into the database."""
    profile = tx.get("profile", "primary")
    tx_id = tx.get("original_id", "")

    # Determine categorization source from confidence
    confidence = tx.get("confidence", "")
    cat_source = tx.get("categorization_source", "")

    if not cat_source:
        if confidence == "manual":
            cat_source = "user"
        elif confidence == "rule":
            cat_source = "rule-high"
        elif confidence in ("high", "medium", "low"):
            cat_source = "llm"
        elif confidence == "fallback":
            cat_source = "fallback"
        else:
            cat_source = "unknown"

    # Use the category already determined by the categorizer pipeline
    # (user rules were already checked in categorizer.py Phase 1.6)
    category = tx.get("category", "Other")

    # Ensure category exists
    conn.execute(
        "INSERT OR IGNORE INTO categories (name, is_system) VALUES (?, 0)",
        (category,),
    )

    # Resolve account_id
    account_id = None
    acct_name = tx.get("account_name", "")
    if acct_name:
        row = conn.execute(
            "SELECT id FROM accounts WHERE account_name = ? AND profile_id = ?",
            (acct_name, profile),
        ).fetchone()
        if row:
            account_id = row[0]

    conn.execute(
        """INSERT OR IGNORE INTO transactions
           (id, account_id, profile_id, date, description, raw_description,
            amount, category, categorization_source, transaction_type,
            counterparty_name, counterparty_type, teller_category,
            account_name, account_type, merchant_name, merchant_domain,
            merchant_industry, merchant_city, merchant_state,
            enriched, confidence)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            tx_id,
            account_id,
            profile,
            tx.get("date", ""),
            tx.get("description", ""),
            tx.get("raw_description", ""),
            float(tx.get("amount", 0)),
            category,
            cat_source,
            tx.get("type", ""),
            tx.get("counterparty_name", ""),
            tx.get("counterparty_type", ""),
            tx.get("teller_category", ""),
            acct_name,
            tx.get("account_type", ""),
            tx.get("merchant_name", ""),
            tx.get("merchant_domain", ""),
            tx.get("merchant_industry", ""),
            tx.get("merchant_city", ""),
            tx.get("merchant_state", ""),
            1 if tx.get("enriched") else 0,
            tx.get("confidence", ""),
        ),
    )


def _check_user_rules(conn, description: str) -> str | None:
    """
    Check if any user-defined category rules match this transaction.
    User rules are checked first (highest priority).
    Returns the category name if a rule matches, None otherwise.
    """
    if not description:
        return None

    rules = conn.execute(
        """SELECT pattern, match_type, category FROM category_rules
           WHERE source = 'user' AND is_active = 1
           ORDER BY priority DESC"""
    ).fetchall()

    desc_upper = description.upper()

    for rule in rules:
        pattern = rule[0]
        match_type = rule[1]
        category = rule[2]

        if match_type == "contains":
            if pattern.upper() in desc_upper:
                return category
        elif match_type == "regex":
            if re.search(pattern, description, re.IGNORECASE):
                return category
        elif match_type == "exact":
            if pattern.upper() == desc_upper:
                return category

    return None


def _snapshot_net_worth(conn, timestamp: str):
    """Take a net worth snapshot after sync."""
    today = timestamp[:10]  # YYYY-MM-DD

    # Get all profiles
    profiles = conn.execute("SELECT id FROM profiles").fetchall()

    for profile_row in profiles:
        profile_id = profile_row[0]

        assets = conn.execute(
            """SELECT COALESCE(SUM(current_balance), 0) FROM accounts
               WHERE profile_id = ? AND account_type IN ('depository', 'investment') AND is_active = 1""",
            (profile_id,),
        ).fetchone()[0]

        owed = conn.execute(
            """SELECT COALESCE(SUM(current_balance), 0) FROM accounts
               WHERE profile_id = ? AND account_type IN ('credit', 'loan') AND is_active = 1""",
            (profile_id,),
        ).fetchone()[0]

        conn.execute(
            """INSERT OR REPLACE INTO net_worth_history
               (date, profile_id, total_assets, total_owed, net_worth)
               VALUES (?, ?, ?, ?, ?)""",
            (today, profile_id, assets, owed, assets - owed),
        )

    # Household snapshot (all accounts)
    total_assets = conn.execute(
        "SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('depository', 'investment') AND is_active = 1"
    ).fetchone()[0]
    total_owed = conn.execute(
        "SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type IN ('credit', 'loan') AND is_active = 1"
    ).fetchone()[0]

    conn.execute(
        """INSERT OR REPLACE INTO net_worth_history
           (date, profile_id, total_assets, total_owed, net_worth)
           VALUES (?, 'household', ?, ?, ?)""",
        (today, total_assets, total_owed, total_assets - total_owed),
    )


# ══════════════════════════════════════════════════════════════════════════════
# WRITE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def update_transaction_category(tx_id: str, new_category: str) -> dict | bool:
    """
    Update a single transaction's category (user override).
    Also automatically creates a user rule for future matching.

    Returns:
        - False if transaction not found
        - dict with {"updated": True} and optionally subscription_prompt data
          if new_category is "Subscriptions"
    """
    with get_db() as conn:
        # Get the transaction
        row = conn.execute(
            "SELECT description, category, amount, merchant_name FROM transactions WHERE id = ?",
            (tx_id,),
        ).fetchone()

        if not row:
            return False

        description = row[0]
        old_category = row[1]
        tx_amount = row[2]
        tx_merchant_name = row[3]

        # Update the transaction
        conn.execute(
            """UPDATE transactions
               SET category = ?, categorization_source = 'user',
                   original_category = COALESCE(original_category, ?),
                   confidence = 'manual',
                   updated_at = datetime('now')
               WHERE id = ?""",
            (new_category, old_category, tx_id),
        )

        # Ensure category exists
        conn.execute(
            "INSERT OR IGNORE INTO categories (name, is_system) VALUES (?, 0)",
            (new_category,),
        )

        # Auto-create a user rule
        pattern = _extract_merchant_pattern(description)
        if pattern and len(pattern) >= 3:
            # Check if a user rule already exists for this pattern
            existing = conn.execute(
                "SELECT id, category FROM category_rules WHERE pattern = ? AND source = 'user' AND is_active = 1",
                (pattern,),
            ).fetchone()

            if existing:
                # Update existing rule
                conn.execute(
                    "UPDATE category_rules SET category = ? WHERE id = ?",
                    (new_category, existing[0]),
                )
            else:
                # Create new rule
                conn.execute(
                    """INSERT INTO category_rules
                       (pattern, match_type, category, priority, source)
                       VALUES (?, 'contains', ?, 1000, 'user')""",
                    (pattern, new_category),
                )

            # Apply rule retroactively: recategorize past transactions with same pattern
            # Only recategorize those that weren't manually set by the user
            escaped_pattern = _escape_like(pattern)
            conn.execute(
                """UPDATE transactions
                   SET category = ?, categorization_source = 'user-rule',
                       updated_at = datetime('now')
                   WHERE UPPER(description) LIKE ? ESCAPE '\\'
                     AND categorization_source != 'user'
                     AND id != ?""",
                (new_category, f"%{escaped_pattern}%", tx_id),
            )

        result = {"updated": True}

        # Enhancement 7: Signal subscription prompt if category is "Subscriptions"
        if new_category.strip().lower() == "subscriptions":
            merchant_pattern = _extract_merchant_pattern(description)
            result["subscription_prompt"] = True
            result["merchant"] = tx_merchant_name if tx_merchant_name else (merchant_pattern or description[:50])
            result["amount"] = round(abs(float(tx_amount)), 2) if tx_amount else 0.0
            result["transaction_id"] = tx_id
        
        return result


def add_category(name: str) -> bool:
    """Add a new user-defined category."""
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO categories (name, is_system) VALUES (?, 0)",
                (name,),
            )
            return True
        except Exception:
            return False


def get_categories() -> list[str]:
    """Get all active category names."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT name FROM categories WHERE is_active = 1 ORDER BY name"
        ).fetchall()
        return [row[0] for row in rows]


def get_category_rules(source: str | None = None) -> list[dict]:
    """Get category rules, optionally filtered by source."""
    with get_db() as conn:
        if source:
            rows = conn.execute(
                """SELECT id, pattern, match_type, category, priority, source, is_active, created_at
                   FROM category_rules WHERE source = ? ORDER BY priority DESC""",
                (source,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, pattern, match_type, category, priority, source, is_active, created_at
                   FROM category_rules ORDER BY priority DESC"""
            ).fetchall()
        return dicts_from_rows(rows)