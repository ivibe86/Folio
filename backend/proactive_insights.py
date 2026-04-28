"""
Deterministic proactive insights for Mira.

The generator intentionally uses closed, explainable checks over local data:
category spend spikes, recurring/subscription changes, and safe-to-spend risk.
"""

from __future__ import annotations

import json
import calendar
from datetime import date, datetime, timedelta
from typing import Any

from database import dicts_from_rows, get_db


NON_SPENDING_CATEGORIES = {
    "Income",
    "Savings Transfer",
    "Personal Transfer",
    "Credit Card Payment",
}


def _scope_profile(profile: str | None) -> str:
    return profile if profile and profile != "household" else "household"


def _profile_clause(profile: str | None, column: str = "profile_id") -> tuple[str, list[Any]]:
    if profile and profile != "household":
        return f" AND {column} = ?", [profile]
    return "", []


def _shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    zero_based = (year * 12 + (month - 1)) + offset
    return zero_based // 12, zero_based % 12 + 1


def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _money(value: float | int | None) -> str:
    return f"${float(value or 0):,.0f}"


def _json_dump(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_load(value: str | None) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _severity_rank(value: str) -> int:
    return {"critical": 0, "warning": 1, "info": 2}.get(value, 3)


def _insert_or_refresh(conn, profile: str | None, insight: dict) -> None:
    scope = _scope_profile(profile)
    conn.execute(
        """
        INSERT INTO proactive_insights (
            profile_id, kind, title, body, severity, evidence_json, fingerprint, status, generated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now'))
        ON CONFLICT(profile_id, fingerprint) DO UPDATE SET
            kind = excluded.kind,
            title = excluded.title,
            body = excluded.body,
            severity = excluded.severity,
            evidence_json = excluded.evidence_json,
            generated_at = CASE
                WHEN proactive_insights.status = 'dismissed' THEN proactive_insights.generated_at
                ELSE excluded.generated_at
            END
        """,
        (
            scope,
            insight["kind"],
            insight["title"],
            insight["body"],
            insight.get("severity") or "info",
            _json_dump(insight.get("evidence")),
            insight["fingerprint"],
        ),
    )


def _list_rows(conn, profile: str | None, include_dismissed: bool = False) -> list[dict]:
    scope = _scope_profile(profile)
    status_clause = "" if include_dismissed else " AND status = 'active'"
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT id, profile_id, kind, title, body, severity, evidence_json,
                   fingerprint, status, generated_at, dismissed_at
            FROM proactive_insights
            WHERE profile_id = ?{status_clause}
            ORDER BY generated_at DESC, id DESC
            LIMIT 12
            """,
            (scope,),
        ).fetchall()
    )
    for row in rows:
        row["evidence"] = _json_load(row.pop("evidence_json", "{}"))
    return sorted(rows, key=lambda row: (_severity_rank(row.get("severity") or "info"), row.get("generated_at") or ""), reverse=False)


def _category_spike_candidates(conn, profile: str | None, today: date) -> list[dict]:
    current = _month_key(today.year, today.month)
    start_year, start_month = _shift_month(today.year, today.month, -3)
    end_year, end_month = _shift_month(today.year, today.month, 1)
    start = f"{_month_key(start_year, start_month)}-01"
    end = f"{_month_key(end_year, end_month)}-01"
    profile_sql, profile_params = _profile_clause(profile)

    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT substr(date, 1, 7) AS month,
                   COALESCE(NULLIF(TRIM(category), ''), 'Uncategorized') AS category,
                   COALESCE(SUM(ABS(amount)), 0) AS total
            FROM transactions_visible
            WHERE amount < 0
              AND date >= ?
              AND date < ?
              AND COALESCE(NULLIF(TRIM(category), ''), 'Uncategorized')
                  NOT IN ({",".join("?" for _ in NON_SPENDING_CATEGORIES)})
              {profile_sql}
            GROUP BY month, category
            """,
            [start, end, *sorted(NON_SPENDING_CATEGORIES), *profile_params],
        ).fetchall()
    )

    by_category: dict[str, dict[str, float]] = {}
    for row in rows:
        by_category.setdefault(row["category"], {})[row["month"]] = float(row["total"] or 0)

    candidates: list[dict] = []
    prior_months = [_month_key(*_shift_month(today.year, today.month, offset)) for offset in (-3, -2, -1)]
    scope = _scope_profile(profile)
    for category, month_totals in by_category.items():
        current_total = month_totals.get(current, 0.0)
        priors = [month_totals.get(month, 0.0) for month in prior_months]
        nonzero_priors = [value for value in priors if value > 0]
        if not nonzero_priors:
            continue
        avg = sum(nonzero_priors) / len(nonzero_priors)
        lift = current_total - avg
        if current_total < 75 or avg < 25 or lift < 50 or current_total < avg * 1.6:
            continue
        severity = "warning" if current_total >= avg * 2 else "info"
        candidates.append(
            {
                "kind": "category_spike",
                "title": f"{category} is running higher",
                "body": (
                    f"{category} spending is {_money(current_total)} this month, "
                    f"versus a recent average of {_money(avg)}."
                ),
                "severity": severity,
                "fingerprint": f"{scope}:category_spike:{current}:{category.lower()}",
                "evidence": {
                    "month": current,
                    "category": category,
                    "current_total": round(current_total, 2),
                    "prior_average": round(avg, 2),
                    "prior_months": prior_months,
                },
            }
        )

    return sorted(candidates, key=lambda item: item["evidence"]["current_total"] - item["evidence"]["prior_average"], reverse=True)[:2]


def _recurring_candidates(conn, profile: str | None, today: date) -> list[dict]:
    profile_sql, profile_params = _profile_clause(profile, "e.profile_id")
    cutoff = (today - timedelta(days=14)).isoformat()
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT e.merchant_key, e.profile_id, e.event_type, e.period_bucket,
                   e.payload_json, e.created_at,
                   o.display_name, o.amount_cents, o.state, o.next_expected_date
            FROM recurring_events_v2 e
            LEFT JOIN recurring_obligations o
              ON o.profile_id = e.profile_id
             AND o.merchant_key = e.merchant_key
            WHERE date(e.created_at) >= date(?)
              {profile_sql}
            ORDER BY e.created_at DESC
            LIMIT 6
            """,
            [cutoff, *profile_params],
        ).fetchall()
    )
    scope = _scope_profile(profile)
    candidates = []
    for row in rows:
        event_type = row.get("event_type") or "recurring_update"
        event_words = event_type.replace("-", "_").split("_")
        merchant = row.get("display_name") or row.get("merchant_key") or "A recurring charge"
        payload = _json_load(row.get("payload_json"))
        amount = float(row.get("amount_cents") or 0) / 100
        if "amount" in event_words or "changed" in event_words:
            title = f"{merchant} may have changed"
            body = f"Mira saw a recent recurring-charge change for {merchant}."
        elif "new" in event_words or "created" in event_words:
            title = f"New recurring charge: {merchant}"
            body = f"{merchant} looks like a new recurring charge."
        else:
            title = f"Recurring update: {merchant}"
            body = f"Mira saw a recent recurring activity signal for {merchant}."
        if amount > 0:
            body = f"{body} Current expected amount is about {_money(amount)}."
        candidates.append(
            {
                "kind": "recurring_change",
                "title": title,
                "body": body,
                "severity": "info",
                "fingerprint": f"{scope}:recurring:{row.get('merchant_key')}:{event_type}:{row.get('period_bucket')}",
                "evidence": {
                    "merchant_key": row.get("merchant_key"),
                    "event_type": event_type,
                    "period_bucket": row.get("period_bucket"),
                    "payload": payload,
                    "expected_amount": round(amount, 2),
                    "next_expected_date": row.get("next_expected_date"),
                },
            }
        )
    return candidates[:2]


def _safe_to_spend_candidate(conn, profile: str | None) -> dict | None:
    from data_manager import get_plan_snapshot_data

    plan = get_plan_snapshot_data(profile=profile, conn=conn) or {}
    safe_to_spend = float(plan.get("safe_to_spend") or 0)
    limit = float(plan.get("safe_to_spend_limit") or 0)
    month = plan.get("month") or date.today().strftime("%Y-%m")
    has_plan_evidence = any(
        float(plan.get(key) or 0) > 0
        for key in (
            "planned_income",
            "income_actual",
            "safe_to_spend_spent",
            "mandatory_projected",
            "mandatory_spend",
            "total_budget",
            "active_goal_count",
        )
    )
    if limit <= 0 or not has_plan_evidence:
        return None
    low_threshold = max(50.0, limit * 0.10) if limit > 0 else 50.0
    if safe_to_spend >= 0 and safe_to_spend > low_threshold:
        return None
    severity = "warning" if safe_to_spend < 0 else "info"
    title = "Safe-to-spend is tight" if safe_to_spend >= 0 else "Safe-to-spend is negative"
    body = (
        f"You have {_money(safe_to_spend)} safe-to-spend remaining for {month}. "
        f"Variable spending is {_money(plan.get('safe_to_spend_spent'))} against a limit of {_money(limit)}."
    )
    return {
        "kind": "safe_to_spend",
        "title": title,
        "body": body,
        "severity": severity,
        "fingerprint": f"{_scope_profile(profile)}:safe_to_spend:{month}",
        "evidence": plan,
    }


def _budget_pace_candidates(conn, profile: str | None, today: date) -> list[dict]:
    scope = _scope_profile(profile)
    month = _month_key(today.year, today.month)
    month_start = f"{month}-01"
    month_end = f"{month}-{calendar.monthrange(today.year, today.month)[1]:02d}"
    profile_sql, profile_params = _profile_clause(profile, "t.profile_id")
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT b.category,
                   b.amount AS budget_amount,
                   COALESCE(SUM(CASE
                       WHEN t.amount < 0 THEN ABS(t.amount)
                       WHEN t.amount > 0 THEN -ABS(t.amount)
                       ELSE 0
                   END), 0) AS spent
              FROM category_budgets b
              LEFT JOIN transactions_visible t
                ON t.category = b.category
               AND t.date >= ?
               AND t.date <= ?
               AND COALESCE(t.is_excluded, 0) = 0
               AND (t.expense_type IS NULL OR t.expense_type NOT IN ('transfer_internal','transfer_household'))
               {profile_sql}
             WHERE b.profile_id = ?
               AND b.amount > 0
             GROUP BY b.category, b.amount
            """,
            [month_start, month_end, *profile_params, scope],
        ).fetchall()
    )
    day_ratio = today.day / calendar.monthrange(today.year, today.month)[1]
    candidates = []
    for row in rows:
        budget = float(row.get("budget_amount") or 0)
        spent = float(row.get("spent") or 0)
        if budget <= 0:
            continue
        expected_by_now = budget * day_ratio
        projected = spent / max(day_ratio, 0.05)
        if spent < 50 or projected < budget * 1.15 or spent < expected_by_now + 40:
            continue
        category = row.get("category") or "Budget"
        candidates.append(
            {
                "kind": "budget_pace",
                "title": f"{category} may exceed budget",
                "body": (
                    f"{category} is at {_money(spent)} of a {_money(budget)} monthly budget. "
                    f"At this pace, it projects near {_money(projected)}."
                ),
                "severity": "warning" if projected >= budget * 1.35 else "info",
                "fingerprint": f"{scope}:budget_pace:{month}:{category.lower()}",
                "evidence": {
                    "month": month,
                    "category": category,
                    "spent": round(spent, 2),
                    "budget": round(budget, 2),
                    "projected": round(projected, 2),
                    "day_ratio": round(day_ratio, 3),
                },
            }
        )
    return sorted(candidates, key=lambda item: item["evidence"]["projected"] - item["evidence"]["budget"], reverse=True)[:2]


def _merchant_anomaly_candidates(conn, profile: str | None, today: date) -> list[dict]:
    scope = _scope_profile(profile)
    profile_sql, profile_params = _profile_clause(profile, "t.profile_id")
    start = (today - timedelta(days=120)).isoformat()
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT t.id, t.date, t.description, t.merchant_name, t.merchant_key,
                   t.amount, t.category
              FROM transactions_visible t
             WHERE t.date >= ?
               AND t.amount < 0
               AND COALESCE(t.is_excluded, 0) = 0
               AND COALESCE(t.category, '') NOT IN ({",".join("?" for _ in NON_SPENDING_CATEGORIES)})
               AND (t.expense_type IS NULL OR t.expense_type NOT IN ('transfer_internal','transfer_household'))
               {profile_sql}
             ORDER BY t.date DESC
             LIMIT 300
            """,
            [start, *sorted(NON_SPENDING_CATEGORIES), *profile_params],
        ).fetchall()
    )
    by_merchant: dict[str, list[dict]] = {}
    for row in rows:
        key = (row.get("merchant_key") or row.get("merchant_name") or row.get("description") or "").strip().upper()
        if not key:
            continue
        by_merchant.setdefault(key, []).append(row)

    candidates = []
    current_month = _month_key(today.year, today.month)
    for key, items in by_merchant.items():
        current_items = [item for item in items if str(item.get("date") or "").startswith(current_month)]
        prior_amounts = [abs(float(item.get("amount") or 0)) for item in items if not str(item.get("date") or "").startswith(current_month)]
        if len(prior_amounts) < 3:
            continue
        avg = sum(prior_amounts) / len(prior_amounts)
        for item in current_items[:2]:
            amount = abs(float(item.get("amount") or 0))
            if amount < 100 or avg < 10 or amount < max(avg * 3, avg + 75):
                continue
            merchant = item.get("merchant_name") or item.get("description") or key
            candidates.append(
                {
                    "kind": "merchant_anomaly",
                    "title": f"Unusual charge at {merchant}",
                    "body": f"{merchant} posted {_money(amount)}, versus a recent average near {_money(avg)}.",
                    "severity": "warning",
                    "fingerprint": f"{scope}:merchant_anomaly:{item.get('id')}",
                    "evidence": {
                        "transaction_id": item.get("id"),
                        "merchant": merchant,
                        "amount": round(amount, 2),
                        "prior_average": round(avg, 2),
                        "sample_size": len(prior_amounts),
                        "date": item.get("date"),
                    },
                }
            )
    return sorted(candidates, key=lambda item: item["evidence"]["amount"] - item["evidence"]["prior_average"], reverse=True)[:2]


def _recurring_calendar_candidates(conn, profile: str | None, today: date) -> list[dict]:
    scope = _scope_profile(profile)
    profile_sql, profile_params = _profile_clause(profile)
    upcoming_end = (today + timedelta(days=7)).isoformat()
    stopped_cutoff = (today - timedelta(days=7)).isoformat()
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT merchant_key, display_name, amount_cents, frequency, state,
                   next_expected_date, last_seen_date
              FROM recurring_obligations
             WHERE next_expected_date IS NOT NULL
               AND state IN ('active', 'confirmed', 'candidate')
               {profile_sql}
             ORDER BY next_expected_date ASC
             LIMIT 30
            """,
            profile_params,
        ).fetchall()
    )
    candidates = []
    for row in rows:
        due = str(row.get("next_expected_date") or "")[:10]
        merchant = row.get("display_name") or row.get("merchant_key") or "Recurring charge"
        amount = float(row.get("amount_cents") or 0) / 100
        if today.isoformat() <= due <= upcoming_end and amount > 0:
            candidates.append(
                {
                    "kind": "recurring_calendar",
                    "title": f"{merchant} is coming up",
                    "body": f"{merchant} is expected around {due} for about {_money(amount)}.",
                    "severity": "info",
                    "fingerprint": f"{scope}:recurring_calendar:{row.get('merchant_key')}:{due}",
                    "evidence": {
                        "merchant_key": row.get("merchant_key"),
                        "next_expected_date": due,
                        "expected_amount": round(amount, 2),
                    },
                }
            )
        elif due < stopped_cutoff and row.get("last_seen_date") and str(row.get("last_seen_date"))[:10] < due:
            candidates.append(
                {
                    "kind": "recurring_stopped",
                    "title": f"{merchant} may have stopped posting",
                    "body": f"{merchant} was expected around {due}, but Mira has not seen a matching charge yet.",
                    "severity": "info",
                    "fingerprint": f"{scope}:recurring_stopped:{row.get('merchant_key')}:{due}",
                    "evidence": {
                        "merchant_key": row.get("merchant_key"),
                        "next_expected_date": due,
                        "last_seen_date": row.get("last_seen_date"),
                    },
                }
            )
    return candidates[:3]


def generate_insights(profile: str | None = None, conn=None) -> list[dict]:
    def _generate(c):
        today = date.today()
        candidates = []
        candidates.extend(_category_spike_candidates(c, profile, today))
        candidates.extend(_recurring_candidates(c, profile, today))
        candidates.extend(_budget_pace_candidates(c, profile, today))
        candidates.extend(_merchant_anomaly_candidates(c, profile, today))
        candidates.extend(_recurring_calendar_candidates(c, profile, today))
        safe = _safe_to_spend_candidate(c, profile)
        if safe:
            candidates.append(safe)
        active_fingerprints = [insight["fingerprint"] for insight in candidates]
        managed_kinds = (
            "category_spike",
            "recurring_change",
            "safe_to_spend",
            "budget_pace",
            "merchant_anomaly",
            "recurring_calendar",
            "recurring_stopped",
        )
        scope = _scope_profile(profile)
        for insight in candidates:
            _insert_or_refresh(c, profile, insight)
        if active_fingerprints:
            c.execute(
                f"""
                UPDATE proactive_insights
                   SET status = 'stale'
                 WHERE profile_id = ?
                   AND status = 'active'
                   AND kind IN ({",".join("?" for _ in managed_kinds)})
                   AND fingerprint NOT IN ({",".join("?" for _ in active_fingerprints)})
                """,
                [scope, *managed_kinds, *active_fingerprints],
            )
        else:
            c.execute(
                f"""
                UPDATE proactive_insights
                   SET status = 'stale'
                 WHERE profile_id = ?
                   AND status = 'active'
                   AND kind IN ({",".join("?" for _ in managed_kinds)})
                """,
                [scope, *managed_kinds],
            )
        return _list_rows(c, profile)

    if conn is not None:
        return _generate(conn)
    with get_db() as c:
        return _generate(c)


def list_insights(profile: str | None = None, include_dismissed: bool = False, conn=None, generate: bool = True) -> list[dict]:
    def _list(c):
        if generate:
            generate_insights(profile=profile, conn=c)
        return _list_rows(c, profile, include_dismissed=include_dismissed)

    if conn is not None:
        return _list(conn)
    with get_db() as c:
        return _list(c)


def dismiss_insight(insight_id: int, profile: str | None = None, conn=None) -> bool:
    def _dismiss(c):
        result = c.execute(
            """
            UPDATE proactive_insights
               SET status = 'dismissed',
                   dismissed_at = datetime('now')
             WHERE id = ?
               AND profile_id = ?
            """,
            (insight_id, _scope_profile(profile)),
        )
        return result.rowcount > 0

    if conn is not None:
        return _dismiss(conn)
    with get_db() as c:
        return _dismiss(c)


def restore_insight(insight_id: int, profile: str | None = None, conn=None) -> bool:
    def _restore(c):
        result = c.execute(
            """
            UPDATE proactive_insights
               SET status = 'active',
                   dismissed_at = NULL,
                   generated_at = datetime('now')
             WHERE id = ?
               AND profile_id = ?
            """,
            (insight_id, _scope_profile(profile)),
        )
        return result.rowcount > 0

    if conn is not None:
        return _restore(conn)
    with get_db() as c:
        return _restore(c)
