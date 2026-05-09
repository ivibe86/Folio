from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

from database import dicts_from_rows
from mira import metric_registry


NON_SPENDING_CATEGORIES = {
    "Income",
    "Credits & Refunds",
    "Savings Transfer",
    "Personal Transfer",
    "Credit Card Payment",
}
INCOME_HINTS = ("payroll", "paycheck", "salary", "direct dep", "direct deposit", "employer")
ONE_OFF_INCOME_TERMS = (
    "refund",
    "tax ref",
    "irs",
    "treas tax",
    "interest",
    "reimbursement",
    "reimburse",
    "transfer",
    "zelle",
    "venmo",
    "cash app",
    "cashback",
    "rebate",
    "dividend",
    "misc credit",
)
PAYCHECK_LIKE_TERMS = (
    "payroll",
    "paycheck",
    "salary",
    "direct dep",
    "direct deposit",
    "employer",
    "wages",
)
DEFAULT_BUFFER = 100.0


def get_cashflow_forecast(
    conn,
    profile: str | None,
    *,
    horizon_days: int | None = None,
    buffer_amount: float | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    today = as_of or date.today()
    starting = _starting_balance(conn, profile)
    income_signals = _income_signals(conn, profile, today)
    next_income = _next_income_event(income_signals, today)

    if horizon_days is None:
        if next_income and next_income["date"] > today:
            horizon_days = max(1, (next_income["date"] - today).days)
        else:
            horizon_days = 14
    horizon_days = max(1, min(int(horizon_days or 14), 90))
    horizon_end = today + timedelta(days=horizon_days)

    obligations = _upcoming_obligations(conn, profile, today, horizon_end)
    discretionary = _expected_discretionary(conn, profile, today, horizon_days)
    expected_income = []
    if next_income and today <= next_income["date"] <= horizon_end:
        expected_income.append(
            {
                "date": next_income["date"].isoformat(),
                "amount": round(next_income["amount"], 2),
                "source": next_income["source"],
                "evidence_transaction_ids": next_income.get("evidence_transaction_ids") or [],
            }
        )

    balance = float(starting["amount"])
    low_point = {"date": today.isoformat(), "amount": balance}
    events: list[dict[str, Any]] = []
    income_by_date = _group_amounts(expected_income)
    obligations_by_date = _group_amounts(obligations, amount_key="amount")
    daily_discretionary = float(discretionary["daily_average"])
    for offset in range(1, horizon_days + 1):
        day = today + timedelta(days=offset)
        day_key = day.isoformat()
        income_amount = income_by_date.get(day_key, 0.0)
        obligation_amount = obligations_by_date.get(day_key, 0.0)
        balance += income_amount
        balance -= obligation_amount
        balance -= daily_discretionary
        if income_amount or obligation_amount:
            events.append(
                {
                    "date": day_key,
                    "income": round(income_amount, 2),
                    "obligations": round(obligation_amount, 2),
                    "discretionary_estimate": round(daily_discretionary, 2),
                    "projected_balance": round(balance, 2),
                }
            )
        if balance < low_point["amount"]:
            low_point = {"date": day_key, "amount": round(balance, 2)}

    caveats = []
    caveats.extend(starting["caveats"])
    caveats.extend(discretionary["caveats"])
    if not income_signals:
        caveats.append("No recurring paycheck pattern was strong enough to include expected income; one-off income such as refunds, interest, reimbursements, transfers, and misc credits is excluded.")
    if not obligations:
        caveats.append("No upcoming recurring obligations were found in the selected horizon.")

    confidence_score = _confidence_score(starting, income_signals, obligations, discretionary)
    confidence = _confidence_label(confidence_score)
    expected_discretionary = float(discretionary["expected_total"])
    projected_ending = round(balance, 2)
    band_width = max(25.0, expected_discretionary * (0.20 if confidence == "high" else 0.35 if confidence == "medium" else 0.60))
    buffer = float(buffer_amount if buffer_amount is not None else DEFAULT_BUFFER)
    sample_ids = []
    for item in income_signals[:5]:
        for tx_id in item.get("evidence_transaction_ids") or []:
            if tx_id not in sample_ids:
                sample_ids.append(tx_id)
    for tx_id in discretionary.get("sample_transaction_ids") or []:
        if tx_id not in sample_ids:
            sample_ids.append(tx_id)

    assumptions = [
        f"Starting balance uses active depository account available balances as of {today.isoformat()}.",
        f"Discretionary spend uses the last {discretionary['lookback_days']} days of non-fixed spending history.",
        "Recurring obligations use stored recurring obligation expected dates and amounts.",
    ]
    if expected_income:
        assumptions.append("Expected income uses the latest recurring income cadence from recent income transactions.")

    result = {
        "forecast_horizon": {
            "start": today.isoformat(),
            "end": horizon_end.isoformat(),
            "days": horizon_days,
        },
        "starting_balance": starting,
        "starting_balance_source": starting["source"],
        "projected_low_point": low_point,
        "projected_ending_balance": projected_ending,
        "confidence_band": {
            "ending_low": round(projected_ending - band_width, 2),
            "ending_high": round(projected_ending + band_width, 2),
            "band_width": round(band_width, 2),
        },
        "expected_income": {
            "total": round(sum(float(item["amount"]) for item in expected_income), 2),
            "items": expected_income,
        },
        "upcoming_obligations": obligations,
        "upcoming_obligation_total": round(sum(float(item["amount"]) for item in obligations), 2),
        "expected_discretionary_spend": {
            "total": round(expected_discretionary, 2),
            "daily_average": round(daily_discretionary, 2),
            "lookback_days": discretionary["lookback_days"],
            "sample_transaction_ids": discretionary.get("sample_transaction_ids") or [],
        },
        "buffer": round(buffer, 2),
        "confidence": confidence,
        "confidence_score": round(confidence_score, 2),
        "assumptions": assumptions,
        "caveats": caveats,
        "events": events[:20],
    }
    return _with_contract(
        result,
        tool="get_cashflow_forecast",
        args={"horizon_days": horizon_days, "buffer_amount": buffer},
        row_count=len(obligations) + len(expected_income) + int(discretionary.get("sample_size") or 0),
        sample_transaction_ids=sample_ids,
        caveats=caveats,
        label=f"{today.isoformat()} to {horizon_end.isoformat()}",
    )


def predict_shortfall(
    conn,
    profile: str | None,
    *,
    horizon_days: int | None = None,
    buffer_amount: float | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    forecast = get_cashflow_forecast(
        conn,
        profile,
        horizon_days=horizon_days,
        buffer_amount=buffer_amount,
        as_of=as_of,
    )
    low = forecast["projected_low_point"]
    buffer = float(forecast.get("buffer") or DEFAULT_BUFFER)
    confidence = str(forecast.get("confidence") or "low")
    crosses_zero = float(low.get("amount") or 0) < 0
    crosses_buffer = float(low.get("amount") or 0) < buffer
    suppressed = confidence == "low" and not crosses_zero
    if suppressed:
        warning = None
    elif crosses_zero or crosses_buffer:
        needed = abs(float(low.get("amount") or 0)) if crosses_zero else buffer - float(low.get("amount") or 0)
        warning = {
            "what": "Projected cash may fall below zero." if crosses_zero else f"Projected cash may fall below the ${buffer:,.0f} buffer.",
            "when": low.get("date"),
            "why": _shortfall_why(forecast),
            "confidence": confidence,
            "recommended_action": (
                f"Try to free up about ${needed:,.0f} before {low.get('date')} by delaying discretionary spend "
                "or moving a non-urgent bill if that is available."
            ),
        }
    else:
        warning = None
    result = {
        "has_shortfall_risk": bool(warning),
        "suppressed": suppressed,
        "suppressed_reason": "Forecast confidence is low, so Mira is not surfacing a buffer warning." if suppressed else "",
        "warning": warning,
        "forecast": forecast,
        "confidence": confidence,
        "caveats": forecast.get("caveats") or [],
    }
    return _with_contract(
        result,
        tool="predict_shortfall",
        args={"horizon_days": horizon_days, "buffer_amount": buffer_amount},
        row_count=int((forecast.get("provenance") or {}).get("row_count") or 0),
        sample_transaction_ids=(forecast.get("provenance") or {}).get("sample_transaction_ids") or [],
        caveats=result["caveats"],
        label=(forecast.get("provenance") or {}).get("range"),
    )


def check_affordability(
    conn,
    profile: str | None,
    *,
    amount: float,
    purpose: str = "",
    category: str = "",
    horizon_days: int | None = None,
    buffer_amount: float | None = None,
    question: str = "",
    as_of: date | None = None,
) -> dict[str, Any]:
    forecast = get_cashflow_forecast(
        conn,
        profile,
        horizon_days=horizon_days,
        buffer_amount=buffer_amount,
        as_of=as_of,
    )
    amount = max(0.0, float(amount or 0))
    category = _resolve_category(conn, profile, category or purpose or question)
    budget = _category_budget_context(conn, profile, category, amount) if category else None
    memories = _relevant_goal_memories(conn, profile, question or f"afford {purpose} {category}")
    memory_constraints = _memory_constraints(memories, category, amount)

    low_after = float((forecast.get("projected_low_point") or {}).get("amount") or 0) - amount
    ending_after = float(forecast.get("projected_ending_balance") or 0) - amount
    buffer = float(forecast.get("buffer") or DEFAULT_BUFFER)
    caveats = list(forecast.get("caveats") or [])
    if forecast.get("confidence") == "low":
        caveats.append("Affordability is cautious because the cash-flow forecast confidence is low.")
    allowed = low_after >= buffer and forecast.get("confidence") != "low"
    if budget and budget.get("has_budget") and float(budget.get("remaining_after_purchase") or 0) < 0:
        allowed = False
    if memory_constraints.get("conflicts"):
        allowed = False

    if allowed:
        recommendation = "Looks affordable within the forecast buffer."
    else:
        reasons = []
        if low_after < buffer:
            reasons.append(f"it would put the projected low point near ${low_after:,.0f}, below the ${buffer:,.0f} buffer")
        if budget and budget.get("has_budget") and float(budget.get("remaining_after_purchase") or 0) < 0:
            reasons.append(f"it would push {category} past the stored budget")
        if memory_constraints.get("conflicts"):
            reasons.append(memory_constraints["conflicts"][0])
        if forecast.get("confidence") == "low":
            reasons.append("forecast confidence is low")
        recommendation = "Not a clean yes: " + "; ".join(reasons[:3]) + "."

    result = {
        "affordable": allowed,
        "amount": round(amount, 2),
        "purpose": purpose,
        "category": category,
        "forecast": forecast,
        "projected_low_after_purchase": round(low_after, 2),
        "projected_ending_after_purchase": round(ending_after, 2),
        "budget_context": budget,
        "memory_context": memory_constraints,
        "confidence": forecast.get("confidence"),
        "recommendation": recommendation,
        "recommended_action": "Keep the purchase below the forecast buffer or reduce flexible spend first." if not allowed else "Proceed only if this still matches your priorities.",
        "assumptions": forecast.get("assumptions") or [],
        "caveats": caveats,
    }
    return _with_contract(
        result,
        tool="check_affordability",
        args={"amount": amount, "purpose": purpose, "category": category, "horizon_days": horizon_days},
        row_count=int((forecast.get("provenance") or {}).get("row_count") or 0),
        sample_transaction_ids=(forecast.get("provenance") or {}).get("sample_transaction_ids") or [],
        caveats=caveats,
        label=(forecast.get("provenance") or {}).get("range"),
    )


def extract_affordability_args(question: str, categories: list[str] | None = None) -> dict[str, Any] | None:
    amount = _extract_amount(question)
    q = question or ""
    if amount is None and not re.search(r"\b(afford|buy|spend)\b", q, re.I):
        return None
    purpose = ""
    match = re.search(r"\b(?:for|on|another|buy)\s+([^?.!]+)", q, re.I)
    if match:
        purpose = match.group(1).strip()
        purpose = re.sub(r"\b(this|that|the|a|an|week|month|today|tomorrow)\b", "", purpose, flags=re.I).strip()
    category = _category_from_text(q, categories or [])
    horizon_days = 7 if re.search(r"\b(this week|week)\b", q, re.I) else None
    if amount is None:
        amount = 0.0
    return {
        "amount": round(float(amount), 2),
        "purpose": purpose,
        "category": category,
        "horizon_days": horizon_days,
        "question": question,
    }


def _starting_balance(conn, profile: str | None) -> dict[str, Any]:
    profile_sql, params = _profile_clause(profile)
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT id, account_name, account_type, account_subtype, available_balance, current_balance, last_synced_at
              FROM accounts
             WHERE COALESCE(is_active, 1) = 1
               AND account_type IN ('depository', 'cash')
               {profile_sql}
            """,
            params,
        ).fetchall()
    )
    total = 0.0
    sources = []
    caveats = []
    for row in rows:
        raw = row.get("available_balance")
        if raw in (None, ""):
            raw = row.get("current_balance")
        amount = float(raw or 0)
        total += amount
        sources.append(
            {
                "account_id": row.get("id"),
                "account_name": row.get("account_name"),
                "balance": round(amount, 2),
                "last_synced_at": row.get("last_synced_at"),
            }
        )
    if not rows:
        caveats.append("No active depository account balance was found.")
    stale = [row for row in rows if row.get("last_synced_at") and str(row.get("last_synced_at"))[:10] < (date.today() - timedelta(days=7)).isoformat()]
    if stale:
        caveats.append("At least one cash account balance may be stale.")
    return {"amount": round(total, 2), "source": "active_depository_accounts", "accounts": sources, "caveats": caveats}


def _income_signals(conn, profile: str | None, today: date) -> list[dict[str, Any]]:
    profile_sql, params = _profile_clause(profile, "profile_id")
    start = (today - timedelta(days=120)).isoformat()
    hint_sql = " OR ".join(["LOWER(description) LIKE ?" for _ in INCOME_HINTS])
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT id, date, description, amount, category, counterparty_name, merchant_name
              FROM transactions_visible
             WHERE amount > 0
               AND date >= ?
               AND (category = 'Income' OR {hint_sql})
               {profile_sql}
             ORDER BY date ASC
            """,
            [start, *[f"%{hint}%" for hint in INCOME_HINTS], *params],
        ).fetchall()
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        parsed_date = _parse_date(row.get("date"))
        amount = float(row.get("amount") or 0)
        if parsed_date is None or amount <= 0:
            continue
        text = " ".join(
            str(row.get(key) or "")
            for key in ("description", "counterparty_name", "merchant_name", "category")
        )
        if _looks_one_off_income(text):
            continue
        key = _income_source_key(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(
            {
                "date": parsed_date,
                "amount": amount,
                "source": row.get("counterparty_name") or row.get("merchant_name") or row.get("description") or "Income",
                "source_key": key,
                "text": text,
                "evidence_transaction_ids": [row.get("id")] if row.get("id") else [],
            }
        )

    recurring: list[dict[str, Any]] = []
    for key, items in grouped.items():
        items = sorted(items, key=lambda item: item["date"])
        if len(items) < 2:
            continue
        dates = [item["date"] for item in items]
        gaps = [(dates[idx] - dates[idx - 1]).days for idx in range(1, len(dates))]
        plausible_gaps = [gap for gap in gaps if 5 <= gap <= 45]
        if not plausible_gaps:
            continue
        paycheck_like = any(_looks_paycheck_like(item.get("text") or "") for item in items)
        amount_values = [float(item.get("amount") or 0) for item in items]
        amount_mid = median(amount_values)
        amount_stable = amount_mid > 0 and all(abs(value - amount_mid) <= max(75.0, amount_mid * 0.35) for value in amount_values[-3:])
        if not paycheck_like and not amount_stable:
            continue
        latest = items[-1]
        recurring.append(
            {
                **latest,
                "source_key": key,
                "cadence_days": int(median(plausible_gaps)),
                "amount": amount_mid,
                "evidence_transaction_ids": [
                    tx_id
                    for item in items[-4:]
                    for tx_id in (item.get("evidence_transaction_ids") or [])
                    if tx_id
                ],
                "evidence_count": len(items),
            }
        )
    return recurring


def _next_income_event(signals: list[dict[str, Any]], today: date) -> dict[str, Any] | None:
    if not signals:
        return None
    amounts = [float(item.get("amount") or 0) for item in signals]
    latest = max(signals, key=lambda item: item["date"])
    cadence = int(latest.get("cadence_days") or 14)
    next_date = latest["date"] + timedelta(days=cadence)
    while next_date <= today:
        next_date += timedelta(days=cadence)
    return {
        "date": next_date,
        "amount": float(latest.get("amount") or (median(amounts) if amounts else 0)),
        "source": latest.get("source") or "Income",
        "evidence_transaction_ids": latest.get("evidence_transaction_ids") or [],
    }


def _upcoming_obligations(conn, profile: str | None, start: date, end: date) -> list[dict[str, Any]]:
    profile_sql, params = _profile_clause(profile)
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT id, obligation_key, merchant_key, display_name, category, amount_cents,
                   frequency, next_expected_date, state, confidence_label, confidence_score, evidence_json
              FROM recurring_obligations
             WHERE next_expected_date IS NOT NULL
               AND next_expected_date >= ?
               AND next_expected_date <= ?
               AND state IN ('active', 'confirmed', 'candidate')
               AND amount_cents > 0
               {profile_sql}
             ORDER BY next_expected_date ASC
            """,
            [start.isoformat(), end.isoformat(), *params],
        ).fetchall()
    )
    items = []
    for row in rows:
        items.append(
            {
                "id": row.get("id"),
                "obligation_key": row.get("obligation_key"),
                "merchant_key": row.get("merchant_key"),
                "merchant": row.get("display_name") or row.get("merchant_key"),
                "category": row.get("category"),
                "amount": round(float(row.get("amount_cents") or 0) / 100, 2),
                "date": str(row.get("next_expected_date") or "")[:10],
                "frequency": row.get("frequency"),
                "state": row.get("state"),
                "confidence_label": row.get("confidence_label"),
                "confidence_score": row.get("confidence_score"),
                "evidence": _json_load(row.get("evidence_json")),
            }
        )
    return items


def _expected_discretionary(conn, profile: str | None, today: date, horizon_days: int) -> dict[str, Any]:
    lookback_days = 30
    start = (today - timedelta(days=lookback_days)).isoformat()
    profile_sql, params = _profile_clause(profile, "t.profile_id")
    rows = dicts_from_rows(
        conn.execute(
            f"""
            SELECT t.id, t.date, t.description, t.amount, t.category
              FROM transactions_visible t
              LEFT JOIN categories c ON c.name = t.category
             WHERE t.amount < 0
               AND t.date >= ?
               AND COALESCE(t.is_excluded, 0) = 0
               AND COALESCE(t.category, '') NOT IN ({",".join("?" for _ in NON_SPENDING_CATEGORIES)})
               AND COALESCE(c.expense_type, 'variable') = 'variable'
               AND (t.expense_type IS NULL OR t.expense_type NOT IN ('transfer_internal','transfer_household'))
               {profile_sql}
             ORDER BY t.date DESC
             LIMIT 200
            """,
            [start, *sorted(NON_SPENDING_CATEGORIES), *params],
        ).fetchall()
    )
    total = sum(abs(float(row.get("amount") or 0)) for row in rows)
    sample_size = len(rows)
    daily = total / lookback_days if lookback_days else 0.0
    caveats = []
    if sample_size < 5:
        caveats.append("Recent discretionary history is thin.")
    return {
        "daily_average": daily,
        "expected_total": daily * horizon_days,
        "lookback_days": lookback_days,
        "sample_size": sample_size,
        "sample_transaction_ids": [row.get("id") for row in rows[:8] if row.get("id")],
        "caveats": caveats,
    }


def _category_budget_context(conn, profile: str | None, category: str, amount: float) -> dict[str, Any]:
    if not category:
        return {"has_budget": False}
    scope = profile if profile and profile != "household" else "household"
    budget = conn.execute(
        "SELECT amount FROM category_budgets WHERE profile_id = ? AND lower(category) = lower(?)",
        (scope, category),
    ).fetchone()
    if not budget:
        return {"has_budget": False, "category": category}
    today = date.today()
    start = today.replace(day=1).isoformat()
    end = today.isoformat()
    profile_sql, params = _profile_clause(profile)
    spent = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(amount)), 0)
          FROM transactions_visible
         WHERE amount < 0
           AND date >= ?
           AND date <= ?
           AND lower(category) = lower(?)
           {profile_sql}
        """,
        [start, end, category, *params],
    ).fetchone()[0]
    budget_amount = float(budget["amount"] or 0)
    spent_amount = float(spent or 0)
    return {
        "has_budget": True,
        "category": category,
        "budget": round(budget_amount, 2),
        "spent_month_to_date": round(spent_amount, 2),
        "remaining_before_purchase": round(budget_amount - spent_amount, 2),
        "remaining_after_purchase": round(budget_amount - spent_amount - amount, 2),
    }


def _relevant_goal_memories(conn, profile: str | None, question: str) -> list[dict[str, Any]]:
    try:
        from mira import memory_v2

        result = memory_v2.retrieve_relevant_memories(
            conn=conn,
            profile=profile,
            question=question,
            route={"intent": "plan", "operation": "affordability"},
            limit=4,
        )
        memories = result.get("memories") if isinstance(result, dict) else []
        return memories if isinstance(memories, list) else []
    except Exception:
        return []


def _memory_constraints(memories: list[dict[str, Any]], category: str, amount: float) -> dict[str, Any]:
    try:
        from mira import memory_v2

        return memory_v2.affordability_constraint_context(memories, category=category, amount=amount)
    except Exception:
        return {"used_memories": [], "conflicts": []}


def _looks_one_off_income(text: str) -> bool:
    lowered = (text or "").lower()
    return any(term in lowered for term in ONE_OFF_INCOME_TERMS)


def _looks_paycheck_like(text: str) -> bool:
    lowered = (text or "").lower()
    return any(term in lowered for term in PAYCHECK_LIKE_TERMS)


def _income_source_key(row: dict[str, Any]) -> str:
    raw = row.get("counterparty_name") or row.get("merchant_name") or row.get("description") or ""
    normalized = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(raw).lower())).strip()
    tokens = [
        token for token in normalized.split()
        if token not in {"ach", "credit", "debit", "deposit", "direct", "dep", "payroll", "paycheck", "payment", "online", "ppd", "id"}
        and not token.isdigit()
    ]
    if not tokens:
        tokens = [token for token in normalized.split() if token and not token.isdigit()]
    return " ".join(tokens[:4])


def _resolve_category(conn, profile: str | None, text: str) -> str:
    text = str(text or "").strip()
    categories = _category_names(conn, profile)
    if not text:
        return ""
    return _category_from_text(text, categories, profile=profile)


def _category_names(conn, profile: str | None) -> list[str]:
    profile_sql, params = _profile_clause(profile)
    names: list[str] = []
    for row in conn.execute(
        f"""
        SELECT category AS name, COUNT(*) AS frequency
          FROM transactions_visible
         WHERE COALESCE(category, '') != ''
           {profile_sql}
         GROUP BY category
         ORDER BY frequency DESC, category
        """,
        params,
    ).fetchall():
        name = str(row["name"] if hasattr(row, "keys") else row[0] or "").strip()
        if name and name not in names:
            names.append(name)
    for row in conn.execute("SELECT name FROM categories WHERE COALESCE(name, '') != '' ORDER BY name").fetchall():
        name = str(row["name"] if hasattr(row, "keys") else row[0] or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def _shortfall_why(forecast: dict[str, Any]) -> str:
    obligations = float(forecast.get("upcoming_obligation_total") or 0)
    discretionary = float(((forecast.get("expected_discretionary_spend") or {}).get("total")) or 0)
    income = float(((forecast.get("expected_income") or {}).get("total")) or 0)
    return (
        f"Expected income is ${income:,.0f}, upcoming obligations are ${obligations:,.0f}, "
        f"and expected discretionary spend is ${discretionary:,.0f} in the forecast window."
    )


def _confidence_score(starting: dict[str, Any], income: list[dict[str, Any]], obligations: list[dict[str, Any]], discretionary: dict[str, Any]) -> float:
    score = 0.15
    if starting.get("accounts"):
        score += 0.25
    if int(discretionary.get("sample_size") or 0) >= 5:
        score += 0.20
    if int(discretionary.get("sample_size") or 0) >= 10:
        score += 0.10
    if len(income) >= 2:
        score += 0.15
    elif income:
        score += 0.08
    if obligations:
        score += 0.12
    if starting.get("caveats"):
        score -= 0.10
    return max(0.0, min(score, 0.95))


def _confidence_label(score: float) -> str:
    if score >= 0.72:
        return "high"
    if score >= 0.50:
        return "medium"
    return "low"


def _with_contract(
    result: dict[str, Any],
    *,
    tool: str,
    args: dict[str, Any],
    row_count: int,
    sample_transaction_ids: list[str],
    caveats: list[str],
    label: str | None,
) -> dict[str, Any]:
    metric_ids = metric_registry.metric_ids_for_tool(tool, args)
    metric_id = metric_ids[0] if metric_ids else None
    contract = {
        "metric_id": metric_id,
        "metric_ids": metric_ids,
        "metric_definition": metric_registry.metric_payload(metric_id),
        "metric_definition_summary": metric_registry.metric_summary(metric_id),
        "row_count": int(row_count or 0),
        "sample_transaction_ids": list(sample_transaction_ids or [])[:8],
        "calculation_basis": metric_registry.metric_summary(metric_id) or "Computed by deterministic Mira cash-flow logic.",
        "data_quality": {"caveats": list(caveats or [])},
        "caveats": list(caveats or []),
        "range": label,
    }
    result.update(contract)
    result["provenance"] = {"tool": tool, "args": {k: v for k, v in args.items() if v not in (None, "", [])}, **contract}
    return result


def _profile_clause(profile: str | None, column: str = "profile_id") -> tuple[str, list[Any]]:
    if profile and profile != "household":
        return f" AND {column} = ?", [profile]
    return "", []


def _json_load(value: str | None) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _parse_date(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _group_amounts(items: list[dict[str, Any]], amount_key: str = "amount") -> dict[str, float]:
    grouped: dict[str, float] = {}
    for item in items:
        day = str(item.get("date") or "")[:10]
        if not day:
            continue
        grouped[day] = grouped.get(day, 0.0) + float(item.get(amount_key) or 0)
    return grouped


def _extract_amount(text: str) -> float | None:
    match = re.search(r"\$\s*([0-9][0-9,]*(?:\.\d{1,2})?)", text or "")
    if not match:
        match = re.search(r"\b([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:dollars|bucks)\b", text or "", re.I)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _category_from_text(text: str, categories: list[str], profile: str | None = None) -> str:
    existing = {str(category).lower(): str(category) for category in categories if str(category or "").strip()}
    try:
        from mira.grounding import ground_category

        result = ground_category(text, categories, profile=profile, limit=4)
        if result.kind in {"exact", "approximate"} and result.value:
            grounded = existing.get(str(result.value).lower())
            if grounded:
                return grounded
            for candidate in result.candidates or []:
                for key in ("value", "display_name", "canonical_id"):
                    grounded = existing.get(str(candidate.get(key) or "").lower())
                    if grounded:
                        return grounded
    except Exception:
        pass
    token_text = " ".join(re.findall(r"[a-z0-9]+", (text or "").lower()))
    if any(token in token_text for token in ("dining", "restaurant", "restaurants", "food")):
        for candidate in ("Food & Dining", "Dining"):
            if candidate.lower() in existing:
                return existing[candidate.lower()]
    if any(token in token_text for token in ("grocery", "groceries", "costco")):
        grounded = existing.get("groceries")
        if grounded:
            return grounded
    for category in categories:
        label = str(category or "")
        if label and label.lower() in token_text:
            return label
    return _category_from_purpose(text)


def _category_from_purpose(purpose: str) -> str:
    text = (purpose or "").lower()
    if any(token in text for token in ("dining", "restaurant", "coffee", "lunch", "dinner")):
        return "Dining"
    if any(token in text for token in ("grocery", "groceries", "costco")):
        return "Groceries"
    if any(token in text for token in ("movie", "game", "concert", "entertainment")):
        return "Entertainment"
    return ""
