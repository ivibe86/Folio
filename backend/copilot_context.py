"""
Live orientation block prepended to Copilot's system prompt.

Shows the top-5 categories and both current-month + all-time top merchants,
along with recurring totals, policy counts, and saved insights. The agent is
instructed elsewhere that these are truncated previews and must call tools
for anything not listed.
"""

from __future__ import annotations

import logging
from datetime import datetime

from data_manager import (
    get_merchant_insights_data,
    get_monthly_analytics_data,
    get_recurring_from_db,
)
import memory

logger = logging.getLogger(__name__)

TOP_N = 5


def _money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _current_month() -> str:
    return datetime.now().strftime("%Y-%m")


def _monthly_snapshot(profile: str | None, conn) -> str:
    try:
        months = get_monthly_analytics_data(profile=profile, conn=conn) or []
    except Exception:
        logger.debug("monthly snapshot failed", exc_info=True)
        return ""
    if not months:
        return ""
    current = months[-1]
    previous = months[-2] if len(months) >= 2 else {}
    lines = [f"Month {current.get('month')}:"]
    lines.append(
        f"  income {_money(current.get('income'))}, "
        f"expenses {_money(current.get('expenses'))}, "
        f"net {_money(current.get('net'))}"
    )
    if previous:
        lines.append(
            f"  prior {previous.get('month')}: "
            f"income {_money(previous.get('income'))}, "
            f"expenses {_money(previous.get('expenses'))}, "
            f"net {_money(previous.get('net'))}"
        )
    return "\n".join(lines)


def _top_categories_current_month(profile: str | None, conn) -> str:
    try:
        month = _current_month()
        rows = conn.execute(
            """
            SELECT category, SUM(ABS(amount)) AS total
            FROM transactions_visible
            WHERE amount < 0
              AND is_excluded = 0
              AND category IS NOT NULL
              AND category != ''
              AND category NOT IN ('Savings Transfer','Credit Card Payment','Income','Personal Transfer')
              AND date LIKE ?
              AND (? IS NULL OR profile_id = ?)
            GROUP BY category
            ORDER BY total DESC
            LIMIT ?
            """,
            (f"{month}%", profile if profile and profile != "household" else None,
             profile if profile and profile != "household" else None, TOP_N),
        ).fetchall()
    except Exception:
        logger.debug("category snapshot failed", exc_info=True)
        return ""
    if not rows:
        return ""
    items = ", ".join(f"{r['category']} {_money(r['total'])}" for r in rows)
    return f"Top {TOP_N} categories this month (more via get_top_categories / get_category_spend): {items}"


def _top_merchants_current_month(profile: str | None, conn) -> str:
    try:
        month = _current_month()
        rows = conn.execute(
            """
            SELECT COALESCE(NULLIF(merchant_name, ''), description) AS name,
                   SUM(ABS(amount)) AS total
            FROM transactions_visible
            WHERE amount < 0
              AND is_excluded = 0
              AND category NOT IN ('Savings Transfer','Credit Card Payment','Income','Personal Transfer')
              AND date LIKE ?
              AND (? IS NULL OR profile_id = ?)
            GROUP BY name
            HAVING name IS NOT NULL AND name != ''
            ORDER BY total DESC
            LIMIT ?
            """,
            (f"{month}%", profile if profile and profile != "household" else None,
             profile if profile and profile != "household" else None, TOP_N),
        ).fetchall()
    except Exception:
        logger.debug("monthly merchant snapshot failed", exc_info=True)
        return ""
    if not rows:
        return ""
    items = ", ".join(f"{r['name']} {_money(r['total'])}" for r in rows)
    return f"Top {TOP_N} merchants THIS MONTH (more via get_top_merchants / get_merchant_spend): {items}"


def _top_merchants_all_time(profile: str | None, conn) -> str:
    try:
        merchants = get_merchant_insights_data(profile=profile, conn=conn) or []
    except Exception:
        logger.debug("all-time merchant snapshot failed", exc_info=True)
        return ""
    rows = merchants[:TOP_N]
    if not rows:
        return ""
    items = ", ".join(f"{m.get('name')} {_money(m.get('total_spent'))}" for m in rows)
    return f"Top {TOP_N} merchants ALL-TIME: {items}"


def _recurring_summary(profile: str | None, conn) -> str:
    try:
        data = get_recurring_from_db(profile=profile, conn=conn) or {}
    except Exception:
        logger.debug("recurring snapshot failed", exc_info=True)
        return ""
    active_count = data.get("active_count") or 0
    if not active_count:
        return ""
    total = data.get("total_monthly") or 0
    return f"Active recurring: {active_count} items, ~{_money(total)}/mo"


def _policy_summary(conn) -> str:
    try:
        rule_count = conn.execute("SELECT COUNT(*) FROM category_rules").fetchone()[0]
        budget_count = conn.execute("SELECT COUNT(*) FROM category_budgets").fetchone()[0]
    except Exception:
        logger.debug("policy snapshot failed", exc_info=True)
        return ""
    parts = []
    if rule_count:
        parts.append(f"{rule_count} category rules")
    if budget_count:
        parts.append(f"{budget_count} budgets")
    return f"Policy: {', '.join(parts)}" if parts else ""


def _persistent_memory(profile: str | None, conn) -> str:
    """
    Inject the persistent personal memory file into the system prompt.
    This is the user's about_user.md — identity, stated preferences, goals,
    recurring concerns, and open questions accumulated across conversations.

    The agent must treat these as ground truth about who the user is, what
    they've committed to, and how they want to be talked to. Inferred entries
    are tagged so the agent knows they're observations, not declarations.
    """
    try:
        body = memory.render_markdown(profile, conn)
    except Exception:
        logger.debug("memory render failed", exc_info=True)
        return ""
    if not body.strip():
        return ""
    return (
        "Persistent memory about the user (read-only here — DO NOT restate verbatim, "
        "use as background to shape your reply):\n"
        f"{body.rstrip()}"
    )


def _saved_insights(profile: str | None, conn, limit: int = 10) -> str:
    try:
        rows = conn.execute(
            """
            SELECT question, answer, kind, pinned
            FROM saved_insights
            WHERE (? IS NULL OR profile_id = ? OR profile_id IS NULL)
            ORDER BY pinned DESC, created_at DESC
            LIMIT ?
            """,
            (profile, profile, limit),
        ).fetchall()
    except Exception:
        logger.debug("insights lookup failed", exc_info=True)
        return ""
    if not rows:
        return ""
    lines = []
    for r in rows:
        tag = "[pinned]" if r["pinned"] else f"[{r['kind']}]"
        lines.append(f"- {tag} Q: {r['question']}  A: {r['answer']}")
    return "Saved insights (more via search_saved_insights):\n" + "\n".join(lines)


def build_copilot_context(profile: str | None, conn) -> str:
    sections = [
        _persistent_memory(profile, conn),
        _monthly_snapshot(profile, conn),
        _top_categories_current_month(profile, conn),
        _top_merchants_current_month(profile, conn),
        _top_merchants_all_time(profile, conn),
        _recurring_summary(profile, conn),
        _policy_summary(conn),
        _saved_insights(profile, conn),
    ]
    body = "\n".join(s for s in sections if s)
    if not body:
        return ""
    header = f"Live orientation (profile={profile or 'household'}) — truncated to top {TOP_N}:"
    return f"{header}\n{body}"
