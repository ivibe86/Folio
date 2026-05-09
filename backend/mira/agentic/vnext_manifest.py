from __future__ import annotations

from typing import Any

from mira.agentic.semantic_catalog import SEMANTIC_TOOL_FAMILIES, semantic_tools_for_selector


LEGACY_TOOL_FAMILIES = (
    (
        "spend_summary",
        "spend totals, top merchants/categories, summaries, priorities, comparisons",
        (
            "get_month_summary",
            "get_period_summary",
            "get_top_categories",
            "get_top_merchants",
            "get_finance_priorities",
            "get_category_spend",
            "get_merchant_spend",
            "get_summary",
            "get_category_breakdown",
            "analyze_subject",
            "compare_periods",
        ),
    ),
    (
        "transactions",
        "transaction search, rows, enrichment, edit previews",
        (
            "get_transactions_for_merchant",
            "get_transactions",
            "find_transactions",
            "find_low_confidence_transactions",
            "explain_transaction_enrichment",
            "preview_set_transaction_note",
            "preview_set_transaction_tags",
            "preview_split_transaction",
        ),
    ),
    (
        "net_worth_dashboard",
        "net worth, balances, dashboard data",
        (
            "get_net_worth_trend",
            "get_account_balances",
            "get_dashboard_bundle",
            "get_dashboard_snapshot",
            "get_net_worth_delta",
        ),
    ),
    (
        "budget_cashflow",
        "budgets, savings, cashflow forecasts, shortfall risk, affordability",
        (
            "get_budget_status",
            "get_budget_plan_summary",
            "get_savings_capacity",
            "get_cashflow_forecast",
            "predict_shortfall",
            "check_affordability",
            "preview_set_budget",
        ),
    ),
    (
        "recurring",
        "recurring charges and obligation previews",
        (
            "get_recurring_summary",
            "get_recurring_changes",
            "preview_confirm_recurring_obligation",
            "preview_dismiss_recurring_obligation",
            "preview_cancel_recurring",
            "preview_restore_recurring",
        ),
    ),
    (
        "memory_rules",
        "memories, insights, category rules",
        (
            "get_category_rules",
            "search_saved_insights",
            "remember_user_context",
            "retrieve_relevant_memories",
            "update_memory",
            "forget_memory",
            "list_mira_memories",
        ),
    ),
    (
        "write_previews",
        "safe previews for edits, goals, balances",
        (
            "preview_bulk_recategorize",
            "preview_create_rule",
            "preview_rename_merchant",
            "preview_create_goal",
            "preview_update_goal_target",
            "preview_mark_goal_funded",
            "preview_mark_reviewed",
            "preview_bulk_mark_reviewed",
            "preview_update_manual_account_balance",
        ),
    ),
    (
        "data_health",
        "data and enrichment quality",
        (
            "get_enrichment_quality_summary",
            "get_data_health_summary",
        ),
    ),
    (
        "charts",
        "trends and charts",
        (
            "get_monthly_spending_trend",
            "plot_chart",
        ),
    ),
    (
        "metric_explain",
        "metric definitions",
        (
            "explain_metric",
        ),
    ),
)

TOOL_FAMILIES = SEMANTIC_TOOL_FAMILIES


def all_tool_schemas() -> list[dict[str, Any]]:
    return semantic_tools_for_selector()


def tools_by_name(tools: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for tool in tools:
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else {}
        name = str(fn.get("name") or "").strip()
        if name and name != "run_sql":
            out[name] = tool
    return out


def build_grouped_tool_manifest(tools: list[dict[str, Any]]) -> str:
    by_name = tools_by_name(tools)
    used: set[str] = set()
    lines = []
    families = _families_for_tools(by_name)
    for family, description, names in families:
        present = [name for name in names if name in by_name]
        if not present:
            continue
        used.update(present)
        lines.append(f"- {family}: {description}. Tools: {', '.join(present)}")

    leftovers = [
        name
        for name in by_name
        if name not in used
    ]
    if leftovers:
        lines.append(f"- other: remaining Folio tools. Tools: {', '.join(leftovers)}")
    return "\n".join(lines)


def build_tool_manifest(tools: list[dict[str, Any]], *, max_description_chars: int = 140) -> str:
    lines = []
    for tool in tools:
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else {}
        name = str(fn.get("name") or "").strip()
        if not name or name == "run_sql":
            continue
        description = first_sentence(str(fn.get("description") or ""), max_chars=max_description_chars)
        lines.append(f"- {name}: {description}")
    return "\n".join(lines)


def family_tool_names(family_name: str, tools: list[dict[str, Any]]) -> list[str]:
    by_name = tools_by_name(tools)
    family_name = str(family_name or "").strip()
    for family, _description, names in _families_for_tools(by_name):
        if family == family_name:
            return [name for name in names if name in by_name]
    return list(by_name)


def build_family_detail_manifest(
    tools: list[dict[str, Any]],
    family_name: str,
    *,
    max_description_chars: int = 96,
) -> str:
    by_name = tools_by_name(tools)
    selected = [by_name[name] for name in family_tool_names(family_name, tools) if name in by_name]
    return build_tool_manifest(selected, max_description_chars=max_description_chars)


def selected_family_name(value: Any) -> str:
    family = str(value or "").strip()
    known = {name for name, _description, _tools in TOOL_FAMILIES + LEGACY_TOOL_FAMILIES}
    return family if family in known else ""


def selector_manifest_coverage(tools: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = tools_by_name(tools)
    families = _families_for_tools(by_name)
    grouped = {name for _family, _description, names in families for name in names}
    return {
        "tool_count": len(by_name),
        "family_count": len(families),
        "tools_not_in_named_families": [name for name in by_name if name not in grouped],
    }


def _families_for_tools(by_name: dict[str, dict[str, Any]]) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    semantic_names = {name for _family, _description, names in TOOL_FAMILIES for name in names}
    if set(by_name) & semantic_names:
        return TOOL_FAMILIES
    return LEGACY_TOOL_FAMILIES


def first_sentence(text: str, *, max_chars: int) -> str:
    compact = " ".join(str(text or "").split())
    if not compact:
        return ""
    period = compact.find(". ")
    if 0 < period < max_chars:
        compact = compact[:period + 1]
    if len(compact) <= max_chars:
        return compact
    trimmed = compact[:max_chars].rsplit(" ", 1)[0].rstrip()
    return trimmed + "..."


__all__ = [
    "TOOL_FAMILIES",
    "all_tool_schemas",
    "build_family_detail_manifest",
    "build_grouped_tool_manifest",
    "build_tool_manifest",
    "family_tool_names",
    "first_sentence",
    "selected_family_name",
    "selector_manifest_coverage",
    "tools_by_name",
]
