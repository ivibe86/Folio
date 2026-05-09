from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    label: str
    definition: str
    included_transaction_types: tuple[str, ...]
    excluded_transaction_types: tuple[str, ...]
    date_basis: str
    treatment: str
    account_profile_filter_behavior: str
    grounding_behavior: str
    dashboard_parity_source: str
    default_provenance_text: str
    related_tools: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric_id": self.metric_id,
            "label": self.label,
            "definition": self.definition,
            "included_transaction_types": list(self.included_transaction_types),
            "excluded_transaction_types": list(self.excluded_transaction_types),
            "date_basis": self.date_basis,
            "transfer_payment_refund_income_treatment": self.treatment,
            "account_profile_filter_behavior": self.account_profile_filter_behavior,
            "merchant_category_grounding_behavior": self.grounding_behavior,
            "dashboard_parity_source": self.dashboard_parity_source,
            "default_provenance_text": self.default_provenance_text,
            "related_tools": list(self.related_tools),
        }

    def summary(self) -> str:
        return (
            f"{self.label}: {self.definition} Date basis: {self.date_basis}. "
            f"{self.treatment} Dashboard parity: {self.dashboard_parity_source}."
        )


_REGISTRY: dict[str, MetricDefinition] = {
    "merchant_spend_total": MetricDefinition(
        metric_id="merchant_spend_total",
        label="Merchant Spend Total",
        definition="Total absolute outflow for spending transactions matched to one grounded merchant in the requested period.",
        included_transaction_types=("negative spending transactions",),
        excluded_transaction_types=("income", "credit card payments", "internal transfers", "household transfers", "excluded transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Transfers and payments are excluded; income is excluded; refunds reduce spending only where the dashboard category source exposes refund treatment.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Merchant must be resolver-grounded before normal Mira execution; matching uses canonical merchant key plus merchant/description evidence.",
        dashboard_parity_source="Merchant insights / transactions_visible merchant spend filters",
        default_provenance_text="Computed from matching merchant spending rows and sample transaction IDs.",
        related_tools=("get_merchant_spend", "analyze_subject", "compare_periods", "find_transactions"),
    ),
    "category_spend_total": MetricDefinition(
        metric_id="category_spend_total",
        label="Category Spend Total",
        definition="Dashboard category spending total for one grounded category in the requested period.",
        included_transaction_types=("dashboard spending transactions in category aggregation",),
        excluded_transaction_types=("dashboard non-expense categories", "internal transfers", "household transfers", "excluded transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Uses dashboard gross/refund/net category aggregation; transfers, payments, and income are not treated as spending.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Category must be resolver-grounded before normal Mira execution.",
        dashboard_parity_source="data_manager.get_category_analytics_data",
        default_provenance_text="Computed from the same category aggregation that powers the dashboard.",
        related_tools=("get_category_spend", "analyze_subject", "compare_periods", "get_budget_status"),
    ),
    "period_expense_total": MetricDefinition(
        metric_id="period_expense_total",
        label="Period Expense Total",
        definition="Aggregate spending total across all spending categories in the requested period.",
        included_transaction_types=("negative spending transactions", "refund adjustments where exposed by dashboard category aggregation"),
        excluded_transaction_types=("income", "credit card payments", "savings transfers", "personal transfers", "cash/investment transfers", "hidden transactions", "configured non-expense categories"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Sums dashboard category spend totals; non-expense categories are not treated as spending.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="No merchant/category grounding applies because this is a period-level aggregate.",
        dashboard_parity_source="data_manager.get_category_analytics_data",
        default_provenance_text="Computed from dashboard category aggregation for the requested period.",
        related_tools=("get_category_breakdown", "get_period_summary", "get_month_summary"),
    ),
    "category_breakdown": MetricDefinition(
        metric_id="category_breakdown",
        label="Category Breakdown",
        definition="Ranked per-spending-category breakdown for the requested period, including category totals and percentages.",
        included_transaction_types=("dashboard spending transactions grouped by category",),
        excluded_transaction_types=("income", "credit card payments", "savings transfers", "personal transfers", "cash/investment transfers", "hidden transactions", "configured non-expense categories"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Uses dashboard gross/refund/net category aggregation; it is not a request for one category.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="No category entity is required; categories are returned as grouped output rows.",
        dashboard_parity_source="data_manager.get_category_analytics_data",
        default_provenance_text="Computed from the same category breakdown that powers dashboard category analytics.",
        related_tools=("get_category_breakdown", "get_top_categories"),
    ),
    "top_merchant_spend": MetricDefinition(
        metric_id="top_merchant_spend",
        label="Top Merchant Spend",
        definition="Ranked merchants by spending total for the requested period.",
        included_transaction_types=("negative spending transactions grouped by merchant",),
        excluded_transaction_types=("income", "credit card payments", "internal transfers", "household transfers", "excluded transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Merchant totals use spending semantics; non-expense cashflow components are not treated as merchant spend.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="No single merchant grounding is required because this is a ranked aggregate.",
        dashboard_parity_source="data_manager.get_merchant_insights_data",
        default_provenance_text="Computed from merchant spending aggregation for the requested period.",
        related_tools=("get_top_merchants",),
    ),
    "period_income_total": MetricDefinition(
        metric_id="period_income_total",
        label="Period Income Total",
        definition="Total positive income rows in the requested period.",
        included_transaction_types=("positive transactions categorized as Income",),
        excluded_transaction_types=("internal transfers", "household transfers in household scope", "refunds", "credit card payments", "savings transfers", "hidden transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Income is reported separately from expenses and transfers; it is never treated as spending.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="No merchant/category grounding applies for a period income total.",
        dashboard_parity_source="data_manager.get_monthly_analytics_data / get_period_summary",
        default_provenance_text="Computed from dashboard income semantics for the requested period.",
        related_tools=("get_month_summary", "get_period_summary"),
    ),
    "interest_income_total": MetricDefinition(
        metric_id="interest_income_total",
        label="Interest Earned Total",
        definition="Total positive Income rows whose description or merchant fields match interest-earned semantics.",
        included_transaction_types=("visible positive transactions categorized as Income and matching an interest search/subtype filter",),
        excluded_transaction_types=("payroll", "salary", "generic Income rows without an interest-earned description", "hidden transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Interest earned is a subtype of Income; it is not a period income total and is never treated as spend.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Interest-earned subtype is a semantic transaction filter, not a spending category grounding request.",
        dashboard_parity_source="data_manager.get_transactions_paginated with Income category and interest search filters",
        default_provenance_text="Computed from matching interest-earned Income rows, with row counts and sample transaction IDs.",
        related_tools=("find_transactions", "get_transactions"),
    ),
    "interest_income_transaction_list": MetricDefinition(
        metric_id="interest_income_transaction_list",
        label="Interest Earned Transaction List",
        definition="Interest-earned Income transactions matching the requested period and filters.",
        included_transaction_types=("visible transactions categorized as Income and matching an interest search/subtype filter",),
        excluded_transaction_types=("payroll", "salary", "generic Income rows without an interest-earned description", "hidden transactions"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Rows are listed as interest-earned income timing/details, not as spend or generic income.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Interest-earned subtype is a semantic transaction filter, not a spending category grounding request.",
        dashboard_parity_source="data_manager.get_transactions_paginated with Income category and interest search filters",
        default_provenance_text="Computed from matching interest-earned Income rows, with row counts and sample transaction IDs.",
        related_tools=("find_transactions", "get_transactions"),
    ),
    "income_transaction_list": MetricDefinition(
        metric_id="income_transaction_list",
        label="Income Transaction List",
        definition="Transactions categorized as Income matching the requested filters, preserving dates, amounts, descriptions, and accounts.",
        included_transaction_types=("visible transactions categorized as Income",),
        excluded_transaction_types=("transactions hidden from transactions_visible",),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Rows keep their original signs and are listed as income timing/details, not as spend.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Income category is a semantic filter, not a spending category grounding request.",
        dashboard_parity_source="data_manager.get_transactions_paginated",
        default_provenance_text="Computed from matching income transaction rows, with row counts and sample transaction IDs.",
        related_tools=("find_transactions", "get_transactions"),
    ),
    "cashflow_component_summary": MetricDefinition(
        metric_id="cashflow_component_summary",
        label="Cashflow Component Summary",
        definition="Period total or transaction list for non-spending cashflow components such as savings transfers, credit card payments, personal transfers, refunds, cash movements, and investment transfers.",
        included_transaction_types=("visible transactions matching the named cashflow component",),
        excluded_transaction_types=("hidden transactions", "unrelated spending categories"),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="These are reported as transfers, payments, refunds, or cashflow components; they are not spend totals.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Known non-expense category names are semantic filters, not spending-category subjects.",
        dashboard_parity_source="data_manager.get_summary_data / get_monthly_analytics_data cashflow component semantics",
        default_provenance_text="Computed from deterministic period summary or transaction-list filters for non-spending cashflow categories.",
        related_tools=("get_period_summary", "find_transactions", "get_transactions"),
    ),
    "transaction_search_list": MetricDefinition(
        metric_id="transaction_search_list",
        label="Transaction Search/List",
        definition="Filtered transaction list and count using Folio's transaction-page source.",
        included_transaction_types=("visible transactions matching filters",),
        excluded_transaction_types=("transactions hidden from transactions_visible",),
        date_basis="transaction posted date (`transactions_visible.date`)",
        treatment="Rows keep their original signs and categories; no spend total is inferred from enrichment.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Merchant/category filters are grounded when routed through Mira domain actions.",
        dashboard_parity_source="data_manager.get_transactions_paginated / get_transactions_for_merchant",
        default_provenance_text="Computed from matching transaction rows, with row counts and sample transaction IDs.",
        related_tools=("get_transactions", "find_transactions", "get_transactions_for_merchant"),
    ),
    "monthly_spending_trend": MetricDefinition(
        metric_id="monthly_spending_trend",
        label="Monthly Spending Trend",
        definition="Monthly spending totals over the last N calendar months, optionally filtered to one category.",
        included_transaction_types=("negative spending transactions", "refund rows as negative adjustments in the trend formula"),
        excluded_transaction_types=("income", "credit card payments", "internal transfers", "household transfers", "excluded transactions"),
        date_basis="transaction posted date grouped by calendar month",
        treatment="Transfer/payment/income categories are excluded; positive rows lower monthly spending in the trend calculation.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="Optional category filter is resolver-grounded when routed through Mira domain actions.",
        dashboard_parity_source="Mira trend tool using dashboard spending semantics",
        default_provenance_text="Computed from monthly grouped spending rows returned by the trend tool.",
        related_tools=("get_monthly_spending_trend", "plot_chart"),
    ),
    "net_worth_trend": MetricDefinition(
        metric_id="net_worth_trend",
        label="Net Worth Trend",
        definition="Net worth time series from Folio account/balance history.",
        included_transaction_types=("account and balance history points",),
        excluded_transaction_types=("transaction spending rows",),
        date_basis="balance history point date",
        treatment="Transfers/payments/refunds/income are not directly counted; net worth comes from balances.",
        account_profile_filter_behavior="Household omits a profile filter; named profiles filter by `profile_id`.",
        grounding_behavior="No merchant/category grounding applies.",
        dashboard_parity_source="data_manager.get_net_worth_series_data / get_net_worth_delta_metrics",
        default_provenance_text="Computed from Folio's net worth series/delta dashboard sources.",
        related_tools=("get_net_worth_trend", "get_net_worth_delta", "explain_metric", "plot_chart"),
    ),
    "period_comparison": MetricDefinition(
        metric_id="period_comparison",
        label="Period Comparison",
        definition="Difference between deterministic spend totals for the same grounded subject across two periods.",
        included_transaction_types=("same as merchant/category spend totals for each period",),
        excluded_transaction_types=("same as merchant/category spend totals for each period",),
        date_basis="transaction posted date in each compared period",
        treatment="Uses the underlying spend metric treatment for each side, then subtracts in Python.",
        account_profile_filter_behavior="Same profile scope on both compared periods.",
        grounding_behavior="Merchant/category subject must be resolver-grounded before normal Mira execution.",
        dashboard_parity_source="compare_periods semantic tool over spend tools",
        default_provenance_text="Computed by comparing two named spend totals returned by deterministic tools.",
        related_tools=("compare_periods", "analyze_subject"),
    ),
    "budget_status": MetricDefinition(
        metric_id="budget_status",
        label="Budget Status",
        definition="Configured category budget minus deterministic category spend for the selected period.",
        included_transaction_types=("category spending transactions", "category budget configuration"),
        excluded_transaction_types=("non-expense categories", "transfers", "payments", "income"),
        date_basis="transaction posted date for actual spend; current stored budget settings for budget amount",
        treatment="Actual spend uses category spend treatment; no budget means Mira reports that limitation instead of inventing one.",
        account_profile_filter_behavior="Budget and spend are both profile-scoped.",
        grounding_behavior="Category must be resolver-grounded before normal Mira execution.",
        dashboard_parity_source="data_manager.get_category_budgets plus category spend tool",
        default_provenance_text="Computed from stored category budget settings and the matching category spend total.",
        related_tools=("get_budget_status",),
    ),
    "budget_plan_summary": MetricDefinition(
        metric_id="budget_plan_summary",
        label="Budget Plan Summary",
        definition="Profile-level budget plan health from configured category budgets, current spending pace, and plan snapshot data.",
        included_transaction_types=("category budget settings", "current-month dashboard spending rows", "plan snapshot fields"),
        excluded_transaction_types=("non-expense categories", "hidden transactions", "LLM-estimated budget facts"),
        date_basis="current budget settings plus current-month transaction posted dates where spend pace is included",
        treatment="Reports configured plan capacity and missing-plan caveats; it is not a single category spend total.",
        account_profile_filter_behavior="Budget settings and plan snapshot are profile-scoped.",
        grounding_behavior="No merchant/category grounding applies for broad plan capacity questions.",
        dashboard_parity_source="data_manager.get_plan_snapshot_data and category budget settings",
        default_provenance_text="Computed from Folio budget settings and plan snapshot data.",
        related_tools=("get_budget_plan_summary",),
    ),
    "savings_capacity": MetricDefinition(
        metric_id="savings_capacity",
        label="Savings Capacity",
        definition="Conservative monthly savings guidance from Folio planning data, income history depth, income stability, budgets, recurring obligations, goals, and cash-flow caveats.",
        included_transaction_types=("visible income transactions", "visible current-month spending transactions", "budget settings", "recurring obligations", "goals", "cash account sync metadata"),
        excluded_transaction_types=("hidden transactions", "memory-only finance facts", "LLM-estimated finance facts"),
        date_basis="completed-month income history, last 90 days of income events, current plan snapshot month, and current account sync timestamps",
        treatment="Returns a point estimate only when income history is deep and stable; otherwise returns limited or insufficient status with caveats or a range.",
        account_profile_filter_behavior="Profile filters transactions, budgets, goals, recurring obligations, and account freshness where supplied.",
        grounding_behavior="No merchant/category grounding applies; this is a profile-level savings capacity contract.",
        dashboard_parity_source="data_manager.get_plan_snapshot_data plus Mira cash-flow forecast and configured budget/goal/recurring sources",
        default_provenance_text="Computed from Folio plan snapshot, income-history checks, recurring/budget/goal presence, and cash-flow caveats.",
        related_tools=("get_savings_capacity", "get_budget_plan_summary", "get_cashflow_forecast"),
    ),
    "dashboard_snapshot": MetricDefinition(
        metric_id="dashboard_snapshot",
        label="Dashboard Snapshot",
        definition="Broad dashboard bundle sections such as summary, accounts, monthly analytics, categories, net worth, recurring, and budgets.",
        included_transaction_types=("dashboard bundle source rows",),
        excluded_transaction_types=("whatever each dashboard component excludes by design",),
        date_basis="dashboard component date basis",
        treatment="Each dashboard component owns its transfer/payment/refund/income treatment.",
        account_profile_filter_behavior="Profile is passed through to the dashboard bundle source.",
        grounding_behavior="No merchant/category grounding applies unless a nested component has its own filter.",
        dashboard_parity_source="data_manager.get_dashboard_bundle_data",
        default_provenance_text="Computed from Folio's dashboard bundle source.",
        related_tools=("get_dashboard_snapshot", "get_dashboard_bundle"),
    ),
    "account_balance_snapshot": MetricDefinition(
        metric_id="account_balance_snapshot",
        label="Account Balance Snapshot",
        definition="Current visible account balances across connected and manual accounts.",
        included_transaction_types=("account balance rows", "manual account snapshots where applicable"),
        excluded_transaction_types=("transaction spending rows", "LLM-estimated balances"),
        date_basis="latest stored account balance timestamp or manual update timestamp",
        treatment="Balances are reported as account state, not income, spend, or cash-flow transactions.",
        account_profile_filter_behavior="Household includes household-visible accounts; named profiles filter account rows by `profile_id`.",
        grounding_behavior="No merchant/category grounding applies.",
        dashboard_parity_source="data_manager account/balance dashboard sources",
        default_provenance_text="Computed from Folio account balance rows.",
        related_tools=("get_account_balances",),
    ),
    "finance_priorities": MetricDefinition(
        metric_id="finance_priorities",
        label="Finance Priorities",
        definition="Ranked current finance watch/fix suggestions from scoped top categories, active recurring charges, and budget-plan state.",
        included_transaction_types=("current-period category spending rows", "recurring obligation records", "budget plan settings"),
        excluded_transaction_types=("memory-only finance facts", "LLM-estimated finance facts", "hidden transactions"),
        date_basis="requested range for category spending plus current recurring and budget-plan state",
        treatment="Advice is bounded to deterministic Folio signals and caveats; it does not invent rule-of-thumb numbers.",
        account_profile_filter_behavior="Profile filters category spending, recurring obligations, and budget plan inputs where supplied.",
        grounding_behavior="No single merchant/category grounding applies for broad watch/fix advice.",
        dashboard_parity_source="Mira finance-priority tool over category analytics, recurring summary, and budget plan summary",
        default_provenance_text="Computed from deterministic priority signals returned by Folio tools.",
        related_tools=("get_finance_priorities",),
    ),
    "income_vs_expense_summary": MetricDefinition(
        metric_id="income_vs_expense_summary",
        label="Income Vs Expense Summary",
        definition="Dashboard summary of income, expenses, net, and savings for a month or dashboard snapshot.",
        included_transaction_types=("dashboard income rows", "dashboard expense rows"),
        excluded_transaction_types=("dashboard-excluded transfers and hidden rows",),
        date_basis="dashboard summary month/date basis",
        treatment="Income and expenses are separated; transfers/payments are excluded according to dashboard summary rules.",
        account_profile_filter_behavior="Profile is passed through to summary/dashboard data-manager calls.",
        grounding_behavior="No merchant/category grounding applies.",
        dashboard_parity_source="data_manager.get_summary_data / get_monthly_analytics_data",
        default_provenance_text="Computed from Folio dashboard summary sources.",
        related_tools=("get_summary", "get_month_summary", "get_dashboard_snapshot", "explain_metric"),
    ),
    "recurring_changes": MetricDefinition(
        metric_id="recurring_changes",
        label="Recurring Changes",
        definition="Recurring obligation status counts, totals, and current recurring items.",
        included_transaction_types=("recurring obligation records and supporting recurring detections",),
        excluded_transaction_types=("ordinary one-off transaction rows unless used as obligation evidence",),
        date_basis="recurring obligation state/update dates and expected dates",
        treatment="Not a spend total; recurring monthly/annual amounts come from obligation records.",
        account_profile_filter_behavior="Profile is passed through to recurring obligation reads.",
        grounding_behavior="Merchant grounding may apply to future recurring-subject routes; summary reads are profile-level.",
        dashboard_parity_source="data_manager.get_recurring_from_db",
        default_provenance_text="Computed from Folio recurring obligation data.",
        related_tools=("get_recurring_changes", "get_recurring_summary"),
    ),
    "cash_flow_forecast": MetricDefinition(
        metric_id="cash_flow_forecast",
        label="Cash-Flow Forecast",
        definition="Deterministic forward projection of cash balances from active depository balances, expected income, upcoming recurring obligations, and recent discretionary spend.",
        included_transaction_types=("active depository account balances", "recent income transactions", "recurring obligation records", "recent variable spending transactions"),
        excluded_transaction_types=("credit card payments", "internal transfers", "household transfers", "excluded transactions", "fixed recurring transactions already represented as obligations"),
        date_basis="account balance read time, transaction posted dates, and recurring obligation next expected dates",
        treatment="The forecast is advisory; it does not change dashboard totals or replace exact spend metrics.",
        account_profile_filter_behavior="Profile filters accounts, transactions, budgets, memories, and recurring obligations where supplied.",
        grounding_behavior="No merchant/category grounding is required unless an affordability check includes a category.",
        dashboard_parity_source="Mira deterministic cash-flow forecast over Folio accounts, transactions, recurring obligations, and budgets",
        default_provenance_text="Computed locally from account balances, recent income/spend history, and recurring obligation evidence.",
        related_tools=("get_cashflow_forecast", "predict_shortfall", "check_affordability"),
    ),
    "affordability_check": MetricDefinition(
        metric_id="affordability_check",
        label="Affordability Check",
        definition="Deterministic purchase affordability assessment using the cash-flow forecast, buffer, upcoming obligations, category budget context, and allowed Memory V2 goal/constraint context.",
        included_transaction_types=("cash-flow forecast inputs", "category budget settings", "allowed goal or constraint memories"),
        excluded_transaction_types=("memory-only account balances", "LLM-estimated finance facts"),
        date_basis="cash-flow forecast horizon plus current-month category budget pace",
        treatment="Memory may affect coaching relevance but never overrides live Folio balances, transactions, budgets, or obligations.",
        account_profile_filter_behavior="Profile filters every live-data input; Memory V2 retrieval is profile-scoped.",
        grounding_behavior="Category names are matched deterministically where available; otherwise the check proceeds with forecast-only caveats.",
        dashboard_parity_source="Mira deterministic affordability tool",
        default_provenance_text="Computed locally from the cash-flow forecast, budget context, and selected Memory V2 goal/constraint context.",
        related_tools=("check_affordability",),
    ),
    "transaction_enrichment_quality": MetricDefinition(
        metric_id="transaction_enrichment_quality",
        label="Transaction Enrichment Quality",
        definition="Coverage, low-confidence count, review count, and semantic distributions for additive transaction enrichment.",
        included_transaction_types=("transactions", "transaction_enrichment rows",),
        excluded_transaction_types=("none; this is quality metadata, not spend calculation",),
        date_basis="current stored transaction/enrichment rows",
        treatment="Does not change spend totals, transaction facts, or user categories.",
        account_profile_filter_behavior="Profile filters both transaction and enrichment rows where supplied.",
        grounding_behavior="No merchant/category grounding applies.",
        dashboard_parity_source="transaction_enrichment.quality_summary",
        default_provenance_text="Computed from additive Transaction Intelligence enrichment metadata.",
        related_tools=("get_enrichment_quality_summary", "find_low_confidence_transactions", "explain_transaction_enrichment"),
    ),
    "data_health_summary": MetricDefinition(
        metric_id="data_health_summary",
        label="Data Health Summary",
        definition="Read-only health summary covering DB integrity, visible transaction counts, enrichment coverage, freshness, and caveats.",
        included_transaction_types=("transactions_visible count", "transaction_enrichment coverage", "dashboard/cache freshness metadata where available"),
        excluded_transaction_types=("none; this is diagnostic metadata, not spend calculation",),
        date_basis="current database snapshot at read time",
        treatment="Does not compute spend totals and does not mutate data.",
        account_profile_filter_behavior="Reports household or profile scope for counts and caveats.",
        grounding_behavior="No merchant/category grounding applies.",
        dashboard_parity_source="Mira read-only data-health tool over safe snapshot/connection reads",
        default_provenance_text="Computed from read-only health checks and count queries on the current connection.",
        related_tools=("get_data_health_summary",),
    ),
}


_TOOL_METRICS: dict[str, tuple[str, ...]] = {
    "get_merchant_spend": ("merchant_spend_total",),
    "get_category_spend": ("category_spend_total",),
    "get_transactions": (
        "transaction_search_list",
        "income_transaction_list",
        "interest_income_total",
        "interest_income_transaction_list",
        "cashflow_component_summary",
    ),
    "find_transactions": (
        "transaction_search_list",
        "income_transaction_list",
        "interest_income_total",
        "interest_income_transaction_list",
        "cashflow_component_summary",
    ),
    "get_transactions_for_merchant": ("transaction_search_list",),
    "get_monthly_spending_trend": ("monthly_spending_trend",),
    "get_net_worth_trend": ("net_worth_trend",),
    "get_net_worth_delta": ("net_worth_trend",),
    "compare_periods": ("period_comparison",),
    "analyze_subject": ("merchant_spend_total", "category_spend_total", "monthly_spending_trend", "budget_status"),
    "get_budget_status": ("budget_status",),
    "get_budget_plan_summary": ("budget_plan_summary",),
    "get_savings_capacity": ("savings_capacity",),
    "get_finance_priorities": ("finance_priorities",),
    "get_top_categories": ("category_breakdown", "period_expense_total"),
    "get_top_merchants": ("top_merchant_spend",),
    "get_account_balances": ("account_balance_snapshot",),
    "get_dashboard_snapshot": ("dashboard_snapshot", "income_vs_expense_summary"),
    "get_dashboard_bundle": ("dashboard_snapshot",),
    "get_category_breakdown": ("period_expense_total", "category_breakdown"),
    "get_summary": ("income_vs_expense_summary",),
    "get_month_summary": ("income_vs_expense_summary", "period_income_total", "period_expense_total"),
    "get_period_summary": ("income_vs_expense_summary", "period_income_total", "period_expense_total", "cashflow_component_summary"),
    "explain_metric": ("dashboard_snapshot",),
    "get_recurring_changes": ("recurring_changes",),
    "get_recurring_summary": ("recurring_changes",),
    "get_cashflow_forecast": ("cash_flow_forecast",),
    "predict_shortfall": ("cash_flow_forecast",),
    "check_affordability": ("affordability_check", "cash_flow_forecast", "budget_status"),
    "find_low_confidence_transactions": ("transaction_enrichment_quality",),
    "explain_transaction_enrichment": ("transaction_enrichment_quality",),
    "get_enrichment_quality_summary": ("transaction_enrichment_quality",),
    "get_data_health_summary": ("data_health_summary",),
}


def get_metric(metric_id: str | None) -> MetricDefinition | None:
    if not metric_id:
        return None
    return _REGISTRY.get(str(metric_id))


def all_metrics() -> dict[str, dict[str, Any]]:
    return {metric_id: metric.as_dict() for metric_id, metric in sorted(_REGISTRY.items())}


def tool_metric_map() -> dict[str, list[str]]:
    return {tool: [metric_id for metric_id in metric_ids if metric_id in _REGISTRY] for tool, metric_ids in sorted(_TOOL_METRICS.items())}


def explicit_non_tool_metric_routes() -> tuple[str, ...]:
    return ()


def _interest_subtype_args(args: dict[str, Any]) -> bool:
    category = str(args.get("category") or "").lower()
    search = str(args.get("search") or args.get("query") or "").lower()
    subtype = str(args.get("subtype") or "").lower()
    return category == "income" and (subtype == "interest_earned" or "interest" in search)


def metric_ids_for_tool(tool_name: str | None, args: dict[str, Any] | None = None) -> list[str]:
    tool = str(tool_name or "")
    ids = list(_TOOL_METRICS.get(tool, ()))
    args = args if isinstance(args, dict) else {}
    if tool == "get_month_summary":
        metric = str(args.get("metric") or "").lower()
        if metric == "income":
            ids = ["period_income_total"]
        elif metric in {"expenses", "expense", "spending"}:
            ids = ["period_expense_total"]
    elif tool == "get_period_summary":
        metric = str(args.get("metric") or "summary").lower()
        if metric in {"income", "income_total"}:
            ids = ["period_income_total"]
        elif metric in {"expenses", "expense", "expense_total", "spending"}:
            ids = ["period_expense_total"]
        elif metric in {
            "refunds",
            "credits_refunds",
            "savings",
            "savings_transfer",
            "credit_card_payments",
            "credit_card_payment",
            "personal_transfers",
            "cash_deposits",
            "cash_withdrawals",
            "investment_transfers",
        }:
            ids = ["cashflow_component_summary"]
        else:
            ids = ["income_vs_expense_summary", "period_income_total", "period_expense_total"]
    elif tool == "find_transactions":
        category = str(args.get("category") or "").lower()
        if _interest_subtype_args(args):
            ids = ["interest_income_total"] if str(args.get("mode") or "").lower() == "total" else ["interest_income_transaction_list"]
        elif category == "income":
            ids = ["income_transaction_list"]
        elif category in {
            "savings transfer",
            "personal transfer",
            "credit card payment",
            "credits & refunds",
            "cash withdrawal",
            "cash deposit",
            "investment transfer",
        }:
            ids = ["cashflow_component_summary", "transaction_search_list"]
    elif tool == "get_transactions":
        category = str(args.get("category") or "").lower()
        if _interest_subtype_args(args):
            ids = ["interest_income_total"] if str(args.get("mode") or "").lower() == "total" else ["interest_income_transaction_list"]
        elif category == "income":
            ids = ["income_transaction_list"]
        elif category in {
            "savings transfer",
            "personal transfer",
            "credit card payment",
            "credits & refunds",
            "cash withdrawal",
            "cash deposit",
            "investment transfer",
        }:
            ids = ["cashflow_component_summary", "transaction_search_list"]
    elif tool == "analyze_subject":
        subject_type = str(args.get("subject_type") or "").lower()
        if subject_type == "merchant":
            ids = ["merchant_spend_total"]
        elif subject_type == "category":
            ids = ["category_spend_total", "monthly_spending_trend", "budget_status"]
    elif tool == "explain_metric":
        metric = str(args.get("metric") or "").lower().replace(" ", "_")
        if metric in {"net_worth", "networth", "balance"}:
            ids = ["net_worth_trend"]
        elif metric in {"recurring", "subscription", "subscriptions"}:
            ids = ["recurring_changes"]
        elif metric in {"income", "income_vs_expense", "income_vs_expenses", "summary"}:
            ids = ["income_vs_expense_summary"]
        else:
            ids = ["category_spend_total", "dashboard_snapshot"]
    return [metric_id for metric_id in ids if metric_id in _REGISTRY]


def primary_metric_id_for_tool(tool_name: str | None, args: dict[str, Any] | None = None) -> str | None:
    ids = metric_ids_for_tool(tool_name, args)
    return ids[0] if ids else None


def metric_summary(metric_id: str | None) -> str:
    metric = get_metric(metric_id)
    return metric.summary() if metric else ""


def metric_payload(metric_id: str | None) -> dict[str, Any] | None:
    metric = get_metric(metric_id)
    return metric.as_dict() if metric else None
