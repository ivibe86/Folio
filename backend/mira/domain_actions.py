from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from mira.grounding import ground_category, ground_merchant, normalize_text


DOMAIN_ACTION_NAMES = {
    "SpendTotal",
    "TransactionSearch",
    "MonthlyTrend",
    "NetWorthTrend",
    "CompareSpend",
    "BudgetStatus",
    "CashFlowForecast",
    "Affordability",
    "WritePreview",
    "ExplainLastAnswer",
    "TransactionEnrichment",
    "OverviewSummary",
    "Memory",
    "GeneralChat",
}
DOMAIN_ACTION_STATUSES = {"ready", "clarify", "unsupported"}

WRITE_PREVIEW_TOOLS = {
    "preview_bulk_recategorize",
    "preview_create_rule",
    "preview_rename_merchant",
    "preview_set_budget",
    "preview_create_goal",
    "preview_update_goal_target",
    "preview_mark_goal_funded",
    "preview_set_transaction_note",
    "preview_set_transaction_tags",
    "preview_mark_reviewed",
    "preview_bulk_mark_reviewed",
    "preview_update_manual_account_balance",
    "preview_split_transaction",
    "preview_confirm_recurring_obligation",
    "preview_dismiss_recurring_obligation",
    "preview_cancel_recurring",
    "preview_restore_recurring",
}

ACTION_TOOL_ALLOWLIST = {
    "SpendTotal": {"get_category_spend", "get_merchant_spend"},
    "TransactionSearch": {"get_transactions", "get_transactions_for_merchant", "find_transactions"},
    "MonthlyTrend": {"get_monthly_spending_trend", "plot_chart"},
    "NetWorthTrend": {"get_net_worth_trend", "plot_chart"},
    "CompareSpend": {"get_category_spend", "get_merchant_spend", "compare_periods", "analyze_subject"},
    "BudgetStatus": {"get_category_spend", "get_merchant_spend", "get_budget_status"},
    "CashFlowForecast": {"get_cashflow_forecast", "predict_shortfall"},
    "Affordability": {"check_affordability"},
    "WritePreview": WRITE_PREVIEW_TOOLS,
    "ExplainLastAnswer": set(),
    "TransactionEnrichment": {
        "find_low_confidence_transactions",
        "explain_transaction_enrichment",
        "get_enrichment_quality_summary",
    },
    "OverviewSummary": {"get_dashboard_snapshot", "explain_metric", "get_recurring_changes", "get_data_health_summary", "get_recurring_summary", "get_top_merchants", "get_top_categories"},
    "Memory": {"remember_user_context", "retrieve_relevant_memories", "update_memory", "forget_memory", "list_mira_memories"},
    "GeneralChat": set(),
}


@dataclass(frozen=True)
class DomainAction:
    name: str
    status: str = "ready"
    slots: dict[str, Any] = field(default_factory=dict)
    tool_plan: list[dict[str, Any]] = field(default_factory=list)
    grounded_entities: list[dict[str, Any]] = field(default_factory=list)
    validated_slots: dict[str, Any] = field(default_factory=dict)
    controller_act: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    clarification_question: str = ""

    def as_dict(self) -> dict[str, Any]:
        name = self.name if self.name in DOMAIN_ACTION_NAMES else "GeneralChat"
        status = self.status if self.status in DOMAIN_ACTION_STATUSES else "unsupported"
        return {
            "name": name,
            "status": status,
            "slots": copy.deepcopy(self.slots or {}),
            "tool_plan": _allowed_tool_plan(name, self.tool_plan),
            "grounded_entities": copy.deepcopy(self.grounded_entities or []),
            "validated_slots": copy.deepcopy(self.validated_slots or {}),
            "controller_act": copy.deepcopy(self.controller_act or {}),
            "reason": str(self.reason or ""),
            "clarification_question": str(self.clarification_question or ""),
        }


def annotate_route(route: dict[str, Any], profile: str | None = None) -> dict[str, Any]:
    route["domain_action"] = domain_action_for_route(route, profile).as_dict()
    return route


def domain_action_for_route(route: dict[str, Any] | None, profile: str | None = None) -> DomainAction:
    route = route or {}
    controller_act = route.get("controller_act") if isinstance(route.get("controller_act"), dict) else {}
    if route.get("needs_clarification"):
        return _clarification_action(route, controller_act)

    operation = str(route.get("operation") or "").strip()
    intent = str(route.get("intent") or "").strip()
    tool_name = str(route.get("tool_name") or "").strip()

    if tool_name == "analyze_subject":
        return _analyze_subject_action(route, controller_act, profile)
    if tool_name == "compare_periods":
        return _semantic_compare_action(route, controller_act, profile)
    if tool_name == "get_budget_status":
        return _semantic_budget_action(route, controller_act, profile)
    if tool_name == "find_transactions":
        return _transaction_search_action(route, controller_act, profile)
    if tool_name in {"get_dashboard_snapshot", "explain_metric", "get_recurring_changes", "get_data_health_summary"}:
        return _overview_action(route, controller_act)
    if tool_name in {"get_cashflow_forecast", "predict_shortfall"}:
        return _cashflow_action(route, controller_act)
    if tool_name == "check_affordability":
        return _affordability_action(route, controller_act)
    if tool_name in {"find_low_confidence_transactions", "explain_transaction_enrichment", "get_enrichment_quality_summary"}:
        return _transaction_enrichment_action(route, controller_act)
    if tool_name in {"remember_user_context", "retrieve_relevant_memories", "update_memory", "forget_memory", "list_mira_memories"} or intent == "memory":
        return _memory_action(route, controller_act)

    if operation == "explain_grounding" or controller_act.get("act") == "explain_provenance":
        return _simple_action("ExplainLastAnswer", route, controller_act)
    if operation in {"context_acknowledge", "local_only_provider", "dialogue_cancelled"} or intent == "chat":
        return _simple_action("GeneralChat", route, controller_act)
    if intent == "overview":
        return _overview_action(route, controller_act)
    if intent == "write":
        return _write_preview_action(route, controller_act, profile)
    if intent == "plan":
        if str((route.get("args") or {}).get("plan_kind") or route.get("operation") or "") == "on_track":
            return _compare_action("BudgetStatus", route, controller_act, profile)
        return _compare_action("CompareSpend", route, controller_act, profile)
    if intent == "chart" or tool_name in {"get_monthly_spending_trend", "get_net_worth_trend"}:
        if tool_name == "get_net_worth_trend" or operation == "net_worth_chart":
            return _net_worth_trend_action(route, controller_act)
        return _monthly_trend_action(route, controller_act, profile)
    if intent in {"spending", "drilldown"} or tool_name in {"get_category_spend", "get_merchant_spend"}:
        return _spend_total_action(route, controller_act, profile)
    if intent == "transactions" or tool_name in {"get_transactions", "get_transactions_for_merchant"}:
        return _transaction_search_action(route, controller_act, profile)
    return _simple_action("GeneralChat", route, controller_act)


def tool_names_for_action(action: dict[str, Any] | DomainAction | None) -> list[str]:
    if isinstance(action, DomainAction):
        action = action.as_dict()
    if not isinstance(action, dict) or action.get("status") != "ready":
        return []
    names: list[str] = []
    for step in action.get("tool_plan") or []:
        name = str((step or {}).get("name") or "")
        if name and name != "run_sql" and name not in names:
            names.append(name)
    return names


def tool_plan_for_route(route: dict[str, Any] | None, action_names: set[str] | None = None) -> list[dict[str, Any]]:
    action = (route or {}).get("domain_action") if isinstance(route, dict) else None
    if not isinstance(action, dict) or action.get("status") != "ready":
        return []
    if action_names and action.get("name") not in action_names:
        return []
    return _allowed_tool_plan(str(action.get("name") or ""), action.get("tool_plan") or [])


def _simple_action(name: str, route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    return DomainAction(
        name=name,
        slots=_base_slots(route),
        controller_act=controller_act,
        reason=str(route.get("shortcut") or route.get("operation") or name),
    )


def _clarification_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    name = _clarification_action_name(route)
    return DomainAction(
        name=name,
        status="clarify",
        slots=_base_slots(route),
        controller_act=controller_act,
        reason=str(route.get("shortcut") or route.get("operation") or "needs clarification"),
        clarification_question=str(route.get("clarification_question") or "I need one more detail to answer that cleanly."),
    )


def _clarification_action_name(route: dict[str, Any]) -> str:
    state = route.get("dialogue_state") if isinstance(route.get("dialogue_state"), dict) else {}
    pending_action = str(state.get("pending_action") or state.get("action") or "").strip()
    if pending_action == "transactions":
        return "TransactionSearch"
    if pending_action == "plan":
        return "CompareSpend"
    if pending_action in {"spending", "spend", "spend_total"}:
        return "SpendTotal"
    operation = str(route.get("operation") or route.get("shortcut") or "").lower()
    if "transaction" in operation:
        return "TransactionSearch"
    if "compare" in operation or "comparison" in operation or "plan" in operation:
        return "CompareSpend"
    if "budget" in operation:
        return "BudgetStatus"
    if "merchant" in operation or "category" in operation or "spend" in operation or "subject" in operation:
        return "SpendTotal"
    if route.get("intent") == "write":
        return "WritePreview"
    return "GeneralChat"


def _overview_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    tool_name = str(route.get("tool_name") or "")
    args = _copy_args(route)
    if tool_name in {"get_dashboard_snapshot", "explain_metric", "get_recurring_changes", "get_data_health_summary"}:
        tool_plan = [{"name": tool_name, "args": args}]
    else:
        tool_plan = [{"name": "get_dashboard_snapshot", "args": {}}]
    return DomainAction(
        name="OverviewSummary",
        slots=_base_slots(route),
        tool_plan=tool_plan,
        validated_slots={},
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "overview summary"),
    )


def _cashflow_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    tool_name = str(route.get("tool_name") or "")
    args = _copy_args(route)
    if tool_name not in ACTION_TOOL_ALLOWLIST["CashFlowForecast"]:
        return _unsupported("CashFlowForecast", route, controller_act, "cash-flow action needs a forecast tool")
    return DomainAction(
        name="CashFlowForecast",
        slots=_base_slots(route),
        tool_plan=[{"name": tool_name, "args": args}],
        validated_slots=args,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "cash-flow forecast"),
    )


def _affordability_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    args = _copy_args(route)
    if not args.get("amount"):
        return DomainAction(
            name="Affordability",
            status="clarify",
            slots=_base_slots(route),
            controller_act=controller_act,
            reason="missing proposed amount",
            clarification_question="How much are you thinking of spending?",
        )
    return DomainAction(
        name="Affordability",
        slots=_base_slots(route),
        tool_plan=[{"name": "check_affordability", "args": args}],
        validated_slots=args,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "affordability"),
    )


def _transaction_enrichment_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    tool_name = str(route.get("tool_name") or "")
    args = _copy_args(route)
    allowed = ACTION_TOOL_ALLOWLIST["TransactionEnrichment"]
    if tool_name not in allowed:
        return _unsupported("TransactionEnrichment", route, controller_act, "transaction enrichment action needs an enrichment tool")
    if tool_name == "explain_transaction_enrichment" and not str(args.get("transaction_id") or args.get("tx_id") or "").strip():
        return DomainAction(
            name="TransactionEnrichment",
            status="clarify",
            slots=_base_slots(route),
            controller_act=controller_act,
            reason="missing transaction id",
            clarification_question="Which transaction ID should I explain?",
        )
    return DomainAction(
        name="TransactionEnrichment",
        slots=_base_slots(route),
        tool_plan=[{"name": tool_name, "args": args}],
        validated_slots=args,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "transaction enrichment"),
    )


def _memory_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    tool_name = str(route.get("tool_name") or "")
    args = _copy_args(route)
    allowed = ACTION_TOOL_ALLOWLIST["Memory"]
    if tool_name not in allowed:
        return _unsupported("Memory", route, controller_act, "memory action needs a memory tool")
    return DomainAction(
        name="Memory",
        slots=_base_slots(route),
        tool_plan=[{"name": tool_name, "args": args}],
        validated_slots=args,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "memory v2"),
    )


def _analyze_subject_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    subject_type = str(args.get("subject_type") or "").strip()
    subject = str(args.get("subject") or args.get(subject_type) or "").strip()
    if subject_type not in {"merchant", "category"}:
        return _clarify_missing_subject("CompareSpend", route, controller_act, "merchant or category")
    if not subject:
        return _clarify_missing_subject("CompareSpend", route, controller_act, subject_type)
    entity = _entity_record(subject_type, subject, profile)
    if not entity.get("grounded") and not route.get("uses_history"):
        return _clarify_missing_subject("CompareSpend", route, controller_act, subject_type, subject)
    validated = {
        "subject_type": subject_type,
        "subject": entity.get("display_name") or subject,
        "range": args.get("range") or "current_month",
    }
    return DomainAction(
        name="CompareSpend",
        slots={**_base_slots(route), **validated},
        tool_plan=[{"name": "analyze_subject", "args": validated}],
        grounded_entities=[entity],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "subject analysis"),
    )


def _semantic_compare_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    subject_type = str(args.get("subject_type") or "").strip()
    subject = str(args.get("subject") or args.get(subject_type) or "").strip()
    if subject_type not in {"merchant", "category"}:
        return _clarify_missing_subject("CompareSpend", route, controller_act, "merchant or category")
    if not subject:
        return _clarify_missing_subject("CompareSpend", route, controller_act, subject_type)
    entity = _entity_record(subject_type, subject, profile)
    if not entity.get("grounded") and not route.get("uses_history"):
        return _clarify_missing_subject("CompareSpend", route, controller_act, subject_type, subject)
    validated = {
        "subject_type": subject_type,
        "subject": entity.get("display_name") or subject,
        "range_a": args.get("range_a") or args.get("range") or "current_month",
        "range_b": args.get("range_b") or "last_month",
    }
    return DomainAction(
        name="CompareSpend",
        slots={**_base_slots(route), **validated},
        tool_plan=[{"name": "compare_periods", "args": validated}],
        grounded_entities=[entity],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "period comparison"),
    )


def _semantic_budget_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    category = str(args.get("category") or args.get("subject") or "").strip()
    if not category:
        return _clarify_missing_subject("BudgetStatus", route, controller_act, "category")
    entity = _entity_record("category", category, profile)
    if not entity.get("grounded") and not route.get("uses_history"):
        return _clarify_missing_subject("BudgetStatus", route, controller_act, "category", category)
    validated = {
        "category": entity.get("display_name") or category,
        "range": args.get("range") or "current_month",
    }
    return DomainAction(
        name="BudgetStatus",
        slots={**_base_slots(route), **validated},
        tool_plan=[{"name": "get_budget_status", "args": validated}],
        grounded_entities=[entity],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "budget status"),
    )


def _spend_total_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    tool_name = str(route.get("tool_name") or "")
    if tool_name not in {"get_category_spend", "get_merchant_spend"}:
        return _unsupported("SpendTotal", route, controller_act, "spend action needs a spend tool")
    subject_type = "category" if tool_name == "get_category_spend" else "merchant"
    subject_key = subject_type
    subject = str(args.get(subject_key) or "").strip()
    if not subject:
        return _clarify_missing_subject("SpendTotal", route, controller_act, subject_type)
    entity = _entity_record(subject_type, subject, profile)
    if not entity.get("grounded") and not route.get("uses_history"):
        return _clarify_missing_subject("SpendTotal", route, controller_act, subject_type, subject)
    slots = {**_base_slots(route), "subject_type": subject_type, "subject": subject, "range": args.get("range") or "current_month"}
    validated = {subject_key: entity.get("display_name") or subject, "range": slots["range"]}
    tool_plan = [{"name": tool_name, "args": validated}]
    return DomainAction(
        name="SpendTotal",
        slots=slots,
        tool_plan=tool_plan,
        grounded_entities=[entity],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "spend total"),
    )


def _transaction_search_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    tool_name = str(route.get("tool_name") or "")
    if tool_name not in {"get_transactions", "get_transactions_for_merchant", "find_transactions"}:
        return _unsupported("TransactionSearch", route, controller_act, "transaction action needs a transaction tool")

    grounded: list[dict[str, Any]] = []
    validated = dict(args)
    if tool_name in {"get_transactions_for_merchant", "find_transactions"} and args.get("merchant"):
        merchant = str(args.get("merchant") or "").strip()
        if not merchant:
            return _clarify_missing_subject("TransactionSearch", route, controller_act, "merchant")
        entity = _entity_record("merchant", merchant, profile)
        if not entity.get("grounded") and not route.get("uses_history"):
            return _clarify_missing_subject("TransactionSearch", route, controller_act, "merchant", merchant)
        grounded.append(entity)
        validated["merchant"] = entity.get("display_name") or merchant
    elif args.get("category"):
        category = str(args.get("category") or "").strip()
        entity = _entity_record("category", category, profile)
        if not entity.get("grounded") and not route.get("uses_history"):
            return _clarify_missing_subject("TransactionSearch", route, controller_act, "category", category)
        grounded.append(entity)
        validated["category"] = entity.get("display_name") or category

    slots = {**_base_slots(route), "limit": validated.get("limit"), "range": validated.get("range")}
    return DomainAction(
        name="TransactionSearch",
        slots=slots,
        tool_plan=[{"name": "find_transactions" if tool_name == "find_transactions" else tool_name, "args": validated}],
        grounded_entities=grounded,
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "transaction search"),
    )


def _monthly_trend_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    try:
        months = max(1, min(int(args.get("months") or 6), 36))
    except (TypeError, ValueError):
        months = 6
    validated: dict[str, Any] = {"months": months}
    grounded: list[dict[str, Any]] = []
    category = str(args.get("category") or "").strip()
    if category:
        entity = _entity_record("category", category, profile)
        if not entity.get("grounded") and not route.get("uses_history"):
            return _clarify_missing_subject("MonthlyTrend", route, controller_act, "category", category)
        grounded.append(entity)
        validated["category"] = entity.get("display_name") or category
    return DomainAction(
        name="MonthlyTrend",
        slots={**_base_slots(route), **validated},
        tool_plan=[{"name": "get_monthly_spending_trend", "args": validated}],
        grounded_entities=grounded,
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "monthly trend"),
    )


def _net_worth_trend_action(route: dict[str, Any], controller_act: dict[str, Any]) -> DomainAction:
    args = _copy_args(route)
    validated = {
        "interval": args.get("interval") or "monthly",
        "limit": args.get("limit") or 24,
    }
    if args.get("range"):
        validated["range"] = args.get("range")
    return DomainAction(
        name="NetWorthTrend",
        slots={**_base_slots(route), **validated},
        tool_plan=[{"name": "get_net_worth_trend", "args": validated}],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or "net worth trend"),
    )


def _compare_action(name: str, route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    args = _copy_args(route)
    subject_type = str(args.get("subject_type") or "").strip()
    subject = str(args.get("subject") or "").strip()
    if subject_type not in {"merchant", "category"}:
        return _clarify_missing_subject(name, route, controller_act, "merchant or category")
    if not subject:
        return _clarify_missing_subject(name, route, controller_act, subject_type)
    entity = _entity_record(subject_type, subject, profile)
    if not entity.get("grounded") and not route.get("uses_history"):
        return _clarify_missing_subject(name, route, controller_act, subject_type, subject)
    plan_kind = str(args.get("plan_kind") or route.get("operation") or "").strip()
    tool_plan = _compare_tool_plan(subject_type, entity.get("display_name") or subject, plan_kind, args.get("months"))
    if not tool_plan:
        return _unsupported(name, route, controller_act, "comparison action needs a supported comparison kind")
    validated = {
        "plan_kind": plan_kind,
        "subject_type": subject_type,
        "subject": entity.get("display_name") or subject,
        "months": _coerce_months(args.get("months"), fallback=6),
    }
    return DomainAction(
        name=name,
        slots={**_base_slots(route), **validated},
        tool_plan=tool_plan,
        grounded_entities=[entity],
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or plan_kind or name),
    )


def _write_preview_action(route: dict[str, Any], controller_act: dict[str, Any], profile: str | None) -> DomainAction:
    tool_name = str(route.get("tool_name") or "")
    args = _copy_args(route)
    if tool_name not in WRITE_PREVIEW_TOOLS:
        return _unsupported("WritePreview", route, controller_act, "write action needs a preview tool")
    grounded: list[dict[str, Any]] = []
    validated = dict(args)
    category = str(args.get("category") or args.get("linked_category") or "").strip()
    if category:
        entity = _entity_record("category", category, profile)
        if not entity.get("grounded"):
            return _clarify_missing_subject("WritePreview", route, controller_act, "category", category)
        grounded.append(entity)
        if args.get("category"):
            validated["category"] = entity.get("display_name") or category
        if args.get("linked_category"):
            validated["linked_category"] = entity.get("display_name") or category

    for key in ("merchant", "old_name"):
        merchant = str(args.get(key) or "").strip()
        if not merchant:
            continue
        entity = _entity_record("merchant", merchant, profile)
        if not entity.get("grounded"):
            return _clarify_missing_subject("WritePreview", route, controller_act, "merchant", merchant)
        grounded.append(entity)
        validated[key] = entity.get("display_name") or merchant

    return DomainAction(
        name="WritePreview",
        slots=_base_slots(route),
        tool_plan=[{"name": tool_name, "args": validated}],
        grounded_entities=grounded,
        validated_slots=validated,
        controller_act=controller_act,
        reason=str(route.get("shortcut") or route.get("operation") or "write preview"),
    )


def _compare_tool_plan(subject_type: str, subject: str, plan_kind: str, months_value: Any) -> list[dict[str, Any]]:
    tool = "get_category_spend" if subject_type == "category" else "get_merchant_spend"
    key = "category" if subject_type == "category" else "merchant"
    months = _coerce_months(months_value, fallback=6)

    def step(range_token: str) -> dict[str, Any]:
        return {"name": tool, "args": {key: subject, "range": range_token}}

    if plan_kind == "current_vs_previous":
        return [{"name": "compare_periods", "args": {"subject_type": subject_type, "subject": subject, "range_a": "current_month", "range_b": "last_month"}}]
    if plan_kind == "on_track" and subject_type == "category":
        return [{"name": "get_budget_status", "args": {"category": subject, "range": "current_month"}}]
    if plan_kind in {"current_vs_average", "on_track"}:
        return [step("current_month"), step(f"last_{months + 1}_months")]
    return []


def _entity_record(entity_type: str, value: str, profile: str | None) -> dict[str, Any]:
    value = str(value or "").strip()
    if not value:
        return {"entity_type": entity_type, "value": None, "grounded": False, "kind": "missing"}
    try:
        if entity_type == "merchant":
            result = ground_merchant(value, profile=profile, include_transaction_evidence=True, limit=3)
        else:
            result = ground_category(value, profile=profile, limit=3)
    except Exception:
        result = None
    if result and result.value:
        same = normalize_text(result.value) == normalize_text(value) or normalize_text(result.display_name or "") == normalize_text(value)
        grounded = result.kind == "exact" and same
        return {
            "entity_type": entity_type,
            "value": result.value,
            "canonical_id": result.canonical_id,
            "display_name": result.display_name or result.value,
            "kind": result.kind,
            "confidence": result.confidence,
            "grounded": grounded,
            "source": "resolver",
            "candidates": result.candidates[:3],
        }
    return {
        "entity_type": entity_type,
        "value": value,
        "canonical_id": value,
        "display_name": value,
        "kind": "missing",
        "confidence": 0.0,
        "grounded": False,
        "source": "resolver",
        "candidates": [],
    }


def _clarify_missing_subject(
    action_name: str,
    route: dict[str, Any],
    controller_act: dict[str, Any],
    subject_type: str,
    subject: str = "",
) -> DomainAction:
    if subject:
        question = f"I couldn't confidently match `{subject}` to a {subject_type} in your data. Which {subject_type} should I use?"
    else:
        question = f"Which {subject_type} should I use?"
    return DomainAction(
        name=action_name,
        status="clarify",
        slots=_base_slots(route),
        controller_act=controller_act,
        reason="missing grounded subject",
        clarification_question=question,
    )


def _unsupported(action_name: str, route: dict[str, Any], controller_act: dict[str, Any], reason: str) -> DomainAction:
    return DomainAction(
        name=action_name,
        status="unsupported",
        slots=_base_slots(route),
        controller_act=controller_act,
        reason=reason,
    )


def _allowed_tool_plan(action_name: str, tool_plan: list[dict[str, Any]] | Any) -> list[dict[str, Any]]:
    if not isinstance(tool_plan, list):
        return []
    allowed = ACTION_TOOL_ALLOWLIST.get(action_name, set())
    cleaned: list[dict[str, Any]] = []
    for step in tool_plan:
        if not isinstance(step, dict):
            continue
        name = str(step.get("name") or "").strip()
        if not name or name == "run_sql" or name not in allowed:
            continue
        args = step.get("args") if isinstance(step.get("args"), dict) else {}
        cleaned.append({"name": name, "args": copy.deepcopy(args)})
    return cleaned


def _base_slots(route: dict[str, Any]) -> dict[str, Any]:
    return {
        "intent": route.get("intent"),
        "operation": route.get("operation"),
        "tool_name": route.get("tool_name"),
        "args": _copy_args(route),
        "uses_history": bool(route.get("uses_history")),
    }


def _copy_args(route: dict[str, Any]) -> dict[str, Any]:
    args = route.get("args") if isinstance(route.get("args"), dict) else {}
    return copy.deepcopy(args)


def _coerce_months(value: Any, *, fallback: int) -> int:
    try:
        return max(1, min(int(value or fallback), 12))
    except (TypeError, ValueError):
        return fallback
