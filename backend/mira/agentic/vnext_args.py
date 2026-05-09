from __future__ import annotations

from typing import Any

from mira.agentic.semantic_catalog import is_semantic_tool
from mira.agentic.semantic_tool_adapter import normalize_semantic_selector_args, semantic_selector_shape_error


UNIVERSAL_ARG_FIELDS = (
    "tool",
    "view",
    "range",
    "range_a",
    "range_b",
    "filters",
    "payload",
    "limit",
    "offset",
    "sort",
    "context_action",
    "range_source",
)


def adapt_universal_args(tool_name: str, call: dict[str, Any], tool_schema: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if is_semantic_tool(tool_name):
        args = normalize_semantic_selector_args(tool_name, call)
        return args, semantic_selector_shape_error(tool_name, args)

    fn = tool_schema.get("function") if isinstance(tool_schema.get("function"), dict) else {}
    params = fn.get("parameters") if isinstance(fn.get("parameters"), dict) else {}
    properties = params.get("properties") if isinstance(params.get("properties"), dict) else {}
    required = [str(item) for item in params.get("required") or []]
    payload = call.get("payload") if isinstance(call.get("payload"), dict) else {}
    source = {**payload, **{key: value for key, value in call.items() if key != "payload"}}

    args: dict[str, Any] = {}
    for prop in properties:
        value = value_for_property(prop, source)
        if value not in (None, "", [], {}):
            args[prop] = value

    if tool_name == "plot_chart" and source.get("source_step_id"):
        args["source_step_id"] = source.get("source_step_id")
        if source.get("chart_type") and not args.get("type"):
            args["type"] = source.get("chart_type")
        required = [name for name in required if name not in {"labels", "values", "series"}]

    missing = [name for name in required if args.get(name) in (None, "", [], {})]
    if missing:
        return args, f"missing required args for {tool_name}: {', '.join(missing)}"
    return args, ""


def value_for_property(prop: str, source: dict[str, Any]) -> Any:
    if prop in source:
        return source[prop]

    subject_type = str(source.get("subject_type") or "").lower()
    subject = source.get("subject")
    if prop == "merchant":
        return source.get("merchant") or source.get("merchant_name") or (subject if subject_type == "merchant" else None) or subject
    if prop == "category":
        return source.get("category") or (subject if subject_type == "category" else None)
    if prop == "account":
        return source.get("account") or source.get("account_name")
    if prop == "account_name":
        return source.get("account_name") or source.get("account")
    if prop == "range":
        return source.get("range")
    if prop == "range_a":
        return source.get("range_a")
    if prop == "range_b":
        return source.get("range_b")
    if prop == "month":
        return range_to_month(source.get("month") or source.get("range"))
    if prop == "subject_type":
        if subject_type:
            return subject_type
        if source.get("merchant"):
            return "merchant"
        if source.get("category"):
            return "category"
        return None
    if prop == "subject":
        return subject or source.get("merchant") or source.get("category") or source.get("account")
    if prop in {
        "limit",
        "amount",
        "metric",
        "focus",
        "purpose",
        "pattern",
        "old_name",
        "new_name",
        "transaction_id",
        "action",
        "source_step_id",
        "chart_type",
        "horizon_days",
        "buffer_amount",
        "interval",
        "months",
        "offset",
        "status",
        "all",
        "search",
        "threshold",
        "include_taxonomy",
        "question",
        "text",
        "memory_type",
        "topic",
        "source_summary",
        "source_turn_id",
        "pinned",
        "expires_at",
        "memory_id",
        "normalized_text",
        "sensitivity",
        "confidence",
        "include_expired",
        "include_inactive",
        "force",
        "keywords",
        "note",
        "tags",
        "reviewed",
        "current_reviewed",
        "start_date",
        "end_date",
        "balance",
        "notes",
        "goal_id",
        "name",
        "goal_type",
        "target_amount",
        "current_amount",
        "target_date",
        "linked_category",
        "linked_account_id",
        "rollover_mode",
        "rollover_balance",
        "frequency",
        "splits",
        "type",
        "title",
        "labels",
        "values",
        "series",
        "annotations",
        "series_name",
        "unit",
    }:
        return source.get(prop)
    return None


def range_to_month(value: Any) -> Any:
    text = str(value or "").strip()
    if text == "current_month":
        return "current"
    if text in {"last_month", "prior_month"}:
        return "prior"
    if len(text) == 7 and text[4] == "-":
        return text
    return None


__all__ = [
    "UNIVERSAL_ARG_FIELDS",
    "adapt_universal_args",
    "range_to_month",
    "value_for_property",
]
