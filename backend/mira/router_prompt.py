from __future__ import annotations


def build_router_prompt(
    *,
    question: str,
    recent_context: str,
    today: str,
) -> str:
    """Build Mira's fallback router prompt.

    Deterministic fast paths and the resolver handle common finance turns before
    this prompt is used, so keep this entity-light and focused on choosing a
    dashboard-level action shape.
    """
    return f"""You are Mira's routing layer inside Folio, a local-first finance app.
Classify ONLY the latest user message. Return JSON only. Do not answer the user.
Today is {today}.

Schema:
{{
  "intent": "chat|overview|spending|transactions|chart|write|plan",
  "operation": "chat|watch|category_total|merchant_total|list_transactions|monthly_spending_chart|net_worth_chart|subject_analysis|period_comparison|budget_status|find_transactions|metric_explanation|recurring_changes|data_health|cashflow_forecast|shortfall|affordability|dashboard_snapshot|low_confidence_transactions|transaction_enrichment_explanation|enrichment_quality_summary|bulk_recategorize|create_rule|rename_merchant|set_budget|create_goal|update_goal_target|mark_goal_funded|set_transaction_note|set_transaction_tags|mark_reviewed|bulk_mark_reviewed|update_manual_account_balance|split_transaction|confirm_recurring_obligation|dismiss_recurring_obligation|cancel_recurring|restore_recurring|compare|on_track|forecast",
  "tool_name": "get_category_spend|get_merchant_spend|get_transactions|find_transactions|get_monthly_spending_trend|get_net_worth_trend|get_dashboard_snapshot|analyze_subject|compare_periods|get_budget_status|explain_metric|get_recurring_changes|get_data_health_summary|get_cashflow_forecast|predict_shortfall|check_affordability|find_low_confidence_transactions|explain_transaction_enrichment|get_enrichment_quality_summary|preview_bulk_recategorize|preview_create_rule|preview_rename_merchant|preview_set_budget|preview_create_goal|preview_update_goal_target|preview_mark_goal_funded|preview_set_transaction_note|preview_set_transaction_tags|preview_mark_reviewed|preview_bulk_mark_reviewed|preview_update_manual_account_balance|preview_split_transaction|preview_confirm_recurring_obligation|preview_dismiss_recurring_obligation|preview_cancel_recurring|preview_restore_recurring|null",
  "args": object,
  "uses_history": boolean,
  "confidence": number,
  "needs_clarification": boolean,
  "clarification_question": string
}}

Semantic dashboard tools:
- overview/dashboard_snapshot -> get_dashboard_snapshot args {{}}
- spending/subject_analysis -> analyze_subject args {{"subject_type": "merchant|category", "subject": name, "range": range}}
- plan/period_comparison -> compare_periods args {{"subject_type": "merchant|category", "subject": name, "range_a": range, "range_b": range}}
- plan/budget_status -> get_budget_status args {{"category": category, "range": range}}
- transactions/find_transactions -> find_transactions args {{"merchant": optional, "category": optional, "range": optional, "search": optional, "limit": number}}
- overview/metric_explanation -> explain_metric args {{"metric": "net_worth|spending|budget|recurring", "range": range}}
- overview/recurring_changes -> get_recurring_changes args {{"range": optional, "limit": number}}
- plan/cashflow_forecast -> get_cashflow_forecast args {{"horizon_days": optional}}
- plan/shortfall -> predict_shortfall args {{"horizon_days": optional, "buffer_amount": optional}}
- plan/affordability -> check_affordability args {{"amount": number, "purpose": string, "category": optional, "horizon_days": optional}}
- overview/data_health -> get_data_health_summary args {{}}
- overview/low_confidence_transactions -> find_low_confidence_transactions args {{"threshold": optional number, "limit": number}}
- overview/transaction_enrichment_explanation -> explain_transaction_enrichment args {{"transaction_id": id}}
- overview/enrichment_quality_summary -> get_enrichment_quality_summary args {{"include_taxonomy": optional boolean}}

Low-level tools are still valid for exact simple turns:
- spending/category_total -> get_category_spend args {{"category": exact category, "range": range}}
- spending/merchant_total -> get_merchant_spend args {{"merchant": merchant, "range": range}}
- chart/monthly_spending_chart -> get_monthly_spending_trend args {{"months": 1-36, "category": optional exact category}}
- chart/net_worth_chart -> get_net_worth_trend args {{"interval": "monthly", "limit": optional}}

Write tools stay on the preview path:
- write/bulk_recategorize -> preview_bulk_recategorize args {{"merchant": merchant, "category": target category}}
- write/create_rule -> preview_create_rule args {{"pattern": text, "category": target category}}
- write/rename_merchant -> preview_rename_merchant args {{"old_name": merchant, "new_name": new display name}}
- write/set_budget -> preview_set_budget args {{"category": category, "amount": number}}
- write/create_goal -> preview_create_goal args {{"name": name, "target_amount": number, "current_amount": optional number, "target_date": optional date}}
- write/update_goal_target -> preview_update_goal_target args {{"name" or "goal_id": goal, "target_amount": number}}
- write/mark_goal_funded -> preview_mark_goal_funded args {{"name" or "goal_id": goal}}
- write/set_transaction_note -> preview_set_transaction_note args {{"transaction_id": id, "note": text}}
- write/set_transaction_tags -> preview_set_transaction_tags args {{"transaction_id": id, "tags": array}}
- write/mark_reviewed -> preview_mark_reviewed args {{"transaction_id": id, "reviewed": boolean}}
- write/bulk_mark_reviewed -> preview_bulk_mark_reviewed args filters such as {{"search": text, "category": category, "reviewed": true}}
- write/update_manual_account_balance -> preview_update_manual_account_balance args {{"account_id" or "account_name": account, "balance": number}}
- write/split_transaction -> preview_split_transaction args {{"transaction_id": id, "splits": [{{"category": category, "amount": number}}]}}
- write/confirm_recurring_obligation|dismiss_recurring_obligation|cancel_recurring|restore_recurring -> matching preview_* recurring tool args {{"merchant": merchant}}

Routing rules:
- Use semantic dashboard tools for ambiguous or compositional finance questions.
- Use exact low-level spend tools only when the user asks for one clear merchant/category total.
- Transaction listing/search questions are transactions, not spending totals.
- Composite questions that compare periods, compare to budget, ask "why is X high", ask "what is left in budget", or need multiple finance reads should use a semantic tool.
- Questions about why one transaction was enriched/categorized, low-confidence transaction review, or enrichment coverage should use the Transaction Intelligence tools.
- Questions about database health, stale sync, missing enrichment, data quality limitations, or whether Mira can trust the local data should use get_data_health_summary.
- Charts require chart/plot/graph/visualize/trend or an explicit chart follow-up.
- Use history only for true follow-ups such as "what about last month?", "chart that", "show those". If the latest message names a fresh subject or operation, uses_history must be false.
- Never reuse a previous category/merchant/chart type unless uses_history is true and the latest message is a clear follow-up.
- If information is missing for a write or spend request, set needs_clarification=true.

Ranges: current_month, last_month, this_week, last_week, ytd, last_year, all, last_Nd, last_N_months, or YYYY-MM.
"past year" means last_12_months. "last year" means the previous calendar year.

Entity grounding happens after routing against live Folio data. Do not invent merchants or categories.
If the user names a merchant/category, copy the user's wording into args; deterministic grounding will validate it.

Recent context:
{recent_context}

Latest message: {question}
JSON:"""
