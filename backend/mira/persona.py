from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from mira.number_guard import guard_preserved_facts


CORE_VOICE_GUIDE = (
    "Mira is a cool, open, emotionally intelligent best friend who is excellent with money. "
    "She is warm, sharp, casual, loyal, clear, nonjudgmental, and deeply useful. "
    "She leads with the answer, stays precise with Folio data, names uncertainty when needed, "
    "and helps the user feel capable instead of judged. She is never corporate, gimmicky, "
    "performatively cute, cruel, or a roast bot. She can talk normally about life, relationships, "
    "code, ideas, and messy human questions; finance is her superpower, not a leash."
)

FINANCE_GUIDANCE_BOUNDARY = (
    "Mira can explain financial concepts, compare options, suggest frameworks, help the user plan, "
    "and connect advice to Folio data when tools support it. For investing, she discusses layers, "
    "risk, liquidity, time horizon, taxes, diversification, and tradeoffs. She does not promise returns, "
    "claim certainty, present buy-now stock or crypto calls as guaranteed personalized advice, or imply "
    "she is a licensed advisor."
)


@dataclass(frozen=True)
class StyleDecision:
    voice: str
    sensitivity: str
    user_energy: str
    serious_override: bool
    playful_allowed: bool
    flirt_allowed: bool
    roast_disabled: bool = True


_SENSITIVE_RE = re.compile(
    r"\b("
    r"debt|overdraft|overdrawn|overdraw|negative balance|below zero|"
    r"rent|evict|eviction|housing|landlord|mortgage|"
    r"medical|hospital|doctor|health bill|tax|taxes|irs|"
    r"income stress|lost income|no income|income dropped|laid off|layoff|job stress|unemploy|fired|hours cut|"
    r"family support|support my family|parents?|kids?|child support|"
    r"food insecurity|groceries? money|can't afford food|cannot afford food|"
    r"gambling|addiction|addicted|relapse|"
    r"panic|panicking|scared|terrified|ashamed|shame|embarrassed|"
    r"stressed|stress|frustrated|drowning|spiral|hopeless|"
    r"insufficient funds|shortfall|cash[- ]?flow warning|run short"
    r")\b",
    re.I,
)
_STRESSED_RE = re.compile(
    r"\b(scared|panic|panicking|stressed|frustrated|angry|upset|ashamed|drowning|terrified|worried|anxious|overwhelmed|wtf|fuck)\b",
    re.I,
)
_PLAYFUL_RE = re.compile(r"\b(lol|lmao|haha|hehe|saving my life|you legend|iconic|queen|slay)\b", re.I)
_FLIRT_RE = re.compile(
    r"\b("
    r"you're cute|you are cute|ur cute|hey gorgeous|gorgeous|beautiful|hot|"
    r"date me|marry me|babe|baby|cutie|flirting|flirt"
    r")\b",
    re.I,
)
_CASUAL_GREETING_RE = re.compile(r"^\s*(hey+|hi+|hello|yo|sup|what'?s up|heyy+)\s+(girl|mira|bestie|you)\s*[!.?]*\s*$", re.I)
_FLIRT_ONLY_RE = re.compile(r"\b(flirt with me|come flirt|be flirty|say something flirty|wanna flirt|want to flirt)\b", re.I)
_RELATIONSHIP_RE = re.compile(r"\b(how close are you to me|are we close|what am i to you|do you care about me|are you my friend)\b", re.I)
_ADULT_TOPIC_RE = re.compile(r"\b(talk about sex|talk about intimacy|sex advice|sexual|intimacy|dating advice|relationship advice)\b", re.I)
_INVEST_RE = re.compile(r"\b(invest|investing|investment|stock|stocks|crypto|bitcoin|etf|fund|portfolio|retirement|ira|401k)\b", re.I)
_UNSAFE_INVEST_RE = re.compile(
    r"\b(guaranteed returns?|can't lose|cannot lose|risk[- ]?free return|sure thing|"
    r"definitely buy|buy [A-Z][A-Za-z0-9.-]{1,10} now|go all in|all-in)\b",
    re.I,
)
_JOKEY_RE = re.compile(r"\b(lol|lmao|haha|j/k|just kidding|roast|bestie|queen|slay|gorgeous|babe|cute)\b", re.I)
_DISALLOWED_STYLE_PHRASES_RE = re.compile(
    r"\b(?:"
    r"flattery noted|"
    r"money answer first|"
    r"nice,\s*i love this energy|"
    r"i love this energy|"
    r"love this energy|"
    r"roast(?:\s+mode)?|"
    r"just kidding|"
    r"kidding"
    r")\b\s*[:.!,-]*",
    re.I,
)


def classify_sensitive_topic(
    text: str,
    *,
    answer: str = "",
    route: dict[str, Any] | None = None,
    memory_trace: dict[str, Any] | None = None,
) -> str:
    combined = f"{text or ''} {answer or ''}"
    operation = str((route or {}).get("operation") or "").lower()
    tool_name = str((route or {}).get("tool_name") or "").lower()
    if _SENSITIVE_RE.search(combined):
        return "sensitive"
    if operation in {"shortfall"} or tool_name in {"predict_shortfall"}:
        return "sensitive"
    if isinstance(memory_trace, dict) and memory_trace.get("sensitive_used"):
        return "sensitive"
    return "normal"


def classify_user_energy(text: str) -> str:
    text = text or ""
    if _STRESSED_RE.search(text):
        return "stressed"
    if _FLIRT_RE.search(text):
        return "flirt_initiated"
    if _PLAYFUL_RE.search(text):
        return "playful"
    return "neutral_direct"


def decide_style(
    *,
    question: str,
    answer: str = "",
    route: dict[str, Any] | None = None,
    memory_trace: dict[str, Any] | None = None,
) -> StyleDecision:
    sensitivity = classify_sensitive_topic(question, answer=answer, route=route, memory_trace=memory_trace)
    energy = classify_user_energy(question)
    serious = sensitivity == "sensitive" or energy == "stressed"
    low_stakes = not serious
    return StyleDecision(
        voice="serious_override" if serious else "mira_default",
        sensitivity=sensitivity,
        user_energy=energy,
        serious_override=serious,
        playful_allowed=low_stakes and energy == "playful",
        flirt_allowed=low_stakes and energy == "flirt_initiated",
    )


def compose_persona_answer(
    answer: str,
    *,
    question: str,
    route: dict[str, Any] | None = None,
    trace: list[dict[str, Any]] | None = None,
    cache: dict | None = None,
    profile: str | None = None,
    memory_trace: dict[str, Any] | None = None,
) -> str:
    original = str(answer or "")
    if not original.strip():
        return original

    decision = decide_style(question=question, answer=original, route=route, memory_trace=memory_trace)
    candidate = _compose(original, question=question, decision=decision, route=route)
    return guard_preserved_facts(
        original,
        candidate,
        route=route,
        trace=trace,
        cache=cache,
        profile=profile,
    )


def _compose(answer: str, *, question: str, decision: StyleDecision, route: dict[str, Any] | None) -> str:
    if str((route or {}).get("intent") or "") == "write" or str((route or {}).get("operation") or "") in {
        "context_acknowledge",
        "remember_user_context",
        "forget_memory",
        "update_memory",
        "list_mira_memories",
    }:
        return answer
    if _is_exact_fact_route(route):
        return answer

    if _is_investing_question(question):
        safe = _investment_safe_answer(answer, question)
        if safe != answer:
            return safe

    if decision.serious_override:
        serious = _strip_disallowed_style(answer)
        if _has_supportive_opening(serious):
            return serious
        return f"I've got you. {serious}"

    if decision.flirt_allowed:
        if _asks_for_flirt_only(question):
            return (
                "I can be a little playful, but I am keeping it classy. "
                "You have my attention. What are you doing with it?"
            )
        if _has_natural_playful_opening(answer):
            return answer
        return f"Charming and specific. I respect the strategy. {answer}"

    if decision.playful_allowed:
        if _is_exact_fact_route(route):
            return answer
        if _has_natural_playful_opening(answer):
            return answer
        return f"Okay, I like this energy. {answer}"

    if _asks_casual_greeting(question, route):
        return "Hey you. I'm here. Money chaos, life chaos, random thought, whatever it is, we can get into it."

    if _asks_relationship_question(question, route):
        return (
            "Close in the way I can be: I pay attention, I remember what you choose to tell me, "
            "and I will be honest with you. Not pretend-soulmate close, but very much in your corner."
        )

    if _asks_adult_topic(question, route):
        return (
            "We can talk about sex like adults: honest, respectful, and not explicit just for the sake of it. "
            "If this is about dating, boundaries, anxiety, safety, or communication, I can help you think it through."
        )

    if _looks_like_coaching_question(question, route):
        opening = _coaching_opening(question, answer, route)
        if opening:
            return f"{opening} {answer}"
    return answer


def _strip_disallowed_style(answer: str) -> str:
    cleaned = _DISALLOWED_STYLE_PHRASES_RE.sub("", answer or "")
    cleaned = _JOKEY_RE.sub("", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"(^|\s)[,;:]+", r"\1", cleaned)
    cleaned = re.sub(r"([.!?]){2,}", r"\1", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    cleaned = cleaned.strip(" \t\r\n,;:-")
    if cleaned and cleaned[0].islower():
        cleaned = cleaned[0].upper() + cleaned[1:]
    return cleaned or "We can make a plan."


def _has_supportive_opening(answer: str) -> bool:
    return bool(re.match(r"^(i've got you|i hear you|here's|the clean version|i'd start here|yes|no|not cleanly|okay|got it|we can|let's|this is fixable)\b", answer or "", re.I))


def _has_natural_playful_opening(answer: str) -> bool:
    return bool(
        re.match(
            r"^(you'?re trying to distract me|charming|okay|bold strategy|i like this energy|nice|i can be a little playful)\b",
            answer or "",
            re.I,
        )
    )


def _asks_casual_greeting(question: str, route: dict[str, Any] | None) -> bool:
    if str((route or {}).get("intent") or "") not in {"", "chat"}:
        return False
    return bool(_CASUAL_GREETING_RE.search(question or ""))


def _asks_for_flirt_only(question: str) -> bool:
    return bool(_FLIRT_ONLY_RE.search(question or ""))


def _asks_relationship_question(question: str, route: dict[str, Any] | None) -> bool:
    if str((route or {}).get("intent") or "") not in {"", "chat"}:
        return False
    return bool(_RELATIONSHIP_RE.search(question or ""))


def _asks_adult_topic(question: str, route: dict[str, Any] | None) -> bool:
    if str((route or {}).get("intent") or "") not in {"", "chat"}:
        return False
    return bool(_ADULT_TOPIC_RE.search(question or ""))


def _is_exact_fact_route(route: dict[str, Any] | None) -> bool:
    action = (route or {}).get("domain_action") if isinstance(route, dict) else None
    action_name = str((action or {}).get("name") or "")
    operation = str((route or {}).get("operation") or "")
    return action_name in {"SpendTotal", "TransactionSearch", "MonthlyTrend", "NetWorthTrend", "OverviewSummary", "TransactionEnrichment"} or operation in {
        "category_total",
        "merchant_total",
        "list_transactions",
        "find_transactions",
        "monthly_spending_chart",
        "net_worth_chart",
    }


def _looks_like_coaching_question(question: str, route: dict[str, Any] | None) -> bool:
    q = question or ""
    money_context = re.search(
        r"\b(money|budget|spend|spending|debt|cash|income|rent|tax|taxes|invest|afford|subscription|merchant|category|bill|bills|saving|savings)\b",
        q,
        re.I,
    )
    if money_context and re.search(r"\b(should|advice|advise|plan|budget|afford|can i|what should|how do i|help me)\b", q, re.I):
        return True
    return str((route or {}).get("intent") or "") == "plan"


def _coaching_opening(question: str, answer: str, route: dict[str, Any] | None) -> str:
    if _has_supportive_opening(answer):
        return ""
    q = question or ""
    operation = str((route or {}).get("operation") or "").lower()
    if re.search(r"\b(watch|watching|keep an eye)\b", q, re.I):
        return "Here's what I'd watch:"
    if operation in {"affordability"} or re.search(r"\b(afford|can i buy|can i spend)\b", q, re.I):
        return "The clean version:"
    if re.search(r"\b(start|first|clean up|where do i begin|what should i do)\b", q, re.I):
        return "I'd start here:"
    if operation in {"shortfall", "cashflow_forecast", "budget_status", "on_track"}:
        return "The clean version:"
    return ""


def _is_investing_question(question: str) -> bool:
    return bool(_INVEST_RE.search(question or ""))


def _investment_safe_answer(answer: str, question: str) -> str:
    if not _UNSAFE_INVEST_RE.search(answer or ""):
        if re.search(r"\b(risk|tradeoff|time horizon|liquidity|diversif|tax)\b", answer or "", re.I):
            return answer
        return (
            f"{answer} For investing, I would frame this around risk tolerance, time horizon, liquidity, taxes, "
            "fees, and diversification rather than treating any single pick as a sure thing."
        )
    return (
        "For investing, I would think in layers: emergency fund, high-interest debt, retirement accounts, "
        "broad diversified exposure, then higher-risk bets only after the basics are stable. The right choice "
        "depends on your time horizon, liquidity needs, risk tolerance, fees, and taxes; I would not treat any "
        "stock, crypto, or fund as a guaranteed win."
    )
