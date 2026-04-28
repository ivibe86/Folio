from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


MONTH_LOOKUP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


@dataclass(frozen=True)
class RangeParse:
    token: str
    explicit: bool
    chart_months: int | None = None


def words(text: str) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    for char in (text or "").lower():
        if char.isalnum():
            current.append(char)
        else:
            if current:
                chunks.append("".join(current))
                current = []
    if current:
        chunks.append("".join(current))
    return chunks


def contains(tokens: list[str], phrase: tuple[str, ...]) -> bool:
    if not phrase or len(phrase) > len(tokens):
        return False
    width = len(phrase)
    return any(tuple(tokens[idx:idx + width]) == phrase for idx in range(len(tokens) - width + 1))


def _int_token(token: str) -> int | None:
    if token.isdigit():
        try:
            return int(token)
        except ValueError:
            return None
    return None


def _month_count_since(tokens: list[str], now: datetime) -> int | None:
    for idx, token in enumerate(tokens[:-1]):
        if token != "since":
            continue
        month = MONTH_LOOKUP.get(tokens[idx + 1])
        if month is None:
            continue
        year = now.year
        if idx + 2 < len(tokens):
            maybe_year = _int_token(tokens[idx + 2])
            if maybe_year and 2000 <= maybe_year <= 2099:
                year = maybe_year
        elif month > now.month:
            year -= 1
        return max(1, min((now.year - year) * 12 + now.month - month + 1, 36))
    return None


def _explicit_month(tokens: list[str]) -> str | None:
    for idx, token in enumerate(tokens[:-1]):
        year = _int_token(token)
        month = _int_token(tokens[idx + 1])
        if year and month and 2000 <= year <= 2099 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"
    return None


def _month_token(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    return idx // 12, idx % 12 + 1


def _coerce_month_token(token: str, now: datetime) -> str | None:
    token = (token or "").strip().lower()
    if token in {"current_month", "this_month", "current"}:
        return _month_token(now.year, now.month)
    if token in {"last_month", "prior_month", "previous_month", "prior"}:
        return _month_token(*_shift_month(now.year, now.month, -1))
    if len(token) == 7 and token[4] == "-":
        try:
            year = int(token[:4])
            month = int(token[5:7])
        except ValueError:
            return None
        if 1 <= month <= 12:
            return _month_token(year, month)
    return None


def _explicit_named_month(tokens: list[str], now: datetime) -> str | None:
    for idx, token in enumerate(tokens):
        month = MONTH_LOOKUP.get(token)
        if month is None:
            continue
        year: int | None = None
        if idx + 1 < len(tokens):
            maybe_year = _int_token(tokens[idx + 1])
            if maybe_year and 2000 <= maybe_year <= 2099:
                year = maybe_year
        if year is None and idx > 0:
            maybe_year = _int_token(tokens[idx - 1])
            if maybe_year and 2000 <= maybe_year <= 2099:
                year = maybe_year
        if year is None:
            year = now.year if month <= now.month else now.year - 1
        return _month_token(year, month)
    return None


def _relative_month_delta(tokens: list[str]) -> int | None:
    if (
        contains(tokens, ("month", "before"))
        or contains(tokens, ("the", "month", "before"))
        or contains(tokens, ("one", "month", "earlier"))
        or contains(tokens, ("month", "earlier"))
        or contains(tokens, ("previous", "month"))
        or contains(tokens, ("prior", "month"))
    ):
        return -1
    if (
        contains(tokens, ("next", "month"))
        or contains(tokens, ("month", "after"))
        or contains(tokens, ("the", "month", "after"))
        or contains(tokens, ("one", "month", "later"))
        or contains(tokens, ("month", "later"))
    ):
        return 1
    return None


def parse_range(question: str, *, default: str = "current_month", now: datetime | None = None) -> RangeParse:
    now = now or datetime.now()
    tokens = words(question)
    token_set = set(tokens)

    explicit_month = _explicit_month(tokens)
    if explicit_month:
        return RangeParse(explicit_month, True)

    since_months = _month_count_since(tokens, now)
    if since_months is not None:
        return RangeParse(f"last_{since_months}_months", True, chart_months=since_months)

    named_month = _explicit_named_month(tokens, now)
    if named_month:
        return RangeParse(named_month, True, chart_months=1)

    if contains(tokens, ("all", "time")) or {"alltime", "ever", "lifetime"} & token_set:
        return RangeParse("all", True)
    if contains(tokens, ("till", "now")) or contains(tokens, ("until", "now")) or contains(tokens, ("to", "date")):
        return RangeParse("all", True)

    if {"ytd"} & token_set or contains(tokens, ("year", "to", "date")) or contains(tokens, ("this", "year")):
        return RangeParse("ytd", True)

    if contains(tokens, ("past", "year")) or contains(tokens, ("previous", "year")) or contains(tokens, ("prior", "year")):
        return RangeParse("last_12_months", True, chart_months=12)
    if contains(tokens, ("over", "the", "past", "year")) or contains(tokens, ("last", "12", "months")):
        return RangeParse("last_12_months", True, chart_months=12)
    if contains(tokens, ("last", "year")):
        return RangeParse("last_year", True)

    if contains(tokens, ("this", "month")) or contains(tokens, ("current", "month")):
        return RangeParse("current_month", True, chart_months=1)
    if contains(tokens, ("last", "month")) or contains(tokens, ("previous", "month")) or contains(tokens, ("prior", "month")):
        return RangeParse("last_month", True, chart_months=1)

    relative_delta = _relative_month_delta(tokens)
    if relative_delta is not None:
        if relative_delta < 0:
            return RangeParse("last_month", True, chart_months=1)
        year, month = _shift_month(now.year, now.month, relative_delta)
        return RangeParse(_month_token(year, month), True, chart_months=1)
    if contains(tokens, ("this", "week")) or contains(tokens, ("current", "week")):
        return RangeParse("this_week", True)
    if contains(tokens, ("last", "week")) or contains(tokens, ("previous", "week")) or contains(tokens, ("prior", "week")):
        return RangeParse("last_week", True)

    if contains(tokens, ("half", "year")) or contains(tokens, ("half", "a", "year")):
        return RangeParse("last_6_months", True, chart_months=6)

    range_heads = {"last", "past", "previous", "prior"}
    for idx, token in enumerate(tokens[:-1]):
        if token not in range_heads and not (token == "over" and idx + 2 < len(tokens) and tokens[idx + 1] == "the"):
            continue
        number_idx = idx + 1 if token != "over" else idx + 3
        if number_idx >= len(tokens):
            continue
        amount = _int_token(tokens[number_idx])
        unit = tokens[number_idx + 1] if number_idx + 1 < len(tokens) else ""
        if amount is None:
            continue
        if unit in {"month", "months"}:
            months = max(1, min(amount, 36))
            return RangeParse(f"last_{months}_months", True, chart_months=months)
        if unit in {"day", "days"}:
            days = max(1, min(amount, 365))
            return RangeParse(f"last_{days}d", True)

    if contains(tokens, ("so", "far")):
        return RangeParse("all", True)

    return RangeParse(default, False)


def resolve_followup_range(text: str, prior_range: str | None, now: datetime | None = None) -> RangeParse:
    """Resolve a follow-up range, allowing relative month language to use the prior answer range."""
    now = now or datetime.now()
    tokens = words(text)
    delta = _relative_month_delta(tokens)
    if delta is not None:
        base = _coerce_month_token(prior_range or "", now) or _coerce_month_token("current_month", now)
        year = int(base[:4])
        month = int(base[5:7])
        shifted_year, shifted_month = _shift_month(year, month, delta)
        return RangeParse(_month_token(shifted_year, shifted_month), True, chart_months=1)
    return parse_range(text, now=now)


def has_explicit_time_scope(question: str) -> bool:
    return parse_range(question).explicit


def chart_months(question: str, fallback: int = 6) -> int:
    parsed = parse_range(question)
    if parsed.chart_months:
        return parsed.chart_months
    if not parsed.explicit:
        return fallback
    if parsed.token == "last_year" or parsed.token == "last_12_months":
        return 12
    return fallback
