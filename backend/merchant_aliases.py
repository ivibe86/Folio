from __future__ import annotations

from dataclasses import dataclass

from mira.grounding import (
    MerchantResolution,
    compact_token_text,
    exact_merchant_for_text,
    ground_merchant,
    resolve_merchant_with_llm,
)


@dataclass(frozen=True)
class MerchantCandidate:
    name: str
    score: float
    match_type: str
    matched_text: str = ""

    @property
    def needs_confirmation(self) -> bool:
        return self.match_type != "exact"


def alias_targets_for_text(text: str) -> list[str]:
    return []


def merchant_candidates_for_text(text: str, merchant_names: list[str], limit: int = 3) -> list[MerchantCandidate]:
    result = ground_merchant(text, merchant_names, limit=limit)
    candidates: list[MerchantCandidate] = []
    for candidate in result.candidates[:limit]:
        name = str(candidate.get("display_name") or candidate.get("value") or "").strip()
        if not name:
            continue
        candidates.append(
            MerchantCandidate(
                name=name,
                score=float(candidate.get("confidence") or 0),
                match_type=str(candidate.get("match_type") or ""),
                matched_text=str(candidate.get("matched_text") or ""),
            )
        )
    return candidates


def resolve_merchant_alias(text: str, merchant_names: list[str]) -> str | None:
    if not merchant_names:
        return None
    return exact_merchant_for_text(text, merchant_names)

