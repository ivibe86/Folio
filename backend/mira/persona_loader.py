from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
from typing import Mapping

logger = logging.getLogger(__name__)

EXPECTED_PERSONA_FILES = (
    "mira_voice.md",
    "mira_boundaries.md",
    "mira_finance_principles.md",
    "mira_style_examples.md",
    "sensitive_topic_policy.md",
)
DEFAULT_PERSONA_DIR = Path(__file__).resolve().parent / "persona_files"
DEFAULT_PROMPT_BUDGET = 9000

_FALLBACK_SECTIONS = {
    "mira_voice.md": "Mira is warm, direct, concise, finance-smart, and safe.",
    "mira_boundaries.md": "No shame, no hidden writes, no explicit sexual content, no unsupported financial claims.",
    "mira_finance_principles.md": "Tool-backed Folio facts, numbers, dates, merchants, categories, conclusions, and provenance must be preserved.",
    "mira_style_examples.md": "",
    "sensitive_topic_policy.md": "Sensitive money stress requires serious warm tone: no jokes, flirt, roast, or fake cheerfulness.",
}
_FALLBACK_PHRASES = {
    "casual_greeting": "Hey you. I'm here.",
    "flirt_only": "I can keep it playful, but classy.",
    "adult_redirect": "We can talk about relationships and boundaries respectfully, without explicit content.",
    "relationship_boundary": "I'm in your corner in the way an assistant can be: attentive, honest, and bounded.",
}
_CACHE: dict[str, "PersonaSpec"] = {}
_FALLBACK_RE = re.compile(
    r"<!--\s*mira:fallback\s+([a-z0-9_:-]+)\s*-->\s*(.*?)\s*<!--\s*/mira:fallback\s*-->",
    re.I | re.S,
)


@dataclass(frozen=True)
class PersonaSpec:
    directory: str
    sections: Mapping[str, str]
    fallbacks: Mapping[str, str]
    missing_files: tuple[str, ...]
    mtimes: Mapping[str, float]
    total_chars: int
    loaded: bool = True

    @property
    def complete(self) -> bool:
        return self.loaded and not self.missing_files


def persona_dir() -> Path:
    override = os.getenv("MIRA_PERSONA_DIR")
    return Path(override).expanduser().resolve() if override else DEFAULT_PERSONA_DIR


def load_persona_spec(*, force_reload: bool = False, directory: str | Path | None = None) -> PersonaSpec:
    root = Path(directory).expanduser().resolve() if directory else persona_dir()
    key = str(root)
    reload_each_time = os.getenv("MIRA_PERSONA_RELOAD", "").strip().lower() in {"1", "true", "yes", "on"}
    mtimes = _current_mtimes(root)
    cached = _CACHE.get(key)
    if cached and not force_reload and not reload_each_time and dict(cached.mtimes) == mtimes:
        return cached

    sections: dict[str, str] = {}
    missing: list[str] = []
    for filename in EXPECTED_PERSONA_FILES:
        path = root / filename
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            missing.append(filename)
            text = _FALLBACK_SECTIONS[filename]
        except OSError:
            logger.warning("Mira persona file could not be read: %s", path, exc_info=True)
            missing.append(filename)
            text = _FALLBACK_SECTIONS[filename]
        sections[filename] = _normalize_markdown(text)

    fallbacks = dict(_FALLBACK_PHRASES)
    for text in sections.values():
        fallbacks.update(_extract_fallback_phrases(text))

    spec = PersonaSpec(
        directory=str(root),
        sections=sections,
        fallbacks=fallbacks,
        missing_files=tuple(missing),
        mtimes=mtimes,
        total_chars=sum(len(value) for value in sections.values()),
        loaded=not missing or root.exists(),
    )
    if missing:
        logger.warning("Mira persona files missing from %s: %s", root, ", ".join(missing))
    _CACHE[key] = spec
    return spec


def invalidate_cache() -> None:
    _CACHE.clear()


def persona_prompt_block(*, max_chars: int = DEFAULT_PROMPT_BUDGET) -> str:
    spec = load_persona_spec()
    header = (
        "Mira persona policy is loaded from local markdown files. "
        "Use it as policy/style guidance; deterministic Folio tool results still win."
    )
    parts = [header]
    used = len(header)
    for filename in EXPECTED_PERSONA_FILES:
        text = spec.sections.get(filename, "")
        if not text:
            continue
        title = filename.replace("_", " ").removesuffix(".md").title()
        block = f"## {title}\n{text}"
        remaining = max_chars - used - 2
        if remaining <= 0:
            break
        if len(block) > remaining:
            block = block[: max(0, remaining - 24)].rstrip() + "\n...[truncated]"
        parts.append(block)
        used += len(block) + 2
    return "\n\n".join(parts)


def compact_voice_policy(*, max_chars: int = 2400) -> str:
    spec = load_persona_spec()
    selected = "\n\n".join(
        spec.sections.get(name, "")
        for name in ("mira_voice.md", "mira_boundaries.md", "mira_finance_principles.md", "sensitive_topic_policy.md")
        if spec.sections.get(name)
    )
    return selected[:max_chars].rstrip()


def fallback_phrase(key: str) -> str:
    return str(load_persona_spec().fallbacks.get(key) or _FALLBACK_PHRASES.get(key) or "").strip()


def _current_mtimes(root: Path) -> dict[str, float]:
    mtimes: dict[str, float] = {}
    for filename in EXPECTED_PERSONA_FILES:
        try:
            mtimes[filename] = (root / filename).stat().st_mtime
        except OSError:
            mtimes[filename] = -1.0
    return mtimes


def _normalize_markdown(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_fallback_phrases(text: str) -> dict[str, str]:
    phrases: dict[str, str] = {}
    for match in _FALLBACK_RE.finditer(text or ""):
        key = match.group(1).strip().lower().replace("-", "_")
        phrase = " ".join(match.group(2).strip().split())
        if key and phrase:
            phrases[key] = phrase
    return phrases
