"""Runtime selector for Folio's transaction categorization backend."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

VALID_CATEGORIZATION_BACKENDS = {"local_llm", "distilbert", "rules_only"}
_TRUE_VALUES = {"1", "true", "yes", "on"}


def env_truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in _TRUE_VALUES


def resolve_categorization_backend() -> str:
    """Resolve the active categorization backend while preserving legacy envs."""
    explicit = os.getenv("CATEGORIZATION_BACKEND", "").strip().lower()
    if explicit in VALID_CATEGORIZATION_BACKENDS:
        return explicit

    if explicit:
        logger.warning(
            "Invalid CATEGORIZATION_BACKEND=%s; falling back to legacy categorization settings",
            explicit,
        )

    if not env_truthy("ENABLE_LLM_CATEGORIZATION", "true"):
        return "rules_only"
    return "local_llm"

