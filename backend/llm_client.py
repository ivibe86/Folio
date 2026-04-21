"""
llm_client.py
LLM abstraction layer. Routes to Ollama or Anthropic based on LLM_PROVIDER env var.

Supported providers (set LLM_PROVIDER in .env):
  anthropic  — calls api.anthropic.com (default)
  ollama     — calls a local Ollama instance
"""

import os
import httpx
import certifi
from dotenv import load_dotenv
from log_config import get_logger

load_dotenv()

logger = get_logger(__name__)

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic").lower()

# Anthropic settings
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_ANTHROPIC_MODEL = "claude-3-haiku-20240307"

# Ollama settings
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL_CATEGORIZE = os.getenv("OLLAMA_MODEL_CATEGORIZE", "gemma4:e4b")
OLLAMA_MODEL_COPILOT = os.getenv("OLLAMA_MODEL_COPILOT", "gemma4:26b")
# Timeouts are generous by default — local inference on a laptop is slow,
# especially categorization batches of 50 transactions.
# Increase further via env if your hardware is particularly slow.
_OLLAMA_TIMEOUT_CATEGORIZE = float(os.getenv("OLLAMA_TIMEOUT_CATEGORIZE", "600"))  # 10 min
_OLLAMA_TIMEOUT_COPILOT = float(os.getenv("OLLAMA_TIMEOUT_COPILOT", "240"))        # 4 min


def is_available() -> bool:
    """Return True if the configured LLM provider has the required credentials/config."""
    if LLM_PROVIDER == "ollama":
        return bool(OLLAMA_BASE_URL)
    return bool(ANTHROPIC_API_KEY)


def complete(prompt: str, max_tokens: int = 1024, purpose: str = "copilot") -> str:
    """
    Send a prompt to the configured LLM and return the response text.

    Args:
        prompt:     The user message content.
        max_tokens: Maximum tokens to generate.
        purpose:    "categorize" or "copilot" — selects the Ollama model when
                    LLM_PROVIDER=ollama. Has no effect for Anthropic.

    Returns:
        Stripped response text from the model.

    Raises:
        Exception on API or network errors.
    """
    if LLM_PROVIDER == "ollama":
        return _complete_ollama(prompt, max_tokens, purpose)
    return _complete_anthropic(prompt, max_tokens)


def _complete_anthropic(prompt: str, max_tokens: int) -> str:
    resp = httpx.post(
        _ANTHROPIC_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": _ANTHROPIC_MODEL,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60.0,
        verify=certifi.where(),
    )
    result = resp.json()
    if "content" not in result:
        raise Exception(f"Anthropic API error: {result}")
    return result["content"][0]["text"].strip()


def _complete_ollama(prompt: str, max_tokens: int, purpose: str) -> str:
    model = OLLAMA_MODEL_CATEGORIZE if purpose == "categorize" else OLLAMA_MODEL_COPILOT
    timeout = _OLLAMA_TIMEOUT_CATEGORIZE if purpose == "categorize" else _OLLAMA_TIMEOUT_COPILOT
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"num_predict": max_tokens},
    }

    if purpose in {"categorize", "copilot"}:
        # Merchant enrichment, categorization, and SQL generation are
        # deterministic extraction/translation tasks, so disable thinking
        # and randomness for faster, steadier output.
        payload["think"] = False
        payload["options"]["temperature"] = 0

    resp = httpx.post(
        url,
        json=payload,
        timeout=timeout,
    )
    result = resp.json()
    if "message" not in result:
        raise Exception(f"Ollama API error: {result}")
    return result["message"]["content"].strip()
