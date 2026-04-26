"""
llm_client.py
LLM abstraction layer. Routes to Ollama or Anthropic based on LLM_PROVIDER env var.

Supported providers (set LLM_PROVIDER in .env):
  anthropic  — calls api.anthropic.com (default)
  ollama     — calls a local Ollama instance
"""

import json as _json
import os
import httpx
import certifi
from dotenv import load_dotenv
from log_config import get_logger
import local_llm

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
_OLLAMA_COPILOT_KEEP_ALIVE = os.getenv(
    "OLLAMA_COPILOT_KEEP_ALIVE",
    os.getenv("OLLAMA_PREWARM_KEEP_ALIVE", "15m"),
).strip() or "15m"


def is_available() -> bool:
    """Return True if the configured LLM provider has the required credentials/config."""
    provider = get_provider()
    if provider == "ollama":
        return bool(get_ollama_config()["base_url"])
    return bool(ANTHROPIC_API_KEY)


def get_provider() -> str:
    try:
        return local_llm.get_provider()
    except Exception:
        return LLM_PROVIDER


def get_ollama_config() -> dict:
    try:
        return local_llm.get_ollama_config()
    except Exception:
        return {
            "base_url": OLLAMA_BASE_URL,
            "categorize_model": OLLAMA_MODEL_CATEGORIZE,
            "copilot_model": OLLAMA_MODEL_COPILOT,
        }


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
    if get_provider() == "ollama":
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


def chat_with_tools(
    messages: list[dict],
    tools: list[dict],
    system: str | None = None,
    max_tokens: int = 2048,
    purpose: str = "copilot",
) -> dict:
    """
    Provider-agnostic tool-capable chat.

    Args:
        messages: list of {"role": "user"|"assistant"|"tool", "content": str,
                   "tool_calls": [{"id","name","args"}]?, "tool_call_id": str?}
        tools:    registry-agnostic tool schemas (see copilot_tools module)
        system:   optional system prompt

    Returns:
        {"content": str, "tool_calls": [{"id","name","args"}], "stop_reason": str}
        If tool_calls is non-empty, the caller should execute each and append a
        tool-role message before calling again.
    """
    if get_provider() == "ollama":
        return _chat_with_tools_ollama(messages, tools, system, max_tokens, purpose)
    return _chat_with_tools_anthropic(messages, tools, system, max_tokens)


def _chat_with_tools_anthropic(
    messages: list[dict],
    tools: list[dict],
    system: str | None,
    max_tokens: int,
) -> dict:
    import copilot_tools
    if tools and isinstance(tools[0], str):
        anth_tools = copilot_tools.tools_for_anthropic(tools)
    else:
        anth_tools = tools or []
    anth_messages = []
    for msg in messages:
        role = msg.get("role")
        if role == "tool":
            anth_messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": msg["tool_call_id"],
                    "content": msg.get("content") or "",
                }],
            })
        elif role == "assistant" and msg.get("tool_calls"):
            blocks = []
            if msg.get("content"):
                blocks.append({"type": "text", "text": msg["content"]})
            for call in msg["tool_calls"]:
                blocks.append({
                    "type": "tool_use",
                    "id": call["id"],
                    "name": call["name"],
                    "input": call.get("args") or {},
                })
            anth_messages.append({"role": "assistant", "content": blocks})
        else:
            anth_messages.append({"role": role, "content": msg.get("content") or ""})

    payload = {
        "model": _ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": anth_messages,
    }
    if system:
        payload["system"] = system
    if anth_tools:
        payload["tools"] = anth_tools

    resp = httpx.post(
        _ANTHROPIC_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json=payload,
        timeout=120.0,
        verify=certifi.where(),
    )
    result = resp.json()
    if "content" not in result:
        raise Exception(f"Anthropic tool API error: {result}")

    text_parts = []
    tool_calls = []
    for block in result["content"]:
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
        elif block.get("type") == "tool_use":
            tool_calls.append({
                "id": block["id"],
                "name": block["name"],
                "args": block.get("input") or {},
            })

    return {
        "content": "".join(text_parts).strip(),
        "tool_calls": tool_calls,
        "stop_reason": result.get("stop_reason", ""),
    }


def _chat_with_tools_ollama(
    messages: list[dict],
    tools: list[dict],
    system: str | None,
    max_tokens: int,
    purpose: str,
) -> dict:
    import copilot_tools
    if tools and isinstance(tools[0], str):
        ollama_tools = copilot_tools.tools_for_ollama(tools)
    else:
        ollama_tools = tools or []
    ollama_config = get_ollama_config()
    model = ollama_config["copilot_model"] if purpose == "copilot" else ollama_config["categorize_model"]
    timeout = _OLLAMA_TIMEOUT_COPILOT if purpose == "copilot" else _OLLAMA_TIMEOUT_CATEGORIZE

    ollama_msgs = []
    if system:
        ollama_msgs.append({"role": "system", "content": system})
    for msg in messages:
        role = msg.get("role")
        if role == "tool":
            ollama_msgs.append({
                "role": "tool",
                "content": msg.get("content") or "",
                "tool_call_id": msg.get("tool_call_id"),
            })
        elif role == "assistant" and msg.get("tool_calls"):
            ollama_msgs.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": [
                    {
                        "id": c["id"],
                        "type": "function",
                        "function": {
                            "name": c["name"],
                            "arguments": c.get("args") or {},
                        },
                    }
                    for c in msg["tool_calls"]
                ],
            })
        else:
            ollama_msgs.append({"role": role, "content": msg.get("content") or ""})

    payload = {
        "model": model,
        "messages": ollama_msgs,
        "stream": False,
        "think": False,
        "options": {"num_predict": max_tokens, "temperature": 0},
    }
    if purpose == "copilot":
        payload["keep_alive"] = _OLLAMA_COPILOT_KEEP_ALIVE
    if ollama_tools:
        payload["tools"] = ollama_tools

    url = f"{ollama_config['base_url'].rstrip('/')}/api/chat"
    resp = httpx.post(url, json=payload, timeout=timeout)
    result = resp.json()
    if "message" not in result:
        raise Exception(f"Ollama tool API error: {result}")
    message = result["message"]

    tool_calls = []
    for idx, call in enumerate(message.get("tool_calls") or []):
        fn = call.get("function") or {}
        args = fn.get("arguments") or {}
        if isinstance(args, str):
            try:
                args = _json.loads(args)
            except Exception:
                args = {}
        tool_calls.append({
            "id": call.get("id") or f"call_{idx}",
            "name": fn.get("name") or "",
            "args": args,
        })

    return {
        "content": (message.get("content") or "").strip(),
        "tool_calls": tool_calls,
        "stop_reason": result.get("done_reason") or "stop",
    }


def chat_with_tools_stream(
    messages: list[dict],
    tools: list[dict],
    system: str | None = None,
    max_tokens: int = 2048,
    purpose: str = "copilot",
):
    """
    Generator yielding ("text", delta_text) | ("tool_call", dict) | ("stop", reason)
    for a single streaming LLM turn. Provider-agnostic.
    """
    if get_provider() == "ollama":
        yield from _chat_with_tools_stream_ollama(messages, tools, system, max_tokens, purpose)
    else:
        yield from _chat_with_tools_stream_anthropic(messages, tools, system, max_tokens)


def _chat_with_tools_stream_anthropic(messages, tools, system, max_tokens):
    import copilot_tools
    if tools and isinstance(tools[0], str):
        anth_tools = copilot_tools.tools_for_anthropic(tools)
    else:
        anth_tools = tools or []
    anth_messages = []
    for msg in messages:
        role = msg.get("role")
        if role == "tool":
            anth_messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": msg["tool_call_id"],
                    "content": msg.get("content") or "",
                }],
            })
        elif role == "assistant" and msg.get("tool_calls"):
            blocks = []
            if msg.get("content"):
                blocks.append({"type": "text", "text": msg["content"]})
            for call in msg["tool_calls"]:
                blocks.append({
                    "type": "tool_use",
                    "id": call["id"],
                    "name": call["name"],
                    "input": call.get("args") or {},
                })
            anth_messages.append({"role": "assistant", "content": blocks})
        else:
            anth_messages.append({"role": role, "content": msg.get("content") or ""})

    payload = {
        "model": _ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": anth_messages,
        "stream": True,
    }
    if system:
        payload["system"] = system
    if anth_tools:
        payload["tools"] = anth_tools

    active_tool = None
    with httpx.stream(
        "POST", _ANTHROPIC_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json=payload, timeout=180.0, verify=certifi.where(),
    ) as resp:
        for raw_line in resp.iter_lines():
            if not raw_line or not raw_line.startswith("data:"):
                continue
            data_str = raw_line[5:].strip()
            if not data_str or data_str == "[DONE]":
                continue
            try:
                event = _json.loads(data_str)
            except Exception:
                continue
            etype = event.get("type")
            if etype == "content_block_start":
                block = event.get("content_block") or {}
                if block.get("type") == "tool_use":
                    active_tool = {
                        "id": block.get("id"),
                        "name": block.get("name"),
                        "args_json": "",
                    }
            elif etype == "content_block_delta":
                delta = event.get("delta") or {}
                dtype = delta.get("type")
                if dtype == "text_delta":
                    text = delta.get("text") or ""
                    if text:
                        yield ("text", text)
                elif dtype == "input_json_delta" and active_tool is not None:
                    active_tool["args_json"] += delta.get("partial_json") or ""
            elif etype == "content_block_stop":
                if active_tool is not None:
                    try:
                        args = _json.loads(active_tool["args_json"]) if active_tool["args_json"] else {}
                    except Exception:
                        args = {}
                    yield ("tool_call", {
                        "id": active_tool["id"],
                        "name": active_tool["name"],
                        "args": args,
                    })
                    active_tool = None
            elif etype == "message_delta":
                reason = (event.get("delta") or {}).get("stop_reason")
                if reason:
                    yield ("stop", reason)


def _chat_with_tools_stream_ollama(messages, tools, system, max_tokens, purpose):
    import copilot_tools
    if tools and isinstance(tools[0], str):
        ollama_tools = copilot_tools.tools_for_ollama(tools)
    else:
        ollama_tools = tools or []
    ollama_config = get_ollama_config()
    model = ollama_config["copilot_model"] if purpose == "copilot" else ollama_config["categorize_model"]
    timeout = _OLLAMA_TIMEOUT_COPILOT if purpose == "copilot" else _OLLAMA_TIMEOUT_CATEGORIZE

    ollama_msgs = []
    if system:
        ollama_msgs.append({"role": "system", "content": system})
    for msg in messages:
        role = msg.get("role")
        if role == "tool":
            ollama_msgs.append({
                "role": "tool",
                "content": msg.get("content") or "",
                "tool_call_id": msg.get("tool_call_id"),
            })
        elif role == "assistant" and msg.get("tool_calls"):
            ollama_msgs.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": [
                    {
                        "id": c["id"],
                        "type": "function",
                        "function": {"name": c["name"], "arguments": c.get("args") or {}},
                    }
                    for c in msg["tool_calls"]
                ],
            })
        else:
            ollama_msgs.append({"role": role, "content": msg.get("content") or ""})

    payload = {
        "model": model,
        "messages": ollama_msgs,
        "stream": True,
        "think": False,
        "options": {"num_predict": max_tokens, "temperature": 0},
    }
    if purpose == "copilot":
        payload["keep_alive"] = _OLLAMA_COPILOT_KEEP_ALIVE
    if ollama_tools:
        payload["tools"] = ollama_tools

    url = f"{ollama_config['base_url'].rstrip('/')}/api/chat"
    with httpx.stream("POST", url, json=payload, timeout=timeout) as resp:
        call_idx = 0
        for raw_line in resp.iter_lines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = _json.loads(line)
            except Exception:
                continue
            message = event.get("message") or {}
            content = message.get("content")
            if content:
                yield ("text", content)
            for call in message.get("tool_calls") or []:
                fn = call.get("function") or {}
                args = fn.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = _json.loads(args)
                    except Exception:
                        args = {}
                yield ("tool_call", {
                    "id": call.get("id") or f"call_{call_idx}",
                    "name": fn.get("name") or "",
                    "args": args,
                })
                call_idx += 1
            if event.get("done"):
                yield ("stop", event.get("done_reason") or "stop")


def _complete_ollama(prompt: str, max_tokens: int, purpose: str) -> str:
    ollama_config = get_ollama_config()
    model = ollama_config["categorize_model"] if purpose == "categorize" else ollama_config["copilot_model"]
    timeout = _OLLAMA_TIMEOUT_CATEGORIZE if purpose == "categorize" else _OLLAMA_TIMEOUT_COPILOT
    url = f"{ollama_config['base_url'].rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"num_predict": max_tokens},
    }
    if purpose == "copilot":
        payload["keep_alive"] = _OLLAMA_COPILOT_KEEP_ALIVE

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
