"""
llm_client.py
Local LLM client for Ollama-backed categorization, Mira, and receipt parsing.
"""

import base64
import json as _json
import os
import httpx
from dotenv import load_dotenv
from log_config import get_logger
import local_llm

load_dotenv()

logger = get_logger(__name__)

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").strip().lower() or "ollama"

# Ollama settings
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL_CATEGORIZE = os.getenv("OLLAMA_MODEL_CATEGORIZE", "gemma4:e4b")
OLLAMA_MODEL_CONTROLLER = os.getenv("OLLAMA_MODEL_CONTROLLER", OLLAMA_MODEL_CATEGORIZE)
OLLAMA_MODEL_COPILOT = os.getenv("OLLAMA_MODEL_COPILOT", "gemma4:26b")
OLLAMA_MODEL_RECEIPT = os.getenv("OLLAMA_MODEL_RECEIPT", "gemma4:e4b")
LLAMACPP_BASE_URL = os.getenv("LLAMACPP_BASE_URL", "http://host.docker.internal:8081")
LLAMACPP_MODEL = os.getenv("LLAMACPP_MODEL", "local")
LLAMACPP_TIMEOUT = float(os.getenv("LLAMACPP_TIMEOUT", os.getenv("OLLAMA_TIMEOUT_COPILOT", "240")))
LLAMACPP_TEMPERATURE = float(os.getenv("LLAMACPP_TEMPERATURE", "1.0"))
LLAMACPP_TOP_P = float(os.getenv("LLAMACPP_TOP_P", "0.95"))
LLAMACPP_TOP_K = int(os.getenv("LLAMACPP_TOP_K", "64"))
LLAMACPP_THINK = os.getenv("LLAMACPP_THINK", "false").strip().lower() in {"1", "true", "yes", "on"}
# Timeouts are generous by default — local inference on a laptop is slow,
# especially categorization batches of 50 transactions.
# Increase further via env if your hardware is particularly slow.
_OLLAMA_TIMEOUT_CATEGORIZE = float(os.getenv("OLLAMA_TIMEOUT_CATEGORIZE", "600"))  # 10 min
_OLLAMA_TIMEOUT_CONTROLLER = float(os.getenv("OLLAMA_TIMEOUT_CONTROLLER", "90"))    # 1.5 min
_OLLAMA_TIMEOUT_COPILOT = float(os.getenv("OLLAMA_TIMEOUT_COPILOT", "240"))        # 4 min
_OLLAMA_CONTROLLER_KEEP_ALIVE = os.getenv(
    "OLLAMA_CONTROLLER_KEEP_ALIVE",
    os.getenv("OLLAMA_PREWARM_KEEP_ALIVE", "15m"),
).strip() or "15m"
_OLLAMA_COPILOT_KEEP_ALIVE = os.getenv(
    "OLLAMA_COPILOT_KEEP_ALIVE",
    os.getenv("OLLAMA_PREWARM_KEEP_ALIVE", "15m"),
).strip() or "15m"


def is_available() -> bool:
    """Return True when a local LLM base URL is configured."""
    provider = get_provider()
    if provider == "llamacpp":
        return bool(get_llamacpp_config()["base_url"])
    return bool(get_ollama_config()["base_url"])


def get_provider() -> str:
    try:
        return local_llm.get_provider()
    except Exception:
        return "llamacpp" if LLM_PROVIDER == "llamacpp" else "ollama"


def get_ollama_config() -> dict:
    try:
        return local_llm.get_ollama_config()
    except Exception:
        return {
            "base_url": OLLAMA_BASE_URL,
            "categorize_model": OLLAMA_MODEL_CATEGORIZE,
            "controller_model": OLLAMA_MODEL_CONTROLLER,
            "copilot_model": OLLAMA_MODEL_COPILOT,
        }


def get_llamacpp_config() -> dict:
    try:
        return local_llm.get_llamacpp_config()
    except Exception:
        return {
            "base_url": LLAMACPP_BASE_URL,
            "model": LLAMACPP_MODEL,
        }


def _model_for_purpose(ollama_config: dict, purpose: str) -> str:
    if purpose == "categorize":
        return ollama_config.get("categorize_model") or OLLAMA_MODEL_CATEGORIZE
    if purpose == "controller":
        return (
            ollama_config.get("controller_model")
            or ollama_config.get("categorize_model")
            or OLLAMA_MODEL_CONTROLLER
        )
    return ollama_config.get("copilot_model") or OLLAMA_MODEL_COPILOT


def _timeout_for_purpose(purpose: str) -> float:
    if purpose == "categorize":
        return _OLLAMA_TIMEOUT_CATEGORIZE
    if purpose == "controller":
        return _OLLAMA_TIMEOUT_CONTROLLER
    return _OLLAMA_TIMEOUT_COPILOT


def _keep_alive_for_purpose(purpose: str) -> str | None:
    if purpose == "controller":
        return _OLLAMA_CONTROLLER_KEEP_ALIVE
    if purpose == "copilot":
        return _OLLAMA_COPILOT_KEEP_ALIVE
    return None


def complete(prompt: str, max_tokens: int = 1024, purpose: str = "copilot") -> str:
    """
    Send a prompt to the configured LLM and return the response text.

    Args:
        prompt:     The user message content.
        max_tokens: Maximum tokens to generate.
        purpose:    "categorize", "controller", or "copilot" selects the local Ollama model.

    Returns:
        Stripped response text from the model.

    Raises:
        Exception on API or network errors.
    """
    if get_provider() == "llamacpp" and purpose in {"controller", "copilot"}:
        return _complete_llamacpp(prompt, max_tokens)
    return _complete_ollama(prompt, max_tokens, purpose)


def complete_vision(
    prompt: str,
    image_bytes: bytes,
    max_tokens: int = 2048,
    purpose: str = "copilot",
    mime_type: str | None = None,
) -> tuple[str, str]:
    """
    Send one image plus text to the configured local vision model.
    Returns (response_text, model_name). Receipt parsing is intentionally local-only.
    """
    return _complete_ollama_vision(prompt, image_bytes, max_tokens, purpose, mime_type)


def chat_with_tools(
    messages: list[dict],
    tools: list[dict],
    system: str | None = None,
    max_tokens: int = 2048,
    purpose: str = "copilot",
) -> dict:
    """
    Tool-capable chat through local Ollama.

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
    if get_provider() == "llamacpp" and purpose == "copilot":
        return _chat_with_tools_llamacpp(messages, tools, system, max_tokens)
    return _chat_with_tools_ollama(messages, tools, system, max_tokens, purpose)


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
    model = _model_for_purpose(ollama_config, purpose)
    timeout = _timeout_for_purpose(purpose)

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
    keep_alive = _keep_alive_for_purpose(purpose)
    if keep_alive:
        payload["keep_alive"] = keep_alive
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
    for a single streaming local Ollama turn.
    """
    if get_provider() == "llamacpp" and purpose == "copilot":
        yield from _chat_with_tools_stream_llamacpp(messages, tools, system, max_tokens)
        return
    yield from _chat_with_tools_stream_ollama(messages, tools, system, max_tokens, purpose)


def _chat_with_tools_stream_ollama(messages, tools, system, max_tokens, purpose):
    import copilot_tools
    if tools and isinstance(tools[0], str):
        ollama_tools = copilot_tools.tools_for_ollama(tools)
    else:
        ollama_tools = tools or []
    ollama_config = get_ollama_config()
    model = _model_for_purpose(ollama_config, purpose)
    timeout = _timeout_for_purpose(purpose)

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
    keep_alive = _keep_alive_for_purpose(purpose)
    if keep_alive:
        payload["keep_alive"] = keep_alive
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


def _messages_for_openai(messages: list[dict], system: str | None) -> list[dict]:
    openai_msgs = []
    if system:
        openai_msgs.append({"role": "system", "content": system})
    for msg in messages:
        role = msg.get("role")
        if role == "tool":
            openai_msgs.append({
                "role": "tool",
                "content": msg.get("content") or "",
                "tool_call_id": msg.get("tool_call_id"),
            })
        elif role == "assistant" and msg.get("tool_calls"):
            openai_msgs.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": [
                    {
                        "id": c["id"],
                        "type": "function",
                        "function": {
                            "name": c["name"],
                            "arguments": _json.dumps(c.get("args") or {}),
                        },
                    }
                    for c in msg["tool_calls"]
                ],
            })
        else:
            openai_msgs.append({"role": role, "content": msg.get("content") or ""})
    return openai_msgs


def _llamacpp_payload(
    messages: list[dict],
    max_tokens: int,
    stream: bool,
    tools: list[dict] | None = None,
) -> dict:
    config = get_llamacpp_config()
    payload = {
        "messages": messages,
        "stream": stream,
        "max_tokens": max_tokens,
        "temperature": LLAMACPP_TEMPERATURE,
        "top_p": LLAMACPP_TOP_P,
        "top_k": LLAMACPP_TOP_K,
        "think": LLAMACPP_THINK,
    }
    model = (config.get("model") or "").strip()
    if model and model != "local":
        payload["model"] = model
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    return payload


def _parse_openai_tool_calls(raw_calls: list[dict] | None) -> list[dict]:
    parsed = []
    for idx, call in enumerate(raw_calls or []):
        fn = call.get("function") or {}
        args = fn.get("arguments") or {}
        if isinstance(args, str):
            try:
                args = _json.loads(args)
            except Exception:
                args = {}
        parsed.append({
            "id": call.get("id") or f"call_{idx}",
            "name": fn.get("name") or "",
            "args": args,
        })
    return parsed


def _chat_with_tools_llamacpp(
    messages: list[dict],
    tools: list[dict],
    system: str | None,
    max_tokens: int,
) -> dict:
    import copilot_tools
    openai_tools = copilot_tools.tools_for_ollama(tools) if tools and isinstance(tools[0], str) else tools or []
    config = get_llamacpp_config()
    payload = _llamacpp_payload(
        _messages_for_openai(messages, system),
        max_tokens=max_tokens,
        stream=False,
        tools=openai_tools,
    )
    url = f"{config['base_url'].rstrip('/')}/v1/chat/completions"
    resp = httpx.post(url, json=payload, timeout=LLAMACPP_TIMEOUT)
    result = resp.json()
    choices = result.get("choices") or []
    if not choices:
        raise Exception(f"llama.cpp API error: {result}")
    message = choices[0].get("message") or {}
    return {
        "content": (message.get("content") or "").strip(),
        "tool_calls": _parse_openai_tool_calls(message.get("tool_calls")),
        "stop_reason": choices[0].get("finish_reason") or "stop",
    }


def _chat_with_tools_stream_llamacpp(messages, tools, system, max_tokens):
    import copilot_tools
    openai_tools = copilot_tools.tools_for_ollama(tools) if tools and isinstance(tools[0], str) else tools or []
    config = get_llamacpp_config()
    payload = _llamacpp_payload(
        _messages_for_openai(messages, system),
        max_tokens=max_tokens,
        stream=True,
        tools=openai_tools,
    )
    url = f"{config['base_url'].rstrip('/')}/v1/chat/completions"
    buffered_tool_calls: dict[int, dict] = {}
    with httpx.stream("POST", url, json=payload, timeout=LLAMACPP_TIMEOUT) as resp:
        for raw_line in resp.iter_lines():
            line = raw_line.strip()
            if not line or not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                event = _json.loads(data)
            except Exception:
                continue
            choices = event.get("choices") or []
            if not choices:
                continue
            delta = choices[0].get("delta") or {}
            content = delta.get("content")
            if content:
                yield ("text", content)
            for call in delta.get("tool_calls") or []:
                idx = int(call.get("index") or 0)
                current = buffered_tool_calls.setdefault(idx, {
                    "id": call.get("id") or f"call_{idx}",
                    "name": "",
                    "arguments": "",
                })
                if call.get("id"):
                    current["id"] = call["id"]
                fn = call.get("function") or {}
                if fn.get("name"):
                    current["name"] += fn["name"]
                if fn.get("arguments"):
                    current["arguments"] += fn["arguments"]
            if choices[0].get("finish_reason"):
                break

    for idx, call in sorted(buffered_tool_calls.items()):
        try:
            args = _json.loads(call.get("arguments") or "{}")
        except Exception:
            args = {}
        yield ("tool_call", {
            "id": call.get("id") or f"call_{idx}",
            "name": call.get("name") or "",
            "args": args,
        })
    yield ("stop", "stop")


def _complete_llamacpp(prompt: str, max_tokens: int) -> str:
    config = get_llamacpp_config()
    payload = _llamacpp_payload(
        [{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        stream=False,
    )
    url = f"{config['base_url'].rstrip('/')}/v1/chat/completions"
    resp = httpx.post(url, json=payload, timeout=LLAMACPP_TIMEOUT)
    result = resp.json()
    choices = result.get("choices") or []
    if not choices:
        raise Exception(f"llama.cpp API error: {result}")
    message = choices[0].get("message") or {}
    return (message.get("content") or "").strip()


def _complete_ollama(prompt: str, max_tokens: int, purpose: str) -> str:
    ollama_config = get_ollama_config()
    model = _model_for_purpose(ollama_config, purpose)
    timeout = _timeout_for_purpose(purpose)
    url = f"{ollama_config['base_url'].rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"num_predict": max_tokens},
    }
    keep_alive = _keep_alive_for_purpose(purpose)
    if keep_alive:
        payload["keep_alive"] = keep_alive

    if purpose in {"categorize", "controller", "copilot"}:
        # Merchant enrichment, categorization, routing, and extraction are
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


def _complete_ollama_vision(
    prompt: str,
    image_bytes: bytes,
    max_tokens: int,
    purpose: str,
    mime_type: str | None = None,
) -> tuple[str, str]:
    ollama_config = get_ollama_config()
    preferred_model = _model_for_purpose(ollama_config, purpose)
    model = _select_ollama_vision_model(preferred_model, ollama_config)
    timeout = _timeout_for_purpose(purpose)
    url = f"{ollama_config['base_url'].rstrip('/')}/api/chat"
    image_b64 = base64.b64encode(image_bytes).decode("ascii")
    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "images": [image_b64],
            "content": prompt,
        }],
        "stream": False,
        "think": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": 0,
        },
    }
    keep_alive = _keep_alive_for_purpose(purpose)
    if keep_alive:
        payload["keep_alive"] = keep_alive

    resp = httpx.post(url, json=payload, timeout=timeout)
    result = resp.json()
    if "message" not in result:
        raise Exception(f"Ollama vision API error: {result}")
    return result["message"]["content"].strip(), model


def _select_ollama_vision_model(preferred_model: str, ollama_config: dict) -> str:
    candidates = [
        preferred_model,
        ollama_config.get("copilot_model"),
        ollama_config.get("categorize_model"),
        OLLAMA_MODEL_RECEIPT,
    ]
    for candidate in candidates:
        model = (candidate or "").strip()
        family = model.lower()
        if model and ("gemma4" in family or "gemma3" in family):
            return model
    return OLLAMA_MODEL_RECEIPT
