from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any


def _extract_first_json_object(text: str) -> dict[str, Any] | None:
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : idx + 1]
                try:
                    parsed = json.loads(candidate)
                except Exception:
                    return None
                return parsed if isinstance(parsed, dict) else None
    return None


def _call_openai_json(*, model: str, prompt: str, temperature: float, max_output_tokens: int, api_key_env: str, retries: int = 2) -> dict[str, Any]:
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f"{api_key_env} is not set. Export it before running OpenAI LLM interpretation.")

    try:
        from openai import OpenAI  # type: ignore
    except Exception as exc:
        raise RuntimeError("OpenAI SDK not installed. Run: pip install openai") from exc

    client = OpenAI(api_key=api_key)

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = client.responses.create(
                model=model,
                input=prompt,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )

            text = getattr(response, "output_text", None)
            if not text:
                text = ""
                for item in getattr(response, "output", []):
                    for content in getattr(item, "content", []):
                        if getattr(content, "type", "") in {"output_text", "text"}:
                            text += getattr(content, "text", "")
            try:
                parsed = json.loads(text)
                return {"parsed": parsed, "raw": text, "parse_error": False}
            except Exception:
                extracted = _extract_first_json_object(text)
                return {"parsed": extracted, "raw": text, "parse_error": extracted is None}
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"OpenAI LLM call failed: {exc}") from exc

    raise RuntimeError(f"OpenAI LLM call failed: {last_error}")


def _call_ollama_json(*, model: str, prompt: str, temperature: float, num_ctx: int, ollama_url: str, timeout_seconds: int) -> dict[str, Any]:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature, "num_ctx": num_ctx},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(ollama_url, data=data, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        if exc.code == 404 or "model" in body.lower() and "not found" in body.lower():
            raise RuntimeError(f"Ollama model {model} is not available. Pull it with: ollama pull {model}") from exc
        raise RuntimeError(f"Ollama request failed ({exc.code}): {body[:240]}") from exc
    except urllib.error.URLError as exc:
        reason = str(getattr(exc, "reason", exc)).lower()
        if "connection refused" in reason or "failed to establish a new connection" in reason:
            raise RuntimeError("Ollama is not running. Start it with: ollama serve") from exc
        if "timed out" in reason:
            raise RuntimeError(f"Ollama request timed out after {timeout_seconds}s") from exc
        raise RuntimeError(f"Ollama connection error: {exc}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"Ollama request timed out after {timeout_seconds}s") from exc

    try:
        resp_json = json.loads(raw)
    except Exception:
        return {"parsed": None, "raw": raw, "parse_error": True}

    text = resp_json.get("response", "")
    if not isinstance(text, str):
        text = json.dumps(text)

    try:
        parsed = json.loads(text)
        return {"parsed": parsed, "raw": text, "parse_error": False}
    except Exception:
        extracted = _extract_first_json_object(text)
        return {"parsed": extracted, "raw": text, "parse_error": extracted is None}


def call_llm_json(*, provider: str, model: str, prompt: str, temperature: float, max_output_tokens: int, api_key_env: str, retries: int = 2, ollama_url: str = "http://127.0.0.1:11434/api/generate", timeout_seconds: int = 600, num_ctx: int = 8192) -> dict[str, Any]:
    if provider == "ollama":
        return _call_ollama_json(
            model=model,
            prompt=prompt,
            temperature=temperature,
            num_ctx=num_ctx,
            ollama_url=ollama_url,
            timeout_seconds=timeout_seconds,
        )
    if provider == "openai":
        return _call_openai_json(
            model=model,
            prompt=prompt,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            api_key_env=api_key_env,
            retries=retries,
        )
    if provider == "none":
        return {"parsed": None, "raw": "", "parse_error": False, "skipped": True}
    raise ValueError("Unsupported llm provider: {provider}. Supported providers: ollama, openai, none".format(provider=provider))
