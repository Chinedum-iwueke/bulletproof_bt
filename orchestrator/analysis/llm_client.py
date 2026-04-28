from __future__ import annotations

import json
import os
import time
from typing import Any


def call_llm_json(
    *,
    provider: str,
    model: str,
    prompt: str,
    temperature: float,
    max_output_tokens: int,
    api_key_env: str,
    retries: int = 2,
) -> dict[str, Any]:
    if provider != "openai":
        raise ValueError(f"Unsupported llm provider: {provider}")

    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f"{api_key_env} is not set. Export it before running LLM interpretation.")

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
                return {"parsed": None, "raw": text, "parse_error": True}
        except Exception as exc:  # pragma: no cover - network/runtime uncertainty
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise

    raise RuntimeError(f"LLM call failed: {last_error}")
