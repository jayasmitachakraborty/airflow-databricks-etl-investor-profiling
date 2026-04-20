"""LLM calls returning strict JSON (extraction + classification)."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from investor_etl.config import Settings

_JSON_FENCE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)


def _extract_json_obj(text: str) -> dict[str, Any]:
    text = text.strip()
    m = _JSON_FENCE.search(text)
    if m:
        text = m.group(1).strip()
    return json.loads(text)


def chat_openai_compatible(
    settings: Settings,
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_s: float = 120.0,
) -> dict[str, Any]:
    if not settings.llm_api_base:
        raise RuntimeError("Set DATABRICKS_MODEL_ENDPOINT (OpenAI-compatible chat URL).")

    headers = {"Content-Type": "application/json"}
    if settings.llm_api_key:
        headers["Authorization"] = f"Bearer {settings.llm_api_key}"

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    with httpx.Client(timeout=timeout_s) as client:
        r = client.post(settings.llm_api_base, headers=headers, json=body)
        r.raise_for_status()
        payload = r.json()

    content = payload["choices"][0]["message"]["content"]
    if isinstance(content, dict):
        return content
    return _extract_json_obj(str(content))


EXTRACTION_SYSTEM = """You extract portfolio companies from a single investor's portfolio page.
Return STRICT JSON only with this shape:
{"companies":[{"company_name":"...","company_website":"https://...","description":null,"evidence_span":"verbatim quote","confidence":0.0}]}
Rules:
- evidence_span must be copied from the input text.
- confidence between 0 and 1.
- Include every distinct company you can support with evidence; omit guesses."""


CLASSIFICATION_SYSTEM = """You classify a company using its website text.
Return STRICT JSON only:
{"theme":"...","main_category":"...","subcategory":"...","confidence":0.0,"rationale":"short explanation"}
confidence between 0 and 1."""
