"""LLM calls via the OpenAI Chat Completions API (strict JSON)."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

from openai import OpenAI

from investor_etl.config import Settings

_JSON_FENCE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)


def _extract_json_obj(text: str) -> dict[str, Any]:
    text = text.strip()
    m = _JSON_FENCE.search(text)
    if m:
        text = m.group(1).strip()
    return json.loads(text)


@lru_cache(maxsize=8)
def _openai_client(api_key: str, base_url: str, timeout_s: float) -> OpenAI:
    return OpenAI(api_key=api_key, base_url=base_url, timeout=timeout_s)


def chat_completions_json(
    settings: Settings,
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_s: float = 120.0,
) -> dict[str, Any]:
    key = (settings.openai_api_key or "").strip()
    if not key:
        raise RuntimeError(
            "Missing credentials for LLM calls. Set OPENAI_API_KEY "
            "(or legacy DATABRICKS_TOKEN when reusing that secret)."
        )

    base_url = settings.openai_base_url.rstrip("/")
    client = _openai_client(key, base_url, timeout_s)
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )

    choice = completion.choices[0]
    content = choice.message.content
    if content is None:
        raise RuntimeError("LLM returned empty message content")
    return _extract_json_obj(content)


EXTRACTION_SYSTEM = """You extract portfolio companies from a single investor's portfolio page.
Return STRICT JSON only with this shape:
{"companies":[{"company_name":"...","company_website":"https://...","description":null,"evidence_span":"verbatim quote","confidence":0.0}]}
Rules:
- evidence_span must be copied from the input text.
- confidence between 0 and 1.
- Include every distinct company you can support with evidence; omit guesses."""

BUILT_WORLD_SYSTEM = """You classify whether a company belongs to the **built world**: the physical built environment
and related industries (e.g. real estate, construction, infrastructure, cities, buildings, civil engineering,
asset-heavy property and facilities value chain). Pure software or finance with no meaningful link to physical
built assets is typically NOT built world.

Return STRICT JSON only:
{"is_built_world": true or false}"""

THEME_SYSTEM = """The company has already been classified as participating in the built-world ecosystem.

Assign exactly ONE high-level **theme** label that best describes the company's primary focus (e.g. sector or strategic theme).
Use concise labels consistent across similar companies.

Return STRICT JSON only:
{"theme":"..."}"""

MAIN_CATEGORY_SYSTEM = """The company is built-world. A **theme** has already been chosen for it.

Choose exactly ONE **main_category** that is coherent WITHIN that theme (narrower than the theme, broader than subcategory).

Return STRICT JSON only:
{"main_category":"..."}"""

SUBCATEGORY_SYSTEM = """The company is built-world. **theme** and **main_category** are already fixed.

Choose exactly ONE **subcategory** that fits WITHIN that main category (most specific label).

Return STRICT JSON only:
{"subcategory":"..."}"""

NON_BUILT_WORLD_THEME_SYSTEM = """The company is NOT built-world, but we still want one best-fit theme label
for analytics/reporting. Use a concise label.

Return STRICT JSON only:
{"theme":"..."}"""
