"""Fetch readable page content via Jina AI Reader."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from investor_etl.config import Settings


@dataclass(frozen=True)
class JinaFetchResult:
    requested_url: str
    final_url: str | None
    http_status: int | None
    raw_text: str | None
    raw_markdown: str | None
    raw_json: str | None
    content_hash: str


def _hash_content(text: str | None) -> str:
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fetch_url(settings: Settings, url: str, timeout_s: float = 120.0) -> JinaFetchResult:
    """
    Uses Jina Reader: GET {base}/{url} with optional Authorization header.
    See https://jina.ai/reader
    """
    reader_url = f"{settings.jina_reader_base_url.rstrip('/')}/{url}"
    headers: dict[str, str] = {"Accept": "application/json"}
    if settings.jina_api_key:
        headers["Authorization"] = f"Bearer {settings.jina_api_key}"

    timeout = httpx.Timeout(timeout_s, connect=min(30.0, timeout_s))
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(reader_url, headers=headers)

    body_text = resp.text
    raw_json: str | None = None
    raw_md: str | None = None
    plain: str | None = body_text

    ct = resp.headers.get("content-type", "")
    if "application/json" in ct:
        try:
            payload: dict[str, Any] = resp.json()
            raw_json = json.dumps(payload, ensure_ascii=False)
            plain = payload.get("data") or payload.get("content") or payload.get("text")
            if isinstance(plain, dict):
                plain = json.dumps(plain, ensure_ascii=False)
            raw_md = payload.get("markdown") or payload.get("md")
        except json.JSONDecodeError:
            raw_json = None

    content_for_hash = plain or body_text
    return JinaFetchResult(
        requested_url=url,
        final_url=str(resp.headers.get("Location") or url),
        http_status=resp.status_code,
        raw_text=plain,
        raw_markdown=raw_md,
        raw_json=raw_json,
        content_hash=_hash_content(content_for_hash),
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
