"""Heuristic portfolio URL candidates from homepage HTML/text."""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

_LINK_RE = re.compile(
    r'href=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_MD_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
_KEYWORDS = (
    "portfolio",
    "companies",
    "investments",
    "portfolio-companies",
    "our-companies",
    "investment",
)


def extract_candidate_urls(homepage_url: str, page_text: str | None) -> list[tuple[str, str, float]]:
    """
    Returns list of (absolute_url, method, confidence in [0,1]).
    """
    if not page_text:
        return []

    base = homepage_url if homepage_url.startswith("http") else f"https://{homepage_url}"
    seen: set[str] = set()
    out: list[tuple[str, str, float]] = []

    link_matches = list(_LINK_RE.finditer(page_text)) + list(
        _MD_LINK_RE.finditer(page_text)
    )
    for m in link_matches:
        href = m.group(1).strip()
        if href.startswith("#") or href.lower().startswith("mailto:"):
            continue
        abs_url = urljoin(base, href)
        low = abs_url.lower()
        path = urlparse(low).path.lower()

        score = 0.0
        reason = "link_keyword"
        for kw in _KEYWORDS:
            if kw in low or kw in path:
                score = max(score, 0.85)
        if score <= 0 and "/team" in path:
            continue
        if score <= 0:
            continue

        if abs_url not in seen:
            seen.add(abs_url)
            out.append((abs_url, reason, score))

    out.sort(key=lambda x: -x[2])
    return out[:10]


def pick_best_portfolio_url(candidates: list[tuple[str, str, float]]) -> str | None:
    if not candidates:
        return None
    return candidates[0][0]
