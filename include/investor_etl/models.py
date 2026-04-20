"""Strict JSON payloads for LLM extraction and classification."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class PortfolioCompanyExtract(BaseModel):
    company_name: str = Field(..., min_length=1)
    company_website: str = Field(..., min_length=1)
    description: str | None = None
    evidence_span: str = Field(..., min_length=1)
    confidence: float = Field(..., ge=0.0, le=1.0)

    @field_validator("company_website")
    @classmethod
    def normalize_url(cls, v: str) -> str:
        u = v.strip()
        if not u.startswith(("http://", "https://")):
            u = "https://" + u
        return u


class CompanyClassification(BaseModel):
    theme: str = Field(..., min_length=1)
    main_category: str = Field(..., min_length=1)
    subcategory: str = Field(..., min_length=1)
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: str = Field(..., min_length=1)


def parse_portfolio_extractions(payload: dict[str, Any]) -> list[PortfolioCompanyExtract]:
    """Expects { \"companies\": [ { ... }, ... ] }."""
    raw_list = payload.get("companies")
    if raw_list is None:
        raise ValueError("Missing 'companies' array in extraction JSON")
    if not isinstance(raw_list, list):
        raise ValueError("'companies' must be an array")
    return [PortfolioCompanyExtract.model_validate(item) for item in raw_list]


def parse_classification(payload: dict[str, Any]) -> CompanyClassification:
    return CompanyClassification.model_validate(payload)
