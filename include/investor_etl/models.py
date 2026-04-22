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


class BuiltWorldClassification(BaseModel):
    """Gate: whether the company participates in the built-world / physical asset ecosystem."""

    is_built_world: bool


class ThemeClassification(BaseModel):
    theme: str = Field(..., min_length=1)


class MainCategoryClassification(BaseModel):
    main_category: str = Field(..., min_length=1)


class SubcategoryClassification(BaseModel):
    subcategory: str = Field(..., min_length=1)


def parse_portfolio_extractions(payload: dict[str, Any]) -> list[PortfolioCompanyExtract]:
    """Expects { \"companies\": [ { ... }, ... ] }."""
    raw_list = payload.get("companies")
    if raw_list is None:
        raise ValueError("Missing 'companies' array in extraction JSON")
    if not isinstance(raw_list, list):
        raise ValueError("'companies' must be an array")
    return [PortfolioCompanyExtract.model_validate(item) for item in raw_list]


def parse_built_world(payload: dict[str, Any]) -> BuiltWorldClassification:
    return BuiltWorldClassification.model_validate(payload)


def parse_theme(payload: dict[str, Any]) -> ThemeClassification:
    return ThemeClassification.model_validate(payload)


def parse_main_category(payload: dict[str, Any]) -> MainCategoryClassification:
    return MainCategoryClassification.model_validate(payload)


def parse_subcategory(payload: dict[str, Any]) -> SubcategoryClassification:
    return SubcategoryClassification.model_validate(payload)
