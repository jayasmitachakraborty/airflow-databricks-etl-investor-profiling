"""Environment-driven settings for pipelines."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _req(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def _opt(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    return v if v else None


@dataclass(frozen=True)
class Settings:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    mysql_investors_query: str
    mysql_taxonomy_database: str

    databricks_host: str
    databricks_http_path: str
    databricks_token: str
    databricks_catalog: str
    databricks_schema: str

    jina_api_key: str | None
    jina_reader_base_url: str

    openai_api_key: str | None
    openai_base_url: str
    llm_extraction_model: str
    llm_classification_model: str
    llm_built_world_model: str
    llm_theme_model: str
    llm_main_category_model: str
    llm_subcategory_model: str

    @classmethod
    def from_env(cls) -> "Settings":
        cls_default = os.getenv("LLM_CLASSIFICATION_MODEL", "gpt-4o-mini")

        raw_base = _opt("OPENAI_BASE_URL") or _opt("OPENAI_API_BASE")
        legacy_endpoint = _opt("DATABRICKS_MODEL_ENDPOINT")
        if raw_base:
            openai_base_url = raw_base.rstrip("/")
        elif legacy_endpoint:
            ep = legacy_endpoint.rstrip("/")
            if ep.endswith("/chat/completions"):
                openai_base_url = ep[: -len("/chat/completions")].rstrip("/")
            else:
                openai_base_url = ep.rstrip("/")
        else:
            openai_base_url = "https://api.openai.com/v1"

        return cls(
            mysql_host=_req("MYSQL_HOST", "localhost"),
            mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
            mysql_user=_req("MYSQL_USER", "root"),
            mysql_password=os.getenv("MYSQL_PASSWORD") or "",
            mysql_database=_req("MYSQL_DATABASE", "investors"),
            mysql_investors_query=_req(
                "MYSQL_INVESTORS_QUERY",
                "SELECT investor_id, investor_name, website AS source_website FROM investors",
            ),
            mysql_taxonomy_database=os.getenv("MYSQL_TAXONOMY_DATABASE", "noa_taxonomy"),
            databricks_host=_req("DATABRICKS_HOST"),
            databricks_http_path=_req("DATABRICKS_HTTP_PATH"),
            databricks_token=_req("DATABRICKS_TOKEN"),
            databricks_catalog=_req("DATABRICKS_CATALOG", "investor_profiling"),
            databricks_schema=_req("DATABRICKS_SCHEMA", "investor_profiling"),
            jina_api_key=_opt("JINA_API_KEY"),
            jina_reader_base_url=os.getenv(
                "JINA_READER_BASE_URL", "https://r.jina.ai"
            ),
            openai_api_key=_opt("OPENAI_API_KEY") or _opt("DATABRICKS_TOKEN"),
            openai_base_url=openai_base_url,
            llm_extraction_model=os.getenv("LLM_EXTRACTION_MODEL", "gpt-4o-mini"),
            llm_classification_model=cls_default,
            llm_built_world_model=os.getenv("LLM_BUILT_WORLD_MODEL") or cls_default,
            llm_theme_model=os.getenv("LLM_THEME_MODEL") or cls_default,
            llm_main_category_model=os.getenv("LLM_MAIN_CATEGORY_MODEL") or cls_default,
            llm_subcategory_model=os.getenv("LLM_SUBCATEGORY_MODEL") or cls_default,
        )


def fully_qualified_table(settings: Settings, name: str) -> str:
    return f"`{settings.databricks_catalog}`.`{settings.databricks_schema}`.`{name}`"
