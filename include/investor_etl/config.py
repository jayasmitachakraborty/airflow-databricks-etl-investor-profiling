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

    databricks_host: str
    databricks_http_path: str
    databricks_token: str
    databricks_catalog: str
    databricks_schema: str

    jina_api_key: str | None
    jina_reader_base_url: str

    llm_api_base: str | None
    llm_api_key: str | None
    llm_extraction_model: str
    llm_classification_model: str

    @classmethod
    def from_env(cls) -> "Settings":
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
            databricks_host=_req("DATABRICKS_HOST"),
            databricks_http_path=_req("DATABRICKS_HTTP_PATH"),
            databricks_token=_req("DATABRICKS_TOKEN"),
            databricks_catalog=_req("DATABRICKS_CATALOG", "main"),
            databricks_schema=_req("DATABRICKS_SCHEMA", "investor_profiling"),
            jina_api_key=_opt("JINA_API_KEY"),
            jina_reader_base_url=os.getenv(
                "JINA_READER_BASE_URL", "https://r.jina.ai"
            ),
            llm_api_base=_opt("DATABRICKS_MODEL_ENDPOINT"),
            llm_api_key=_opt("DATABRICKS_TOKEN"),
            llm_extraction_model=os.getenv("LLM_EXTRACTION_MODEL", "databricks-meta-llama-3-3-70b-instruct"),
            llm_classification_model=os.getenv(
                "LLM_CLASSIFICATION_MODEL", "databricks-meta-llama-3-3-70b-instruct"
            ),
        )


def fully_qualified_table(settings: Settings, name: str) -> str:
    return f"`{settings.databricks_catalog}`.`{settings.databricks_schema}`.`{name}`"
