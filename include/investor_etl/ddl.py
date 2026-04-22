"""Databricks Delta DDL for bronze / silver / gold staging tables."""

from __future__ import annotations

from investor_etl.config import Settings, fully_qualified_table


def create_all_tables_sql(settings: Settings) -> list[str]:
    cat = settings.databricks_catalog
    sch = settings.databricks_schema
    fq = lambda t: fully_qualified_table(settings, t)

    return [
        f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`",
        f"""
CREATE TABLE IF NOT EXISTS {fq("bronze_investors")} (
  investor_id STRING NOT NULL,
  investor_name STRING NOT NULL,
  source_website STRING NOT NULL,
  source_system STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("bronze_web_fetches")} (
  fetch_id STRING NOT NULL,
  entity_type STRING NOT NULL,
  entity_id STRING NOT NULL,
  requested_url STRING NOT NULL,
  final_url STRING,
  http_status INT,
  fetched_at TIMESTAMP NOT NULL,
  raw_text STRING,
  raw_markdown STRING,
  raw_json STRING,
  content_hash STRING,
  crawl_status STRING
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("bronze_portfolio_url_candidates")} (
  investor_id STRING NOT NULL,
  candidate_url STRING NOT NULL,
  rank INT NOT NULL,
  detection_method STRING NOT NULL,
  detection_confidence DOUBLE NOT NULL,
  created_at TIMESTAMP NOT NULL
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("silver_investor_pages")} (
  investor_id STRING NOT NULL,
  homepage_url STRING,
  portfolio_url STRING,
  detection_method STRING,
  detection_confidence DOUBLE,
  last_verified_at TIMESTAMP
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("silver_investor_company_candidates")} (
  candidate_id STRING NOT NULL,
  investor_id STRING NOT NULL,
  portfolio_url STRING NOT NULL,
  company_name STRING NOT NULL,
  company_website STRING NOT NULL,
  description STRING,
  evidence_text STRING NOT NULL,
  confidence DOUBLE NOT NULL,
  extraction_run_id STRING NOT NULL,
  extracted_at TIMESTAMP NOT NULL
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("silver_companies")} (
  company_id STRING NOT NULL,
  canonical_name STRING NOT NULL,
  canonical_website STRING NOT NULL,
  homepage_text STRING,
  about_text STRING,
  normalized_domain STRING NOT NULL,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("silver_investor_company_map")} (
  investor_id STRING NOT NULL,
  company_id STRING NOT NULL,
  source_portfolio_url STRING NOT NULL,
  extraction_run_id STRING NOT NULL,
  evidence_text STRING,
  confidence DOUBLE,
  is_active BOOLEAN NOT NULL,
  discovered_at TIMESTAMP NOT NULL
) USING DELTA
""",
        f"""
CREATE TABLE IF NOT EXISTS {fq("silver_company_classification")} (
  company_id STRING NOT NULL,
  classifier_version STRING NOT NULL,
  classified_at TIMESTAMP NOT NULL,
  is_built_world BOOLEAN NOT NULL,
  theme STRING,
  main_category STRING,
  subcategory STRING
) USING DELTA
""",
    ]
