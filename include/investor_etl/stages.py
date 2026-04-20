"""Stage runners invoked from Airflow DAG tasks."""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

from investor_etl.config import Settings, fully_qualified_table
from investor_etl.databricks_sql import execute_sql, fetch_all
from investor_etl.ddl import create_all_tables_sql
from investor_etl.jina_client import fetch_url, utcnow
from investor_etl.llm_client import (
    CLASSIFICATION_SYSTEM,
    EXTRACTION_SYSTEM,
    chat_openai_compatible,
)
from investor_etl.models import parse_classification, parse_portfolio_extractions
from investor_etl.mysql_source import iter_investors
from investor_etl.portfolio_detection import extract_candidate_urls, pick_best_portfolio_url
from investor_etl.sql_escape import lit


def normalize_domain(url: str) -> str:
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    netloc = urlparse(u).netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def stable_company_id(normalized_domain: str) -> str:
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    return str(uuid.uuid5(ns, normalized_domain))


def ensure_schema_and_tables(settings: Settings) -> None:
    from investor_etl.databricks_sql import execute_many

    execute_many(settings, create_all_tables_sql(settings))


def stage1_mysql_to_bronze(settings: Settings, source_system: str = "mysql") -> int:
    inv = iter_investors(settings)
    t = fully_qualified_table(settings, "bronze_investors")
    now = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    count = 0
    for row in inv:
        sql = f"""
MERGE INTO {t} AS t
USING (SELECT
  {lit(row.investor_id)} AS investor_id,
  {lit(row.investor_name)} AS investor_name,
  {lit(row.source_website)} AS source_website,
  {lit(source_system)} AS source_system,
  TIMESTAMP('{now}') AS ingested_at
) AS s
ON t.investor_id = s.investor_id
WHEN MATCHED THEN UPDATE SET
  investor_name = s.investor_name,
  source_website = s.source_website,
  source_system = s.source_system,
  ingested_at = s.ingested_at
WHEN NOT MATCHED THEN INSERT (
  investor_id, investor_name, source_website, source_system, ingested_at
) VALUES (
  s.investor_id, s.investor_name, s.source_website, s.source_system, s.ingested_at
)
"""
        execute_sql(settings, sql)
        count += 1
    return count


def stage1_fetch_homepages(settings: Settings) -> int:
    t_inv = fully_qualified_table(settings, "bronze_investors")
    t_fetch = fully_qualified_table(settings, "bronze_web_fetches")
    t_pages = fully_qualified_table(settings, "silver_investor_pages")
    rows = fetch_all(
        settings,
        f"SELECT investor_id, source_website FROM {t_inv}",
    )
    n = 0
    for investor_id, website in rows:
        fr = fetch_url(settings, website)
        fetch_id = str(uuid.uuid4())
        now = utcnow().strftime("%Y-%m-%d %H:%M:%S")
        crawl_status = (
            "success"
            if fr.http_status and 200 <= fr.http_status < 400
            else "error"
        )
        sql_ins = f"""
INSERT INTO {t_fetch} VALUES (
  {lit(fetch_id)},
  {lit("investor")},
  {lit(str(investor_id))},
  {lit(fr.requested_url)},
  {lit(fr.final_url)},
  {fr.http_status if fr.http_status is not None else "NULL"},
  TIMESTAMP('{now}'),
  {lit(fr.raw_text)},
  {lit(fr.raw_markdown)},
  {lit(fr.raw_json)},
  {lit(fr.content_hash)},
  {lit(crawl_status)}
)
"""
        execute_sql(settings, sql_ins)

        merge_pages = f"""
MERGE INTO {t_pages} AS t
USING (SELECT
  {lit(str(investor_id))} AS investor_id,
  {lit(website)} AS homepage_url,
  CAST(NULL AS STRING) AS portfolio_url,
  {lit("seed")} AS detection_method,
  CAST(NULL AS DOUBLE) AS detection_confidence,
  TIMESTAMP('{now}') AS last_verified_at
) AS s
ON t.investor_id = s.investor_id
WHEN MATCHED THEN UPDATE SET
  homepage_url = s.homepage_url,
  last_verified_at = s.last_verified_at
WHEN NOT MATCHED THEN INSERT (
  investor_id, homepage_url, portfolio_url, detection_method, detection_confidence, last_verified_at
) VALUES (
  s.investor_id, s.homepage_url, s.portfolio_url, s.detection_method, s.detection_confidence, s.last_verified_at
)
"""
        execute_sql(settings, merge_pages)
        n += 1
    return n


def stage1_detect_portfolio_candidates(settings: Settings) -> int:
    t_inv = fully_qualified_table(settings, "bronze_investors")
    t_fetch = fully_qualified_table(settings, "bronze_web_fetches")
    t_cand = fully_qualified_table(settings, "bronze_portfolio_url_candidates")
    t_pages = fully_qualified_table(settings, "silver_investor_pages")

    investors = fetch_all(settings, f"SELECT investor_id, source_website FROM {t_inv}")
    total = 0
    now = utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for investor_id, homepage in investors:
        rows = fetch_all(
            settings,
            f"""
SELECT raw_text, raw_markdown
FROM {t_fetch}
WHERE entity_type = 'investor' AND entity_id = {lit(str(investor_id))}
ORDER BY fetched_at DESC
LIMIT 1
""",
        )
        text = None
        if rows:
            raw_text, raw_md = rows[0]
            text = raw_text or raw_md or ""

        candidates = extract_candidate_urls(str(homepage), text)
        best = pick_best_portfolio_url(candidates)

        for rank, (url, method, conf) in enumerate(candidates, start=1):
            execute_sql(
                settings,
                f"""
INSERT INTO {t_cand} VALUES (
  {lit(str(investor_id))},
  {lit(url)},
  {rank},
  {lit(method)},
  {conf},
  TIMESTAMP('{now}')
)
""",
            )
            total += 1

        if best:
            execute_sql(
                settings,
                f"""
MERGE INTO {t_pages} AS t
USING (SELECT
  {lit(str(investor_id))} AS investor_id,
  {lit(best)} AS portfolio_url,
  {lit("keyword_link_heuristic")} AS detection_method,
  {candidates[0][2] if candidates else 0.5} AS detection_confidence,
  TIMESTAMP('{now}') AS last_verified_at
) AS s
ON t.investor_id = s.investor_id
WHEN MATCHED THEN UPDATE SET
  portfolio_url = s.portfolio_url,
  detection_method = s.detection_method,
  detection_confidence = s.detection_confidence,
  last_verified_at = s.last_verified_at
WHEN NOT MATCHED THEN INSERT (
  investor_id, homepage_url, portfolio_url, detection_method, detection_confidence, last_verified_at
) VALUES (
  s.investor_id, NULL, s.portfolio_url, s.detection_method, s.detection_confidence, s.last_verified_at
)
""",
            )
    return total


def stage2_portfolio_extract(settings: Settings) -> int:
    t_pages = fully_qualified_table(settings, "silver_investor_pages")
    t_fetch = fully_qualified_table(settings, "bronze_web_fetches")
    t_cand = fully_qualified_table(settings, "silver_investor_company_candidates")

    pages = fetch_all(
        settings,
        f"""
SELECT investor_id, homepage_url, portfolio_url
FROM {t_pages}
WHERE portfolio_url IS NOT NULL
""",
    )
    inserted = 0
    extraction_run_id = str(uuid.uuid4())
    now = utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for investor_id, _home, portfolio_url in pages:
        if not portfolio_url:
            continue
        fr = fetch_url(settings, str(portfolio_url))
        fetch_id = str(uuid.uuid4())
        crawl_status = (
            "success"
            if fr.http_status and 200 <= fr.http_status < 400
            else "error"
        )
        execute_sql(
            settings,
            f"""
INSERT INTO {t_fetch} VALUES (
  {lit(fetch_id)},
  {lit("portfolio_page")},
  {lit(str(investor_id))},
  {lit(fr.requested_url)},
  {lit(fr.final_url)},
  {fr.http_status if fr.http_status is not None else "NULL"},
  TIMESTAMP('{now}'),
  {lit(fr.raw_text)},
  {lit(fr.raw_markdown)},
  {lit(fr.raw_json)},
  {lit(fr.content_hash)},
  {lit(crawl_status)}
)
""",
        )

        body = fr.raw_text or fr.raw_markdown or ""
        user_prompt = f"Portfolio page text:\n\n{body[:120000]}"
        raw = chat_openai_compatible(
            settings,
            model=settings.llm_extraction_model,
            system_prompt=EXTRACTION_SYSTEM,
            user_prompt=user_prompt,
        )
        companies = parse_portfolio_extractions(raw)
        for c in companies:
            cid = str(uuid.uuid4())
            execute_sql(
                settings,
                f"""
INSERT INTO {t_cand} VALUES (
  {lit(cid)},
  {lit(str(investor_id))},
  {lit(str(portfolio_url))},
  {lit(c.company_name)},
  {lit(c.company_website)},
  {lit(c.description)},
  {lit(c.evidence_span)},
  {c.confidence},
  {lit(extraction_run_id)},
  TIMESTAMP('{now}')
)
""",
            )
            inserted += 1
    return inserted


def stage3_company_enrichment(settings: Settings) -> int:
    """One Jina fetch per deduped company website; investor↔company map kept for all rows."""
    t_cand = fully_qualified_table(settings, "silver_investor_company_candidates")
    t_comp = fully_qualified_table(settings, "silver_companies")
    t_map = fully_qualified_table(settings, "silver_investor_company_map")
    t_fetch = fully_qualified_table(settings, "bronze_web_fetches")

    distinct_companies = fetch_all(
        settings,
        f"""
SELECT
  company_website,
  MAX_BY(company_name, extracted_at) AS company_name
FROM {t_cand}
GROUP BY company_website
""",
    )

    now = utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for website, company_name in distinct_companies:
        nd = normalize_domain(str(website))
        cid = stable_company_id(nd)

        fr = fetch_url(settings, str(website))
        fetch_id = str(uuid.uuid4())
        crawl_status = (
            "success"
            if fr.http_status and 200 <= fr.http_status < 400
            else "error"
        )
        execute_sql(
            settings,
            f"""
INSERT INTO {t_fetch} VALUES (
  {lit(fetch_id)},
  {lit("company")},
  {lit(cid)},
  {lit(fr.requested_url)},
  {lit(fr.final_url)},
  {fr.http_status if fr.http_status is not None else "NULL"},
  TIMESTAMP('{now}'),
  {lit(fr.raw_text)},
  {lit(fr.raw_markdown)},
  {lit(fr.raw_json)},
  {lit(fr.content_hash)},
  {lit(crawl_status)}
)
""",
        )

        homepage_text = fr.raw_text or fr.raw_markdown or ""
        about_text = homepage_text[:200000]
        domain_last_seen[nd] = now

        execute_sql(
            settings,
            f"""
MERGE INTO {t_comp} AS t
USING (SELECT
  {lit(cid)} AS company_id,
  {lit(str(company_name))} AS canonical_name,
  {lit(str(website))} AS canonical_website,
  {lit(homepage_text)} AS homepage_text,
  {lit(about_text)} AS about_text,
  {lit(nd)} AS normalized_domain,
  TIMESTAMP('{now}') AS first_seen_at,
  TIMESTAMP('{now}') AS last_seen_at
) AS s
ON t.company_id = s.company_id
WHEN MATCHED THEN UPDATE SET
  canonical_name = s.canonical_name,
  canonical_website = s.canonical_website,
  homepage_text = s.homepage_text,
  about_text = s.about_text,
  last_seen_at = s.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  company_id, canonical_name, canonical_website, homepage_text, about_text,
  normalized_domain, first_seen_at, last_seen_at
) VALUES (
  s.company_id, s.canonical_name, s.canonical_website, s.homepage_text, s.about_text,
  s.normalized_domain, s.first_seen_at, s.last_seen_at
)
""",
        )

    links = fetch_all(
        settings,
        f"""
SELECT
  investor_id,
  company_website,
  evidence_text,
  confidence,
  portfolio_url,
  extraction_run_id
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY investor_id, company_website
      ORDER BY extracted_at DESC
    ) AS rn
  FROM {t_cand}
) z
WHERE rn = 1
""",
    )

    upserts = 0
    for investor_id, website, evidence, conf, port_url, ex_run in links:
        nd = normalize_domain(str(website))
        cid = stable_company_id(nd)
        execute_sql(
            settings,
            f"""
MERGE INTO {t_map} AS t
USING (SELECT
  {lit(str(investor_id))} AS investor_id,
  {lit(cid)} AS company_id,
  {lit(str(port_url))} AS source_portfolio_url,
  {lit(str(ex_run))} AS extraction_run_id,
  {lit(str(evidence))} AS evidence_text,
  {conf if conf is not None else "NULL"} AS confidence,
  true AS is_active,
  TIMESTAMP('{now}') AS discovered_at
) AS s
ON t.investor_id = s.investor_id AND t.company_id = s.company_id
WHEN MATCHED THEN UPDATE SET
  source_portfolio_url = s.source_portfolio_url,
  extraction_run_id = s.extraction_run_id,
  evidence_text = s.evidence_text,
  confidence = s.confidence,
  is_active = s.is_active,
  discovered_at = s.discovered_at
WHEN NOT MATCHED THEN INSERT (
  investor_id, company_id, source_portfolio_url, extraction_run_id,
  evidence_text, confidence, is_active, discovered_at
) VALUES (
  s.investor_id, s.company_id, s.source_portfolio_url, s.extraction_run_id,
  s.evidence_text, s.confidence, s.is_active, s.discovered_at
)
""",
        )
        upserts += 1
    return upserts


def stage4_classify(settings: Settings, classifier_version: str | None = None) -> int:
    version = classifier_version or os.getenv("CLASSIFIER_VERSION", "v1")
    t_comp = fully_qualified_table(settings, "silver_companies")
    t_cls = fully_qualified_table(settings, "silver_company_classification")

    companies = fetch_all(
        settings,
        f"""
SELECT c.company_id, c.homepage_text, c.about_text
FROM {t_comp} c
LEFT ANTI JOIN {t_cls} cl
  ON c.company_id = cl.company_id AND cl.classifier_version = {lit(version)}
""",
    )

    n = 0
    now = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for company_id, home_txt, about_txt in companies:
        blob = f"{home_txt or ''}\n\n{about_txt or ''}"[:120000]
        user_prompt = f"Company website content:\n\n{blob}"
        raw = chat_openai_compatible(
            settings,
            model=settings.llm_classification_model,
            system_prompt=CLASSIFICATION_SYSTEM,
            user_prompt=user_prompt,
        )
        cls = parse_classification(raw)
        execute_sql(
            settings,
            f"""
INSERT INTO {t_cls} VALUES (
  {lit(str(company_id))},
  {lit(cls.theme)},
  {lit(cls.main_category)},
  {lit(cls.subcategory)},
  {lit(version)},
  {cls.confidence},
  {lit(cls.rationale)},
  TIMESTAMP('{now}')
)
""",
        )
        n += 1
    return n


def stage5_dbt(project_dir: str, profiles_dir: str | None = None) -> None:
    import subprocess

    env = os.environ.copy()
    if profiles_dir:
        env["DBT_PROFILES_DIR"] = profiles_dir

    subprocess.run(
        ["dbt", "source", "freshness"],
        cwd=project_dir,
        env=env,
        check=True,
    )
    subprocess.run(
        ["dbt", "build"],
        cwd=project_dir,
        env=env,
        check=True,
    )
