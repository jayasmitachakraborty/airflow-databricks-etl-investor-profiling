# Airflow → Databricks investor profiling pipeline

This repository implements a five-stage Airflow DAG set plus a dbt project. The goal is to discover investors from MySQL, crawl public pages through [Jina AI Reader](https://jina.ai/reader), extract portfolio companies with an LLM, enrich each company website, classify themes and categories, then publish curated marts for dashboards.

## Architecture

| DAG | ID | Responsibility |
|-----|----|----------------|
| 1 | `investor_discovery_raw_crawl` | MySQL → `bronze_investors`; Jina homepage crawl → `bronze_web_fetches`; portfolio URL heuristics → `bronze_portfolio_url_candidates` + `silver_investor_pages` |
| 2 | `portfolio_extraction_llm` | Best portfolio URL from `silver_investor_pages`; Jina fetch; strict JSON extraction → `silver_investor_company_candidates` |
| 3 | `company_enrichment` | Dedupe by website; one Jina fetch per domain; `silver_companies` + `silver_investor_company_map` |
| 4 | `company_classification` | LLM classification (JSON + Pydantic validation) → `silver_company_classification` |
| 5 | `dbt_build_marts` | `dbt source freshness`, `dbt build` (staging, conformed dims/facts, marts) |

Relationships, page text, and classification are stored separately so you can **reclassify without re-scraping** (new `classifier_version` rows) or **re-scrape without losing history** (append-only bronze fetches and timestamped silver rows).

## Repository layout

- `dags/` — Airflow DAG definitions (Python)
- `include/investor_etl/` — Shared connectors, DDL, LLM prompts, and stage runners
- `dbt/` — dbt-databricks project (staging, `dim_company` / `fact_company_classification`, dashboard marts)

Point `AIRFLOW__CORE__DAGS_FOLDER` at `dags/` and add `include/` to `PYTHONPATH` (or rely on the `sys.path` bootstrap in each DAG file).

## Run with Docker

Quick start from the repo root:

```bash
cp .env.example .env
# Set AIRFLOW_UID=$(id -u) on Linux/macOS for ./logs permissions; add DATABRICKS_*, MYSQL_*, etc.
docker compose up --build -d
```

Then open [http://localhost:8080](http://localhost:8080) (default **admin** / **admin**).

**Full Docker documentation** — services, bind mounts, env vars, and troubleshooting: [docker/README.md](docker/README.md).

## Environment variables

### MySQL (DAG 1)

| Variable | Description |
|----------|-------------|
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Connection to the source CRM or internal list |
| `MYSQL_INVESTORS_QUERY` | Must return columns `investor_id`, `investor_name`, `source_website` |

### Databricks SQL warehouse (all DAGs touching Delta)

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace host (no `https://`) |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DATABRICKS_TOKEN` | PAT with access to the catalog/schema |
| `DATABRICKS_CATALOG` | Unity Catalog catalog (default `main`) |
| `DATABRICKS_SCHEMA` | Schema for raw and silver tables (default `investor_profiling`) |

### Jina Reader (DAGs 1–3)

| Variable | Description |
|----------|-------------|
| `JINA_API_KEY` | Optional; omit for public reader limits |
| `JINA_READER_BASE_URL` | Default `https://r.jina.ai` |

### LLM (DAGs 2 and 4)

The code calls an **OpenAI-compatible** chat-completions endpoint and requests `response_format: json_object`. Point it at your Databricks Model Serving route or any compatible gateway.

| Variable | Description |
|----------|-------------|
| `DATABRICKS_MODEL_ENDPOINT` | Full HTTPS URL to the chat completions API |
| `LLM_EXTRACTION_MODEL` | Model id for portfolio extraction |
| `LLM_CLASSIFICATION_MODEL` | Model id for company classification |
| `CLASSIFIER_VERSION` | Logical version string stored on each classification row (default `v1`) |

### dbt (DAG 5)

| Variable | Description |
|----------|-------------|
| `DBT_PROJECT_DIR` | Defaults to `./dbt` relative to the repo root |
| `DBT_PROFILES_DIR` | Directory containing `profiles.yml` (local: `dbt/profiles.yml.example`; Docker: `docker/dbt/profiles.yml` mounted at `/opt/airflow/.dbt`) |

## dbt setup

1. Copy `dbt/profiles.yml.example` into your dbt profile directory and adjust targets.
2. Ensure `DATABRICKS_*` variables match the Delta tables written by Airflow.
3. Run locally:

```bash
cd dbt
dbt debug
dbt source freshness
dbt build
```

Freshness is configured on `bronze_investors.ingested_at` so stale upstream loads surface before marts rebuild.

## Operational notes

- **DAG ordering**: Run DAG 1 first; DAGs 2–4 are triggered manually or via orchestration (`TriggerDagRunOperator`, datasets, or your scheduler policies).
- **Secrets**: Prefer Airflow Connections / Variables or a secret backend instead of plain environment variables in production.
- **Retries**: Network-heavy tasks (Jina, LLM) may need backoff and rate limits tuned per provider SLA.
- **Databricks-native LLMs**: For fully hosted flows, swap the HTTP client for Databricks Foundation Model APIs or Mosaic AI Model Serving; keep the **strict JSON contract** and Pydantic validation at the boundary before writes to silver tables.

## Python dependencies

- **Docker:** dependencies are baked in via `Dockerfile` + `requirements-docker.txt`.
- **Non-Docker:** install Airflow and libraries from `requirements.txt` on your scheduler and workers.

---

This design mirrors the layered medallion layout:
bronze captures immutable fetch artifacts, silver separates entities and relationships, classification is versioned, and gold marts aggregate for analytics without collapsing those concerns into a single wide table.
