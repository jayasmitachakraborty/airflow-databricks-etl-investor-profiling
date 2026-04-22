# Airflow → Databricks investor profiling pipeline

This repository implements a five-stage Airflow DAG set plus a dbt project. The goal is to discover investors from MySQL, crawl public pages through [Jina AI Reader](https://jina.ai/reader), extract portfolio companies with an LLM, enrich each company website, classify themes and categories, then publish curated marts for dashboards.

## Architecture

| DAG | ID | Responsibility |
|-----|----|----------------|
| 1 | `investor_discovery_raw_crawl` | MySQL → `bronze_investors`; Jina homepage crawl → `bronze_web_fetches`; portfolio URL heuristics → `bronze_portfolio_url_candidates` + `silver_investor_pages` |
| 2 | `portfolio_extraction_llm` | Best portfolio URL from `silver_investor_pages`; Jina fetch; strict JSON extraction → `silver_investor_company_candidates` |
| 3 | `company_enrichment` | Dedupe by website; one Jina fetch per domain; `silver_companies` + `silver_investor_company_map` |
| 4 | `company_classification` | Built-world gate, then theme → main category → subcategory (four LLM calls when built-world; strict JSON + Pydantic) → `silver_company_classification` |
| 5 | `dbt_build_marts` | `dbt source freshness`, `dbt build` (staging, conformed dims/facts, marts) |

Relationships, page text, and classification are stored separately so you can **reclassify without re-scraping** (new `classifier_version` rows) or **re-scrape without losing history** (append-only bronze fetches and timestamped silver rows).

## Repository layout

- `dags/` — Airflow DAG definitions (Python)
- `include/investor_etl/` — Shared connectors, DDL, LLM prompts, and stage runners
- `dbt/` — dbt-databricks project (staging, `dim_company` / `fact_company_classification`, dashboard marts)

Point `AIRFLOW__CORE__DAGS_FOLDER` at `dags/` and add `include/` to `PYTHONPATH` (or rely on the `sys.path` bootstrap in each DAG file).

## Run with Docker

`docker-compose.yml` and `Dockerfile` live at the **repository root** (not under `docker/`). Compose is aimed at **local development**; see [Compose vs production](#compose-vs-production) below.

### Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/) and [Docker Compose V2](https://docs.docker.com/compose/) (`docker compose version`).
- Enough resources for Postgres + Airflow (about **4 GB+** RAM is a practical minimum).
- Network reachability from the stack to **MySQL**, **Databricks**, **Jina**, and your **LLM endpoint**, depending on which DAGs you run.

### What gets started

| Service | Role |
|---------|------|
| `postgres` | Airflow metadata database (internal Docker network; no host port published). |
| `airflow-init` | One-shot: `airflow db migrate` and bootstrap admin user. |
| `airflow-scheduler` | Schedules and runs tasks (`LocalExecutor`). |
| `airflow-webserver` | Web UI at [http://localhost:8080](http://localhost:8080). |

The image extends **`apache/airflow:2.8.4-python3.11`** and installs **`requirements-docker.txt`** (pipeline libraries; the base image supplies Airflow).

### First-time setup

From the repo root:

```bash
cp .env.example .env
```

Edit `.env`:

1. **`AIRFLOW_UID`** — On Linux and macOS, set this to your host user id so bind-mounted `./logs` is writable (`id -u`). The example file may use a numeric fallback.
2. **Pipeline variables** — Set at least `DATABRICKS_*`, `MYSQL_*`, and any LLM/Jina values you need (see [Environment variables](#environment-variables)). Compose passes root `.env` into the Airflow containers.
3. **`AIRFLOW_FERNET_KEY`** (optional for local dev) — The compose file may default a development key. For shared or long-lived environments, generate one and set it explicitly:

   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

Build and start:

```bash
docker compose up --build -d
```

Default UI login: **admin** / **admin**. Change the password after first login.

### Bind mounts

| Host path | Container path | Purpose |
|-----------|----------------|---------|
| `./dags` | `/opt/airflow/dags` | DAG definitions. |
| `./include` | `/opt/airflow/include` | `investor_etl` package (`PYTHONPATH` includes this). |
| `./logs` | `/opt/airflow/logs` | Task logs. |
| `./dbt` | `/opt/airflow/dbt` | dbt project (DAG 5). |
| `./docker/dbt` | `/opt/airflow/.dbt` | dbt `profiles.yml`. |

Edit DAGs or `include/` on the host; the scheduler picks up DAG changes on its refresh interval. Rebuild the image only when `Dockerfile` or `requirements-docker.txt` change.

### Everyday commands

From the repo root:

```bash
docker compose logs -f airflow-scheduler airflow-webserver
docker compose exec airflow-scheduler bash
docker compose down
docker compose down -v   # also removes Postgres volume (full metadata reset; next `up` re-runs init)
```

### Troubleshooting (Docker)

- **Permission errors on `./logs`** — Set `AIRFLOW_UID` to your host `id -u` and bring the stack up again; you may need `chown -R` on `logs/` once.
- **DAG import errors** — Check `docker compose logs airflow-scheduler`; ensure `.env` satisfies `Settings.from_env()` for tasks you run.
- **Cannot reach MySQL or Databricks** — Inside a container, `localhost` is not your host. Use `host.docker.internal` (Docker Desktop on Mac/Windows), the host LAN IP, or put services on the same Docker network.
- **dbt in DAG 5** — Confirm `DATABRICKS_*`, `docker/dbt/profiles.yml`, and warehouse access to Unity Catalog.

### Compose vs production

This compose stack is for **local development**. In production, use a managed metadata database, proper secrets, rotated `AIRFLOW_FERNET_KEY` and admin credentials, and an executor/deployment pattern (Celery, Kubernetes, or managed Airflow) that matches scale and HA. Restrict exposure of the web UI (TLS, auth, reverse proxy).

## Environment variables

### Airflow (Docker Compose)

| Variable | Description |
|----------|-------------|
| `AIRFLOW_UID` | Host user id for `./logs` ownership (Linux/macOS); run `id -u` and set in `.env`. |
| `AIRFLOW_FERNET_KEY` | Encrypts connections and variables in the metadata DB; set explicitly outside throwaway local dev. |

### MySQL (DAG 1)

| Variable | Description |
|----------|-------------|
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Connection to the source CRM or internal list |
| `MYSQL_INVESTORS_QUERY` | Must return columns `investor_id`, `investor_name`, `source_website` |

### Databricks SQL warehouse (all DAGs touching Delta)

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace host (no `https://`) |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path (SQL Warehouses → connection details in the Databricks UI) |
| `DATABRICKS_TOKEN` | Personal access token with access to the catalog/schema (user settings → Developer → access tokens) |
| `DATABRICKS_CATALOG` | Unity Catalog catalog (default `investor_profiling`) |
| `DATABRICKS_SCHEMA` | Schema for raw and silver tables (default `investor_profiling`) |

### Jina Reader (DAGs 1–3)

| Variable | Description |
|----------|-------------|
| `JINA_API_KEY` | Optional; omit for public reader limits |
| `JINA_READER_BASE_URL` | Default `https://r.jina.ai` |

### LLM (DAGs 2 and 4)

The code uses the official **OpenAI Python SDK** (`chat.completions.create`) with `response_format: {"type": "json_object"}` and `temperature: 0`. Set `OPENAI_BASE_URL` to the API root (default `https://api.openai.com/v1`) so the same code works against OpenAI, Azure OpenAI (resource base URL), or other OpenAI-compatible gateways.

**DAG 4 workflow:** (1) **built world** classifier → if `is_built_world`, run **theme**, then **main_category** (conditioned on the chosen theme), then **subcategory** (conditioned on theme + main category). Each step uses its **own model id** (fallback: `LLM_CLASSIFICATION_MODEL`). Theme/category steps are skipped when not built-world; those columns are stored as null.

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | API key for the chat completions host (falls back to `DATABRICKS_TOKEN` only if you still reuse that secret) |
| `OPENAI_BASE_URL` | API root including `/v1` when applicable (default `https://api.openai.com/v1` if unset and no legacy endpoint) |
| `DATABRICKS_MODEL_ENDPOINT` | **Legacy:** if set to a full `.../chat/completions` URL and `OPENAI_BASE_URL` is unset, the base URL is derived for the SDK |
| `LLM_EXTRACTION_MODEL` | Model id for portfolio extraction (DAG 2) |
| `LLM_CLASSIFICATION_MODEL` | Default model id for any classification step without its own override |
| `LLM_BUILT_WORLD_MODEL` | Built-world gate (DAG 4) |
| `LLM_THEME_MODEL` | Theme label (DAG 4; only if built-world) |
| `LLM_MAIN_CATEGORY_MODEL` | Main category within theme (DAG 4) |
| `LLM_SUBCATEGORY_MODEL` | Subcategory within main category (DAG 4) |
| `CLASSIFIER_VERSION` | Logical version string stored on each classification row (default `v1`) |

If you previously created `silver_company_classification` with the older column layout, **drop or migrate** that table before running DAG 4 again so `CREATE TABLE` / inserts match the new hierarchy columns.

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
- **Other hosts**: Point `OPENAI_BASE_URL` at any OpenAI-compatible server; keep the **strict JSON contract** and validation at the boundary before writes to silver tables.

## Python dependencies

- **Docker:** dependencies are baked in via `Dockerfile` + `requirements-docker.txt`.
- **Non-Docker:** install Airflow and libraries from `requirements.txt` on your scheduler and workers.

---

This design mirrors the layered medallion layout:
bronze captures immutable fetch artifacts, silver separates entities and relationships, classification is versioned, and gold marts aggregate for analytics without collapsing those concerns into a single wide table.
