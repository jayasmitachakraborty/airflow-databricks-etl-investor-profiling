# Docker setup for Airflow

This guide covers running the investor profiling DAGs with **Docker Compose** from the **repository root** (`docker-compose.yml` and `Dockerfile` live next to `dags/`, not inside this folder).

## Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/) and [Docker Compose V2](https://docs.docker.com/compose/) (`docker compose version`).
- Enough disk and RAM for Postgres + Airflow (roughly **4 GB+** RAM recommended for local use).
- Network access from the stack to **MySQL**, **Databricks**, **Jina**, and your **LLM endpoint** (as required by the DAGs you enable).

## What gets started

| Service | Role |
|---------|------|
| `postgres` | Airflow metadata database (internal Docker network only; no host port is published). |
| `airflow-init` | One-shot: `airflow db migrate` and bootstrap admin user. |
| `airflow-scheduler` | Schedules and runs DAG tasks (`LocalExecutor`). |
| `airflow-webserver` | Web UI on **http://localhost:8080**. |

The custom image extends **`apache/airflow:2.8.4-python3.11`** and installs **`requirements-docker.txt`** (pipeline libraries only; the base image already provides Airflow).

## First-time setup

From the **repo root**:

```bash
cp .env.example .env
```

Edit `.env`:

1. **`AIRFLOW_UID`** — On Linux and macOS, set this to your host user id so bind-mounted `./logs` is writable:

   ```bash
   id -u
   ```

   Put that number in `.env` as `AIRFLOW_UID=...` (the example file uses `50000` as a fallback).

2. **Pipeline secrets** — Set at least the variables you need for the DAGs you plan to run (see tables below). Compose injects these into every Airflow container.

3. **`AIRFLOW_FERNET_KEY`** (optional for local dev) — The compose file defaults to a development key. For shared or long-lived environments, generate one and set it explicitly:

   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

Build and start:

```bash
docker compose up --build -d
```

Default UI login: **`admin` / `admin`**. Change the password after first login.

## Environment variables (Docker)

Compose reads `.env` in the repo root for **variable substitution** and passes the following into containers (defaults shown where applicable).

### Airflow

| Variable | Purpose |
|----------|---------|
| `AIRFLOW_UID` | Host UID for `./logs` ownership (Linux/macOS). |
| `AIRFLOW_FERNET_KEY` | Encrypts connections/variables in the metadata DB. |

### Databricks (DAGs 1–5, dbt)

| Variable | Purpose |
|----------|---------|
| `DATABRICKS_HOST` | Workspace hostname **without** `https://`. |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path. |
| `DATABRICKS_TOKEN` | Personal access token. |
| `DATABRICKS_CATALOG` | Unity Catalog name (default `main`). |
| `DATABRICKS_SCHEMA` | Schema for bronze/silver tables (default `investor_profiling`). |

### MySQL (DAG 1)

| Variable | Purpose |
|----------|---------|
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Source DB for investor list. |
| `MYSQL_INVESTORS_QUERY` | Must return `investor_id`, `investor_name`, `source_website`. |

### Crawling and LLM

| Variable | Purpose |
|----------|---------|
| `JINA_API_KEY` | Optional Jina Reader API key. |
| `JINA_READER_BASE_URL` | Default `https://r.jina.ai`. |
| `DATABRICKS_MODEL_ENDPOINT` | OpenAI-compatible chat completions URL for extraction/classification. |
| `LLM_EXTRACTION_MODEL` / `LLM_CLASSIFICATION_MODEL` | Model identifiers for the chat API. |
| `CLASSIFIER_VERSION` | Stored on classification rows (default `v1`). |

### dbt (DAG 5)

Inside the container:

- `DBT_PROJECT_DIR=/opt/airflow/dbt` (bind mount of repo `./dbt`).
- `DBT_PROFILES_DIR=/opt/airflow/.dbt` (bind mount of `./docker/dbt`, which contains `profiles.yml`).

`profiles.yml` reads the same `DATABRICKS_*` variables as the DAGs.

## Bind mounts

| Host path | Container path | Purpose |
|-----------|----------------|---------|
| `./dags` | `/opt/airflow/dags` | DAG definitions. |
| `./include` | `/opt/airflow/include` | `investor_etl` package (`PYTHONPATH` includes this). |
| `./logs` | `/opt/airflow/logs` | Task logs. |
| `./dbt` | `/opt/airflow/dbt` | dbt project for DAG 5. |
| `./docker/dbt` | `/opt/airflow/.dbt` | dbt `profiles.yml`. |

Edit DAGs or `include/` on the host; the scheduler picks up DAG changes on its refresh interval. **Rebuilding the image** is only required when you change `Dockerfile` or `requirements-docker.txt`.

## Everyday commands

Run from the **repo root**:

```bash
# Follow scheduler/webserver logs
docker compose logs -f airflow-scheduler airflow-webserver

# Open a shell in the same environment as tasks
docker compose exec airflow-scheduler bash

# Stop everything (keeps Postgres volume)
docker compose down

# Stop and delete the Postgres volume (full metadata reset)
docker compose down -v
```

After `down -v`, the next `up` re-runs `airflow-init` (migrations + admin user bootstrap).

## Troubleshooting

- **Permission errors on `./logs`** — Set `AIRFLOW_UID` to your host `id -u` and `docker compose up` again; you may need to `chown -R` the existing `logs/` directory once.
- **DAG import errors** — Check `docker compose logs airflow-scheduler` for Python tracebacks; confirm `.env` has all required variables for `Settings.from_env()` if tasks fail at runtime.
- **Cannot reach MySQL or Databricks** — On Docker Desktop, `localhost` inside a container is **not** your host. Use `host.docker.internal` (Mac/Windows) or the host’s LAN IP, or run dependent services on the Docker network.
- **dbt fails in DAG 5** — Ensure `DATABRICKS_*` are set, `docker/dbt/profiles.yml` matches your workspace, and the SQL warehouse can reach Unity Catalog.

## Production notes

This compose file is aimed at **local development**. For production:

- Use a managed database for Airflow metadata and strong secrets management.
- Rotate `AIRFLOW_FERNET_KEY` and admin credentials; disable default passwords.
- Choose an executor and deployment pattern (Celery, Kubernetes, managed Airflow) that matches scale and HA requirements.
- Restrict network exposure (reverse proxy, TLS, auth) for the webserver.

For the full pipeline design and non-Docker setup, see the [main README](../README.md).
