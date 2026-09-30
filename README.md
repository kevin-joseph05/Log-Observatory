# NASA Web Server Log Analytics

Batch observability pipeline over NASA Kennedy Space Center HTTP access logs (Jul–Aug 1995, ~3.5M requests, Apache Common Log Format). PySpark parse + clean → Parquet star-schema → DuckDB analytics → CSV reports.

## What it does

- **Parse:** `src/pipeline/parse_logs.py:parse_raw_log` extracts host, timestamp, method, endpoint, status, bytes from `data/raw/access_log_Jul95` and `access_log_Aug95` via CLF regex.
- **Clean:** `src/pipeline/clean_logs.py:CleanLogs` drops failed parses, maps `-` bytes to 0, strips query strings, casts `dd/MMM/yyyy:HH:mm:ss Z` to timestamp.
- **Model:** `src/schema/star_schema.py:StarSchema` builds `dim_timestamp`, `dim_endpoint`, `dim_status`, `dim_host` + `fact_requests` (partitioned by `year,month`) to `data/curated/`.
- **Analyze:** `analytics/queries.py` runs 4 DuckDB queries to `data/reports/weekly_error_trend.csv`, `p95_bytes.csv`, `top10_errors.csv`, `daily_active_endpoints.csv`.

## Tech stack

Python 3.11, PySpark 3.5.1 (local mode), Parquet, DuckDB, dbt-athena-community, uv, pytest / ruff / mypy, Jupyter. CI: lint + format + typecheck on `push/PR to dev,main`.

## Repo layout

| Path | Purpose |
| ---- | ------- |
| `src/pipeline/` | `parse_logs.py`, `clean_logs.py`, `run_pipeline.py` orchestrator |
| `src/schema/` | `star_schema.py`, `http-codes.json` status lookup |
| `src/utils/` | `spark_session.py` shared local Spark session |
| `analytics/queries.py` | DuckDB views + 4 report queries |
| `data/raw/` | `access_log_Jul95`, `access_log_Aug95` inputs |
| `data/curated/` | `fact_requests/`, `dim_*/` Parquet outputs |
| `data/reports/` | Generated CSV reports |
| `notebooks/01_exploratory_analysis.ipynb` | Spark SQL exploration |
| `observability_dbt/` | dbt staging models (Athena target, in progress) |
| `tests/unit/` | `test_parse_logs`, `test_clean_logs`, `test_star_schema` |

## Quickstart

```bash
uv sync --extra dev
uv run python src/pipeline/run_pipeline.py
uv run python analytics/queries.py
uv run pytest tests/ -v
```

`queries.py` reads `data/curated/**/*.parquet` and writes to `data/reports/`. Lint/typecheck parity with CI: `uv run ruff check src/ && uv run ruff format --check src/ && uv run mypy src/`.

## Results

Week-over-week error rate trend with `LAG()`, P95 response bytes by endpoint category with `PERCENTILE_CONT`, top-10 error endpoints by 4xx/5xx rate, and daily active endpoint counts. See `data/reports/` and `notebooks/01_exploratory_analysis.ipynb`.
