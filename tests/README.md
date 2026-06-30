# Tests

## Prerequisites

```bash
uv sync --extra dev
```

## Run all tests

```bash
uv run pytest tests/ -v
```

## Run by category

```bash
uv run pytest tests/unit/ -v
uv run pytest tests/integration/ -v
uv run pytest tests/e2e/ -v
```

## Run a specific file

```bash
uv run pytest tests/unit/test_parse_logs.py -v
```

## Run a specific test

```bash
uv run pytest tests/unit/test_parse_logs.py::test_parse_raw_log_extracts_timestamp -v
```

## Structure

| Directory | What goes here |
|-----------|---------------|
| `unit/` | Single-class, single-function tests with tiny synthetic data. No external dependencies. |
| `integration/ | Tests that wire multiple modules together (e.g. parse → clean) or hit real files. |
| `e2e/` | Full pipeline end-to-end on a real-data subset. |

## Current tests

| File | What it tests |
|------|---------------|
| `unit/test_parse_logs.py` | `parse_raw_log()` — regex extraction of all 6 fields from raw Apache CLF lines |
| `unit/test_clean_logs.py` | *(future)* `CleanLogs` — normalization, stripping, timestamp casting |
| `unit/test_star_schema.py` | *(future)* `StarSchema` — dimension builds |
| `integration/test_pipeline.py` | *(future)* Wired parse → clean → star schema on sample data |
| `e2e/test_full_workflow.py` | *(future)* Full pipeline on a real data subset |

## Conventions

- Every test receives the `spark` fixture from `conftest.py` (session-scoped `SparkSession`).
- **Unit tests**: Use `tempfile.TemporaryDirectory` as needed. Test one assertion per function.
- **Integration tests**: May read from `data/raw/` or pre-computed fixtures.
- **E2E tests**: Operate on a small slice of real logs (e.g. first 100 lines).
