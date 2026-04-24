# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Run all tests
uv run pytest tests/ -v

# Run unit tests only (no database required)
uv run pytest tests/unit/ -v

# Run integration tests (requires Hologres database)
uv run pytest tests/integration/ -v -m integration

# Run specific test file
uv run pytest tests/unit/test_tools.py -v

# Run specific test class or method
uv run pytest tests/unit/test_new_tools.py::TestListDynamicTables -v
uv run pytest tests/integration/test_mcp_integration.py::TestMCPTools::test_execute_select -v

# Run with coverage
uv run pytest tests/unit/ --cov=hologres_mcp_server --cov-report=term-missing

# Lint and format
uv run ruff check .
uv run ruff check . --fix
uv run ruff format .
uv run ruff format . --check

# Build package
uv build
```

## Integration Test Setup

Integration tests require a real Hologres connection. Configure via:

```bash
cp tests/integration/.test_mcp_client_env_example tests/integration/.test_mcp_client_env
```

Edit the file with your Hologres credentials (`HOLOGRES_HOST`, `HOLOGRES_PORT`, `HOLOGRES_USER`, `HOLOGRES_PASSWORD`, `HOLOGRES_DATABASE`). Tests will be skipped if the config file is missing or incomplete.

## Architecture

This is a FastMCP v3-based MCP server for Alibaba Cloud Hologres (a PostgreSQL-compatible data warehouse). The entry point is `__init__.py:main()` → `server.py:main()` which parses CLI arguments (`--transport`, `--host`, `--port`) and starts the FastMCP app with the selected transport (stdio by default, also supports streamable-http and sse).

### Core Files

- **`server.py`**: All MCP capabilities registered on a single `FastMCP` app instance via decorators:
  - 39 tools (`@app.tool`) with tags (`query`, `dml`, `ddl`, `admin`, `analysis`, `schema`)
  - 13 resources (`@app.resource`) including parameterized URI templates (`hologres:///{schema}/{table}/...`) and system resources (`system:///...`)
  - 3 prompts (`@app.prompt`)
  - A `@lifespan` handler that validates the DB connection on startup (warns but doesn't fail)

- **`utils.py`**: Database operations layer:
  - `handle_call_tool()` / `handle_read_resource()` — used by the original tools to execute SQL and return formatted results
  - `connect_with_retry()` — opens connections via a lazy singleton `psycopg_pool.ConnectionPool` (min=0, max=5, idle=300s) with automatic fallback to direct `psycopg.connect()` if pool unavailable; retries up to 3 times with 5s delay
  - SQL validation functions (`validate_select_query`, `validate_dml_query`, `validate_ddl_query`)
  - `pglast`-based `try_infer_view_comments()` for propagating column comments from source tables to views

- **`settings.py`**: `get_db_config()` reads environment variables. Falls back from `HOLOGRES_USER`/`HOLOGRES_PASSWORD` to `ALIBABA_CLOUD_ACCESS_KEY_ID`/`ALIBABA_CLOUD_ACCESS_KEY_SECRET` + optional STS token.

- **`__init__.py`**: Re-exports `server.main()` as the package entry point. Registered in `pyproject.toml` as the `hologres-mcp-server` CLI script.

### Tool Implementation Pattern

Tools in server.py follow two delegation patterns:

1. **Original tools** (e.g. `execute_hg_select_sql`, `list_hg_schemas`): Build a SQL string and pass it to `handle_call_tool()` or `handle_read_resource()` from utils.py.
2. **Newer tools** (e.g. `cancel_hg_query`, `list_hg_recyclebin`): Delegate to private `_helper()` functions in server.py that call `connect_with_retry()` directly.

When adding new tools, follow pattern 2: create a private `_helper()` function with the logic, then a thin `@app.tool` wrapper that delegates to it. Helper functions handle their own `try/except` and return formatted strings (never raise).

### Version-Gated Features

Some tools require specific Hologres versions and check at runtime:
- **V1.3+**: `get_hg_table_info_trend` (checks via error message)
- **V3.0+**: `list_hg_query_queues`, `manage_hg_query_queue`, `manage_hg_classifier`, `set_hg_query_queue_property`, `list_hg_external_databases` (check via "does not exist" in error)
- **V4.1+**: `query_hg_external_files` (checks `EXTERNAL_FILES` function availability)

### Key Dependencies

- **fastmcp** (v3+): MCP server framework with decorator-based tool/resource/prompt registration
- **psycopg** (v3) + **psycopg-pool**: PostgreSQL driver and connection pooling for Hologres connectivity
- **pglast** (v7.5+): PostgreSQL SQL parser, used to parse view definitions for comment inference
- **matplotlib**: Chart generation for `query_and_plotly_chart` tool

### SQL Validation

Tools validate SQL type before execution (in `utils.py`):
- `execute_hg_select_sql`: Must start with `SELECT` or `WITH...SELECT` (regex-based)
- `execute_hg_dml_sql`: Must start with `INSERT`, `UPDATE`, `DELETE`, or `REFRESH`
- `execute_hg_ddl_sql`: Must start with `CREATE`, `ALTER`, `DROP`, or `COMMENT ON`

### Environment Variables

Required for database connection:
- `HOLOGRES_HOST`, `HOLOGRES_PORT`, `HOLOGRES_DATABASE`
- `HOLOGRES_USER`, `HOLOGRES_PASSWORD` (or `ALIBABA_CLOUD_ACCESS_KEY_ID`/`ALIBABA_CLOUD_ACCESS_KEY_SECRET` + optional `ALIBABA_CLOUD_SECURITY_TOKEN`)

Optional for integration tests:
- `HOLOGRES_TEST_SCHEMA` (default: public)
- `HOLOGRES_TEST_TABLE`

### Test Structure

- `tests/conftest.py`: Shared fixtures — mock DB connections, environment variables, MCP session setup. The `_reset_connection_pool` autouse fixture disables the connection pool during unit tests.
- `tests/unit/`: 13 test files using mocked DB connections (no Hologres required). Unit tests use `_make_mock_conn()` helper to build mock connections/cursors and patch `connect_with_retry`.
- `tests/integration/`: 3 test files with real MCP client sessions via `StdioServerParameters`. Each test spawns the server as a subprocess. Integration test objects use `mcp_test_` prefix and are cleaned up in `finally` blocks.
- `pytest-asyncio` with `asyncio_mode = "auto"` handles async test functions.
