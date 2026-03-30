# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Run all tests
uv run pytest tests/ -v

# Run unit tests only (no database required)
uv run pytest tests/unit/ -v

# Run integration tests (requires Hologres database)
uv run pytest tests/integration/ -v

# Run specific test file
uv run pytest tests/unit/test_tools.py -v

# Run specific test class
uv run pytest tests/integration/test_mcp_integration.py::TestMCPTools -v

# Run with coverage
uv run pytest tests/unit/ --cov=src/hologres_mcp_server --cov-report=html

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

Edit the file with your Hologres credentials. Tests will be skipped if the config file is missing or incomplete.

## Architecture

This is a FastMCP v3-based MCP server for Alibaba Cloud Hologres (a PostgreSQL-compatible data warehouse). The entry point is `__init__.py:main()` → `server.py:main()` which starts the FastMCP app.

### Core Files

- **`server.py`**: All MCP capabilities registered on a single `FastMCP` app instance via decorators:
  - 12 tools (`@app.tool`) with tags (`query`, `dml`, `ddl`, `admin`, `analysis`, `schema`)
  - 13 resources (`@app.resource`) including parameterized URI templates (`hologres:///{schema}/{table}/...`) and system resources (`system:///...`)
  - 3 prompts (`@app.prompt`)
  - A `@lifespan` handler that validates the DB connection on startup (warns but doesn't fail)

- **`utils.py`**: Database operations layer — all SQL execution flows through `handle_call_tool()` or `handle_read_resource()`, which open connections via `connect_with_retry()` (retries up to 3 times with 5s delay). Also contains SQL validation functions and the `pglast`-based `try_infer_view_comments()` which parses view definitions to propagate column comments from source tables.

- **`settings.py`**: `get_db_config()` reads environment variables. Falls back from `HOLOGRES_USER`/`HOLOGRES_PASSWORD` to `ALIBABA_CLOUD_ACCESS_KEY_ID`/`ALIBABA_CLOUD_ACCESS_KEY_SECRET` + optional STS token.

- **`__init__.py`**: Re-exports `server.main()` as the package entry point. Registered in `pyproject.toml` as the `hologres-mcp-server` CLI script.

### Key Dependencies

- **fastmcp** (v3+): MCP server framework with decorator-based tool/resource/prompt registration
- **psycopg** (v3): PostgreSQL driver for Hologres connectivity
- **pglast** (v7.5+): PostgreSQL SQL parser, used to parse view definitions for comment inference

### SQL Validation

Tools validate SQL type before execution (in `utils.py`):
- `execute_hg_select_sql`: Must start with `SELECT` or `WITH...SELECT` (regex-based)
- `execute_hg_dml_sql`: Must start with `INSERT`, `UPDATE`, or `DELETE`
- `execute_hg_ddl_sql`: Must start with `CREATE`, `ALTER`, `DROP`, or `COMMENT ON`

### Serverless Computing

`execute_hg_select_sql_with_serverless` sets `hg_computing_resource='serverless'` before executing the query. This is used as a fallback when regular execution hits memory limits.

### Environment Variables

Required for database connection:
- `HOLOGRES_HOST`, `HOLOGRES_PORT`, `HOLOGRES_DATABASE`
- `HOLOGRES_USER`, `HOLOGRES_PASSWORD` (or `ALIBABA_CLOUD_ACCESS_KEY_ID`/`ALIBABA_CLOUD_ACCESS_KEY_SECRET` + optional `ALIBABA_CLOUD_SECURITY_TOKEN`)

Optional for integration tests:
- `HOLOGRES_TEST_SCHEMA` (default: public)
- `HOLOGRES_TEST_TABLE`

### Test Structure

- `tests/conftest.py`: Shared fixtures for both unit and integration tests — mock DB connections, environment variables, MCP session setup
- `tests/unit/`: 326 test cases using mocked DB connections (no Hologres required)
- `tests/integration/`: 61 test cases across 12 test classes using real MCP client sessions via `StdioServerParameters`
- `pytest-asyncio` with `asyncio_mode = "auto"` handles async test functions
