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
```

## Integration Test Setup

Integration tests require a real Hologres connection. Configure via:

```bash
cp tests/integration/.test_mcp_client_env_example tests/integration/.test_mcp_client_env
```

Edit the file with your Hologres credentials. Tests will be skipped if the config file is missing or incomplete.

## Architecture

### Core Components

- **`server.py`**: FastMCP-based MCP server implementation. Contains:
  - 12 tools (SQL execution, DDL/DML, statistics, query plans)
  - 11 resources (schemas, tables, DDL, statistics, system info)
  - 3 prompts (performance analysis, query optimization, schema exploration)

- **`utils.py`**: Database operations layer:
  - `connect_with_retry()`: Connection pooling with retry logic
  - `handle_read_resource()`: Generic resource query handler
  - `handle_call_tool()`: Generic tool execution handler

- **`settings.py`**: Configuration from environment variables. Supports:
  - Direct credentials (`HOLOGRES_USER`/`HOLOGRES_PASSWORD`)
  - Alibaba Cloud STS token authentication

### SQL Validation

Tools validate SQL type before execution:
- `execute_hg_select_sql`: Must start with SELECT or WITH...SELECT
- `execute_hg_dml_sql`: Must start with INSERT/UPDATE/DELETE
- `execute_hg_ddl_sql`: Must start with CREATE/ALTER/DROP/COMMENT ON

### Environment Variables

Required for database connection:
- `HOLOGRES_HOST`, `HOLOGRES_PORT`, `HOLOGRES_DATABASE`
- `HOLOGRES_USER`, `HOLOGRES_PASSWORD` (or Alibaba Cloud STS tokens)

Optional for integration tests:
- `HOLOGRES_TEST_SCHEMA` (default: public)
- `HOLOGRES_TEST_TABLE` (for DDL/statistics tests)