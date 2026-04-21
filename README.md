English | [中文](README_ZH.md)

# Hologres MCP Server

Hologres MCP Server serves as a universal interface between AI Agents and Hologres databases. It enables seamless communication between AI Agents and Hologres, helping AI Agents retrieve Hologres database metadata and execute SQL operations.

## Configuration

### Mode 1: Using Local File

#### Download

Download from Github

```bash
git clone https://github.com/aliyun/alibabacloud-hologres-mcp-server.git
```

#### MCP Integration

Add the following configuration to the MCP client configuration file:

```json
{
    "mcpServers": {
        "hologres-mcp-server": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/alibabacloud-hologres-mcp-server",
                "run",
                "hologres-mcp-server"
            ],
            "env": {
                "HOLOGRES_HOST": "host",
                "HOLOGRES_PORT": "port",
                "HOLOGRES_USER": "access_id",
                "HOLOGRES_PASSWORD": "access_key",
                "HOLOGRES_DATABASE": "database"
            }
        }
    }
}
```

### Mode 2: Using PIP Mode

#### Installation

Install MCP Server using the following package:

```bash
pip install hologres-mcp-server
```

#### MCP Integration

Add the following configuration to the MCP client configuration file:

Use uv mode

```json
{
    "mcpServers": {
        "hologres-mcp-server": {
            "command": "uv",
            "args": [
                "run",
                "--with",
                "hologres-mcp-server",
                "hologres-mcp-server"
            ],
            "env": {
                "HOLOGRES_HOST": "host",
                "HOLOGRES_PORT": "port",
                "HOLOGRES_USER": "access_id",
                "HOLOGRES_PASSWORD": "access_key",
                "HOLOGRES_DATABASE": "database"
            }
        }
    }
}
```
Use uvx mode

```json
{
    "mcpServers": {
        "hologres-mcp-server": {
            "command": "uvx",
            "args": [
                "hologres-mcp-server"
            ],
            "env": {
                "HOLOGRES_HOST": "host",
                "HOLOGRES_PORT": "port",
                "HOLOGRES_USER": "access_id",
                "HOLOGRES_PASSWORD": "access_key",
                "HOLOGRES_DATABASE": "database"
            }
        }
    }
}
```

## Using with Claude Code

```bash
# Add to Claude Code
claude mcp add hologres-mcp-server \
  -e HOLOGRES_HOST=<your_host> \
  -e HOLOGRES_PORT=<your_port> \
  -e HOLOGRES_USER=<your_access_id> \
  -e HOLOGRES_PASSWORD=<your_access_key> \
  -e HOLOGRES_DATABASE=<your_database> \
  -- uvx hologres-mcp-server
```

## Components

### Tools

* `execute_hg_select_sql`: Execute a SELECT SQL query in Hologres database
* `execute_hg_select_sql_with_serverless`: Execute a SELECT SQL query in Hologres database with serverless computing
* `execute_hg_dml_sql`: Execute a DML (INSERT, UPDATE, DELETE) SQL query in Hologres database
* `execute_hg_ddl_sql`: Execute a DDL (CREATE, ALTER, DROP, COMMENT ON) SQL query in Hologres database
* `gather_hg_table_statistics`: Collect table statistics in Hologres database
  - Parameters: `schema_name` (string), `table` (string)
* `get_hg_query_plan`: Get query plan in Hologres database
* `get_hg_execution_plan`: Get execution plan in Hologres database
* `call_hg_procedure`: Invoke a procedure in Hologres database
* `create_hg_maxcompute_foreign_table`: Create MaxCompute foreign tables in Hologres database.

Since some Agents do not support resources and resource templates, the following tools are provided to obtain the metadata of schemas, tables, views, and external tables.
* `list_hg_schemas`: Lists all schemas in the current Hologres database, excluding system schemas.
* `list_hg_tables_in_a_schema`: Lists all tables in a specific schema, including their types (table, view, external table, partitioned table).
  - Parameters: `schema_name` (string)
* `show_hg_table_ddl`: Show the DDL script of a table, view, or external table in the Hologres database.
  - Parameters: `schema_name` (string), `table` (string)
* `query_and_plotly_chart`: Execute a SELECT SQL query and generate a chart (bar, line, scatter, pie, histogram, area). Returns query results and a base64-encoded PNG image.
  - Parameters: `query` (string), `chart_type` (string, default "bar"), `x_column` (string), `y_column` (string), `title` (string)
* `analyze_hg_query_by_id`: Analyze a specific query's performance profile by its query_id from hg_query_log. Returns detailed metrics including duration, memory, CPU time, read/write stats.
  - Parameters: `query_id` (string)
* `get_hg_slow_queries`: Get slow queries from hg_query_log ordered by duration.
  - Parameters: `min_duration_ms` (int, default 1000), `limit` (int, default 20)
* `list_hg_dynamic_tables`: List all Dynamic Tables with their status, freshness settings, and last refresh info.
  - Parameters: `schema_name` (string, optional)
* `get_hg_dynamic_table_refresh_history`: Get refresh history for a specific Dynamic Table, including duration, status, and latency.
  - Parameters: `schema_name` (string), `table_name` (string), `limit` (int, default 10)
* `list_hg_recyclebin`: List all tables in the Hologres recycle bin (dropped tables that can be restored).
* `restore_hg_table_from_recyclebin`: Restore a dropped table from the Hologres recycle bin.
  - Parameters: `table_name` (string), `schema_name` (string, default "public")
* `list_hg_warehouses`: List all computing groups (warehouses) with their CPU, memory, cluster count, and status.
* `switch_hg_warehouse`: Switch the current session's computing resource to a specified warehouse.
  - Parameters: `warehouse_name` (string)
* `get_hg_table_storage_size`: Get storage size details of a table, including total, data, index, and metadata breakdown.
  - Parameters: `schema_name` (string), `table` (string)
* `cancel_hg_query`: Cancel or terminate a running query by its process ID.
  - Parameters: `pid` (int), `terminate` (bool, default false)
* `list_hg_active_queries`: List currently active queries and connections from pg_stat_activity.
  - Parameters: `state` (string: "active", "idle", or "all", default "active")
* `list_hg_query_queues`: List all Query Queues and their classifiers (concurrency limits, routing rules). Requires V3.0+.
* `get_hg_table_properties`: Get table properties including distribution_key, clustering_key, segment_key, bitmap_columns, binlog settings, etc.
  - Parameters: `schema_name` (string), `table` (string)

### Resources

#### Built-in Resources

* `hologres:///schemas`: Get all schemas in Hologres database

#### Resource Templates

* `hologres:///{schema}/tables`: List all tables in a schema in Hologres database
* `hologres:///{schema}/{table}/partitions`: List all partitions of a partitioned table in Hologres database
* `hologres:///{schema}/{table}/ddl`: Get table DDL in Hologres database
* `hologres:///{schema}/{table}/statistic`: Show collected table statistics in Hologres database
* `system:///{+system_path}`:
  System paths include:

  * `hg_instance_version` - Shows the hologres instance version.
  * `guc_value/<guc_name>` - Shows the guc (Grand Unified Configuration) value.
  * `missing_stats_tables` - Shows the tables that are missing statistics.
  * `stat_activity` - Shows the information of current running queries.
  * `query_log/latest/<row_limits>` - Get recent query log history with specified number of rows.
  * `query_log/user/<user_name>/<row_limits>` - Get query log history for a specific user with row limits.
  * `query_log/application/<application_name>/<row_limits>` - Get query log history for a specific application with row limits.
  * `query_log/failed/<interval>/<row_limits>` - Get failed query log history with interval and specified number of rows.

### Prompts

* `analyze_table_performance`: Generate a prompt to analyze table performance in Hologres
* `optimize_query`: Generate a prompt to optimize a SQL query in Hologres
* `explore_schema`: Generate a prompt to explore a schema in Hologres database

## Testing

The project includes comprehensive unit tests and integration tests.

### Unit Tests

Unit tests do not require a database connection and use mocked dependencies. The test suite includes **326 test cases** covering:

- Tools functionality and SQL validation
- Resources and resource templates
- Prompts generation
- Utility functions and error handling
- Concurrency scenarios
- SQL injection protection

```bash
# Run all unit tests
uv run pytest tests/unit/ -v

# Run specific test file
uv run pytest tests/unit/test_tools.py -v

# Run with coverage
uv run pytest tests/unit/ --cov=src/hologres_mcp_server --cov-report=html
```

### Integration Tests

Integration tests require a real Hologres database connection. The test suite includes **61 test cases** organized into 12 test classes:

| Test Class | Tests | Description |
|------------|-------|-------------|
| `TestMCPConnection` | 5 | MCP server connection and basic functionality |
| `TestMCPResources` | 14 | Resource reading functionality (schemas, tables, DDL, statistics, partitions, query logs) |
| `TestMCPTools` | 10 | Tool calls for read-only operations |
| `TestMCPProcedureTools` | 3 | Stored procedure tool calls |
| `TestMCPMaxComputeTools` | 1 | MaxCompute foreign table creation |
| `TestMCPDDLTools` | 5 | DDL operations (CREATE, ALTER, DROP, COMMENT) |
| `TestMCPDMLTools` | 3 | DML operations (INSERT, UPDATE, DELETE) |
| `TestErrorHandling` | 3 | Error handling and edge cases |
| `TestMCPPrompts` | 4 | Prompt generation functionality |
| `TestMCPConcurrency` | 3 | Concurrent MCP operations |
| `TestMCPBoundaryConditions` | 4 | Edge cases (Unicode, NULL, empty results) |
| `TestMCPPerformance` | 3 | Performance scenarios (large/wide result sets) |

1. Create a configuration file from the example:

```bash
cp tests/integration/.test_mcp_client_env_example tests/integration/.test_mcp_client_env
```

2. Edit the configuration file with your Hologres credentials:

```
HOLOGRES_HOST=your-hologres-instance.hologres.aliyuncs.com
HOLOGRES_PORT=80
HOLOGRES_USER=your_username
HOLOGRES_PASSWORD=your_password
HOLOGRES_DATABASE=your_database
```

3. Run the integration tests:

```bash
# Run all integration tests
uv run pytest tests/integration/ -v -m integration

# Run specific test class
uv run pytest tests/integration/test_mcp_integration.py::TestMCPTools -v

# Run all tests (unit + integration)
uv run pytest tests/ -v
```

**Note:** Integration tests will be skipped if the `.test_mcp_client_env` file is missing or contains incomplete configuration.

## Code Quality

This project uses [ruff](https://docs.astral.sh/ruff/) for code linting and formatting.

```bash
# Install dev dependencies
uv sync --dev
uv pip install ruff

# Check code style
uv run ruff check .

# Check and auto-fix
uv run ruff check . --fix

# Format code
uv run ruff format .

# Format check only (no changes)
uv run ruff format . --check
```

## Build & Publish

### Build

This project uses [hatchling](https://hatch.pypa.io/) as the build backend. Build artifacts will be generated in the `dist/` directory.

```bash
# Using uv (recommended)
uv build

# Or using python build module
pip install build
python -m build
```

### Publish to PyPI

```bash
# Install twine
pip install twine

# Upload to PyPI
twine upload dist/*

# Or upload to Test PyPI first for verification
twine upload --repository testpypi dist/*
```

### Release Workflow

```bash
# 1. Update version in pyproject.toml
# 2. Clean old build artifacts
rm -rf dist/

# 3. Build
uv build

# 4. Publish
twine upload dist/*

# 5. Tag the release
git tag -a v1.0.2 -m "Release v1.0.2"
git push origin v1.0.2
```

### Update CLI Feature

```bash
# Use FastMCP framework to generate CLI code and Skill
uv run fastmcp generate-cli hologres-mcp-server hologres_mcp_cli/hologres_mcp_cli.py -f
```
