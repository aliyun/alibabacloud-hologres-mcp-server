# Release Notes

## Version 1.0.2

### New Features

#### Data Visualization
- **query_and_plotly_chart**: Execute SQL query and generate charts (bar/line/scatter/pie/histogram/area), returns base64-encoded PNG image
  - Uses matplotlib for chart generation, no Chrome/browser dependency

#### Query Performance Analysis
- **analyze_hg_query_by_id**: Analyze a query's full performance profile (48 columns) from `hg_query_log` by query_id
- **get_hg_slow_queries**: List slow queries ordered by duration with configurable threshold

#### Dynamic Table Management (V3.0+)
- **list_hg_dynamic_tables**: List all Dynamic Tables with status, freshness settings, and last refresh info
- **get_hg_dynamic_table_refresh_history**: View refresh history for a specific Dynamic Table

#### Recycle Bin Management (V3.1+)
- **list_hg_recyclebin**: List dropped tables that can be restored from recycle bin
- **restore_hg_table_from_recyclebin**: Restore a dropped table using `RECOVER TABLE ... WITH (table_id = N)` syntax

#### Computing Group (Warehouse) Management (V3.0+)
- **list_hg_warehouses**: List all computing groups with CPU, memory, cluster count, and status
- **switch_hg_warehouse**: Switch session's computing resource via `CALL hg_set_default_warehouse()`
- **manage_hg_warehouse**: Suspend, resume, restart, rename, or resize a computing group (V3.1+)
- **get_hg_warehouse_status**: Get detailed running status and scaling progress via `hg_get_warehouse_status()`
- **rebalance_hg_warehouse**: Trigger shard rebalancing via `hg_rebalance_warehouse()` to eliminate data skew

#### Query Queue Management (V3.0+)
- **list_hg_query_queues**: View all Query Queue configurations and classifier rules
- **manage_hg_query_queue**: Create, drop, or clear a Query Queue via stored procedures
- **manage_hg_classifier**: Create or drop classifiers for Query Queue routing
- **set_hg_query_queue_property**: Set or remove properties on queues (concurrency, timeout) and classifiers (routing rules)

#### Query Monitoring & Control
- **list_hg_active_queries**: List active/idle/all connections from `pg_stat_activity` with state filter
- **cancel_hg_query**: Cancel or terminate running queries via `pg_cancel_backend()` / `pg_terminate_backend()`
- **get_hg_lock_diagnostics**: Diagnose lock contention by joining `pg_locks` with `pg_stat_activity`

#### Table Analysis & Schema
- **get_hg_table_storage_size**: View table storage breakdown (total/data/index/meta) via `pg_relation_size` and `hg_relation_size`
- **get_hg_table_properties**: View table properties (distribution_key, clustering_key, segment_key, etc.) from `hg_table_properties`
- **get_hg_table_shard_info**: View Table Group and shard_count configuration for data skew diagnosis
- **get_hg_table_info_trend**: View daily storage/file/row count trends from `hg_table_info` (T+1 data, 30-day retention)

#### External Data & Lake (V3.0+/V4.1+)
- **list_hg_external_databases**: List External Databases and Foreign Servers for Lakehouse federation
- **query_hg_external_files**: Query OSS files directly using `EXTERNAL_FILES()` function without creating foreign tables (V4.1+, supports CSV/Parquet/ORC)

#### Security & Configuration
- **list_hg_data_masking_rules**: List column-level and user-level data masking rules from `hg_anon` extension (V3.1+)
- **get_hg_guc_config**: Get current value of any GUC parameter via `SHOW`

#### REFRESH DYNAMIC TABLE Support
- Added support for `REFRESH DYNAMIC TABLE` SQL statement in `execute_hg_dml_sql`
  - Supports all REFRESH variants including `REFRESH OVERWRITE`, `PARTITION`, and `WITH` clauses

### Infrastructure
- **Connection Pool**: Added `psycopg_pool` connection pool (min=0, max=5, idle=300s) with automatic fallback to direct connection when pool is unavailable
- **CLI Commands**: All 27 new tools have corresponding CLI commands via `cyclopts`

### Dependencies
- Added `matplotlib>=3.5.0` for chart generation
- Added `psycopg-pool>=3.0.0` for connection pooling (optional, graceful fallback)

### Testing
- Unit tests: 336 passed, ruff lint clean
- Integration tests verified for Dynamic Table, Recycle Bin, Warehouse management
- Total MCP tools: **39** (up from 12)

## Version 1.0.1

### Security
- **CVE-2026-34073**: Updated `cryptography` dependency from 46.0.5 to 46.0.6
  - Fixed a bug where name constraints were not applied to peer names during verification when the leaf certificate contains a wildcard DNS SAN

## Version 1.0.0

### Breaking Changes
- **Framework Migration**: Completely migrated from `mcp` library to **FastMCP v3.0.0** framework
  - All tools, resources, and prompts now use FastMCP decorators
  - Simplified and more maintainable codebase

### New Features
- Added new system resource: `system:///guc_value/{guc_name}` - Get GUC (Grand Unified Configuration) value

### Dependencies
- Replaced `mcp` dependency with `fastmcp>=3.0.0`
- Updated version to 1.0.0

## Version 0.2.1

### Code Quality
- Added [ruff](https://docs.astral.sh/ruff/) for code linting and formatting
- Fixed all linting issues (unused imports, unused variables, trailing whitespace, import sorting)
- Added ruff configuration in `pyproject.toml`

### Testing
- Unit test cases increased from 295 to **326**
- Added more comprehensive test coverage

### Documentation
- Added "Code Quality" section to README with ruff usage instructions
- Updated version references in release workflow examples

### Dependencies
- Upgraded `mcp` dependency from 1.4.1 to **1.23.0**
- Updated `h11` indirect dependency to 0.16.0 (security fix)

## Version 0.2.0
### Breaking Changes
The following tools have renamed their `schema` parameter to `schema_name` to avoid Pydantic field shadowing warnings:
- `gather_hg_table_statistics`
- `list_hg_tables_in_a_schema`
- `show_hg_table_ddl`

MCP clients must update their tool calls to use the new parameter name:

```python
# Before 0.2.0
await session.call_tool("gather_hg_table_statistics", {"schema": "public", "table": "users"})

# After 0.2.0
await session.call_tool("gather_hg_table_statistics", {"schema_name": "public", "table": "users"})
```

### Refactoring
- Migrated from low-level `mcp.server.Server` to `mcp.server.fastmcp.FastMCP` framework
- Simplified codebase by leveraging FastMCP decorators (`@app.tool()`, `@app.resource()`, `@app.prompt()`)
- Removed pydantic dependency (FastMCP handles validation internally)

### New Features
Added 3 prompts for AI-assisted database operations:
- `analyze_table_performance`: Generate a prompt to analyze table performance in Hologres
- `optimize_query`: Generate a prompt to optimize a SQL query in Hologres
- `explore_schema`: Generate a prompt to explore a schema in Hologres database

### Testing
- Restructured test suite with separate `unit` and `integration` directories
- Added environment variable support for test fixtures (`HOLOGRES_TEST_SCHEMA`, `HOLOGRES_TEST_TABLE`)
- Added comprehensive unit tests for tools, resources, prompts, SQL validation, and utils
- Added integration tests with MCP client session management

### Testing Improvements
Enhanced integration test coverage with 14 new test cases across 4 new test classes:

- **TestMCPPrompts** (4 tests): Tests for MCP prompt functionality
  - `analyze_table_performance` prompt generation and content validation
  - `optimize_query` prompt with SQL query parameter
  - `explore_schema` prompt with schema exploration
  - Default parameter handling for prompts

- **TestMCPConcurrency** (3 tests): Tests for concurrent MCP operations
  - Concurrent SELECT queries using `asyncio.gather`
  - Concurrent mixed operations (read/write)
  - Concurrent resource reads

- **TestMCPBoundaryConditions** (4 tests): Tests for edge cases
  - Unicode character handling in SQL queries (Chinese, Japanese, emoji)
  - Empty result set handling
  - NULL value handling in query results
  - Special SQL characters (quotes, semicolons) in strings

- **TestMCPPerformance** (3 tests): Tests for performance scenarios
  - Large result sets (1000 rows using `generate_series`)
  - Wide result sets (50 columns)
  - Complex JOIN queries with CTEs and aggregations

Total integration tests: 61 (up from 47)

## Version 0.1.9
### Bugfix
Fix the configuration issue when the STS token is not defined.

## Version 0.1.8
### Enhancement
Add tools
- `execute_hg_select_sql_with_serverless`: Execute a SELECT SQL query in Hologres database with serverless computing
- `create_hg_maxcompute_foreign_table`: Create MaxCompute foreign tables in Hologres database.

Since some Agents do not support resources and resource templates, the following tools are provided to obtain the metadata of schemas, tables, views, and external tables.
- `list_hg_schemas`: Lists all schemas in the current Hologres database, excluding system schemas.
- `list_hg_tables_in_a_schema`: Lists all tables in a specific schema, including their types (table, view, external table, partitioned table).
- `show_hg_table_ddl`: Show the DDL script of a table, view, or external table in the Hologres database.

In order for the AI Agent to better recognize the Tools, please rename the following Tools as follows.
- Rename `execute_select_sql` to `execute_hg_select_sql`
- Rename `execute_dml_sql` to `execute_hg_dml_sql`
- Rename `execute_ddl_sql` to `execute_hg_ddl_sql`
- Rename `gather_table_statistics` to `gather_hg_table_statistics`
- Rename `get_query_plan` to `get_hg_query_plan`
- Rename `get_execution_plan` to `get_hg_execution_plan`
- Rename `call_procedure` to `call_hg_procedure`

## Version 0.1.7
### Bugfix
Fix some bugs when using in Python 3.11.

## Version 0.1.6
### Enhancement
update psycopg2 to psycopg3.
select, dml, ddl use different tools to execute.

## Version 0.1.5
### Enhancement
Now compatible with Python 3.10 and newer (previously required 3.13+).

## Version 0.1.4
### Enhancement
The URI of the resource template has been refactored to enable the large language model (LLM) to use it more concisely.

## Version 0.1.2 (Initial Release)
### Description
Hologres MCP Server serves as a universal interface between AI Agents and Hologres databases. It enables rapid implementation of seamless communication between AI Agents and Hologres, helping AI Agents retrieve Hologres database metadata and execute SQL for various operations.

### Key Features
- **SQL Execution**
  - Execute SQL in Hologres, including DDL, DML, and Queries
  - Execute ANALYZE commands to collect statistics
- **Database Metadata**
  - Display all schemas
  - Display all tables under a schema
  - Show table DDL
  - View table statistics
- **System Information**
  - Query execution logs
  - Query missing statistics

### Dependencies
- Python 3.10 or higher
- Required packages
  - mcp >= 1.23.0
  - psycopg >= 3.1.0

### Configuration
MCP Server requires the following environment variables to connect to Hologres instance:
- `HOLOGRES_HOST`
- `HOLOGRES_PORT`
- `HOLOGRES_USER`
- `HOLOGRES_PASSWORD`
- `HOLOGRES_DATABASE`

### Installation
Install MCP Server using the following package:
```bash
pip install hologres-mcp-server
```

### MCP Integration
Add the following configuration to the MCP client configuration file:
```json
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
```
