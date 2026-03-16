# Release Notes
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
  - mcp >= 1.4.0
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
