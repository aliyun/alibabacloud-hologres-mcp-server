# Release Notes
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
