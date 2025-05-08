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

## Components

### Tools

* `execute_hg_select_sql`: Execute a SELECT SQL query in Hologres database
* `execute_hg_select_sql_with_serverless`: Execute a SELECT SQL query in Hologres database with serverless computing
* `execute_hg_dml_sql`: Execute a DML (INSERT, UPDATE, DELETE) SQL query in Hologres database
* `execute_hg_ddl_sql`: Execute a DDL (CREATE, ALTER, DROP, COMMENT ON) SQL query in Hologres database
* `gather_hg_table_statistics`: Collect table statistics in Hologres database
* `get_hg_query_plan`: Get query plan in Hologres database
* `get_hg_execution_plan`: Get execution plan in Hologres database
* `call_hg_procedure`: Invoke a procedure in Hologres database
* `create_hg_maxcompute_foreign_table`: Create MaxCompute foreign tables in Hologres database.

Since some Agents do not support resources and resource templates, the following tools are provided to obtain the metadata of schemas, tables, views, and external tables.
* `list_hg_schemas`: Lists all schemas in the current Hologres database, excluding system schemas.
* `list_hg_tables_in_a_schema`: Lists all tables in a specific schema, including their types (table, view, external table, partitioned table).
* `show_hg_table_ddl`: Show the DDL script of a table, view, or external table in the Hologres database.

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

None at this time
