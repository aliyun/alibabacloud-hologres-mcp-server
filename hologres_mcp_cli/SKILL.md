---
name: "hologres-mcp-server-cli"
description: "CLI for the hologres-mcp-server MCP server. Call tools, list resources, and get prompts."
---

# hologres-mcp-server CLI

## Tool Commands

### execute_hg_select_sql

Execute SELECT SQL to query data from Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool execute_hg_select_sql --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The (SELECT) SQL query to execute in Hologres database. |

### execute_hg_select_sql_with_serverless

Use Serverless Computing resources to execute SELECT SQL to query data in Hologres database. When the error like 'Total memory used by all existing queries exceeded memory limitation' occurs during execute_hg_select_sql execution, you can re-execute the SQL with this tool.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool execute_hg_select_sql_with_serverless --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The (SELECT) SQL query to execute with serverless computing in Hologres database |

### execute_hg_dml_sql

Execute (INSERT, UPDATE, DELETE) SQL to insert, update, and delete data in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool execute_hg_dml_sql --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The DML SQL query to execute in Hologres database |

### execute_hg_ddl_sql

Execute (CREATE, ALTER, DROP) SQL statements to CREATE, ALTER, or DROP tables, views, procedures, GUCs etc. in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool execute_hg_ddl_sql --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The DDL SQL query to execute in Hologres database |

### gather_hg_table_statistics

Execute the ANALYZE TABLE command to have Hologres collect table statistics, enabling QO to generate better query plans.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool gather_hg_table_statistics --schema-name <value> --table <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |

### get_hg_query_plan

Get query plan for a SQL query in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_query_plan --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The SQL query to analyze in Hologres database |

### get_hg_execution_plan

Get actual execution plan with runtime statistics for a SQL query in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_execution_plan --query <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The SQL query to analyze in Hologres database |

### call_hg_procedure

Call a stored procedure in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool call_hg_procedure --procedure-name <value> --arguments <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--procedure-name` | string | yes | The name of the stored procedure to call in Hologres database |
| `--arguments` | string | no | The arguments to pass to the stored procedure in Hologres database (JSON string) |

### create_hg_maxcompute_foreign_table

Create a MaxCompute foreign table in Hologres database to accelerate queries on MaxCompute data.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool create_hg_maxcompute_foreign_table --maxcompute-project <value> --maxcompute-tables <value> --maxcompute-schema <value> --local-schema <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--maxcompute-project` | string | yes | The MaxCompute project name (required) |
| `--maxcompute-tables` | array[string] | yes | The MaxCompute table names (required) |
| `--maxcompute-schema` | string | no | The MaxCompute schema name (optional, default: 'default') |
| `--local-schema` | string | no | The local schema name in Hologres (optional, default: 'public') |

### list_hg_schemas

List all schemas in the current Hologres database, excluding system schemas.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_schemas
```

### list_hg_tables_in_a_schema

List all tables in a specific schema in the current Hologres database, including their types (table, view, foreign table, partitioned table).

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_tables_in_a_schema --schema-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name to list tables from in Hologres database |

### show_hg_table_ddl

Show DDL script for a table, view, or foreign table in Hologres database.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool show_hg_table_ddl --schema-name <value> --table <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |

## Utility Commands

```bash
uv run --with fastmcp python hologres_mcp_cli.py list-tools
uv run --with fastmcp python hologres_mcp_cli.py list-resources
uv run --with fastmcp python hologres_mcp_cli.py read-resource <uri>
uv run --with fastmcp python hologres_mcp_cli.py list-prompts
uv run --with fastmcp python hologres_mcp_cli.py get-prompt <name> [key=value ...]
```
