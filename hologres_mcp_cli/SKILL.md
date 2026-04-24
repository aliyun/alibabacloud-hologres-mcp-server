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

Execute (INSERT, UPDATE, DELETE, REFRESH DYNAMIC TABLE) SQL to insert, update, delete data, and refresh dynamic tables in Hologres database.

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
uv run --with fastmcp python hologres_mcp_cli.py call-tool call_hg_procedure --procedure-name <value> --procedure-args <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--procedure-name` | string | yes | The name of the stored procedure to call in Hologres database |
| `--procedure-args` | string | no | The arguments to pass to the stored procedure in Hologres database (JSON string) |

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

### query_and_plotly_chart

Execute a SELECT SQL query and generate a chart from the results. Returns both the query result data and a base64-encoded PNG image of the chart.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool query_and_plotly_chart --query <value> --chart-type <value> --x-column <value> --y-column <value> --title <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query` | string | yes | The SELECT SQL query to execute |
| `--chart-type` | string | no | Chart type: 'bar', 'line', 'scatter', 'pie', 'histogram', 'area' |
| `--x-column` | string | no | Column name for X axis (uses first column if not specified) |
| `--y-column` | string | no | Column name for Y axis (uses second column if not specified) |
| `--title` | string | no | Chart title |

### analyze_hg_query_by_id

Analyze a specific query's performance profile by its query_id from hg_query_log. Returns detailed metrics including duration, memory usage, CPU time, read/write stats, and execution plan.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool analyze_hg_query_by_id --query-id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--query-id` | string | yes | The query_id from hg_query_log to analyze |

### get_hg_slow_queries

Get slow queries from hg_query_log ordered by duration. Useful for identifying performance bottlenecks.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_slow_queries --min-duration-ms <value> --limit <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--min-duration-ms` | integer | no | Minimum query duration in milliseconds to filter (default 1000) |
| `--limit` | integer | no | Maximum number of queries to return (default 20) |

### list_hg_dynamic_tables

List all Dynamic Tables with their status, freshness settings, and last refresh info.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_dynamic_tables --schema-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | no | Schema name to filter (empty for all schemas) |

### get_hg_dynamic_table_refresh_history

Get refresh history for a specific Dynamic Table, including duration, status, and latency.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_dynamic_table_refresh_history --schema-name <value> --table-name <value> --limit <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name of the dynamic table |
| `--table-name` | string | yes | Dynamic table name |
| `--limit` | integer | no | Maximum number of history records (default 10) |

### list_hg_recyclebin

List all tables in the Hologres recycle bin (dropped tables that can be restored).

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_recyclebin
```

### restore_hg_table_from_recyclebin

Restore a dropped table from the Hologres recycle bin. Only works if the table is still in the recycle bin.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool restore_hg_table_from_recyclebin --table-name <value> --schema-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--table-name` | string | yes | The original table name to restore from recycle bin |
| `--schema-name` | string | no | Schema name of the table (default 'public') |

### list_hg_warehouses

List all computing groups (warehouses) in the Hologres instance, including their CPU, memory, cluster count, and status.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_warehouses
```

### switch_hg_warehouse

Switch the current session's computing resource to a specified warehouse (computing group). Use 'local' for the default computing group.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool switch_hg_warehouse --warehouse-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--warehouse-name` | string | yes | The warehouse (computing group) name to switch to |

### get_hg_table_storage_size

Get storage size details of a table, including total size, data, index, and metadata breakdown.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_table_storage_size --schema-name <value> --table <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |

### cancel_hg_query

Cancel or terminate a running query by its process ID. Use list_hg_active_queries to find the pid first.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool cancel_hg_query --pid <value> --terminate
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--pid` | integer | yes | The process ID (pid) of the query to cancel |
| `--terminate` | boolean | no | If True, forcefully terminate the backend process (default False, just cancel the query) |

### list_hg_active_queries

List currently active queries and connections from pg_stat_activity. Useful for monitoring running queries and finding PIDs to cancel.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_active_queries --state <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--state` | string | no | Filter by state: 'active', 'idle', 'all' (default 'active') |

### list_hg_query_queues

List all Query Queues and their classifiers, showing concurrency limits, queue sizes, and routing rules. Requires Hologres V3.0+.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_query_queues
```

### get_hg_table_properties

Get table properties including distribution_key, clustering_key, segment_key, bitmap_columns, dictionary_columns, binlog settings, etc.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_table_properties --schema-name <value> --table <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |

### get_hg_table_shard_info

Get table's Table Group and shard count info. Useful for diagnosing data skew and shard configuration.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_table_shard_info --schema-name <value> --table <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |

### list_hg_external_databases

List all External Databases (federated databases for Lakehouse acceleration). Requires Hologres V3.0+.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_external_databases
```

### get_hg_lock_diagnostics

Diagnose lock contention by showing blocking and waiting queries. Useful for identifying lock waits that cause query hangs.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_lock_diagnostics
```

### get_hg_table_info_trend

Get table storage trend from hg_table_info, showing daily storage size, file count, and row count changes. Data is T+1, retained for 30 days.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_table_info_trend --schema-name <value> --table <value> --days <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--schema-name` | string | yes | Schema name in Hologres database |
| `--table` | string | yes | Table name in Hologres database |
| `--days` | integer | no | Number of days to look back (default 7) |

### manage_hg_query_queue

Create, drop, or clear a Query Queue. Requires Hologres V3.0+ and superuser privileges.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool manage_hg_query_queue --action <value> --queue-name <value> --max-concurrency <value> --max-queue-size <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--action` | string | yes | Action: 'create', 'drop', or 'clear' |
| `--queue-name` | string | yes | Name of the query queue |
| `--max-concurrency` | integer | no | Max concurrency for the queue (required for 'create') |
| `--max-queue-size` | integer | no | Max queue size (required for 'create') |

### manage_hg_classifier

Create or drop a classifier for a Query Queue. Use set_hg_query_queue_property to configure classifier rules after creation. Requires Hologres V3.0+.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool manage_hg_classifier --action <value> --queue-name <value> --classifier-name <value> --priority <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--action` | string | yes | Action: 'create' or 'drop' |
| `--queue-name` | string | yes | Name of the query queue the classifier belongs to |
| `--classifier-name` | string | yes | Name of the classifier |
| `--priority` | integer | no | Priority for the classifier (required for 'create', higher = matched first) |

### set_hg_query_queue_property

Set or remove properties on a Query Queue or classifier. For classifier rules, use property_key as condition_name and property_value as condition_value. Requires Hologres V3.0+.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool set_hg_query_queue_property --target <value> --queue-name <value> --property-key <value> --property-value <value> --classifier-name <value> --action <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--target` | string | yes | Target type: 'queue' or 'classifier' |
| `--queue-name` | string | yes | Name of the query queue |
| `--property-key` | string | yes | Property key to set (e.g. 'max_concurrency', 'query_timeout_ms' for queue; 'condition_name' for classifier rule) |
| `--property-value` | string | yes | Property value to set |
| `--classifier-name` | string | no | Classifier name (required when target='classifier') |
| `--action` | string | no | Action: 'set' to add/update, 'remove' to delete the property (default 'set') |

### manage_hg_warehouse

Manage a computing group (warehouse): suspend, resume, restart, rename, or resize. Requires superuser privileges.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool manage_hg_warehouse --action <value> --warehouse-name <value> --cu <value> --new-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--action` | string | yes | Action: 'suspend', 'resume', 'restart', 'rename', or 'resize' |
| `--warehouse-name` | string | yes | Name of the warehouse (computing group) |
| `--cu` | integer | no | CU count for resize action |
| `--new-name` | string | no | New name for rename action |

### get_hg_warehouse_status

Get detailed running status and scaling progress of a computing group (warehouse).

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_warehouse_status --warehouse-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--warehouse-name` | string | yes | Name of the warehouse (computing group) |

### rebalance_hg_warehouse

Trigger shard rebalancing for a computing group to eliminate data skew across nodes.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool rebalance_hg_warehouse --warehouse-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--warehouse-name` | string | yes | Name of the warehouse (computing group) to rebalance |

### list_hg_data_masking_rules

List all data masking rules configured via hg_anon extension, including column-level and user-level masking labels.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool list_hg_data_masking_rules
```

### query_hg_external_files

Query files directly from OSS using EXTERNAL_FILES function without creating foreign tables. Requires Hologres V4.1+. Supports CSV, Parquet, ORC formats.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool query_hg_external_files --path <value> --format <value> --columns <value> --oss-endpoint <value> --role-arn <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--path` | string | yes | OSS path, e.g. 'oss://bucket/path/to/files' |
| `--format` | string | yes | File format: 'csv', 'parquet', or 'orc' |
| `--columns` | string | no | Optional column definitions for AS clause, e.g. 'id int, name text'. Leave empty for auto schema inference. |
| `--oss-endpoint` | string | no | OSS endpoint (internal), e.g. 'oss-cn-hangzhou-internal.aliyuncs.com'. Leave empty if already configured. |
| `--role-arn` | string | no | RAM role ARN for accessing OSS. Leave empty if already configured. |

### get_hg_guc_config

Get the current value of a GUC (Grand Unified Configuration) parameter.

```bash
uv run --with fastmcp python hologres_mcp_cli.py call-tool get_hg_guc_config --guc-name <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--guc-name` | string | yes | The GUC parameter name to query, e.g. 'hg_computing_resource', 'statement_timeout', 'hg_experimental_enable_fixed_dispatcher' |

## Utility Commands

```bash
uv run --with fastmcp python hologres_mcp_cli.py list-tools
uv run --with fastmcp python hologres_mcp_cli.py list-resources
uv run --with fastmcp python hologres_mcp_cli.py read-resource <uri>
uv run --with fastmcp python hologres_mcp_cli.py list-prompts
uv run --with fastmcp python hologres_mcp_cli.py get-prompt <name> [key=value ...]
```
