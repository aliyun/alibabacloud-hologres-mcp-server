"""
Hologres MCP Server - FastMCP Implementation
Migrated from low-level mcp.server.Server to mcp.server.fastmcp.FastMCP
"""

import re
from typing import Annotated, Optional
from pydantic import AnyUrl
from mcp.server.fastmcp import FastMCP
from hologres_mcp_server.utils import (
    try_infer_view_comments,
    handle_read_resource,
    handle_call_tool,
)

# Initialize FastMCP server
app = FastMCP("hologres-mcp-server")


# ============================================================================
# TOOLS - 12 tools migrated
# ============================================================================

@app.tool()
def execute_hg_select_sql(
    query: Annotated[str, "The (SELECT) SQL query to execute in Hologres database."]
) -> str:
    """Execute SELECT SQL to query data from Hologres database."""
    if not re.match(r"^\s*WITH\s+.*?SELECT\b", query, re.IGNORECASE | re.DOTALL) and not re.match(r"^\s*SELECT\b", query, re.IGNORECASE):
        raise ValueError("Query must be a SELECT statement or start with WITH followed by a SELECT statement")
    return handle_call_tool("execute_hg_select_sql", query, serverless=False)


@app.tool()
def execute_hg_select_sql_with_serverless(
    query: Annotated[str, "The (SELECT) SQL query to execute with serverless computing in Hologres database"]
) -> str:
    """Use Serverless Computing resources to execute SELECT SQL to query data in Hologres database. When the error like 'Total memory used by all existing queries exceeded memory limitation' occurs during execute_hg_select_sql execution, you can re-execute the SQL with this tool."""
    if not re.match(r"^\s*WITH\s+.*?SELECT\b", query, re.IGNORECASE | re.DOTALL) and not re.match(r"^\s*SELECT\b", query, re.IGNORECASE):
        raise ValueError("Query must be a SELECT statement or start with WITH followed by a SELECT statement")
    return handle_call_tool("execute_hg_select_sql_with_serverless", query, serverless=True)


@app.tool()
def execute_hg_dml_sql(
    query: Annotated[str, "The DML SQL query to execute in Hologres database"]
) -> str:
    """Execute (INSERT, UPDATE, DELETE) SQL to insert, update, and delete data in Hologres database."""
    if not any(query.strip().upper().startswith(keyword) for keyword in ["INSERT", "UPDATE", "DELETE"]):
        raise ValueError("Query must be a DML statement (INSERT, UPDATE, DELETE)")
    return handle_call_tool("execute_hg_dml_sql", query, serverless=False)


@app.tool()
def execute_hg_ddl_sql(
    query: Annotated[str, "The DDL SQL query to execute in Hologres database"]
) -> str:
    """Execute (CREATE, ALTER, DROP) SQL statements to CREATE, ALTER, or DROP tables, views, procedures, GUCs etc. in Hologres database."""
    if not any(query.strip().upper().startswith(keyword) for keyword in ["CREATE", "ALTER", "DROP", "COMMENT ON"]):
        raise ValueError("Query must be a DDL statement (CREATE, ALTER, DROP, COMMENT ON)")
    return handle_call_tool("execute_hg_ddl_sql", query, serverless=False)


@app.tool()
def gather_hg_table_statistics(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"]
) -> str:
    """Execute the ANALYZE TABLE command to have Hologres collect table statistics, enabling QO to generate better query plans."""
    query = f"ANALYZE {schema_name}.{table}"
    return handle_call_tool("gather_hg_table_statistics", query, serverless=False)


@app.tool()
def get_hg_query_plan(
    query: Annotated[str, "The SQL query to analyze in Hologres database"]
) -> str:
    """Get query plan for a SQL query in Hologres database."""
    explain_query = f"EXPLAIN {query}"
    return handle_call_tool("get_hg_query_plan", explain_query, serverless=False)


@app.tool()
def get_hg_execution_plan(
    query: Annotated[str, "The SQL query to analyze in Hologres database"]
) -> str:
    """Get actual execution plan with runtime statistics for a SQL query in Hologres database."""
    explain_query = f"EXPLAIN ANALYZE {query}"
    return handle_call_tool("get_hg_execution_plan", explain_query, serverless=False)


@app.tool()
def call_hg_procedure(
    procedure_name: Annotated[str, "The name of the stored procedure to call in Hologres database"],
    arguments: Annotated[Optional[list[str]], "The arguments to pass to the stored procedure in Hologres database"] = None
) -> str:
    """Call a stored procedure in Hologres database."""
    args_str = ', '.join(arguments) if arguments else ''
    query = f"CALL {procedure_name}({args_str})"
    return handle_call_tool("call_hg_procedure", query, serverless=False)


@app.tool()
def create_hg_maxcompute_foreign_table(
    maxcompute_project: Annotated[str, "The MaxCompute project name (required)"],
    maxcompute_tables: Annotated[list[str], "The MaxCompute table names (required)"],
    maxcompute_schema: Annotated[str, "The MaxCompute schema name (optional, default: 'default')"] = "default",
    local_schema: Annotated[str, "The local schema name in Hologres (optional, default: 'public')"] = "public"
) -> str:
    """Create a MaxCompute foreign table in Hologres database to accelerate queries on MaxCompute data."""
    maxcompute_table_list = ", ".join(maxcompute_tables)
    query = f"""
        IMPORT FOREIGN SCHEMA "{maxcompute_project}#{maxcompute_schema}"
        LIMIT TO ({maxcompute_table_list})
        FROM SERVER odps_server
        INTO {local_schema};
    """
    return handle_call_tool("create_hg_maxcompute_foreign_table", query, serverless=False)


@app.tool()
def list_hg_schemas() -> str:
    """List all schemas in the current Hologres database, excluding system schemas."""
    query = """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
        GROUP BY table_schema
        ORDER BY table_schema;
    """
    return handle_call_tool("list_hg_schemas", query, serverless=False)


@app.tool()
def list_hg_tables_in_a_schema(
    schema_name: Annotated[str, "Schema name to list tables from in Hologres database"]
) -> str:
    """List all tables in a specific schema in the current Hologres database, including their types (table, view, foreign table, partitioned table)."""
    query = f"""
        SELECT
            tab.table_name,
            CASE WHEN tab.table_type = 'VIEW' THEN ' (view)'
                WHEN tab.table_type = 'FOREIGN' THEN ' (foreign table)'
                WHEN p.partrelid IS NOT NULL THEN ' (partitioned table)'
                ELSE ''
            END AS table_type_info
        FROM
            information_schema.tables AS tab
        LEFT JOIN pg_class AS cls ON tab.table_name = cls.relname
        LEFT JOIN pg_namespace AS ns ON tab.table_schema = ns.nspname
        LEFT JOIN pg_inherits AS inh ON cls.oid = inh.inhrelid
        LEFT JOIN pg_partitioned_table AS p ON cls.oid = p.partrelid
        WHERE
            tab.table_schema NOT IN ('pg_catalog', 'information_schema', 'hologres', 'hologres_statistic', 'hologres_streaming_mv')
            AND tab.table_schema = '{schema_name}'
            AND (inh.inhrelid IS NULL OR NOT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inh.inhrelid = pg_inherits.inhrelid
            ))
        ORDER BY
            tab.table_name;
    """
    return handle_call_tool("list_hg_tables_in_a_schema", query, serverless=False)


@app.tool()
def show_hg_table_ddl(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"]
) -> str:
    """Show DDL script for a table, view, or foreign table in Hologres database."""
    query = f"SELECT hg_dump_script('\"{schema_name}\".\"{table}\"')"
    result = handle_call_tool("show_hg_table_ddl", query, serverless=False)

    # Handle VIEW DDL with comment inference
    if "Type: VIEW" in result:
        ddl = handle_read_resource("list_ddl", query)
        if ddl and ddl[0]:
            view_content = ddl[0][0].replace('\n\nEND;', '')
            comments = try_infer_view_comments(schema_name, table)
            return view_content + comments + "\n\nEND."
    return result


# ============================================================================
# RESOURCES - 6 resources migrated
# ============================================================================

@app.resource("hologres:///schemas")
def list_schemas() -> str:
    """List all schemas in Hologres database."""
    query = """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
        GROUP BY table_schema
        ORDER BY table_schema;
    """
    schemas = handle_read_resource("list_schemas", query)
    return "\n".join([schema[0] for schema in schemas])


@app.resource("hologres:///{schema}/tables")
def list_tables_in_schema(schema: str) -> str:
    """List all tables in a specific schema in Hologres database."""
    query = f"""
        SELECT
            tab.table_name,
            CASE WHEN tab.table_type = 'VIEW' THEN ' (view)'
                WHEN tab.table_type = 'FOREIGN' THEN ' (foreign table)'
                WHEN p.partrelid IS NOT NULL THEN ' (partitioned table)'
                ELSE ''
            END AS table_type_info
        FROM
            information_schema.tables AS tab
        LEFT JOIN pg_class AS cls ON tab.table_name = cls.relname
        LEFT JOIN pg_namespace AS ns ON tab.table_schema = ns.nspname
        LEFT JOIN pg_inherits AS inh ON cls.oid = inh.inhrelid
        LEFT JOIN pg_partitioned_table AS p ON cls.oid = p.partrelid
        WHERE
            tab.table_schema NOT IN ('pg_catalog', 'information_schema', 'hologres', 'hologres_statistic', 'hologres_streaming_mv')
            AND tab.table_schema = '{schema}'
            AND (inh.inhrelid IS NULL OR NOT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inh.inhrelid = pg_inherits.inhrelid
            ))
        ORDER BY
            tab.table_name;
    """
    tables = handle_read_resource("list_tables_in_schema", query)
    return "\n".join(['"' + table[0].replace('"', '""') + '"' + table[1] for table in tables])


@app.resource("hologres:///{schema}/{table}/ddl")
def get_table_ddl(schema: str, table: str) -> str:
    """Get the DDL script of a table in a specific schema in Hologres database."""
    query = f"SELECT hg_dump_script('\"{schema}\".\"{table}\"')"
    ddl = handle_read_resource("list_ddl", query)

    if ddl and ddl[0]:
        if "Type: VIEW" in ddl[0][0]:
            view_content = ddl[0][0].replace('\n\nEND;', '')
            comments = try_infer_view_comments(schema, table)
            return view_content + comments + "\n\nEND;"
        else:
            return ddl[0][0]
    return f"No DDL found for {schema}.{table}"


@app.resource("hologres:///{schema}/{table}/statistic")
def get_table_statistics(schema: str, table: str) -> str:
    """Get statistics information of a table in Hologres database."""
    query = f"""
        SELECT
            schema_name,
            table_name,
            schema_version,
            statistic_version,
            total_rows,
            analyze_timestamp
        FROM hologres_statistic.hg_table_statistic
        WHERE schema_name = '{schema}'
        AND table_name = '{table}'
        ORDER BY analyze_timestamp DESC;
    """
    rows = handle_read_resource("get_table_statistics", query)
    if not rows:
        return f"No statistics found for {schema}.{table}"

    headers = ["Schema", "Table", "Schema Version", "Stats Version", "Total Rows", "Analyze Time"]
    result = ["\t".join(headers)]
    for row in rows:
        result.append("\t".join(map(str, row)))
    return "\n".join(result)


@app.resource("hologres:///{schema}/{table}/partitions")
def get_table_partitions(schema: str, table: str) -> str:
    """List all partitions of a partitioned table in Hologres database."""
    query = f"""
        with inh as (
            SELECT i.inhrelid, i.inhparent
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent
            where n.nspname='{schema}' and c.relname='{table}'
        )
        select
            c.relname as table_name
        from inh
        join pg_catalog.pg_class c on inh.inhrelid = c.oid
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        join pg_partitioned_table p on p.partrelid = inh.inhparent order by table_name;
    """
    tables = handle_read_resource("get_table_partitions", query)
    return "\n".join([table[0] for table in tables])


@app.resource("system:///hg_instance_version")
def get_hg_instance_version() -> str:
    """Get Hologres instance version."""
    query = "SELECT HG_VERSION();"
    version = handle_read_resource("get_instance_version", query)[0][0]
    version_number = version.split(' ')[1]
    return version_number


@app.resource("system:///missing_stats_tables")
def get_missing_stats_tables() -> str:
    """Get tables with missing statistics."""
    query = """
        SELECT *
        FROM hologres_statistic.hg_stats_missing
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
        ORDER BY schemaname, tablename;
    """
    rows, headers = handle_read_resource("get_missing_stats_tables", query, with_headers=True)
    if not rows:
        return "No tables found with missing statistics"
    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


@app.resource("system:///stat_activity")
def get_stat_activity() -> str:
    """Get current database activity."""
    query = """
        SELECT * FROM hg_stat_activity ORDER BY pid;
    """
    rows, headers = handle_read_resource("get_stat_activity", query, with_headers=True)
    if not rows:
        return "No queries found with current running status"
    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


@app.resource("system:///guc_value/{guc_name}")
def get_guc_value(guc_name: str) -> str:
    """Get GUC (Grand Unified Configuration) value by name."""
    if not guc_name:
        return "GUC name cannot be empty"
    query = f"SHOW {guc_name};"
    rows = handle_read_resource("get_guc_value", query)
    if not rows:
        return f"No GUC found with name {guc_name}"
    return f"{guc_name}: {rows[0][0]}"


@app.resource("system:///query_log/latest/{row_limits}")
def get_query_log_latest(row_limits: str) -> str:
    """Get latest query log entries."""
    try:
        limit = int(row_limits)
        if limit <= 0:
            return "Row limits must be a positive integer"
        query = f"SELECT * FROM hologres.hg_query_log ORDER BY query_start DESC LIMIT {limit}"
        rows, headers = handle_read_resource("get_latest_query_log", query, with_headers=True)
    except ValueError:
        return "Invalid row limits format, must be an integer"

    if not rows:
        return "No query logs found"

    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


@app.resource("system:///query_log/user/{user_name}/{row_limits}")
def get_query_log_user(user_name: str, row_limits: str) -> str:
    """Get query log entries for a specific user."""
    if not user_name:
        return "Username cannot be empty"
    try:
        limit = int(row_limits)
        if limit <= 0:
            return "Row limits must be a positive integer"
        query = f"SELECT * FROM hologres.hg_query_log WHERE usename = '{user_name}' ORDER BY query_start DESC LIMIT {limit}"
        rows, headers = handle_read_resource("get_user_query_log", query, with_headers=True)
    except ValueError:
        return "Invalid row limits format, must be an integer"

    if not rows:
        return "No query logs found"

    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


@app.resource("system:///query_log/application/{application_name}/{row_limits}")
def get_query_log_application(application_name: str, row_limits: str) -> str:
    """Get query log entries for a specific application."""
    if not application_name:
        return "Application name cannot be empty"
    try:
        limit = int(row_limits)
        if limit <= 0:
            return "Row limits must be a positive integer"
        query = f"SELECT * FROM hologres.hg_query_log WHERE application_name = '{application_name}' ORDER BY query_start DESC LIMIT {limit}"
        rows, headers = handle_read_resource("get_application_query_log", query, with_headers=True)
    except ValueError:
        return "Invalid row limits format, must be an integer"

    if not rows:
        return "No query logs found"

    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


@app.resource("system:///query_log/failed/{interval}/{row_limits}")
def get_query_log_failed(interval: str, row_limits: str) -> str:
    """Get failed query log entries within a time interval."""
    if not interval:
        return "Interval cannot be empty"
    try:
        limit = int(row_limits)
        if limit <= 0:
            return "Row limits must be a positive integer"
        query = f"SELECT * FROM hologres.hg_query_log WHERE status = 'FAILED' AND query_start >= NOW() - INTERVAL '{interval}' ORDER BY query_start DESC LIMIT {limit}"
        rows, headers = handle_read_resource("get_failed_query_log", query, with_headers=True)
    except ValueError:
        return "Invalid row limits format, must be an integer"

    if not rows:
        return "No query logs found"

    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


# ============================================================================
# PROMPTS - New feature in FastMCP
# ============================================================================

@app.prompt()
def analyze_table_performance(schema: str, table: str) -> str:
    """Generate a prompt to analyze table performance in Hologres."""
    return f"""Please analyze the performance of table {schema}.{table} in Hologres:

1. First, check the table DDL using show_hg_table_ddl tool
2. Get table statistics using the resource hologres:///{schema}/{table}/statistic
3. If it's a partitioned table, check partitions using hologres:///{schema}/{table}/partitions
4. Analyze query patterns and suggest optimizations

Focus on:
- Distribution key selection
- Clustering key optimization
- Partition strategy (if applicable)
- Statistics freshness
- Missing statistics alerts"""

@app.prompt()
def optimize_query(query: str) -> str:
    """Generate a prompt to optimize a SQL query in Hologres."""
    return f"""Please help optimize this Hologres SQL query:

```sql
{query}
```

Steps:
1. Get the query plan using get_hg_query_plan tool
2. Get the actual execution plan using get_hg_execution_plan tool
3. Analyze for potential optimizations

Consider:
- Join order and types
- Filter selectivity
- Distribution skew
- Memory usage
- Serverless computing option for large queries"""

@app.prompt()
def explore_schema(schema: str = "public") -> str:
    """Generate a prompt to explore a schema in Hologres database."""
    return f"""Please explore the {schema} schema in Hologres:

1. List all tables using list_hg_tables_in_a_schema tool
2. For each table, show:
   - Table type (table, view, foreign table, partitioned table)
   - DDL using show_hg_table_ddl tool
   - Statistics status using hologres:///{schema}/<table>/statistic resource
3. Identify any tables with missing statistics
4. Summarize the schema structure and relationships"""


def main():
    """Main entry point to run the MCP server."""
    app.run()


if __name__ == "__main__":
    main()