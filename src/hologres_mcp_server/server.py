"""
Hologres MCP Server - FastMCP v3 Implementation
"""

import base64
from typing import Annotated

from fastmcp import FastMCP
from fastmcp.server.lifespan import lifespan

from hologres_mcp_server.settings import SERVER_VERSION
from hologres_mcp_server.utils import (
    SYSTEM_SCHEMAS_EXCLUDED,
    connect_with_retry,
    format_tabular_result,
    get_list_schemas_query,
    get_list_tables_query,
    handle_call_tool,
    handle_read_resource,
    try_infer_view_comments,
    validate_ddl_query,
    validate_dml_query,
    validate_positive_integer,
    validate_select_query,
)

# Initialize FastMCP server


@lifespan
async def validate_connection(server):
    """Validate database connection on server startup."""
    try:
        conn = connect_with_retry(retries=1)
        conn.close()
        print("Database connection validated successfully.")
    except Exception as e:
        print(f"Warning: Database connection validation failed: {e}")
    yield {}


app = FastMCP(
    name="hologres-mcp-server",
    version=SERVER_VERSION,
    instructions="""
    Hologres MCP Server provides tools and resources for interacting with
    Alibaba Cloud Hologres databases. Use the tools to execute SQL queries,
    manage schemas and tables, and analyze query performance.
    """,
    lifespan=validate_connection,
)


# ============================================================================
# TOOLS - 12 tools migrated
# ============================================================================


@app.tool(tags={"query"})
def execute_hg_select_sql(query: Annotated[str, "The (SELECT) SQL query to execute in Hologres database."]) -> str:
    """Execute SELECT SQL to query data from Hologres database."""
    validate_select_query(query)
    return handle_call_tool("execute_hg_select_sql", query, serverless=False)


@app.tool(tags={"query"})
def execute_hg_select_sql_with_serverless(
    query: Annotated[str, "The (SELECT) SQL query to execute with serverless computing in Hologres database"],
) -> str:
    """Use Serverless Computing resources to execute SELECT SQL to query data in Hologres database. When the error like 'Total memory used by all existing queries exceeded memory limitation' occurs during execute_hg_select_sql execution, you can re-execute the SQL with this tool."""
    validate_select_query(query)
    return handle_call_tool("execute_hg_select_sql_with_serverless", query, serverless=True)


@app.tool(tags={"dml"})
def execute_hg_dml_sql(query: Annotated[str, "The DML SQL query to execute in Hologres database"]) -> str:
    """Execute (INSERT, UPDATE, DELETE, REFRESH DYNAMIC TABLE) SQL to insert, update, delete data, and refresh dynamic tables in Hologres database."""
    validate_dml_query(query)
    return handle_call_tool("execute_hg_dml_sql", query, serverless=False)


@app.tool(tags={"ddl"})
def execute_hg_ddl_sql(query: Annotated[str, "The DDL SQL query to execute in Hologres database"]) -> str:
    """Execute (CREATE, ALTER, DROP) SQL statements to CREATE, ALTER, or DROP tables, views, procedures, GUCs etc. in Hologres database."""
    validate_ddl_query(query)
    return handle_call_tool("execute_hg_ddl_sql", query, serverless=False)


@app.tool(tags={"admin"})
def gather_hg_table_statistics(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
) -> str:
    """Execute the ANALYZE TABLE command to have Hologres collect table statistics, enabling QO to generate better query plans."""
    query = f"ANALYZE {schema_name}.{table}"
    return handle_call_tool("gather_hg_table_statistics", query, serverless=False)


@app.tool(tags={"analysis"})
def get_hg_query_plan(query: Annotated[str, "The SQL query to analyze in Hologres database"]) -> str:
    """Get query plan for a SQL query in Hologres database."""
    explain_query = f"EXPLAIN {query}"
    return handle_call_tool("get_hg_query_plan", explain_query, serverless=False)


@app.tool(tags={"analysis"})
def get_hg_execution_plan(query: Annotated[str, "The SQL query to analyze in Hologres database"]) -> str:
    """Get actual execution plan with runtime statistics for a SQL query in Hologres database."""
    explain_query = f"EXPLAIN ANALYZE {query}"
    return handle_call_tool("get_hg_execution_plan", explain_query, serverless=False)


@app.tool(tags={"admin"})
def call_hg_procedure(
    procedure_name: Annotated[str, "The name of the stored procedure to call in Hologres database"],
    arguments: Annotated[list[str] | None, "The arguments to pass to the stored procedure in Hologres database"] = None,
) -> str:
    """Call a stored procedure in Hologres database."""
    args_str = ", ".join(arguments) if arguments else ""
    query = f"CALL {procedure_name}({args_str})"
    return handle_call_tool("call_hg_procedure", query, serverless=False)


@app.tool(tags={"ddl"})
def create_hg_maxcompute_foreign_table(
    maxcompute_project: Annotated[str, "The MaxCompute project name (required)"],
    maxcompute_tables: Annotated[list[str], "The MaxCompute table names (required)"],
    maxcompute_schema: Annotated[str, "The MaxCompute schema name (optional, default: 'default')"] = "default",
    local_schema: Annotated[str, "The local schema name in Hologres (optional, default: 'public')"] = "public",
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


@app.tool(tags={"schema"})
def list_hg_schemas() -> str:
    """List all schemas in the current Hologres database, excluding system schemas."""
    return handle_call_tool("list_hg_schemas", get_list_schemas_query(), serverless=False)


@app.tool(tags={"schema"})
def list_hg_tables_in_a_schema(
    schema_name: Annotated[str, "Schema name to list tables from in Hologres database"],
) -> str:
    """List all tables in a specific schema in the current Hologres database, including their types (table, view, foreign table, partitioned table)."""
    return handle_call_tool("list_hg_tables_in_a_schema", get_list_tables_query(schema_name), serverless=False)


@app.tool(tags={"schema"})
def show_hg_table_ddl(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
) -> str:
    """Show DDL script for a table, view, or foreign table in Hologres database."""
    query = f'SELECT hg_dump_script(\'"{schema_name}"."{table}"\')'
    result = handle_call_tool("show_hg_table_ddl", query, serverless=False)

    if "Type: VIEW" in result:
        ddl = handle_read_resource("list_ddl", query)
        if ddl and ddl[0]:
            return _build_view_ddl_with_comments(schema_name, table, ddl[0][0])
    return result


@app.tool(tags={"query", "visualization"})
def query_and_plotly_chart(
    query: Annotated[str, "The SELECT SQL query to execute"],
    chart_type: Annotated[str, "Chart type: 'bar', 'line', 'scatter', 'pie', 'histogram', 'area'"] = "bar",
    x_column: Annotated[str, "Column name for X axis (uses first column if not specified)"] = "",
    y_column: Annotated[str, "Column name for Y axis (uses second column if not specified)"] = "",
    title: Annotated[str, "Chart title"] = "",
) -> str:
    """Execute a SELECT SQL query and generate a chart from the results. Returns both the query result data and a base64-encoded PNG image of the chart."""
    validate_select_query(query)
    return _query_and_chart(query, chart_type, x_column, y_column, title)


# ============================================================================
# RESOURCES - Helpers
# ============================================================================


def _query_resource_as_table(resource_name, query, empty_message="No data found"):
    """Execute a resource query and return formatted tabular output.

    Handles error strings from handle_read_resource gracefully.
    """
    result = handle_read_resource(resource_name, query, with_headers=True)
    if isinstance(result, str):
        return result
    rows, headers = result
    if not rows:
        return empty_message
    return format_tabular_result(rows, headers)


def _query_log_resource(resource_name, where_clauses, row_limits):
    """Shared handler for query log resources."""
    limit, error = validate_positive_integer(row_limits)
    if error:
        return error
    where_sql = " AND ".join(where_clauses)
    if where_sql:
        where_sql = " WHERE " + where_sql
    query = f"SELECT * FROM hologres.hg_query_log{where_sql} ORDER BY query_start DESC LIMIT {limit}"
    return _query_resource_as_table(resource_name, query, "No query logs found")


def _build_view_ddl_with_comments(schema, table, raw_ddl):
    """Process view DDL: strip trailing END;, infer comments, reassemble."""
    view_content = raw_ddl.replace("\n\nEND;", "")
    comments = try_infer_view_comments(schema, table)
    return view_content + comments + "\n\nEND;"


def _query_and_chart(query, chart_type, x_column, y_column, title):
    """Execute SQL query and generate a chart, returning data + base64 PNG."""
    try:
        import io

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                headers = [desc[0] for desc in cursor.description]

        if not rows:
            return "Query returned no data."

        # Determine columns
        x_col = x_column if x_column else headers[0]
        y_col = y_column if y_column else (headers[1] if len(headers) > 1 else headers[0])
        x_idx = headers.index(x_col) if x_col in headers else 0
        y_idx = headers.index(y_col) if y_col in headers else (1 if len(headers) > 1 else 0)

        x_data = [row[x_idx] for row in rows]
        y_data = [row[y_idx] for row in rows]

        # Try to convert y to numeric
        try:
            y_data = [float(v) if v is not None else 0 for v in y_data]
        except (ValueError, TypeError):
            pass

        # Generate chart
        fig, ax = plt.subplots(figsize=(10, 6))
        chart_title = title if title else f"{chart_type.capitalize()} Chart: {y_col} by {x_col}"

        if chart_type == "bar":
            ax.bar(range(len(x_data)), y_data, tick_label=[str(x) for x in x_data])
            plt.xticks(rotation=45, ha="right")
        elif chart_type == "line":
            ax.plot(range(len(x_data)), y_data, marker="o")
            ax.set_xticks(range(len(x_data)))
            ax.set_xticklabels([str(x) for x in x_data], rotation=45, ha="right")
        elif chart_type == "scatter":
            try:
                x_numeric = [float(v) if v is not None else 0 for v in x_data]
                ax.scatter(x_numeric, y_data)
                ax.set_xlabel(x_col)
            except (ValueError, TypeError):
                ax.scatter(range(len(x_data)), y_data)
        elif chart_type == "pie":
            ax.pie(y_data, labels=[str(x) for x in x_data], autopct="%1.1f%%")
        elif chart_type == "histogram":
            ax.hist(y_data, bins="auto", edgecolor="black")
        elif chart_type == "area":
            ax.fill_between(range(len(x_data)), y_data, alpha=0.4)
            ax.plot(range(len(x_data)), y_data)
            ax.set_xticks(range(len(x_data)))
            ax.set_xticklabels([str(x) for x in x_data], rotation=45, ha="right")
        else:
            plt.close(fig)
            return f"Unsupported chart type: {chart_type}. Supported: bar, line, scatter, pie, histogram, area"

        ax.set_title(chart_title)
        if chart_type != "pie":
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
        plt.tight_layout()

        # Convert to base64
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode("utf-8")

        # Format data table
        data_parts = ["\t".join(headers)]
        for row in rows[:50]:  # Limit to 50 rows in output
            data_parts.append("\t".join(str(v) if v is not None else "NULL" for v in row))

        result_parts = [
            "## Query Results",
            f"Rows: {len(rows)}",
            "",
            "\n".join(data_parts),
            "",
            "## Chart",
            f"![{chart_title}](data:image/png;base64,{img_base64})",
        ]
        return "\n".join(result_parts)

    except Exception as e:
        return f"Error generating chart: {str(e)}"


# ============================================================================
# RESOURCES - Resource handlers
# ============================================================================


@app.resource("hologres:///schemas")
def list_schemas() -> str:
    """List all schemas in Hologres database."""
    schemas = handle_read_resource("list_schemas", get_list_schemas_query())
    return "\n".join([schema[0] for schema in schemas])


@app.resource("hologres:///{schema}/tables")
def list_tables_in_schema(schema: str) -> str:
    """List all tables in a specific schema in Hologres database."""
    tables = handle_read_resource("list_tables_in_schema", get_list_tables_query(schema))
    return "\n".join(['"' + table[0].replace('"', '""') + '"' + table[1] for table in tables])


@app.resource("hologres:///{schema}/{table}/ddl")
def get_table_ddl(schema: str, table: str) -> str:
    """Get the DDL script of a table in a specific schema in Hologres database."""
    query = f'SELECT hg_dump_script(\'"{schema}"."{table}"\')'
    ddl = handle_read_resource("list_ddl", query)

    if ddl and ddl[0]:
        if "Type: VIEW" in ddl[0][0]:
            return _build_view_ddl_with_comments(schema, table, ddl[0][0])
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
    return format_tabular_result(rows, headers)


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
    version_number = version.split(" ")[1]
    return version_number


@app.resource("system:///missing_stats_tables")
def get_missing_stats_tables() -> str:
    """Get tables with missing statistics."""
    query = f"""
        SELECT *
        FROM hologres_statistic.hg_stats_missing
        WHERE schemaname NOT IN ('{SYSTEM_SCHEMAS_EXCLUDED}')
        ORDER BY schemaname, tablename;
    """
    return _query_resource_as_table("get_missing_stats_tables", query, "No tables found with missing statistics")


@app.resource("system:///stat_activity")
def get_stat_activity() -> str:
    """Get current database activity."""
    query = "SELECT * FROM hg_stat_activity ORDER BY pid;"
    return _query_resource_as_table("get_stat_activity", query, "No queries found with current running status")


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
    return _query_log_resource("get_latest_query_log", [], row_limits)


@app.resource("system:///query_log/user/{user_name}/{row_limits}")
def get_query_log_user(user_name: str, row_limits: str) -> str:
    """Get query log entries for a specific user."""
    if not user_name:
        return "Username cannot be empty"
    return _query_log_resource("get_user_query_log", [f"usename = '{user_name}'"], row_limits)


@app.resource("system:///query_log/application/{application_name}/{row_limits}")
def get_query_log_application(application_name: str, row_limits: str) -> str:
    """Get query log entries for a specific application."""
    if not application_name:
        return "Application name cannot be empty"
    return _query_log_resource("get_application_query_log", [f"application_name = '{application_name}'"], row_limits)


@app.resource("system:///query_log/failed/{interval}/{row_limits}")
def get_query_log_failed(interval: str, row_limits: str) -> str:
    """Get failed query log entries within a time interval."""
    if not interval:
        return "Interval cannot be empty"
    return _query_log_resource(
        "get_failed_query_log", ["status = 'FAILED'", f"query_start >= NOW() - INTERVAL '{interval}'"], row_limits
    )


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
