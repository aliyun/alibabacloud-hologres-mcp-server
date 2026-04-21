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


@app.tool(tags={"analysis"})
def analyze_hg_query_by_id(
    query_id: Annotated[str, "The query_id from hg_query_log to analyze"],
) -> str:
    """Analyze a specific query's performance profile by its query_id from hg_query_log. Returns detailed metrics including duration, memory usage, CPU time, read/write stats, and execution plan."""
    return _analyze_query_by_id(query_id)


@app.tool(tags={"analysis"})
def get_hg_slow_queries(
    min_duration_ms: Annotated[int, "Minimum query duration in milliseconds to filter (default 1000)"] = 1000,
    limit: Annotated[int, "Maximum number of queries to return (default 20)"] = 20,
) -> str:
    """Get slow queries from hg_query_log ordered by duration. Useful for identifying performance bottlenecks."""
    return _get_slow_queries(min_duration_ms, limit)


@app.tool(tags={"admin"})
def list_hg_dynamic_tables(
    schema_name: Annotated[str, "Schema name to filter (empty for all schemas)"] = "",
) -> str:
    """List all Dynamic Tables with their status, freshness settings, and last refresh info."""
    return _list_dynamic_tables(schema_name)


@app.tool(tags={"admin"})
def get_hg_dynamic_table_refresh_history(
    schema_name: Annotated[str, "Schema name of the dynamic table"],
    table_name: Annotated[str, "Dynamic table name"],
    limit: Annotated[int, "Maximum number of history records (default 10)"] = 10,
) -> str:
    """Get refresh history for a specific Dynamic Table, including duration, status, and latency."""
    return _get_dynamic_table_refresh_history(schema_name, table_name, limit)


@app.tool(tags={"admin"})
def list_hg_recyclebin() -> str:
    """List all tables in the Hologres recycle bin (dropped tables that can be restored)."""
    return _list_recyclebin()


@app.tool(tags={"admin"})
def restore_hg_table_from_recyclebin(
    table_name: Annotated[str, "The original table name to restore from recycle bin"],
    schema_name: Annotated[str, "Schema name of the table (default 'public')"] = "public",
) -> str:
    """Restore a dropped table from the Hologres recycle bin. Only works if the table is still in the recycle bin."""
    return _restore_from_recyclebin(schema_name, table_name)


@app.tool(tags={"admin"})
def list_hg_warehouses() -> str:
    """List all computing groups (warehouses) in the Hologres instance, including their CPU, memory, cluster count, and status."""
    return _list_warehouses()


@app.tool(tags={"admin"})
def switch_hg_warehouse(
    warehouse_name: Annotated[str, "The warehouse (computing group) name to switch to"],
) -> str:
    """Switch the current session's computing resource to a specified warehouse (computing group). Use 'local' for the default computing group."""
    return _switch_warehouse(warehouse_name)


@app.tool(tags={"analysis"})
def get_hg_table_storage_size(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
) -> str:
    """Get storage size details of a table, including total size, data, index, and metadata breakdown."""
    return _get_table_storage_size(schema_name, table)


@app.tool(tags={"admin"})
def cancel_hg_query(
    pid: Annotated[int, "The process ID (pid) of the query to cancel"],
    terminate: Annotated[bool, "If True, forcefully terminate the backend process (default False, just cancel the query)"] = False,
) -> str:
    """Cancel or terminate a running query by its process ID. Use list_hg_active_queries to find the pid first."""
    return _cancel_query(pid, terminate)


@app.tool(tags={"analysis"})
def list_hg_active_queries(
    state: Annotated[str, "Filter by state: 'active', 'idle', 'all' (default 'active')"] = "active",
) -> str:
    """List currently active queries and connections from pg_stat_activity. Useful for monitoring running queries and finding PIDs to cancel."""
    return _list_active_queries(state)


@app.tool(tags={"admin"})
def list_hg_query_queues() -> str:
    """List all Query Queues and their classifiers, showing concurrency limits, queue sizes, and routing rules. Requires Hologres V3.0+."""
    return _list_query_queues()


@app.tool(tags={"schema"})
def get_hg_table_properties(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
) -> str:
    """Get table properties including distribution_key, clustering_key, segment_key, bitmap_columns, dictionary_columns, binlog settings, etc."""
    return _get_table_properties(schema_name, table)


@app.tool(tags={"analysis"})
def get_hg_table_shard_info(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
) -> str:
    """Get table's Table Group and shard count info. Useful for diagnosing data skew and shard configuration."""
    return _get_table_shard_info(schema_name, table)


@app.tool(tags={"schema"})
def list_hg_external_databases() -> str:
    """List all External Databases (federated databases for Lakehouse acceleration). Requires Hologres V3.0+."""
    return _list_external_databases()


@app.tool(tags={"analysis"})
def get_hg_lock_diagnostics() -> str:
    """Diagnose lock contention by showing blocking and waiting queries. Useful for identifying lock waits that cause query hangs."""
    return _get_lock_diagnostics()


@app.tool(tags={"analysis"})
def get_hg_table_info_trend(
    schema_name: Annotated[str, "Schema name in Hologres database"],
    table: Annotated[str, "Table name in Hologres database"],
    days: Annotated[int, "Number of days to look back (default 7)"] = 7,
) -> str:
    """Get table storage trend from hg_table_info, showing daily storage size, file count, and row count changes. Data is T+1, retained for 30 days."""
    return _get_table_info_trend(schema_name, table, days)


@app.tool(tags={"admin"})
def manage_hg_query_queue(
    action: Annotated[str, "Action: 'create', 'drop', or 'clear'"],
    queue_name: Annotated[str, "Name of the query queue"],
    max_concurrency: Annotated[int, "Max concurrency for the queue (required for 'create')"] = 0,
    max_queue_size: Annotated[int, "Max queue size (required for 'create')"] = 0,
) -> str:
    """Create, drop, or clear a Query Queue. Requires Hologres V3.0+ and superuser privileges."""
    return _manage_query_queue(action, queue_name, max_concurrency, max_queue_size)


@app.tool(tags={"admin"})
def manage_hg_classifier(
    action: Annotated[str, "Action: 'create' or 'drop'"],
    queue_name: Annotated[str, "Name of the query queue the classifier belongs to"],
    classifier_name: Annotated[str, "Name of the classifier"],
    priority: Annotated[int, "Priority for the classifier (required for 'create', higher = matched first)"] = 0,
) -> str:
    """Create or drop a classifier for a Query Queue. Use set_hg_query_queue_property to configure classifier rules after creation. Requires Hologres V3.0+."""
    return _manage_classifier(action, queue_name, classifier_name, priority)


@app.tool(tags={"admin"})
def set_hg_query_queue_property(
    target: Annotated[str, "Target type: 'queue' or 'classifier'"],
    queue_name: Annotated[str, "Name of the query queue"],
    property_key: Annotated[str, "Property key to set (e.g. 'max_concurrency', 'query_timeout_ms' for queue; 'condition_name' for classifier rule)"],
    property_value: Annotated[str, "Property value to set"],
    classifier_name: Annotated[str, "Classifier name (required when target='classifier')"] = "",
    action: Annotated[str, "Action: 'set' to add/update, 'remove' to delete the property (default 'set')"] = "set",
) -> str:
    """Set or remove properties on a Query Queue or classifier. For classifier rules, use property_key as condition_name and property_value as condition_value. Requires Hologres V3.0+."""
    return _set_query_queue_property(target, queue_name, property_key, property_value, classifier_name, action)


@app.tool(tags={"admin"})
def manage_hg_warehouse(
    action: Annotated[str, "Action: 'suspend', 'resume', 'restart', 'rename', or 'resize'"],
    warehouse_name: Annotated[str, "Name of the warehouse (computing group)"],
    cu: Annotated[int, "CU count for resize action"] = 0,
    new_name: Annotated[str, "New name for rename action"] = "",
) -> str:
    """Manage a computing group (warehouse): suspend, resume, restart, rename, or resize. Requires superuser privileges."""
    return _manage_warehouse(action, warehouse_name, cu, new_name)


@app.tool(tags={"admin"})
def get_hg_warehouse_status(
    warehouse_name: Annotated[str, "Name of the warehouse (computing group)"],
) -> str:
    """Get detailed running status and scaling progress of a computing group (warehouse)."""
    return _get_warehouse_status(warehouse_name)


@app.tool(tags={"admin"})
def rebalance_hg_warehouse(
    warehouse_name: Annotated[str, "Name of the warehouse (computing group) to rebalance"],
) -> str:
    """Trigger shard rebalancing for a computing group to eliminate data skew across nodes."""
    return _rebalance_warehouse(warehouse_name)


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


def _analyze_query_by_id(query_id):
    """Analyze a query's performance profile from hg_query_log."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM hologres.hg_query_log WHERE query_id = %s",
                    [query_id],
                )
                row = cursor.fetchone()
                if not row:
                    return f"No query found with query_id: {query_id}"
                headers = [desc[0] for desc in cursor.description]

                parts = [f"## Query Profile: {query_id}", ""]

                # Key metrics
                data = dict(zip(headers, row, strict=False))
                key_fields = [
                    ("status", "Status"),
                    ("duration", "Duration (ms)"),
                    ("query_start", "Start Time"),
                    ("usename", "User"),
                    ("application_name", "Application"),
                    ("read_bytes", "Read Bytes"),
                    ("write_bytes", "Write Bytes"),
                    ("memory_bytes", "Memory"),
                    ("shuffle_bytes", "Shuffle Bytes"),
                    ("cpu_time_ms", "CPU Time (ms)"),
                    ("physical_reads", "Physical Reads"),
                    ("query_detail", "Query"),
                ]

                parts.append("### Key Metrics")
                for field, label in key_fields:
                    if field in data and data[field] is not None:
                        value = data[field]
                        if "bytes" in field.lower() and isinstance(value, (int, float)):
                            value = _format_bytes(value)
                        parts.append(f"- **{label}**: {value}")

                # All other fields
                parts.append("")
                parts.append("### All Fields")
                for h, v in zip(headers, row, strict=False):
                    if v is not None:
                        parts.append(f"- {h}: {v}")

                return "\n".join(parts)
    except Exception as e:
        return f"Error analyzing query: {str(e)}"


def _get_slow_queries(min_duration_ms, limit):
    """Get slow queries from hg_query_log."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT query_id, usename, status, duration, query_start,
                           read_bytes, memory_bytes, cpu_time_ms,
                           LEFT(query_detail, 200) as query_preview
                    FROM hologres.hg_query_log
                    WHERE duration >= %s
                    ORDER BY duration DESC
                    LIMIT %s
                    """,
                    [min_duration_ms, limit],
                )
                rows = cursor.fetchall()
                if not rows:
                    return f"No queries found with duration >= {min_duration_ms}ms."

                headers = [desc[0] for desc in cursor.description]
                parts = [f"## Slow Queries (duration >= {min_duration_ms}ms)", ""]
                parts.append("\t".join(headers))
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                parts.append(f"\nTotal: {len(rows)} queries")
                return "\n".join(parts)
    except Exception as e:
        return f"Error getting slow queries: {str(e)}"


def _format_bytes(bytes_val):
    """Format bytes to human-readable string."""
    try:
        bytes_val = float(bytes_val)
    except (TypeError, ValueError):
        return str(bytes_val)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(bytes_val) < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def _list_dynamic_tables(schema_name=""):
    """List all Dynamic Tables with status and refresh info."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT
                        schema_name,
                        table_name,
                        status,
                        freshness,
                        last_refresh_time,
                        last_refresh_duration_ms,
                        last_refresh_rows
                    FROM hologres.hg_dynamic_table_status
                """
                params = []
                if schema_name:
                    query += " WHERE schema_name = %s"
                    params.append(schema_name)
                query += " ORDER BY schema_name, table_name"
                cursor.execute(query, params)
                rows = cursor.fetchall()

                if not rows:
                    return "No Dynamic Tables found."

                parts = ["## Dynamic Tables", ""]
                parts.append("Schema\tTable\tStatus\tFreshness\tLast Refresh\tDuration(ms)\tRows")
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                parts.append(f"\nTotal: {len(rows)} Dynamic Tables")
                return "\n".join(parts)
    except Exception as e:
        return f"Error listing dynamic tables: {str(e)}"


def _get_dynamic_table_refresh_history(schema_name, table_name, limit=10):
    """Get refresh history for a specific Dynamic Table."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        refresh_id,
                        status,
                        start_time,
                        end_time,
                        duration_ms,
                        refreshed_rows,
                        data_latency_ms
                    FROM hologres.hg_dynamic_table_refresh_history
                    WHERE schema_name = %s AND table_name = %s
                    ORDER BY start_time DESC
                    LIMIT %s
                    """,
                    [schema_name, table_name, limit],
                )
                rows = cursor.fetchall()

                if not rows:
                    return f"No refresh history found for {schema_name}.{table_name}."

                parts = [f"## Refresh History: {schema_name}.{table_name}", ""]
                parts.append("ID\tStatus\tStart\tEnd\tDuration(ms)\tRows\tLatency(ms)")
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                return "\n".join(parts)
    except Exception as e:
        return f"Error getting refresh history: {str(e)}"


def _list_recyclebin():
    """List tables in the Hologres recycle bin."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        table_id,
                        schema_name,
                        table_name,
                        drop_time,
                        remaining_time
                    FROM hologres.hg_recyclebin
                    ORDER BY drop_time DESC
                    """
                )
                rows = cursor.fetchall()

                if not rows:
                    return "Recycle bin is empty."

                parts = ["## Recycle Bin", ""]
                parts.append("Table ID\tSchema\tTable Name\tDrop Time\tRemaining Time")
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                parts.append(f"\nTotal: {len(rows)} tables in recycle bin")
                return "\n".join(parts)
    except Exception as e:
        return f"Error listing recycle bin: {str(e)}"


def _restore_from_recyclebin(schema_name, table_name):
    """Restore a table from the recycle bin."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                # Find the table in recyclebin
                cursor.execute(
                    """
                    SELECT table_id, table_name
                    FROM hologres.hg_recyclebin
                    WHERE schema_name = %s AND table_name = %s
                    ORDER BY drop_time DESC
                    LIMIT 1
                    """,
                    [schema_name, table_name],
                )
                row = cursor.fetchone()
                if not row:
                    return f"Table '{schema_name}.{table_name}' not found in recycle bin."

                table_id = row[0]
                # Use RECOVER TABLE syntax
                cursor.execute(
                    f'RECOVER TABLE "{schema_name}"."{table_name}" WITH (table_id = {int(table_id)})'
                )
                return f"Successfully restored table '{schema_name}.{table_name}' (table_id={table_id}) from recycle bin."
    except Exception as e:
        return f"Error restoring table: {str(e)}"


def _list_warehouses():
    """List all computing groups (warehouses)."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                # Get current warehouse
                cursor.execute("SHOW hg_computing_resource")
                current = cursor.fetchone()[0] if cursor.description else "unknown"

                # Get all warehouses
                cursor.execute(
                    """
                    SELECT
                        warehouse_id,
                        warehouse_name,
                        cpu,
                        memory,
                        cluster_count,
                        status,
                        is_default
                    FROM hologres.hg_warehouses
                    ORDER BY warehouse_id
                    """
                )
                rows = cursor.fetchall()

                if not rows:
                    return f"Current computing resource: {current}\nNo warehouses found (single-warehouse mode)."

                parts = [
                    "## Computing Groups (Warehouses)",
                    f"Current: {current}",
                    "",
                    "ID\tName\tCPU\tMemory\tClusters\tStatus\tDefault",
                ]
                for row in rows:
                    wh_id, name, cpu, mem, clusters, status, is_default = row
                    default_str = "Yes" if is_default else "No"
                    parts.append(f"{wh_id}\t{name}\t{cpu}\t{mem}\t{clusters}\t{status}\t{default_str}")
                return "\n".join(parts)
    except Exception as e:
        return f"Error listing warehouses: {str(e)}"


def _switch_warehouse(warehouse_name):
    """Switch the default warehouse (computing group)."""
    try:
        # For 'local' or 'serverless', use SET directly
        if warehouse_name.lower() in ("local", "serverless"):
            with connect_with_retry() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SET hg_computing_resource = '{warehouse_name.lower()}'")
                    return f"Successfully switched computing resource to '{warehouse_name.lower()}'."

        # For named warehouses, validate first then use CALL
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                # Validate warehouse exists
                cursor.execute(
                    "SELECT warehouse_name FROM hologres.hg_warehouses WHERE warehouse_name = %s",
                    [warehouse_name],
                )
                if not cursor.fetchone():
                    return f"Warehouse '{warehouse_name}' not found. Use list_hg_warehouses to see available options."

                # CALL doesn't support parameterized queries, use validated name
                safe_name = warehouse_name.replace("'", "''")
                cursor.execute(f"CALL hg_set_default_warehouse('{safe_name}')")
                return f"Successfully switched default warehouse to '{warehouse_name}'."
    except Exception as e:
        return f"Error switching warehouse: {str(e)}"


def _get_table_storage_size(schema_name, table):
    """Get storage size breakdown for a table."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                full_name = f'"{schema_name}"."{table}"'
                # Total size
                cursor.execute(f"SELECT pg_total_relation_size('{full_name}')")
                total = cursor.fetchone()[0]
                # Relation size (data only)
                cursor.execute(f"SELECT pg_relation_size('{full_name}')")
                data_size = cursor.fetchone()[0]

                parts = [f"## Storage Size: {schema_name}.{table}", ""]
                parts.append(f"- **Total**: {_format_bytes(total)}")
                parts.append(f"- **Data**: {_format_bytes(data_size)}")
                parts.append(f"- **Index + Meta**: {_format_bytes(total - data_size)}")

                # Try hologres.hg_relation_size for detailed breakdown
                try:
                    cursor.execute(
                        f"SELECT hologres.hg_relation_size('{full_name}', 'data')"
                    )
                    hg_data = cursor.fetchone()[0]
                    cursor.execute(
                        f"SELECT hologres.hg_relation_size('{full_name}', 'index')"
                    )
                    hg_index = cursor.fetchone()[0]
                    cursor.execute(
                        f"SELECT hologres.hg_relation_size('{full_name}', 'meta')"
                    )
                    hg_meta = cursor.fetchone()[0]
                    parts.append("")
                    parts.append("### Detailed Breakdown (hg_relation_size)")
                    parts.append(f"- Data: {_format_bytes(hg_data)}")
                    parts.append(f"- Index: {_format_bytes(hg_index)}")
                    parts.append(f"- Meta: {_format_bytes(hg_meta)}")
                except Exception:
                    pass  # hg_relation_size not available in older versions

                return "\n".join(parts)
    except Exception as e:
        return f"Error getting table storage size: {str(e)}"


def _cancel_query(pid, terminate=False):
    """Cancel or terminate a running query."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if terminate:
                    cursor.execute("SELECT pg_terminate_backend(%s)", [pid])
                    result = cursor.fetchone()[0]
                    if result:
                        return f"Successfully terminated backend process (pid={pid})."
                    else:
                        return f"Failed to terminate backend (pid={pid}). Process may not exist or already finished."
                else:
                    cursor.execute("SELECT pg_cancel_backend(%s)", [pid])
                    result = cursor.fetchone()[0]
                    if result:
                        return f"Successfully cancelled query (pid={pid})."
                    else:
                        return f"Failed to cancel query (pid={pid}). Process may not exist or already finished."
    except Exception as e:
        return f"Error cancelling query: {str(e)}"


def _list_active_queries(state="active"):
    """List active queries from pg_stat_activity."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT
                        pid,
                        usename,
                        datname,
                        state,
                        application_name,
                        query_start,
                        NOW() - query_start AS duration,
                        LEFT(query, 200) AS query_preview
                    FROM pg_stat_activity
                    WHERE pid != pg_backend_pid()
                """
                if state == "active":
                    query += " AND state = 'active'"
                elif state == "idle":
                    query += " AND state LIKE 'idle%'"
                # 'all' - no extra filter
                query += " ORDER BY query_start ASC"
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    return f"No {state} queries found."

                headers = [desc[0] for desc in cursor.description]
                parts = [f"## Active Queries (state={state})", f"Total: {len(rows)}", ""]
                parts.append("\t".join(headers))
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                return "\n".join(parts)
    except Exception as e:
        return f"Error listing active queries: {str(e)}"


def _list_query_queues():
    """List all Query Queues and classifiers."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                # Get query queues
                cursor.execute(
                    """
                    SELECT *
                    FROM hologres.hg_query_queues
                    ORDER BY queue_name
                    """
                )
                queue_rows = cursor.fetchall()
                queue_headers = [desc[0] for desc in cursor.description] if cursor.description else []

                # Get classifiers
                cursor.execute(
                    """
                    SELECT *
                    FROM hologres.hg_classifiers
                    ORDER BY classifier_name
                    """
                )
                classifier_rows = cursor.fetchall()
                classifier_headers = [desc[0] for desc in cursor.description] if cursor.description else []

                parts = ["## Query Queues"]

                if not queue_rows:
                    parts.append("No query queues configured.")
                else:
                    parts.append(f"\nTotal queues: {len(queue_rows)}")
                    parts.append("\t".join(queue_headers))
                    for row in queue_rows:
                        parts.append("\t".join(str(v) if v is not None else "" for v in row))

                parts.append("")
                parts.append("## Classifiers")
                if not classifier_rows:
                    parts.append("No classifiers configured.")
                else:
                    parts.append(f"\nTotal classifiers: {len(classifier_rows)}")
                    parts.append("\t".join(classifier_headers))
                    for row in classifier_rows:
                        parts.append("\t".join(str(v) if v is not None else "" for v in row))

                return "\n".join(parts)
    except Exception as e:
        if "does not exist" in str(e):
            return "Query Queue feature not available. Requires Hologres V3.0+."
        return f"Error listing query queues: {str(e)}"


def _get_table_properties(schema_name, table):
    """Get table properties from hologres.hg_table_properties."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT property_key, property_value
                    FROM hologres.hg_table_properties
                    WHERE schema_name = %s AND table_name = %s
                    ORDER BY property_key
                    """,
                    [schema_name, table],
                )
                rows = cursor.fetchall()

                if not rows:
                    return f"No properties found for {schema_name}.{table}."

                parts = [f"## Table Properties: {schema_name}.{table}", ""]
                for key, value in rows:
                    parts.append(f"- **{key}**: {value}")
                return "\n".join(parts)
    except Exception as e:
        return f"Error getting table properties: {str(e)}"


def _get_table_shard_info(schema_name, table):
    """Get Table Group and shard info for a table."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                # Get table group from hg_table_properties
                cursor.execute(
                    """
                    SELECT property_value
                    FROM hologres.hg_table_properties
                    WHERE schema_name = %s AND table_name = %s
                      AND property_key = 'table_group'
                    """,
                    [schema_name, table],
                )
                tg_row = cursor.fetchone()
                table_group = tg_row[0] if tg_row else "unknown"

                # Get table group properties (shard_count)
                parts = [f"## Shard Info: {schema_name}.{table}", ""]
                parts.append(f"- **Table Group**: {table_group}")

                if table_group and table_group != "unknown":
                    cursor.execute(
                        """
                        SELECT property_key, property_value
                        FROM hologres.hg_table_group_properties
                        WHERE tablegroup_name = %s
                        ORDER BY property_key
                        """,
                        [table_group],
                    )
                    tg_props = cursor.fetchall()
                    if tg_props:
                        parts.append("")
                        parts.append("### Table Group Properties")
                        for key, value in tg_props:
                            parts.append(f"- **{key}**: {value}")

                return "\n".join(parts)
    except Exception as e:
        return f"Error getting shard info: {str(e)}"


def _list_external_databases():
    """List all External Databases."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        d.datname AS database_name,
                        d.datdba::regrole AS owner,
                        pg_catalog.shobj_description(d.oid, 'pg_database') AS description
                    FROM pg_database d
                    WHERE d.datname LIKE 'external_%'
                       OR d.oid IN (
                           SELECT oid FROM pg_database
                           WHERE datistemplate = false
                             AND datallowconn = false
                       )
                    ORDER BY d.datname
                    """
                )
                rows = cursor.fetchall()

                if not rows:
                    # Try alternative: look for foreign servers
                    cursor.execute(
                        """
                        SELECT
                            s.srvname AS server_name,
                            s.srvtype AS server_type,
                            s.srvoptions AS options
                        FROM pg_foreign_server s
                        ORDER BY s.srvname
                        """
                    )
                    srv_rows = cursor.fetchall()
                    if not srv_rows:
                        return "No External Databases or Foreign Servers found."

                    parts = ["## Foreign Servers", ""]
                    parts.append("Name\tType\tOptions")
                    for row in srv_rows:
                        parts.append("\t".join(str(v) if v is not None else "" for v in row))
                    return "\n".join(parts)

                parts = ["## External Databases", ""]
                parts.append("Database\tOwner\tDescription")
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                parts.append(f"\nTotal: {len(rows)} external databases")
                return "\n".join(parts)
    except Exception as e:
        return f"Error listing external databases: {str(e)}"


def _get_lock_diagnostics():
    """Diagnose lock contention by joining pg_locks with pg_stat_activity."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        blocked.pid AS blocked_pid,
                        blocked.usename AS blocked_user,
                        blocking.pid AS blocking_pid,
                        blocking.usename AS blocking_user,
                        blocked.state AS blocked_state,
                        blocked.wait_event_type,
                        blocked.wait_event,
                        NOW() - blocked.query_start AS blocked_duration,
                        LEFT(blocked.query, 150) AS blocked_query,
                        LEFT(blocking.query, 150) AS blocking_query
                    FROM pg_stat_activity blocked
                    JOIN pg_locks blocked_locks ON blocked.pid = blocked_locks.pid
                    JOIN pg_locks blocking_locks ON blocked_locks.locktype = blocking_locks.locktype
                        AND blocked_locks.relation = blocking_locks.relation
                        AND blocked_locks.pid != blocking_locks.pid
                    JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
                    WHERE NOT blocked_locks.granted
                      AND blocking_locks.granted
                    ORDER BY blocked_duration DESC
                    """
                )
                rows = cursor.fetchall()

                if not rows:
                    return "No lock contention detected."

                headers = [desc[0] for desc in cursor.description]
                parts = ["## Lock Diagnostics", f"Blocked queries: {len(rows)}", ""]
                parts.append("\t".join(headers))
                for row in rows:
                    parts.append("\t".join(str(v) if v is not None else "" for v in row))
                return "\n".join(parts)
    except Exception as e:
        return f"Error getting lock diagnostics: {str(e)}"


def _get_table_info_trend(schema_name, table, days=7):
    """Get table storage trend from hologres.hg_table_info."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        collect_date,
                        hot_storage_size,
                        cold_storage_size,
                        hot_file_count,
                        row_count,
                        read_sql_count,
                        write_sql_count
                    FROM hologres.hg_table_info
                    WHERE schema_name = %s
                      AND table_name = %s
                      AND collect_date >= CURRENT_DATE - %s
                    ORDER BY collect_date DESC
                    """,
                    [schema_name, table, days],
                )
                rows = cursor.fetchall()

                if not rows:
                    return f"No hg_table_info data found for {schema_name}.{table} in the last {days} days."

                parts = [f"## Storage Trend: {schema_name}.{table} (last {days} days)", ""]
                parts.append("Date\tHot Storage\tCold Storage\tFiles\tRows\tRead SQL\tWrite SQL")
                for row in rows:
                    date, hot, cold, files, row_count, reads, writes = row
                    parts.append(
                        f"{date}\t{_format_bytes(hot or 0)}\t{_format_bytes(cold or 0)}\t"
                        f"{files or 0}\t{row_count or 0}\t{reads or 0}\t{writes or 0}"
                    )
                return "\n".join(parts)
    except Exception as e:
        if "does not exist" in str(e):
            return "hg_table_info not available. Requires Hologres V1.3+."
        return f"Error getting table info trend: {str(e)}"


def _manage_query_queue(action, queue_name, max_concurrency=0, max_queue_size=0):
    """Create, drop, or clear a Query Queue."""
    try:
        action = action.lower().strip()
        safe_name = queue_name.replace("'", "''")
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if action == "create":
                    if max_concurrency <= 0 or max_queue_size <= 0:
                        return "Error: max_concurrency and max_queue_size must be positive integers for 'create' action."
                    cursor.execute(
                        f"CALL hg_create_query_queue('{safe_name}', {int(max_concurrency)}, {int(max_queue_size)})"
                    )
                    return f"Successfully created query queue '{queue_name}' (max_concurrency={max_concurrency}, max_queue_size={max_queue_size})."
                elif action == "drop":
                    cursor.execute(f"CALL hg_drop_query_queue('{safe_name}')")
                    return f"Successfully dropped query queue '{queue_name}'."
                elif action == "clear":
                    cursor.execute(f"CALL hg_clear_query_queue('{safe_name}')")
                    return f"Successfully cleared all queued requests in queue '{queue_name}'."
                else:
                    return f"Unknown action '{action}'. Supported: 'create', 'drop', 'clear'."
    except Exception as e:
        return f"Error managing query queue: {str(e)}"


def _manage_classifier(action, queue_name, classifier_name, priority=0):
    """Create or drop a classifier."""
    try:
        action = action.lower().strip()
        safe_queue = queue_name.replace("'", "''")
        safe_classifier = classifier_name.replace("'", "''")
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if action == "create":
                    cursor.execute(
                        f"CALL hg_create_classifier('{safe_queue}', '{safe_classifier}', {int(priority)})"
                    )
                    return f"Successfully created classifier '{classifier_name}' in queue '{queue_name}' (priority={priority})."
                elif action == "drop":
                    cursor.execute(
                        f"CALL hg_drop_classifier('{safe_queue}', '{safe_classifier}')"
                    )
                    return f"Successfully dropped classifier '{classifier_name}' from queue '{queue_name}'."
                else:
                    return f"Unknown action '{action}'. Supported: 'create', 'drop'."
    except Exception as e:
        return f"Error managing classifier: {str(e)}"


def _set_query_queue_property(target, queue_name, property_key, property_value, classifier_name="", action="set"):
    """Set or remove a property on a query queue or classifier."""
    try:
        target = target.lower().strip()
        action = action.lower().strip()
        safe_queue = queue_name.replace("'", "''")
        safe_key = property_key.replace("'", "''")
        safe_value = property_value.replace("'", "''")

        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if target == "queue":
                    if action == "set":
                        cursor.execute(
                            f"CALL hg_set_query_queue_property('{safe_queue}', '{safe_key}', '{safe_value}')"
                        )
                        return f"Successfully set queue '{queue_name}' property '{property_key}' = '{property_value}'."
                    elif action == "remove":
                        cursor.execute(
                            f"CALL hg_remove_query_queue_property('{safe_queue}', '{safe_key}')"
                        )
                        return f"Successfully removed property '{property_key}' from queue '{queue_name}'."
                    else:
                        return f"Unknown action '{action}'. Supported: 'set', 'remove'."
                elif target == "classifier":
                    if not classifier_name:
                        return "Error: classifier_name is required when target='classifier'."
                    safe_classifier = classifier_name.replace("'", "''")
                    if action == "set":
                        cursor.execute(
                            f"CALL hg_set_classifier_rule_condition_value('{safe_queue}', '{safe_classifier}', '{safe_key}', '{safe_value}')"
                        )
                        return f"Successfully set classifier '{classifier_name}' rule: {property_key} = '{property_value}'."
                    elif action == "remove":
                        cursor.execute(
                            f"CALL hg_remove_classifier_rule_condition_value('{safe_queue}', '{safe_classifier}', '{safe_key}', '{safe_value}')"
                        )
                        return f"Successfully removed classifier '{classifier_name}' rule: {property_key} = '{property_value}'."
                    else:
                        return f"Unknown action '{action}'. Supported: 'set', 'remove'."
                else:
                    return f"Unknown target '{target}'. Supported: 'queue', 'classifier'."
    except Exception as e:
        return f"Error setting property: {str(e)}"


def _manage_warehouse(action, warehouse_name, cu=0, new_name=""):
    """Manage a computing group: suspend, resume, restart, rename, or resize."""
    try:
        action = action.lower().strip()
        safe_name = warehouse_name.replace("'", "''")
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if action == "suspend":
                    cursor.execute(f"CALL hg_suspend_warehouse('{safe_name}')")
                    return f"Successfully suspended warehouse '{warehouse_name}'."
                elif action == "resume":
                    cursor.execute(f"CALL hg_resume_warehouse('{safe_name}')")
                    return f"Successfully resumed warehouse '{warehouse_name}'."
                elif action == "restart":
                    cursor.execute(f"CALL hg_restart_warehouse('{safe_name}')")
                    return f"Successfully restarted warehouse '{warehouse_name}'."
                elif action == "rename":
                    if not new_name:
                        return "Error: new_name is required for 'rename' action."
                    safe_new = new_name.replace("'", "''")
                    cursor.execute(f"CALL hg_rename_warehouse('{safe_name}', '{safe_new}')")
                    return f"Successfully renamed warehouse '{warehouse_name}' to '{new_name}'."
                elif action == "resize":
                    if cu <= 0:
                        return "Error: cu must be a positive integer for 'resize' action."
                    cursor.execute(f"CALL hg_alter_warehouse('{safe_name}', {int(cu)})")
                    return f"Successfully resized warehouse '{warehouse_name}' to {cu} CU."
                else:
                    return f"Unknown action '{action}'. Supported: 'suspend', 'resume', 'restart', 'rename', 'resize'."
    except Exception as e:
        return f"Error managing warehouse: {str(e)}"


def _get_warehouse_status(warehouse_name):
    """Get warehouse running status."""
    try:
        safe_name = warehouse_name.replace("'", "''")
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT hg_get_warehouse_status('{safe_name}')")
                status = cursor.fetchone()[0]

                parts = [f"## Warehouse Status: {warehouse_name}", ""]
                parts.append(f"**Status**: {status}")

                # Also get rebalance status
                try:
                    cursor.execute(f"SELECT hg_get_rebalance_warehouse_status('{safe_name}')")
                    rebalance = cursor.fetchone()[0]
                    parts.append(f"**Rebalance Status**: {rebalance}")
                except Exception:
                    pass

                # Get current warehouse info from hg_warehouses
                try:
                    cursor.execute(
                        """
                        SELECT warehouse_id, cpu, memory, cluster_count, status, is_default
                        FROM hologres.hg_warehouses
                        WHERE warehouse_name = %s
                        """,
                        [warehouse_name],
                    )
                    row = cursor.fetchone()
                    if row:
                        wh_id, cpu, mem, clusters, db_status, is_default = row
                        parts.append("")
                        parts.append("### Configuration")
                        parts.append(f"- **ID**: {wh_id}")
                        parts.append(f"- **CPU**: {cpu}")
                        parts.append(f"- **Memory**: {mem}")
                        parts.append(f"- **Clusters**: {clusters}")
                        parts.append(f"- **DB Status**: {db_status}")
                        parts.append(f"- **Default**: {'Yes' if is_default else 'No'}")
                except Exception:
                    pass

                return "\n".join(parts)
    except Exception as e:
        return f"Error getting warehouse status: {str(e)}"


def _rebalance_warehouse(warehouse_name):
    """Trigger shard rebalancing for a warehouse."""
    try:
        safe_name = warehouse_name.replace("'", "''")
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT hg_rebalance_warehouse('{safe_name}')")
                result = cursor.fetchone()[0]
                return f"Shard rebalance triggered for warehouse '{warehouse_name}'. Result: {result}\nUse get_hg_warehouse_status to monitor progress."
    except Exception as e:
        return f"Error rebalancing warehouse: {str(e)}"


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
