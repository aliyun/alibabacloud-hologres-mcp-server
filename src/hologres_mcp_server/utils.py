import re
import time

import pglast
import psycopg
from psycopg import sql

from hologres_mcp_server.settings import get_db_config

# Pre-compiled regex patterns for SQL validation
WITH_SELECT_PATTERN = re.compile(r"^\s*WITH\s+.*?SELECT\b", re.IGNORECASE | re.DOTALL)
SELECT_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE)

# SQL statement type keywords
DML_KEYWORDS = ("INSERT", "UPDATE", "DELETE")
DDL_KEYWORDS = ("CREATE", "ALTER", "DROP", "COMMENT ON")

# System schemas to exclude from listings
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "hologres", "hologres_statistic", "hologres_streaming_mv")

# Default connection retry settings
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_DELAY = 5  # seconds


def connect_with_retry(retries=DEFAULT_RETRY_COUNT):
    config = get_db_config()  # Fetch config once before retry loop
    attempt = 0
    err_msg = ""
    while attempt <= retries:
        try:
            conn = psycopg.connect(**config)
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()
            return conn
        except psycopg.Error as e:
            err_msg = f"Connection failed: {e}"
            attempt += 1
            if attempt <= retries:
                print(f"Retrying connection (attempt {attempt + 1} of {retries + 1})...")
                time.sleep(DEFAULT_RETRY_DELAY)
    raise psycopg.Error(f"Failed to connect to Hologres database after retrying: {err_msg}")


def handle_read_resource(resource_name, query, with_headers=False):
    """Handle readResource method."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                headers = [desc[0] for desc in cursor.description]
                if with_headers:
                    return rows, headers
                return rows
    except Exception as e:
        return f"Error executing query: {str(e)}"


def handle_call_tool(tool_name, query, serverless=False):
    """Handle callTool method."""
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                if serverless:
                    cursor.execute("set hg_computing_resource='serverless'")

                cursor.execute(query)

                if tool_name == "gather_hg_table_statistics":
                    return f"Successfully {query}"

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return "\n".join([",".join(columns)] + result)
                elif tool_name == "execute_hg_dml_sql":
                    row_count = cursor.rowcount
                    return f"Query executed successfully. {row_count} rows affected."
                else:
                    return "Query executed successfully"
    except Exception as e:
        return f"Error executing query: {str(e)}"


def get_view_definition(cursor, schema_name, view_name):
    cursor.execute(
        sql.SQL("""
        SELECT definition
        FROM pg_views
        WHERE schemaname = %s AND viewname = %s
    """),
        [schema_name, view_name],
    )
    result = cursor.fetchone()
    return result[0] if result else None


def get_column_comment(cursor, schema_name, table_name, column_name):
    cursor.execute(
        sql.SQL("""
        SELECT col_description(att.attrelid, att.attnum)
        FROM pg_attribute att
        JOIN pg_class cls ON att.attrelid = cls.oid
        JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
        WHERE cls.relname = %s AND att.attname = %s AND nsp.nspname = %s
    """),
        [table_name, column_name, schema_name],
    )
    result = cursor.fetchone()
    return result[0] if result else None


def try_infer_view_comments(schema_name, view_name):
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                view_definition = get_view_definition(cursor, schema_name, view_name)
                if not view_definition:
                    print(f"View '{view_name}' not found.")
                    return ""
                comment_statements = []
                parsed = pglast.parser.parse_sql(view_definition)

                for raw_stmt in parsed:
                    stmt = raw_stmt.stmt
                    if isinstance(stmt, pglast.ast.SelectStmt):
                        for target in stmt.targetList:
                            if isinstance(target, pglast.ast.ResTarget):
                                if isinstance(target.val, pglast.ast.ColumnRef):
                                    source_table = target.val.fields[0].sval
                                    source_column = target.val.fields[1].sval
                                    target_column = target.name or source_column
                                    column_comment = get_column_comment(
                                        cursor, schema_name, source_table, source_column
                                    )
                                    if column_comment:
                                        cursor.execute(
                                            sql.SQL("""
                                            SELECT col_description((SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)), attnum)
                                            FROM pg_attribute
                                            WHERE attname = %s AND attrelid = (SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s))
                                        """),
                                            [view_name, schema_name, target_column, view_name, schema_name],
                                        )
                                        view_column_comment = cursor.fetchone()
                                        if not view_column_comment or view_column_comment[0] is None:
                                            statement = f"COMMENT ON COLUMN {schema_name}.{view_name}.{target_column} IS '{column_comment}';"
                                            comment_statements.append(statement)
                if comment_statements:
                    comment_statements.insert(0, "-- Infer view column comments from related tables")
                return "\n".join(comment_statements)

    except Exception:
        return ""


# ============================================================================
# SQL Validation Helpers
# ============================================================================


def validate_select_query(query: str) -> None:
    """Validate that query is SELECT or WITH...SELECT. Raises ValueError if not."""
    if not WITH_SELECT_PATTERN.match(query) and not SELECT_PATTERN.match(query):
        raise ValueError("Query must be a SELECT statement or start with WITH followed by a SELECT statement")


def validate_dml_query(query: str) -> None:
    """Validate that query starts with INSERT/UPDATE/DELETE. Raises ValueError if not."""
    if not any(query.strip().upper().startswith(keyword) for keyword in DML_KEYWORDS):
        raise ValueError(f"Query must be a DML statement ({', '.join(DML_KEYWORDS)})")


def validate_ddl_query(query: str) -> None:
    """Validate that query starts with CREATE/ALTER/DROP/COMMENT ON. Raises ValueError if not."""
    if not any(query.strip().upper().startswith(keyword) for keyword in DDL_KEYWORDS):
        raise ValueError(f"Query must be a DDL statement ({', '.join(DDL_KEYWORDS)})")


def validate_positive_integer(value: str, param_name: str = "Row limits") -> tuple[int, str | None]:
    """Validate string is a positive integer.

    Returns:
        tuple of (parsed_value, error_message). If valid, error_message is None.
    """
    try:
        limit = int(value)
        if limit <= 0:
            return 0, f"{param_name} must be a positive integer"
        return limit, None
    except ValueError:
        return 0, f"Invalid {param_name.lower()} format, must be an integer"


# ============================================================================
# Result Formatting Helpers
# ============================================================================


def format_tabular_result(rows: list, headers: list) -> str:
    """Format query results as tab-separated values with headers."""
    result = ["\t".join(headers)]
    for row in rows:
        formatted_row = [str(val) if val is not None else "NULL" for val in row]
        result.append("\t".join(formatted_row))
    return "\n".join(result)


# ============================================================================
# Query Generators
# ============================================================================


def get_list_schemas_query() -> str:
    """Return the SQL query for listing schemas."""
    excluded = "', '".join(SYSTEM_SCHEMAS)
    return f"""
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('{excluded}')
        GROUP BY table_schema
        ORDER BY table_schema;
    """


def get_list_tables_query(schema_name: str) -> str:
    """Return the SQL query for listing tables in a schema."""
    excluded = "', '".join(SYSTEM_SCHEMAS)
    return f"""
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
            tab.table_schema NOT IN ('{excluded}')
            AND tab.table_schema = '{schema_name}'
            AND (inh.inhrelid IS NULL OR NOT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inh.inhrelid = pg_inherits.inhrelid
            ))
        ORDER BY
            tab.table_name;
    """
