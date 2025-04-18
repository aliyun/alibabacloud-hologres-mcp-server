import psycopg
from psycopg import sql
import pglast
import time
from hologres_mcp_server.settings import get_db_config


def connect_with_retry(retries=3):
    attempt = 0
    err_msg = ""
    while attempt <= retries:
        try:
            config = get_db_config()
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
                time.sleep(5)  # 等待 2 秒后再次尝试连接
    raise psycopg.Error(f"Failed to connect to Hologres database after retrying: {err_msg}")


# 处理resource的通用函数
def handle_read_resource(resource_name, query, with_headers = False):
    """Handle readResource method."""
    config = get_db_config()
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                headers = [desc[0] for desc in cursor.description]
                if with_headers:
                    return rows, headers
                else:
                    return rows
    except Exception as e:
        return f"Error executing query: {str(e)}"


# 处理tool的通用函数
def handle_call_tool(tool_name, query, serverless = False):
    """Handle callTool method."""
    config = get_db_config()
    try:
        with connect_with_retry() as conn:
            with conn.cursor() as cursor:

                # 特殊处理 serverless computing 查询
                if serverless:
                    cursor.execute("set hg_computing_resource='serverless'")
                
                # Execute the query
                cursor.execute(query)
                
                # 特殊处理 ANALYZE 命令
                if tool_name == "gather_hg_table_statistics":
                    return f"Successfully {query}"
                
                # 处理其他有返回结果的查询
                if cursor.description:  # SELECT query
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return "\n".join([",".join(columns)] + result)
                elif tool_name == "execute_dml_sql":  # Non-SELECT query
                    row_count = cursor.rowcount
                    return f"Query executed successfully. {row_count} rows affected."
                else:
                    return "Query executed successfully"
    except Exception as e:
        return f"Error executing query: {str(e)}"

def get_view_definition(cursor, schema_name, view_name):
    cursor.execute(sql.SQL("""
        SELECT definition 
        FROM pg_views 
        WHERE schemaname = %s AND viewname = %s
    """), [schema_name, view_name])
    result = cursor.fetchone()
    return result[0] if result else None

def get_column_comment(cursor, schema_name, table_name, column_name):
    cursor.execute(sql.SQL("""
        SELECT col_description(att.attrelid, att.attnum)
        FROM pg_attribute att
        JOIN pg_class cls ON att.attrelid = cls.oid
        JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
        WHERE cls.relname = %s AND att.attname = %s AND nsp.nspname = %s
    """), [table_name, column_name, schema_name])
    result = cursor.fetchone()
    return result[0] if result else None

def try_infer_view_comments(schema_name, view_name):
    try:
        config = get_db_config()
        with psycopg.connect(**config) as conn:
            conn.autocommit = True
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
                                    column_comment = get_column_comment(cursor, schema_name, source_table, source_column)
                                    if column_comment:
                                        cursor.execute(sql.SQL("""
                                            SELECT col_description((SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)), attnum)
                                            FROM pg_attribute
                                            WHERE attname = %s AND attrelid = (SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s))
                                        """), [view_name, schema_name, target_column, view_name, schema_name])
                                        view_column_comment = cursor.fetchone()
                                        if not view_column_comment or view_column_comment[0] is None:
                                            statement = f"COMMENT ON COLUMN {schema_name}.{view_name}.{target_column} IS '{column_comment}';"
                                            comment_statements.append(statement)
                if comment_statements:
                    comment_statements.insert(0, "-- Infer view column comments from related tables")
                return "\n".join(comment_statements)
            
    except Exception as e:
        return ""