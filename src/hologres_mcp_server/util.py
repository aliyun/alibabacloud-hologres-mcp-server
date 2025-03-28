import psycopg
from psycopg import sql
import pglast

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

def try_infer_view_comments(cursor, schema_name, view_name):
    try:
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
