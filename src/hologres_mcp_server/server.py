import asyncio
import logging
import os
import psycopg
from psycopg import OperationalError as Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent, ResourceTemplate
from pydantic import AnyUrl
from hologres_mcp_server.util import try_infer_view_comments

"""
# 修改日志配置，只使用文件处理器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hologres_mcp_log.out')  # 只保留文件处理器
    ]
)
logger = logging.getLogger("hologres-mcp-server")
"""

SERVER_VERSION = "0.1.7"

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("HOLOGRES_HOST", "localhost"),
        "port": os.getenv("HOLOGRES_PORT", "5432"),
        "user": os.getenv("HOLOGRES_USER"),
        "password": os.getenv("HOLOGRES_PASSWORD"),
        "dbname": os.getenv("HOLOGRES_DATABASE"),
        "application_name": f"hologres-mcp-server-{SERVER_VERSION}"
    }
    if not all([config["user"], config["password"], config["dbname"]]):
        # logger.error("Missing required database configuration. Please check environment variables:")
        # logger.error("HOLOGRES_USER, HOLOGRES_PASSWORD, and HOLOGRES_DATABASE are required")
        raise ValueError("Missing required database configuration")
    
    return config

# Initialize server
app = Server("hologres-mcp-server")

# 定义 Resources
@app.list_resources()
async def list_resources() -> list[Resource]:
    """List basic Hologres resources."""
    return [
        Resource(
            uri="hologres:///schemas",
            name="All Schemas",
            description="Hologres is a PostgreSQL-compatible OLAP product. List all schemas in Hologres database",
            mimeType="text/plain"
        )
    ]

HOLO_SYSTEM_DESC = '''
System information in Hologres, following are some common system_paths:

'missing_stats_tables'    Shows the tables that are missing statistics.
'stat_activity'    Shows the information of current running queries.
'query_log/latest/<row_limits>'    Get recent query log history with specified number of rows.
'query_log/user/<user_name>/<row_limits>'    Get query log history for a specific user with row limits.
'query_log/application/<application_name>/<row_limits>'    Get query log history for a specific application with row limits.
'query_log/failed/<interval>/<row_limits>' - Get failed query log history with interval and specified number of rows.
'''

@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """Define resource URI templates for dynamic resources."""
    return [
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/tables",
            name="Schema Tables",
            description="List all tables in a specific schema",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/ddl",
            name="Table DDL",
            description="Get the DDL script of a table in a specific schema",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/statistic",
            name="Table Statistics",
            description="Get statistics information of a table",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/partitions",
            name="Table Statistics",
            description="List all partitions of a partitioned table",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="system:///{+system_path}",
            name="System internal Information",
            description=HOLO_SYSTEM_DESC,
            mimeType="text/plain"
        )
    ]

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read resource content based on URI."""
    config = get_db_config()
    uri_str = str(uri)
    
    if not (uri_str.startswith("hologres:///") or uri_str.startswith("system:///")):
        raise ValueError(f"Invalid URI scheme: {uri_str}")
    
    try:
        with psycopg.connect(**config) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
        
                # Handle hologres:/// URIs
                if uri_str.startswith("hologres:///"):
                    path_parts = uri_str[12:].split('/')
                    
                    if path_parts[0] == "schemas":
                        # List all schemas
                        query = """
                            SELECT table_schema 
                            FROM information_schema.tables 
                            WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
                            GROUP BY table_schema
                            ORDER BY table_schema;
                        """
                        cursor.execute(query)
                        schemas = cursor.fetchall()
                        return "\n".join([schema[0] for schema in schemas])
                        
                    elif len(path_parts) == 2 and path_parts[1] == "tables":
                        # List tables in specific schema
                        schema = path_parts[0]
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
                        cursor.execute(query)
                        tables = cursor.fetchall()
                        # 修复 SyntaxError 问题：f-string中不能包含反斜杠
                        return "\n".join(['"' + table[0].replace('"', '""') + '"' + table[1] for table in tables])
                        
                    elif len(path_parts) == 3 and path_parts[2] == "partitions":
                        # Get partitions
                        schema = path_parts[0]
                        table = path_parts[1]
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
                        cursor.execute(query)
                        tables = cursor.fetchall()
                        return "\n".join([table[0] for table in tables])

                    elif len(path_parts) == 3 and path_parts[2] == "ddl":
                        # Get table DDL
                        schema = path_parts[0]
                        table = path_parts[1]
                        query = f"SELECT hg_dump_script('\"{schema}\".\"{table}\"')"
                        cursor.execute(query)
                        ddl = cursor.fetchone()
                        if ddl and ddl[0]:
                            if "Type: VIEW" in ddl[0]:
                                # 修复 SyntaxError 问题：使用字符串连接而不是在f-string中使用反斜杠
                                view_content = ddl[0].replace('\n\nEND;', '')
                                comments = try_infer_view_comments(cursor, schema, table)
                                return view_content + comments + "\n\nEND;"
                            else:
                                return ddl[0]
                        else:
                            return f"No DDL found for {schema}.{table}"
                        
                    elif len(path_parts) == 3 and path_parts[2] == "statistic":
                        # Get table statistics
                        schema = path_parts[0]
                        table = path_parts[1]
                        query = """
                            SELECT 
                                schema_name,
                                table_name,
                                schema_version,
                                statistic_version,
                                total_rows,
                                analyze_timestamp
                            FROM hologres_statistic.hg_table_statistic
                            WHERE schema_name = %s
                            AND table_name = %s
                            ORDER BY analyze_timestamp DESC;
                        """
                        cursor.execute(query, (schema, table))
                        rows = cursor.fetchall()
                        if not rows:
                            return f"No statistics found for {schema}.{table}"
                        
                        headers = ["Schema", "Table", "Schema Version", "Stats Version", "Total Rows", "Analyze Time"]
                        result = ["\t".join(headers)]
                        for row in rows:
                            result.append("\t".join(map(str, row)))
                        return "\n".join(result)
                        
                # Handle system:/// URIs
                elif uri_str.startswith("system:///"):
                    path_parts = uri_str[10:].split('/')
                    
                    if path_parts[0] == "missing_stats_tables":
                        # Shows the tables that are missing statistics.
                        query = """
                            SELECT 
                                *
                            FROM hologres_statistic.hg_stats_missing
                            WHERE schemaname NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
                            ORDER BY schemaname, tablename;
                        """
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        if not rows:
                            return "No tables found with missing statistics"
                        
                        columns = [desc[0] for desc in cursor.description]
                        result = ["\t".join(columns)]
                        for row in rows:
                            formatted_row = [str(val) if val is not None else "NULL" for val in row]
                            result.append("\t".join(formatted_row))
                        return "\n".join(result)

                    elif path_parts[0] == "stat_activity":
                        # Shows the information of current running queries.
                        query = """
                            SELECT
                                *
                            FROM
                                hg_stat_activity
                            ORDER BY pid;
                        """
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        if not rows:
                            return "No queries found with current running status"
                        
                        columns = [desc[0] for desc in cursor.description]
                        result = ["\t".join(columns)]
                        for row in rows:
                            formatted_row = [str(val) if val is not None else "NULL" for val in row]
                            result.append("\t".join(formatted_row))
                        return "\n".join(result)
                        
                    elif path_parts[0] == "query_log":
                        if path_parts[1] == "latest" and len(path_parts) == 3:
                            try:
                                row_limits = int(path_parts[2])
                                if row_limits <= 0:
                                    return "Row limits must be a positive integer"
                                query = f"SELECT * FROM hologres.hg_query_log ORDER BY query_start DESC LIMIT {row_limits}"
                                cursor.execute(query)
                            except ValueError:
                                return "Invalid row limits format, must be an integer"
                            
                        elif path_parts[1] == "user" and len(path_parts) == 4:
                            user_name = path_parts[2]
                            if not user_name:
                                return "Username cannot be empty"
                            try:
                                row_limits = int(path_parts[3])
                                if row_limits <= 0:
                                    return "Row limits must be a positive integer"
                                query = "SELECT * FROM hologres.hg_query_log WHERE usename = %s ORDER BY query_start DESC LIMIT %s"
                                cursor.execute(query, (user_name, row_limits))
                            except ValueError:
                                return "Invalid row limits format, must be an integer"
                                
                        elif path_parts[1] == "application" and len(path_parts) == 4:
                            application_name = path_parts[2]
                            if not application_name:
                                return "Application name cannot be empty"
                            try:
                                row_limits = int(path_parts[3])
                                if row_limits <= 0:
                                    return "Row limits must be a positive integer"
                                query = "SELECT * FROM hologres.hg_query_log WHERE application_name = %s ORDER BY query_start DESC LIMIT %s"
                                cursor.execute(query, (application_name, row_limits))
                            except ValueError:
                                return "Invalid row limits format, must be an integer"
                        
                        elif path_parts[1] == "failed" and len(path_parts) == 4:
                            interval = path_parts[2]
                            if not interval:
                                return "Interval cannot be empty"
                            try:
                                row_limits = int(path_parts[3])
                                if row_limits <= 0:
                                    return "Row limits must be a positive integer"
                                query = f"SELECT * FROM hologres.hg_query_log WHERE status = 'FAILED' AND query_start >= NOW() - INTERVAL '{interval}' ORDER BY query_start DESC LIMIT {row_limits}"
                                cursor.execute(query)
                            except ValueError:
                                return "Invalid row limits format, must be an integer"
                        
                        else:
                            raise ValueError(f"Invalid query log URI format: {uri_str}")

                        rows = cursor.fetchall()
                        if not rows:
                            return "No query logs found"
                        
                        columns = [desc[0] for desc in cursor.description]
                        result = ["\t".join(columns)]
                        for row in rows:
                            formatted_row = [str(val) if val is not None else "NULL" for val in row]
                            result.append("\t".join(formatted_row))
                        return "\n".join(result)
                
                raise ValueError(f"Invalid resource URI format: {uri_str}")
      
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

# 定义 Tools
@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available Hologres tools."""
    # logger.info("Listing tools...")
    return [
        Tool(
            name="execute_select_sql",
            description="Execute SELECT SQL to query data from Hologres database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The (SELECT) SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="execute_dml_sql",
            description="Execute (INSERT, UPDATE, DELETE) SQL to insert, update, and delete data in Hologres databse.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The DML SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="execute_ddl_sql",
            description="Execute (CREATE, ALTER, DROP) SQL statements to CREATE, ALTER, or DROP tables, views, procedures, GUCs etc. in Hologres databse.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The DDL SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        # 移除了 query_log 工具
        Tool(
            name="gather_table_statistics",
            description="Execute the ANALYZE TABLE command to have Hologres collect table statistics, enabling QO to generate better query plans",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Schema name"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name"
                    }
                },
                "required": ["schema", "table"]
            }
        ),
        Tool(
            name="get_query_plan",
            description="Get query plan for a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_execution_plan",
            description="Get actual execution plan with runtime statistics for a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    }
                },
                "required": ["query"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    config = get_db_config()
    
    if name == "execute_select_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not query.strip().upper().startswith("SELECT"):
            raise ValueError("Query must be a SELECT statement")
    elif name == "execute_dml_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not any(query.strip().upper().startswith(keyword) for keyword in ["INSERT", "UPDATE", "DELETE"]):
            raise ValueError("Query must be a DML statement (INSERT, UPDATE, DELETE)")
    elif name == "execute_ddl_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not any(query.strip().upper().startswith(keyword) for keyword in ["CREATE", "ALTER", "DROP"]):
            raise ValueError("Query must be a DDL statement (CREATE, ALTER, DROP)")
    elif name == "gather_table_statistics":
        schema = arguments.get("schema")
        table = arguments.get("table")
        if not all([schema, table]):
            raise ValueError("Schema and table are required")
        query = f"ANALYZE {schema}.{table}"
    elif name == "get_query_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN {query}"
    elif name == "get_execution_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN ANALYZE {query}"
    else:
        raise ValueError(f"Unknown tool: {name}")
    
    try:
        with psycopg.connect(**config) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                
                # Execute the query
                cursor.execute(query)
                
                # 特殊处理 ANALYZE 命令
                if name == "gather_table_statistics":
                    return [TextContent(type="text", text=f"Successfully analyzed table {schema}.{table}")]
                
                # 处理其他有返回结果的查询
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
                else:
                    return [TextContent(type="text", text="Query executed successfully")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    # logger.info("Starting Hologres MCP server...")
    config = get_db_config()
    # logger.info(f"Database config: {config['host']}:{config['port']}/{config['database']} as {config['user']}")
    
    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            # logger.error(f"Server error: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    asyncio.run(main())
