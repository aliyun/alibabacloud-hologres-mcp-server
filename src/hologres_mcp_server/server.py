import asyncio
import logging
import os
import psycopg2
from psycopg2 import Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent, ResourceTemplate
from pydantic import AnyUrl

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

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("HOLOGRES_HOST", "localhost"),
        "port": os.getenv("HOLOGRES_PORT", "5432"),
        "user": os.getenv("HOLOGRES_USER"),
        "password": os.getenv("HOLOGRES_PASSWORD"),
        "database": os.getenv("HOLOGRES_DATABASE")
    }
    if not all([config["user"], config["password"], config["database"]]):
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
    # logger.info("Listing resources...")
    return [
        Resource(
            uri="hologres:///schemas",  # 修改这里，从 schema 改为 schemas
            name="All Schemas",
            description="List all schemas in Hologres database",
            mimeType="text/plain"
        ),
        Resource(
            uri="hologres:///system_info/missing_stats_tables",  # 修改这里，从 hg_stats_missing 改为 missing_stats_tables
            name="Tables Missing Statistics",
            description="List all tables that are missing statistics information",
            mimeType="text/plain"
        )
    ]

@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """Define resource URI templates for dynamic resources."""
    # logger.info("Listing resource templates...")  # 添加日志记录
    return [
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/ddl",  # 修改这里
            name="Table DDL",
            description="Get the DDL script of a table in a specific schema",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/statistic",  # 新增统计信息模板
            name="Table Statistics",
            description="Get statistics information of a table",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/tables",  # 修改这里
            name="Schema Tables",
            description="List all tables in a specific schema",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///system_info/latest_query_log/{row_limits}",  # 修改这里，从 query_log 改为 latest_query_log
            name="Query Log History",
            description="Get recent query log history with specified number of rows",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///system_info/user_query_log/{user}",  # 新增用户查询日志模板
            name="User Query Log",
            description="Get query log history for a specific user",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///system_info/application_query_log/{application}",  # 新增应用程序查询日志模板
            name="Application Query Log",
            description="Get query log history for a specific application",
            mimeType="text/plain"
        )
    ]

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read resource content based on URI."""
    config = get_db_config()
    uri_str = str(uri)
    # logger.info(f"Reading resource: {uri_str}")
    
    if not uri_str.startswith("hologres:///"):  # 修改这里
        raise ValueError(f"Invalid URI scheme: {uri_str}")
    
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True  # 设置自动提交
        cursor = conn.cursor()
        
        path_parts = uri_str[12:].split('/')
        
        # Handle different resource types
        if path_parts[0] == "schemas":  # 修改这里，从 schema 改为 schemas
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
            
        elif path_parts[0] == "system_info" and path_parts[1] == "missing_stats_tables":  # 修改这里，适配新的URI路径
            # List tables missing statistics
            query = """
                SELECT 
                    schemaname,
                    tablename,
                    nattrs,
                    tablekind,
                    fdwname
                FROM hologres_statistic.hg_stats_missing
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
                ORDER BY schemaname, tablename;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return "No tables found with missing statistics"
            
            # 格式化输出结果
            headers = ["Schema", "Table", "Num Attrs", "Table Kind", "FDW Name"]
            result = ["\t".join(headers)]
            for row in rows:
                result.append("\t".join(map(str, row)))
            return "\n".join(result)
            
        elif path_parts[0] == "system_info" and path_parts[1] == "latest_query_log" and len(path_parts) == 3:  # 修改这里，从 query_log 改为 latest_query_log
            # 获取查询日志
            try:
                row_limits = int(path_parts[2])
                if row_limits <= 0:
                    return "Row limits must be a positive integer"
            except ValueError:
                return "Invalid row limits format, must be an integer"
                
            query = f"SELECT * FROM hologres.hg_query_log ORDER BY query_start DESC LIMIT {row_limits}"
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return "No query logs found"
            
            # 格式化输出结果
            columns = [desc[0] for desc in cursor.description]
            result = ["\t".join(columns)]
            for row in rows:
                # 将所有值转换为字符串，并处理None值
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                result.append("\t".join(formatted_row))
            return "\n".join(result)
            
        elif path_parts[0] == "system_info" and path_parts[1] == "user_query_log" and len(path_parts) == 3:
            # 获取指定用户的查询日志
            user = path_parts[2]
            if not user:
                return "Username cannot be empty"
                
            query = "SELECT * FROM hologres.hg_query_log WHERE usename = %s ORDER BY query_start DESC"
            cursor.execute(query, (user,))
            rows = cursor.fetchall()
            if not rows:
                return f"No query logs found for user {user}"
            
            # 格式化输出结果
            columns = [desc[0] for desc in cursor.description]
            result = ["\t".join(columns)]
            for row in rows:
                # 将所有值转换为字符串，并处理None值
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                result.append("\t".join(formatted_row))
            return "\n".join(result)
            
        elif path_parts[0] == "system_info" and path_parts[1] == "application_query_log" and len(path_parts) == 3:
            # 获取指定应用程序的查询日志
            application = path_parts[2]
            if not application:
                return "Application name cannot be empty"
                
            query = "SELECT * FROM hologres.hg_query_log WHERE application_name = %s ORDER BY query_start DESC"
            cursor.execute(query, (application,))
            rows = cursor.fetchall()
            if not rows:
                return f"No query logs found for application {application}"
            
            # 格式化输出结果
            columns = [desc[0] for desc in cursor.description]
            result = ["\t".join(columns)]
            for row in rows:
                # 将所有值转换为字符串，并处理None值
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                result.append("\t".join(formatted_row))
            return "\n".join(result)
            
        elif len(path_parts) == 2 and path_parts[1] == "tables":
            # List tables in specific schema
            schema = path_parts[0]
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
                AND table_schema = %s
                GROUP BY table_name
                ORDER BY table_name;
            """
            cursor.execute(query, (schema,))
            tables = cursor.fetchall()
            return "\n".join([table[0] for table in tables])
            
        elif len(path_parts) == 3 and path_parts[2] == "ddl":
            # Get table DDL
            schema = path_parts[0]
            table = path_parts[1]
            query = f"SELECT hg_dump_script('{schema}.{table}')"
            cursor.execute(query)
            ddl = cursor.fetchone()
            return ddl[0] if ddl and ddl[0] else f"No DDL found for {schema}.{table}"
            
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
            
            # 格式化输出结果
            headers = ["Schema", "Table", "Schema Version", "Stats Version", "Total Rows", "Analyze Time"]
            result = ["\t".join(headers)]
            for row in rows:
                result.append("\t".join(map(str, row)))
            return "\n".join(result)
            
        else:
            raise ValueError(f"Invalid resource URI format: {uri_str}")
            
    except Error as e:
        # logger.error(f"Database error reading resource {uri}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# 定义 Tools
@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available Hologres tools."""
    # logger.info("Listing tools...")
    return [
        Tool(
            name="execute_sql",
            description="Execute an SQL query on the Hologres server",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        # 移除了 query_log 工具
        Tool(
            name="analyze_table",
            description="Analyze table to collect statistics information",
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
    
    if name == "execute_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
    # 移除了 query_log 的处理逻辑
    elif name == "analyze_table":
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
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # 特殊处理 ANALYZE 命令
        if name == "analyze_table":
            return [TextContent(type="text", text=f"Successfully analyzed table {schema}.{table}")]
        
        # 处理其他有返回结果的查询
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [",".join(map(str, row)) for row in rows]
        return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
                
    except Exception as e:
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]
    finally:
        cursor.close()
        conn.close()

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
