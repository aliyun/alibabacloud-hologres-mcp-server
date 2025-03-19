import asyncio
import logging
import os
import psycopg2
from psycopg2 import Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent, ResourceTemplate
from pydantic import AnyUrl

# 修改日志配置，只使用文件处理器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hologres_mcp_log.out')  # 只保留文件处理器
    ]
)
logger = logging.getLogger("hologres_mcp_server")

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
        logger.error("Missing required database configuration. Please check environment variables:")
        logger.error("HOLOGRES_USER, HOLOGRES_PASSWORD, and HOLOGRES_DATABASE are required")
        raise ValueError("Missing required database configuration")
    
    return config

# Initialize server
app = Server("hologres_mcp_server")

@app.list_resources()
async def list_resources() -> list[Resource]:
    """List basic Hologres resources."""
    logger.info("Listing resources...")
    return [
        Resource(
            uri="hologres:///schemas",  # 修改这里，从 schema 改为 schemas
            name="All Schemas",
            description="List all schemas in Hologres database",
            mimeType="text/plain"
        ),
        Resource(
            uri="hologres:///hg_stats_missing",
            name="Tables Missing Statistics",
            description="List all tables that are missing statistics information",
            mimeType="text/plain"
        )
    ]

@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """Define resource URI templates for dynamic resources."""
    logger.info("Listing resource templates...")  # 添加日志记录
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
        )
    ]

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read resource content based on URI."""
    config = get_db_config()
    uri_str = str(uri)
    logger.info(f"Reading resource: {uri_str}")
    
    if not uri_str.startswith("hologres:///"):  # 修改这里
        raise ValueError(f"Invalid URI scheme: {uri_str}")
    
    try:
        conn = psycopg2.connect(**config)
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
            
        elif path_parts[0] == "hg_stats_missing":
            # List tables missing statistics
            query = """
                SELECT 
                    schemaname,
                    tablename,
                    nattrs,
                    tablekind,
                    fdwname
                FROM hologres_statistic.hg_stats_missing 
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
        logger.error(f"Database error reading resource {uri}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available Hologres tools."""
    logger.info("Listing tools...")
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
        Tool(
            name="query_log",
            description="Query Hologres query log history",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of log entries to return",
                        "minimum": 1,
                        "default": 100
                    }
                },
                "required": ["limit"]
            }
        ),
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
    logger.info(f"Calling tool: {name} with arguments: {arguments}")
    
    if name == "execute_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
    elif name == "query_log":
        limit = arguments.get("limit", 100)
        query = f"SELECT * FROM hologres.hg_query_log ORDER BY query_start DESC LIMIT {limit}"
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
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Get results
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [",".join(map(str, row)) for row in rows]
        cursor.close()
        conn.close()
        return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
                
    except Exception as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    logger.info("Starting Hologres MCP server...")
    config = get_db_config()
    logger.info(f"Database config: {config['host']}:{config['port']}/{config['database']} as {config['user']}")
    
    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    asyncio.run(main())
