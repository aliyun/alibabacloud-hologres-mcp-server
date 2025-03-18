import asyncio
import logging
import os
import psycopg2
from psycopg2 import Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    """List Hologres tables as resources."""
    config = get_db_config()
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        cursor = conn.cursor()
        
        # Query to list all tables across all schemas (excluding system schemas)
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
            ORDER BY table_schema, table_name;
        """)
        tables = cursor.fetchall()
        logger.info(f"Found tables: {tables}")
        
        resources = []
        for schema, table in tables:
            resources.append(
                Resource(
                    uri=f"postgres://{schema}/{table}/data",
                    name=f"Table: {schema}.{table}",
                    mimeType="text/plain",
                    description=f"Data in table: {schema}.{table}"
                )
            )
        
        cursor.close()
        conn.close()
        return resources
        
    except Error as e:
        logger.error(f"Failed to list resources: {str(e)}")
        return []

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read table contents and metadata."""
    config = get_db_config()
    uri_str = str(uri)
    logger.info(f"Reading resource: {uri_str}")
    
    if not uri_str.startswith("postgres://"):
        raise ValueError(f"Invalid URI scheme: {uri_str}")
        
    parts = uri_str[11:].split('/')
    if len(parts) != 3:
        raise ValueError(f"Invalid URI format. Expected: postgres://schema/table/data")
    
    schema = parts[0]
    table = parts[1]
    
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        cursor = conn.cursor()
        
        # 获取 Hologres 表属性
        properties_query = """
            SELECT 
                table_namespace,
                table_name,
                property_key,
                property_value
            FROM hologres.hg_table_properties
            WHERE table_namespace = %s AND table_name = %s
            ORDER BY property_key;
        """
        cursor.execute(properties_query, (schema, table))
        properties_info = cursor.fetchall()
        
        # 构建元数据信息字符串
        metadata = ["=== Table Metadata ==="]
        metadata.append(f"\nSchema: {schema}")
        metadata.append(f"Table: {table}")
        
        # 添加 Hologres 属性信息
        if properties_info:
            metadata.append("\n=== Hologres Properties ===")
            for prop in properties_info:
                _, _, key, value = prop
                metadata.append(f"{key}: {value}")
        
        # 获取表的列信息
        column_query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        cursor.execute(column_query, (schema, table))
        columns_info = cursor.fetchall()
        
        # 添加列信息
        metadata.append("\n=== Columns ===")
        for col in columns_info:
            col_name, data_type, char_max_len, num_precision, num_scale, nullable = col
            type_info = data_type
            if char_max_len:
                type_info += f"({char_max_len})"
            elif num_precision:
                if num_scale:
                    type_info += f"({num_precision},{num_scale})"
                else:
                    type_info += f"({num_precision})"
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            metadata.append(f"{col_name}: {type_info} {nullable_str}")
        
        # 获取表数据
        cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT 10')
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        data = [",".join(map(str, row)) for row in rows]
        
        cursor.close()
        conn.close()
        
        # 组合元数据和数据
        return "\n".join([
            "\n".join(metadata),
            "\n=== Table Sample Data 10 Rows ===",
            ",".join(columns),
            *data
        ])
                
    except Error as e:
        logger.error(f"Database error reading resource {uri}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")

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
    else:
        raise ValueError(f"Unknown tool: {name}")
    
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
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
