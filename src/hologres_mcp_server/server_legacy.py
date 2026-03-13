import asyncio
import logging
import os
import psycopg
import re
from psycopg import OperationalError as Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent, ResourceTemplate
from pydantic import AnyUrl
from hologres_mcp_server.utils import try_infer_view_comments, handle_read_resource, handle_call_tool

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

# Initialize server
app = Server("hologres-mcp-server")

# 定义 Resources
@app.list_resources()
async def list_resources() -> list[Resource]:
    """List basic Hologres resources."""
    return [
        Resource(
            uri="hologres:///schemas",
            name="All Schemas in Hologres database",
            description="Hologres is a PostgreSQL-compatible OLAP product. List all schemas in Hologres database",
            mimeType="text/plain"
        )
    ]

HOLO_SYSTEM_DESC = '''
System information in Hologres database, following are some common system_paths:

'hg_instance_version'    Shows the hologres instance version.
'guc_value/<guc_name>'    Shows the guc(Grand Unified Configuration) value.
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
            name="List all tables in a specific schema in Hologres database",
            description="List all tables in a specific schema in Hologres database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/ddl",
            name="Table DDL in Hologres database",
            description="Get the DDL script of a table in a specific schema in Hologres database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/statistic",
            name="Table Statistics in Hologres database",
            description="Get statistics information of a table in Hologres database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="hologres:///{schema}/{table}/partitions",
            name="Table Partitions in Hologres database",
            description="List all partitions of a partitioned table in Hologres database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="system:///{+system_path}",
            name="System internal Information in Hologres database",
            description=HOLO_SYSTEM_DESC,
            mimeType="text/plain"
        )
    ]

@app.read_resource()
async def read_resource(uri: AnyUrl):
    """Read resource content based on URI."""
    uri_str = str(uri)
    
    if not (uri_str.startswith("hologres:///") or uri_str.startswith("system:///")):
        raise ValueError(f"Invalid URI scheme: {uri_str}")
    
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
            schemas = handle_read_resource("list_schemas", query)
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
            tables = handle_read_resource("list_tables_in_schema", query)
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
            tables = handle_read_resource("get_table_partitions", query)
            return "\n".join([table[0] for table in tables])

        elif len(path_parts) == 3 and path_parts[2] == "ddl":
            # Get table DDL
            schema = path_parts[0]
            table = path_parts[1]
            query = f"SELECT hg_dump_script('\"{schema}\".\"{table}\"')"
            ddl = handle_read_resource("list_ddl", query)[0]
            
            if ddl and ddl[0]:
                if "Type: VIEW" in ddl[0]:
                    # 修复 SyntaxError 问题：使用字符串连接而不是在f-string中使用反斜杠
                    view_content = ddl[0].replace('\n\nEND;', '')
                    comments = try_infer_view_comments(schema, table)
                    return view_content + comments + "\n\nEND;"
                else:
                    return ddl[0]
            else:
                return f"No DDL found for {schema}.{table}"
            
        elif len(path_parts) == 3 and path_parts[2] == "statistic":
            # Get table statistics
            schema = path_parts[0]
            table = path_parts[1]
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
            

    # Handle system:/// URIs
    elif uri_str.startswith("system:///"):
        path_parts = uri_str[10:].split('/')
        
        if path_parts[0] == "hg_instance_version":
            # Execute the SQL to get the version of the Hologres instance
            query = "SELECT HG_VERSION();"
            version = handle_read_resource("get_instance_version", query)[0][0]
            # Extract the version number from the full version string
            version_number = version.split(' ')[1]
            return version_number

        elif path_parts[0] == "missing_stats_tables":
            # Shows the tables that are missing statistics.
            query = """
                SELECT 
                    *
                FROM hologres_statistic.hg_stats_missing
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
                ORDER BY schemaname, tablename;
            """
            rows, headers = handle_read_resource("get_holo_instance_version", query, with_headers=True)

            if not rows:
                return "No tables found with missing statistics"
            result = ["\t".join(headers)]
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
            rows, headers = handle_read_resource("get_stat_activity", query, with_headers=True)
            if not rows:
                return "No queries found with current running status"
            result = ["\t".join(headers)]
            for row in rows:
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                result.append("\t".join(formatted_row))
            return "\n".join(result)
            
        elif path_parts[0] == "query_log":
            rows = None
            headers = None
            if path_parts[1] == "latest" and len(path_parts) == 3:
                try:
                    row_limits = int(path_parts[2])
                    if row_limits <= 0:
                        return "Row limits must be a positive integer"
                    query = f"SELECT * FROM hologres.hg_query_log ORDER BY query_start DESC LIMIT {row_limits}"
                    rows, headers = handle_read_resource("get_latest_query_log", query, with_headers=True)
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
                    query = f"SELECT * FROM hologres.hg_query_log WHERE usename = '{user_name}' ORDER BY query_start DESC LIMIT {row_limits}"
                    rows, headers = handle_read_resource("get_user_query_log", query, with_headers=True)
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
                    query = f"SELECT * FROM hologres.hg_query_log WHERE application_name = '{application_name}' ORDER BY query_start DESC LIMIT {row_limits}"
                    rows, headers = handle_read_resource("get_application_query_log", query, with_headers=True)
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
                    rows, headers = handle_read_resource("get_failed_query_log", query, with_headers=True)
                except ValueError:
                    return "Invalid row limits format, must be an integer"
            
            else:
                raise ValueError(f"Invalid query log URI format: {uri_str}")

            if not rows:
                return "No query logs found"
            
            result = ["\t".join(headers)]
            for row in rows:
                formatted_row = [str(val) if val is not None else "NULL" for val in row]
                result.append("\t".join(formatted_row))
            return "\n".join(result)

        elif path_parts[0] == "guc_value":
            if len(path_parts) != 2:
                raise ValueError(f"Invalid GUC URI format: {uri_str}")
            guc_name = path_parts[1]
            if not guc_name:
                return "GUC name cannot be empty"
            query = f"SHOW {guc_name};"
            rows = handle_read_resource("get_guc_value", query)
            if not rows:
                return f"No GUC found with name {guc_name}"
            result = [f"{guc_name}: {rows[0][0]}"]
            return "\n".join(result)
    
    raise ValueError(f"Invalid resource URI format: {uri_str}")

# 定义 Tools
@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available Hologres tools."""
    # logger.info("Listing tools...")
    return [
        Tool(
            name="execute_hg_select_sql",
            description="Execute SELECT SQL to query data from Hologres database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The (SELECT) SQL query to execute in Hologres database."
                    }
                },
                "required": ["query"]
            }
        ),
        # 新增 execute_hg_select_sql_with_serverless 工具
        Tool(
            name="execute_hg_select_sql_with_serverless",
            description="Use Serverless Computing resources to execute SELECT SQL to query data in Hologres database. When the error like \"Total memory used by all existing queries exceeded memory limitation\" occurs during execute_hg_select_sql execution, you can re-execute the SQL with the tool execute_hg_select_sql_with_serverless.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The (SELECT) SQL query to execute with serverless computing in Hologres database"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="execute_hg_dml_sql",
            description="Execute (INSERT, UPDATE, DELETE) SQL to insert, update, and delete data in Hologres databse.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The DML SQL query to execute in Hologres database"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="execute_hg_ddl_sql",
            description="Execute (CREATE, ALTER, DROP) SQL statements to CREATE, ALTER, or DROP tables, views, procedures, GUCs etc. in Hologres databse.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The DDL SQL query to execute in Hologres database"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="gather_hg_table_statistics",
            description="Execute the ANALYZE TABLE command to have Hologres collect table statistics, enabling QO to generate better query plans",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Schema name in Hologres database"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name in Hologres database"
                    }
                },
                "required": ["schema", "table"]
            }
        ),
        Tool(
            name="get_hg_query_plan",
            description="Get query plan for a SQL query in Hologres database",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze in Hologres database"
                    }
                },
                "required": ["query"]
            }
        ),
    Tool(
        name="get_hg_execution_plan",
        description="Get actual execution plan with runtime statistics for a SQL query in Hologres database",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL query to analyze in Hologres database"
                }
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="call_hg_procedure",
        description="Call a stored procedure in Hologres database.",
        inputSchema={
            "type": "object",
            "properties": {
                "procedure_name": {
                    "type": "string",
                    "description": "The name of the stored procedure to call in Hologres database"
                },
                "arguments": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "The arguments to pass to the stored procedure in Hologres database"
                }
            },
            "required": ["procedure_name"]
        }
    ),
    Tool(
        name="create_hg_maxcompute_foreign_table",
        description="Create a MaxCompute foreign table in Hologres database to accelerate queries on MaxCompute data.",
        inputSchema={
            "type": "object",
            "properties": {
                "maxcompute_project": {
                    "type": "string",
                    "description": "The MaxCompute project name (required)"
                },
                "maxcompute_schema": {
                    "type": "string",
                    "default": "default",
                    "description": "The MaxCompute schema name (optional, default: 'default')"
                },
                "maxcompute_tables": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "The MaxCompute table names (required)"
                },
                "local_schema": {
                    "type": "string",
                    "default": "public",
                    "description": "The local schema name in Hologres (optional, default: 'public')"
                }
            },
            "required": ["maxcompute_project", "maxcompute_tables"]
        }
    ),
    # 新增 list_hg_schemas 工具
    Tool(
        name="list_hg_schemas",
        description="List all schemas in the current Hologres database, excluding system schemas.",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": []
        }
    ),
    # 新增 list_hg_tables_in_a_schema 工具
    Tool(
        name="list_hg_tables_in_a_schema",
        description="List all tables in a specific schema in the current Hologres database, including their types (table, view, foreign table, partitioned table).",
        inputSchema={
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Schema name to list tables from in Hologres database"
                }
            },
            "required": ["schema"]
        }
    ),
    # 新增 show_hg_table_ddl 工具
    Tool(
        name="show_hg_table_ddl",
        description="Show DDL script for a table, view, or foreign table in Hologres database.",
        inputSchema={
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Schema name in Hologres database"
                },
                "table": {
                    "type": "string",
                    "description": "Table name in Hologres database"
                }
            },
            "required": ["schema", "table"]
        }
    )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    serverless = False
    if name == "execute_hg_select_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not re.match(r"^\s*WITH\s+.*?SELECT\b", query, re.IGNORECASE) and not re.match(r"^\s*SELECT\b", query, re.IGNORECASE):
            raise ValueError("Query must be a SELECT statement or start with WITH followed by a SELECT statement")
    elif name == "execute_hg_select_sql_with_serverless":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not query.strip().upper().startswith("SELECT"):
            raise ValueError("Query must be a SELECT statement")
        # 修改 serverless computing 设置方式
        serverless = True
    elif name == "execute_hg_dml_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not any(query.strip().upper().startswith(keyword) for keyword in ["INSERT", "UPDATE", "DELETE"]):
            raise ValueError("Query must be a DML statement (INSERT, UPDATE, DELETE)")
    elif name == "execute_hg_ddl_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        if not any(query.strip().upper().startswith(keyword) for keyword in ["CREATE", "ALTER", "DROP", "COMMENT ON"]):
            raise ValueError("Query must be a DDL statement (CREATE, ALTER, DROP, COMMENT ON)")
    elif name == "gather_hg_table_statistics":
        schema = arguments.get("schema")
        table = arguments.get("table")
        if not all([schema, table]):
            raise ValueError("Schema and table are required")
        query = f"ANALYZE {schema}.{table}"
    elif name == "get_hg_query_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN {query}"
    elif name == "get_hg_execution_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN ANALYZE {query}"
    elif name == "call_hg_procedure":
        procedure_name = arguments.get("procedure_name")
        arguments_list = arguments.get("arguments")
        if not procedure_name:
            raise ValueError("Procedure name are required")
        query = f"CALL {procedure_name}({', '.join(arguments_list)})"
    elif name == "create_hg_maxcompute_foreign_table":
        maxcompute_project = arguments.get("maxcompute_project")
        maxcompute_schema = arguments.get("maxcompute_schema", "default")
        maxcompute_tables = arguments.get("maxcompute_tables")
        local_schema = arguments.get("local_schema", "public")
        if not all([maxcompute_project, maxcompute_tables]):
            raise ValueError("maxcompute_project and maxcompute_tables are required")
        maxcompute_table_list = ", ".join(maxcompute_tables)
        # 修复 SQL 语句，确保正确拼接项目名称和 schema
        query = f"""
            IMPORT FOREIGN SCHEMA "{maxcompute_project}#{maxcompute_schema}"
            LIMIT TO ({maxcompute_table_list})
            FROM SERVER odps_server
            INTO {local_schema};
        """
    # 处理list_hg_schemas工具
    elif name == "list_hg_schemas":
        query = """
            SELECT table_schema 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema','hologres','hologres_statistic','hologres_streaming_mv')
            GROUP BY table_schema
            ORDER BY table_schema;
        """
    # 处理list_hg_tables_in_a_schema工具
    elif name == "list_hg_tables_in_a_schema":
        schema = arguments.get("schema")
        if not schema:
            raise ValueError("Schema name is required")
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
    elif name == "show_hg_table_ddl":
        schema = arguments.get("schema")
        table = arguments.get("table")
        if not all([schema, table]):
            raise ValueError("Schema and table are required")
        query = f"SELECT hg_dump_script('\"{schema}\".\"{table}\"')"
    else:
        raise ValueError(f"Unknown tool: {name}")
    
    res = handle_call_tool(name, query, serverless)
    return [TextContent(type="text", text=f"{str(res)}")]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    # logger.info("Starting Hologres MCP server...")
    # config = get_db_config()
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
