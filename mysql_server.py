# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import io
import time
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
import mysql.connector


SERVER_VERSION = "0.1.0"

server = Server("mcp-server-starrocks", SERVER_VERSION)

global_connection = None


def get_connection():
    global global_connection
    if global_connection is None:
        global_connection = mysql.connector.connect(
            host=os.getenv('STARROCKS_HOST', 'localhost'),
            port=os.getenv('STARROCKS_PORT', '9030'),
            user=os.getenv('STARROCKS_USER', 'root'),
            password=os.getenv('STARROCKS_PASSWORD', '')
        )
    return global_connection


def reset_connection():
    global global_connection
    if global_connection is not None:
        global_connection.close()
        global_connection = None



def handle_single_column_query(conn, query):
    # return csv like result set, with column names as first row
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        if rows:
            return "\n".join([row[0] for row in rows])
        else:
            return f"None"
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        cursor.close()


def handle_read_query(conn, query):
    # return csv like result set, with column names as first row
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]  # Get column names
        rows = cursor.fetchall()

        output = io.StringIO()

        # Convert rows to CSV-like format
        def to_csv_line(row):
            return ",".join(
                str(item).replace("\"", "\"\"") if isinstance(item, str) else str(item) for item in row)

        output.write(to_csv_line(columns) + "\n")  # Write column names
        for row in rows:
            output.write(to_csv_line(row) + "\n")  # Write data rows

        output.write(f"\n{len(rows)} rows in set\n")
        return output.getvalue()
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        cursor.close()


def handle_write_query(conn, query):
    cursor = conn.cursor()
    start_time = time.time()
    try:
        cursor.execute(query)
        affected_rows = cursor.rowcount
        elapsed_time = time.time() - start_time
        return f"{affected_rows} rows affected ({elapsed_time:.2f} sec)"
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        cursor.close()


@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    return [
        types.Resource(
            uri="starrocks:///databases",
            name="All Databases",
            description="List all databases in StarRocks",
            mimeType="text/plain"
        )
    ]


SR_PROC_DESC = '''
Internal information exposed by StarRocks similar to linux /proc, following are some common paths:

'/frontends'	Shows the information of FE nodes.
'/backends'	Shows the information of BE nodes if this SR is non cloud native deployment.
'/compute_nodes'	Shows the information of CN nodes if this SR is cloud native deployment.
'/dbs'	Shows the information of databases.
'/dbs/<DB_ID>'	Shows the information of a database by database ID.
'/dbs/<DB_ID>/<TABLE_ID>'	Shows the information of tables by database ID.
'/dbs/<DB_ID>/<TABLE_ID>/partitions'	Shows the information of partitions by database ID and table ID.
'/transactions'	Shows the information of transactions by database.
'/transactions/<DB_ID>' Show the information of transactions by database ID.
'/transactions/<DB_ID>/running' Show the information of running transactions by database ID.
'/transactions/<DB_ID>/finished' Show the information of finished transactions by database ID.
'/jobs'	Shows the information of jobs.
'/statistic'	Shows the statistics of each database.
'/tasks'	Shows the total number of all generic tasks and the failed tasks.
'/cluster_balance'	Shows the load balance information.
'/routine_loads'	Shows the information of Routine Load.
'/colocation_group'	Shows the information of Colocate Join groups.
'/catalog'	Shows the information of catalogs.
'''


@server.list_resource_templates()
async def handle_list_resource_templates() -> list[types.ResourceTemplate]:
    return [
        types.ResourceTemplate(
            uriTemplate="starrocks:///{db}/{table}/schema",
            name="Table Schema",
            description="Get the schema of a table using SHOW CREATE TABLE",
            mimeType="text/plain"
        ),
        types.ResourceTemplate(
            uriTemplate="starrocks:///{db}/tables",
            name="Database Tables",
            description="List all tables in a specific database",
            mimeType="text/plain"
        ),
        types.ResourceTemplate(
            uriTemplate="proc:///{+path}",
            name="System internal information",
            description=SR_PROC_DESC,
            mimeType="text/plain"
        )
    ]


@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    try:
        conn = get_connection()
        if uri.scheme == 'proc':
            return handle_read_query(conn, f"show proc '{uri.path}'")
        if uri.scheme != "starrocks":
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path_parts = uri.path.strip('/').split('/')
        if len(path_parts) == 3 and path_parts[2] == "schema":
            db, table = path_parts[:2]
            return handle_single_column_query(conn, f"SHOW CREATE TABLE {db}.{table}")
        elif len(path_parts) == 1 and path_parts[0] == "databases":
            return handle_single_column_query(conn, "SHOW DATABASES")
        elif len(path_parts) == 2 and path_parts[1] == "tables":
            return handle_single_column_query(conn, f"SHOW TABLES FROM {path_parts[0]}")
    except Exception as e:
        reset_connection()
        raise ValueError(f"Error retrieving resource: {str(e)}")


@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    return []


@server.get_prompt()
async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
    raise ValueError(f"Unsupported get_prompt")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="read_query",
            description="Execute a SELECT query or commands that return a ResultSet",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to execute"},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="write_query",
            description="Execute an DDL/DML or other StarRocks command that do not have a ResultSet",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL to execute"},
                },
                "required": ["query"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
        name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """
    Handle tool execution requests.
    Tools can modify server state and notify clients of changes.
    """
    try:
        conn = get_connection()
        if name == "read_query":
            return [types.TextContent(type="text", text=handle_read_query(conn, arguments["query"]))]
        if name == "write_query":
            return [types.TextContent(type="text", text=handle_write_query(conn, arguments["query"]))]
    except Exception as e:
        reset_connection()
        return [types.TextContent(type="text", text=str(e))]


async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp-server-starrocks",
                server_version=SERVER_VERSION,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
