# Hologres MCP Server
Hologres MCP Server serves as a universal interface between AI Agents and Hologres databases. It enables seamless communication between AI Agents and Hologres, helping AI Agents retrieve Hologres database metadata and execute SQL operations.

## Configuration
### Mode 1: Using Local File
#### Download
Download from Github
```shell
git clone https://github.com/aliyun/alibabacloud-hologres-mcp-server.git
```
#### MCP Integration
Add the following configuration to the MCP client configuration file:
```json
"mcpServers": {
  "hologres-mcp-server": {
    "command": "uv",
    "args": [
      "--directory",
      "/path/to/alibabacloud-hologres-mcp-server",
      "run",
      "hologres-mcp-server"
    ],
    "env": {
      "HOLOGRES_HOST": "host",
      "HOLOGRES_PORT": "port",
      "HOLOGRES_USER": "access_id",
      "HOLOGRES_PASSWORD": "access_key",
      "HOLOGRES_DATABASE": "database"
    }
  }
}
```

### Mode 2: Using PIP Mode
#### Installation
Install MCP Server using the following package:
```bash
pip install hologres-mcp-server
```

#### MCP Integration
Add the following configuration to the MCP client configuration file:
```json
  "mcpServers": {
    "hologres-mcp-server": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "hologres-mcp-server",
        "hologres-mcp-server"
      ],
      "env": {
        "HOLOGRES_HOST": "host",
        "HOLOGRES_PORT": "port",
        "HOLOGRES_USER": "access_id",
        "HOLOGRES_PASSWORD": "access_key",
        "HOLOGRES_DATABASE": "database"
      }
    }
  }
```

## Components
### Tools
* `execute_sql`: Execute queries in Hologres

* `analyze_table`: Collect table statistics

* `get_query_plan`: Get query plan

* `get_execution_plan`: Get execution plan

### Resources
#### Built-in Resources
* `hologres:///schemas`: Get all schemas in the database

#### Resource Templates
* `hologres:///{schema}/tables`: List all tables in a schema

* `hologres:///{schema}/{table}/ddl`: Get table DDL

* `hologres:///{schema}/{table}/statistic`: Show collected table statistics

* `system:///{+system_path}`: 
  System paths include:
  * missing_stats_tables - Shows the tables that are missing statistics.
  * stat_activity - Shows the information of current running queries.
  * query_log/latest/<row_limits> - Get recent query log history with specified number of rows.
  * query_log/user/<user_name>/<row_limits> - Get query log history for a specific user with row limits.
  * query_log/application/<application_name>/<row_limits> - Get query log history for a specific application with row limits.

### Prompts
None at this time