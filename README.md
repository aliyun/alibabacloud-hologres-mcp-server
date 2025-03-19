# Starrocks Official MCP server

The StarRocks MCP Server acts as a bridge between AI assistants and StarRocks databases, allowing for direct SQL execution and database exploration without requiring complex setup or configuration.


## Configuration

MCP server config

```json
{
  "mcpServers": {
    "mcp-server-starrocks": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "mcp-server-starrocks",
        "mcp-server-starrocks"
      ],
      "env": {
        "STARROCKS_HOST": "default localhost",
        "STARROCKS_PORT": "default 9030",
        "STARROCKS_USER": "default root",
        "STARROCKS_PASSWORD": "default empty"
      }
    }
  }
}
```

If mcp-server-starrocks is not installed as python package(in dev env), can run using local dir 

```json
{
  "mcpServers": {
    "mcp-server-starrocks": {
      "command": "uv",
      "args": [
        "--directory",
        "path/to/mcp-server-starrocks",
        "run",
        "mcp-server-starrocks"
      ],
      "env": {
        "STARROCKS_HOST": "default localhost",
        "STARROCKS_PORT": "default 9030",
        "STARROCKS_USER": "default root",
        "STARROCKS_PASSWORD": "default empty"
      }
    }
  }
}
```

## Components

### Tools

* `read_query`
  - Execute a SELECT query or commands that return a ResultSet

* `write_query`
  - Execute an DDL/DML or other StarRocks command that do not have a ResultSet

### Resources

#### Direct Resources

* `starrocks:///databases`
  - Lists all databases in StarRocks

#### Resource Templates

* `starrocks:///{db}/{table}/schema`
  - Gets the schema of a table using SHOW CREATE TABLE

* `starrocks:///{db}/tables`
  - Lists all tables in a specific database

* `proc:///{+path}`
  - System internal information exposed by StarRocks similar to linux /proc
  - Common paths include:
    - `/frontends` - Shows the information of FE nodes
    - `/backends` - Shows the information of BE nodes if this SR is non cloud native deployment
    - `/compute_nodes` - Shows the information of CN nodes if this SR is cloud native deployment
    - `/dbs` - Shows the information of databases
    - `/dbs/<DB_ID>` - Shows the information of a database by database ID
    - `/dbs/<DB_ID>/<TABLE_ID>` - Shows the information of tables by database ID
    - `/dbs/<DB_ID>/<TABLE_ID>/partitions` - Shows the information of partitions by database ID and table ID
    - `/transactions` - Shows the information of transactions by database
    - `/transactions/<DB_ID>` - Shows the information of transactions by database ID
    - `/transactions/<DB_ID>/running` - Shows the information of running transactions by database ID
    - `/transactions/<DB_ID>/finished` - Shows the information of finished transactions by database ID
    - `/jobs` - Shows the information of jobs
    - `/statistic` - Shows the statistics of each database
    - `/tasks` - Shows the total number of all generic tasks and the failed tasks
    - `/cluster_balance` - Shows the load balance information
    - `/routine_loads` - Shows the information of Routine Load
    - `/colocation_group` - Shows the information of Colocate Join groups
    - `/catalog` - Shows the information of catalogs

### Prompts

None
