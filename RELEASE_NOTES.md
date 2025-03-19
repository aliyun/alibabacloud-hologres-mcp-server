# Hologres MCP Server Release Notes

## Version 0.1.0 (Initial Release)

We are excited to announce the first release of the StarRocks MCP (Model Context Protocol) Server. This server enables AI assistants to interact directly with StarRocks databases, providing a seamless interface for executing queries and retrieving database information.

### Description

The StarRocks MCP Server acts as a bridge between AI assistants and StarRocks databases, allowing for direct SQL execution and database exploration without requiring complex setup or configuration. This initial release provides essential functionality for database interaction while maintaining security and performance.

### Features

- **SQL Query Execution**
  - `read_query` tool for executing SELECT queries and commands that return result sets
  - `write_query` tool for executing DDL/DML statements and other StarRocks commands
  - Proper error handling and connection management

- **Database Exploration**
  - List all databases in a StarRocks instance
  - View table schemas using SHOW CREATE TABLE
  - List all tables within a specific database

- **System Information Access**
  - Access to StarRocks internal system information via proc-like interface
  - Visibility into FE nodes, BE nodes, CN nodes, databases, tables, partitions, transactions, jobs, and more

- **Flexible Configuration**
  - Configurable connection parameters (host, port, user, password)
  - Support for both package installation and local directory execution

### Requirements

- Python 3.13 or higher
- Dependencies:
  - mcp >= 1.4.0
  - psycopg2 >= 2.9.5

### Configuration

The server must be configured through environment variables:

- `STARROCKS_HOST` 
- `STARROCKS_PORT` 
- `STARROCKS_USER` 
- `STARROCKS_PASSWORD` 

### Installation

The server can be installed as a Python package:

```bash
pip install mcp-server-starrocks
```

Or run directly from the source:

```bash
uv --directory path/to/mcp-server-starrocks run mcp-server-starrocks
```

### MCP Integration

Add the following configuration to your MCP settings file:

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
        "STARROCKS_HOST": "localhost",
        "STARROCKS_PORT": "9030",
        "STARROCKS_USER": "root",
        "STARROCKS_PASSWORD": ""
      }
    }
  }
}
```

---

We welcome feedback and contributions to improve the StarRocks MCP Server. Please report any issues or suggestions through our GitHub repository.
