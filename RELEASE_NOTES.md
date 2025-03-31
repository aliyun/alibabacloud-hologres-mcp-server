# Release Notes

## Version 0.1.7

### Bugfix

Fix some bugs when using in Python 3.11.

## Version 0.1.6

### Enhancement

update psycopg2 to psycopg3.

select, dml, ddl use different tools to execute.

## Version 0.1.5

### Enhancement

Now compatible with Python 3.10 and newer (previously required 3.13+).

## Version 0.1.4

### Enhancement

The URI of the resource template has been refactored to enable the large language model (LLM) to use it more concisely.

## Version 0.1.2 (Initial Release)

### Description

Hologres MCP Server serves as a universal interface between AI Agents and Hologres databases. It enables rapid implementation of seamless communication between AI Agents and Hologres, helping AI Agents retrieve Hologres database metadata and execute SQL for various operations.

### Key Features

- **SQL Execution**
  - Execute SQL in Hologres, including DDL, DML, and Queries
  - Execute ANALYZE commands to collect statistics
- **Database Metadata**
  - Display all schemas
  - Display all tables under a schema
  - Show table DDL
  - View table statistics
- **System Information**
  - Query execution logs
  - Query missing statistics

### Dependencies

- Python 3.10 or higher
- Required packages
  - mcp >= 1.4.0
  - psycopg2 >= 2.9.5

### Configuration

MCP Server requires the following environment variables to connect to Hologres instance:

- `HOLOGRES_HOST`
- `HOLOGRES_PORT`
- `HOLOGRES_USER`
- `HOLOGRES_PASSWORD`
- `HOLOGRES_DATABASE`

### Installation

Install MCP Server using the following package:

```bash
pip install hologres-mcp-server
```

### MCP Integration

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
