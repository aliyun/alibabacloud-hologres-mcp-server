# Release Notes

## 版本 0.1.0 (初始化版本)

### 描述

Hologres MCP Server 是 AI Agent 与 Hologres 数据库之间的通用接口。可以快速实现 AI Agent与 Hologres的无缝通信，帮助 AI Agent获取Hologres数据库的元数据，和执行SQL完成各类操作。

### 主要功能

- **执行SQL**
  - 在Hologers中执行SQL，包括 DDL、DML 和查询
  - 执行ANALYZE命令，收集统计信息

- **数据库元数据**
  - 显示所有的Schema
  - 显示Schema下所有的表
  - 显示表的DDL
  - 查看表的统计信息

- **系统信息**
  - 查询 Query Log
  - 查询缺失的统计信息

### 依赖

- Python 3.13 或更高版本
- 依赖
  - mcp >= 1.4.0
  - psycopg2 >= 2.9.5

### 配置

MCP Server 必须配置以下环境变量，以连接 Hologres 实例

- `HOLOGRES_HOST` 
- `HOLOGRES_PORT` 
- `HOLOGRES_USER` 
- `HOLOGRES_PASSWORD` 
- `HOLOGRES_DATABASE`

### 安装

可以使用如下安装包安装 MCP Server

```bash
pip install /path/to/dist/hologres_mcp_server-0.1.0-py3-none-any.whl
```

### MCP 集成

在 Client 端的 MCP 配置文件中增加如下配置，用以配置 MCP Server

```json
{
  "mcpServers": {
    "hologres-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/hologres-mcp-server",
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
}
```
