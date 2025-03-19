# Hologres MCP Server

Hologres MCP Server 是 AI Agent 与 Hologres 数据库之间的通用接口。可以快速实现 AI Agent与 Hologres的无缝通信，帮助 AI Agent获取Hologres数据库的元数据，和执行SQL完成各类操作。

## 配置

MCP server 配置

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

## 组件

### Tools

* `excute_sql`: 在Hologres中执行查询

* `query_log`: 显示查询日志

* `analyze_table`: 收集表的统计信息

* `get_query_plan`: 获取查询的查询计划

* `get_execution_plan`: 获取查询的执行计划

### Resources

#### Resources

* `hologres:///schemas`: 获取数据库中所有的 Schema

#### Resource Templates

* `hologres:///{schema}/{table}/ddl`: 获取表的 DDL

* `hologres:///{schema}/tables`: 显示 Schema 下所有表的清淡

### Prompts

暂无
