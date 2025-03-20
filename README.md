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

* `analyze_table`: 收集表的统计信息

* `get_query_plan`: 获取查询的查询计划

* `get_execution_plan`: 获取查询的执行计划

### Resources

#### Resources

* `hologres:///schemas`: 获取数据库中所有的 Schema

* `hologres:///system_info/missing_stats_tables`: 获取数据库中所有缺失统计信息的表

#### Resource Templates

* `hologres:///{schema}/tables`: 显示 Schema 下所有表的清单

* `hologres:///{schema}/{table}/ddl`: 获取表的 DDL

* `hologres:///{schema}/{table}/statistic`: 显示表已经采集的统计信息

* `hologres:///system_info/latest_query_log/{row_limits}`: 查看最近的查询日志

* `hologres:///system_info/user_query_log/{user}`: 显示特定用户的查询日志

* `hologres:///system_info/application_query_log/{application}`: 显示特定应用的查询日志

### Prompts

暂无
