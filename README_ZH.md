[English](README.md) | 中文

# Hologres MCP 服务器

Hologres MCP 服务器作为 AI 代理与 Hologres 数据库之间的通用接口。它实现了 AI 代理与 Hologres 之间的无缝通信，帮助 AI 代理获取 Hologres 数据库元数据并执行 SQL 操作。

## 配置

### 模式 1：使用本地文件

#### 下载

从 Github 下载

```shell
git clone https://github.com/aliyun/alibabacloud-hologres-mcp-server.git
```

#### MCP 集成
在 MCP 客户端配置文件中添加以下配置：

```json
{
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
}
```

### 模式 2：使用 PIP 模式 安装
使用以下命令安装 MCP 服务器：

```bash
pip install hologres-mcp-server
```

#### MCP 集成
在 MCP 客户端配置文件中添加以下配置：

使用 UV 模式

```json
{
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
}
```
使用 uvx 模式
```json
{
    "mcpServers": {
        "hologres-mcp-server": {
            "command": "uvx",
            "args": [
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
### 工具
- `execute_hg_select_sql` ：在 Hologres 数据库中执行 SELECT SQL 查询
- `execute_hg_select_sql_with_serverless` ：在 Hologres 数据库中使用无服务器计算执行 SELECT SQL 查询
- `execute_hg_dml_sql` ：在 Hologres 数据库中执行 DML（INSERT、UPDATE、DELETE）SQL 查询
- `execute_hg_ddl_sql` ：在 Hologres 数据库中执行 DDL（CREATE、ALTER、DROP、COMMENT ON）SQL 查询
- `gather_hg_table_statistics` ：收集 Hologres 数据库中的表统计信息
- `get_hg_query_plan` ：获取 Hologres 数据库中的查询计划
- `get_hg_execution_plan` ：获取 Hologres 数据库中的执行计划
- `call_hg_procedure` ：调用 Hologres 数据库中的存储过程
- `create_hg_maxcompute_foreign_table` ：在 Hologres 数据库中创建 MaxCompute 外部表

由于某些代理不支持资源和资源模板，提供了以下工具来获取模式、表、视图和外部表的元数据：

- `list_hg_schemas` ：列出当前 Hologres 数据库中的所有模式，不包括系统模式
- `list_hg_tables_in_a_schema` ：列出特定模式中的所有表，包括它们的类型（表、视图、外部表、分区表）
- `show_hg_table_ddl` ：显示 Hologres 数据库中表、视图或外部表的 DDL 脚本

### 资源 内置资源
- `hologres:///schemas` ：获取 Hologres 数据库中的所有模式 资源模板
- `hologres:///{schema}/tables` ：列出 Hologres 数据库中某个模式下的所有表
- `hologres:///{schema}/{table}/partitions` ：列出 Hologres 数据库中分区表的所有分区
- `hologres:///{schema}/{table}/ddl` ：获取 Hologres 数据库中的表 DDL
- `hologres:///{schema}/{table}/statistic` ：显示 Hologres 数据库中收集的表统计信息
- `system:///{+system_path}` ：
  系统路径包括：

  - `hg_instance_version` - 显示 hologres 实例版本
  - `guc_value/<guc_name>` - 显示 guc（统一配置）值
  - `missing_stats_tables` - 显示缺少统计信息的表
  - `stat_activity` - 显示当前运行查询的信息
  - `query_log/latest/<row_limits>` - 获取指定行数的最近查询日志历史
  - `query_log/user/<user_name>/<row_limits>` - 获取特定用户的查询日志历史，带行数限制
  - `query_log/application/<application_name>/<row_limits>` - 获取特定应用程序的查询日志历史，带行数限制
  - `query_log/failed/<interval>/<row_limits>` - 获取失败的查询日志历史，带时间间隔和指定行数