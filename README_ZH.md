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

## 在 Claude Code 中使用

```bash
# 添加到 Claude Code
claude mcp add hologres-mcp-server \
  -e HOLOGRES_HOST=<your_host> \
  -e HOLOGRES_PORT=<your_port> \
  -e HOLOGRES_USER=<your_access_id> \
  -e HOLOGRES_PASSWORD=<your_access_key> \
  -e HOLOGRES_DATABASE=<your_database> \
  -- uvx hologres-mcp-server
```

## 组件
### 工具
- `execute_hg_select_sql` ：在 Hologres 数据库中执行 SELECT SQL 查询
- `execute_hg_select_sql_with_serverless` ：在 Hologres 数据库中使用无服务器计算执行 SELECT SQL 查询
- `execute_hg_dml_sql` ：在 Hologres 数据库中执行 DML（INSERT、UPDATE、DELETE）SQL 查询
- `execute_hg_ddl_sql` ：在 Hologres 数据库中执行 DDL（CREATE、ALTER、DROP、COMMENT ON）SQL 查询
- `gather_hg_table_statistics` ：收集 Hologres 数据库中的表统计信息
  - 参数：`schema_name`（字符串），`table`（字符串）
- `get_hg_query_plan` ：获取 Hologres 数据库中的查询计划
- `get_hg_execution_plan` ：获取 Hologres 数据库中的执行计划
- `call_hg_procedure` ：调用 Hologres 数据库中的存储过程
- `create_hg_maxcompute_foreign_table` ：在 Hologres 数据库中创建 MaxCompute 外部表

由于某些代理不支持资源和资源模板，提供了以下工具来获取模式、表、视图和外部表的元数据：

- `list_hg_schemas` ：列出当前 Hologres 数据库中的所有模式，不包括系统模式
- `list_hg_tables_in_a_schema` ：列出特定模式中的所有表，包括它们的类型（表、视图、外部表、分区表）
  - 参数：`schema_name`（字符串）
- `show_hg_table_ddl` ：显示 Hologres 数据库中表、视图或外部表的 DDL 脚本
  - 参数：`schema_name`（字符串），`table`（字符串）

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

### 提示

- `analyze_table_performance`：生成分析 Hologres 中表性能的提示
- `optimize_query`：生成优化 Hologres SQL 查询的提示
- `explore_schema`：生成探索 Hologres 数据库中模式的提示

## 测试

项目包含完整的单元测试和集成测试。

### 单元测试

单元测试不需要数据库连接，使用模拟依赖。测试套件包含 **326 个测试用例**，覆盖：

- 工具功能和 SQL 验证
- 资源和资源模板
- 提示生成
- 工具函数和错误处理
- 并发场景
- SQL 注入防护

```bash
# 安装依赖
uv pip install pytest pytest-cov pytest-asyncio

# 运行所有单元测试
uv run pytest tests/unit/ -v

# 运行特定测试文件
uv run pytest tests/unit/test_tools.py -v

# 运行并生成覆盖率报告
uv run pytest tests/unit/ --cov=src/hologres_mcp_server --cov-report=html
```

### 集成测试

集成测试需要真实的 Hologres 数据库连接。测试套件包含 **61 个测试用例**，组织为 12 个测试类：

| 测试类 | 测试数 | 描述 |
|--------|--------|------|
| `TestMCPConnection` | 5 | MCP 服务器连接和基本功能 |
| `TestMCPResources` | 14 | 资源读取功能（模式、表、DDL、统计信息、分区、查询日志） |
| `TestMCPTools` | 10 | 只读操作的工具调用 |
| `TestMCPProcedureTools` | 3 | 存储过程工具调用 |
| `TestMCPMaxComputeTools` | 1 | MaxCompute 外部表创建 |
| `TestMCPDDLTools` | 5 | DDL 操作（CREATE、ALTER、DROP、COMMENT） |
| `TestMCPDMLTools` | 3 | DML 操作（INSERT、UPDATE、DELETE） |
| `TestErrorHandling` | 3 | 错误处理和边界情况 |
| `TestMCPPrompts` | 4 | 提示生成功能 |
| `TestMCPConcurrency` | 3 | 并发 MCP 操作 |
| `TestMCPBoundaryConditions` | 4 | 边界条件（Unicode、NULL、空结果） |
| `TestMCPPerformance` | 3 | 性能场景（大型/宽结果集） |

1. 从示例文件创建配置文件：

```bash
cp tests/integration/.test_mcp_client_env_example tests/integration/.test_mcp_client_env
```

2. 编辑配置文件，填入您的 Hologres 凭证：

```
HOLOGRES_HOST=your-hologres-instance.hologres.aliyuncs.com
HOLOGRES_PORT=80
HOLOGRES_USER=your_username
HOLOGRES_PASSWORD=your_password
HOLOGRES_DATABASE=your_database
```

3. 运行集成测试：

```bash
# 安装依赖
uv pip install pytest pytest-cov pytest-asyncio

# 运行所有集成测试
uv run pytest tests/integration/ -v -m integration

# 运行特定测试类
uv run pytest tests/integration/test_mcp_integration.py::TestMCPTools -v

# 运行所有测试（单元测试 + 集成测试）
uv run pytest tests/ -v
```

**注意：** 如果缺少 `.test_mcp_client_env` 文件或配置不完整，集成测试将被跳过。

## 代码质量

本项目使用 [ruff](https://docs.astral.sh/ruff/) 进行代码检查和格式化。

```bash
# 安装开发依赖
uv sync --dev
uv pip install ruff

# 检查代码风格
uv run ruff check .

# 检查并自动修复
uv run ruff check . --fix

# 格式化代码
uv run ruff format .

# 仅检查格式（不修改）
uv run ruff format . --check
```

## 构建与发布

### 构建

本项目使用 [hatchling](https://hatch.pypa.io/) 作为构建后端。构建产物将生成在 `dist/` 目录下。

```bash
# 使用 uv（推荐）
uv build

# 或使用 python build 模块
pip install build
python -m build
```

### 发布到 PyPI

```bash
# 安装 twine
uv pip install twine

# 上传到 PyPI
uv run twine upload dist/*

# 或先上传到测试 PyPI 进行验证
uv run twine upload --repository testpypi dist/*
```

### 发版流程

```bash
# 1. 更新 pyproject.toml 中的版本号
# 2. 清理旧的构建产物
rm -rf dist/

# 3. 构建
uv build

# 4. 发布
twine upload dist/*

# 5. 打标签
git tag -a v1.0.2 -m "Release v1.0.2"
git push origin v1.0.2
```

### 更新 cli 功能

```bash
# 使用 FastMCP 框架，生成 cli 代码和 Skill
uv run fastmcp generate-cli hologres-mcp-server hologres_mcp_cli/hologres_mcp_cli.py -f
```