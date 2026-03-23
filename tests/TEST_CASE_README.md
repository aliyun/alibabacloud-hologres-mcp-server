# Hologres MCP Server 测试

本目录包含用于测试 Hologres MCP Server 的单元测试和集成测试。

## 测试结构

- `unit/`: 单元测试（326 个测试用例，不需要数据库连接）
- `integration/`: 集成测试（61 个测试用例，需要真实 Hologres 连接）
- `conftest.py`: pytest 配置和共享 fixture
- `requirements.txt`: 测试依赖

## 环境配置

集成测试使用 `integration/.test_mcp_client_env` 文件中的环境变量连接到 Hologres 数据库。在运行测试前，请先配置此文件：

```bash
# 复制环境变量文件
cp integration/.test_mcp_client_env_example integration/.test_mcp_client_env
# 编辑环境变量文件
vim integration/.test_mcp_client_env
```

`.test_mcp_client_env` 文件内容示例：

```
HOLOGRES_HOST=your_host
HOLOGRES_PORT=your_port
HOLOGRES_USER=your_user
HOLOGRES_PASSWORD=your_password
HOLOGRES_DATABASE=your_database
```

请将上述配置替换为您的 Hologres 数据库连接信息。

## 安装依赖

在运行测试前，请确保已安装所需的依赖：

```bash
# 使用 uv（推荐）
uv sync --dev
uv pip install pytest pytest-cov pytest-asyncio

# 或使用 pip
pip install -r requirements.txt
```

## 运行测试

### 单元测试

```bash
# 运行所有单元测试
uv run pytest unit/ -v

# 运行特定测试文件
uv run pytest unit/test_tools.py -v

# 运行并生成覆盖率报告
uv run pytest unit/ --cov=src/hologres_mcp_server --cov-report=html
```

### 集成测试

```bash
# 运行所有集成测试
uv run pytest integration/ -v -m integration

# 运行特定测试类
uv run pytest integration/test_mcp_integration.py::TestMCPTools -v
```

### 运行所有测试

```bash
uv run pytest . -v
```

## 代码质量检查

在提交代码前，请运行代码质量检查：

```bash
# 检查代码风格
uv run ruff check .

# 自动修复
uv run ruff check . --fix

# 格式化代码
uv run ruff format .
```

## 测试覆盖范围

### 单元测试（326 个测试用例）

- 工具功能和 SQL 验证
- 资源和资源模板
- 提示生成
- 工具函数和错误处理
- 并发场景
- SQL 注入防护

### 集成测试（61 个测试用例）

| 测试类 | 测试数 | 描述 |
|--------|--------|------|
| `TestMCPConnection` | 5 | MCP 服务器连接和基本功能 |
| `TestMCPResources` | 14 | 资源读取功能 |
| `TestMCPTools` | 10 | 只读操作的工具调用 |
| `TestMCPProcedureTools` | 3 | 存储过程工具调用 |
| `TestMCPMaxComputeTools` | 1 | MaxCompute 外部表创建 |
| `TestMCPDDLTools` | 5 | DDL 操作 |
| `TestMCPDMLTools` | 3 | DML 操作 |
| `TestErrorHandling` | 3 | 错误处理和边界情况 |
| `TestMCPPrompts` | 4 | 提示生成功能 |
| `TestMCPConcurrency` | 3 | 并发 MCP 操作 |
| `TestMCPBoundaryConditions` | 4 | 边界条件 |
| `TestMCPPerformance` | 3 | 性能场景 |

## 注意事项

- 如果缺少 `.test_mcp_client_env` 文件或配置不完整，集成测试将被跳过
- 单元测试使用 mock 依赖，不需要实际数据库连接