# Hologres MCP Server 测试

本目录包含用于测试 Hologres MCP Server 的客户端测试脚本。这些测试脚本使用 MCP Python SDK 连接到 Hologres MCP Server，并测试其提供的资源和工具。

## 测试脚本

- `test_mcp_client.py`: 基础测试脚本，测试 MCP Server 的基本功能
- `test_mcp_client_comprehensive.py`: 全面测试脚本，测试 MCP Server 的所有资源和工具

## 环境配置

测试脚本使用 `.test_mcp_client_env` 文件中的环境变量连接到 Hologres 数据库。在运行测试前，请先配置此文件：

```bash
# 复制环境变量文件
cp .test_mcp_client_env.example .test_mcp_client_env
# 编辑环境变量文件
vim .test_mcp_client_env
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
pip install -r requirements.txt
```

## 运行测试

### 基础测试

```bash
python test_mcp_client.py
```

### 全面测试

```bash
python test_mcp_client_comprehensive.py
```

默认情况下，测试脚本会使用项目中的 `src/hologres_mcp_server/server.py` 作为服务器脚本。您也可以通过命令行参数指定服务器脚本路径：

```bash
python test_mcp_client.py /path/to/server.py
```

## 测试输出

测试脚本会输出每个测试步骤的结果，包括：

- 资源列表
- 资源模板列表
- 资源内容
- 工具列表
- 工具调用结果

如果测试过程中出现错误，脚本会输出错误信息，但会继续执行后续测试。

## 注意事项

测试脚本中包含了一个 `to_serializable()` 辅助函数，用于将 MCP 对象转换为可 JSON 序列化的格式。这是因为 MCP 返回的对象（如 `ListResourcesResult`）默认不是 JSON 可序列化的。该函数会递归地将对象的属性转换为基本数据类型，以便进行 JSON 序列化。