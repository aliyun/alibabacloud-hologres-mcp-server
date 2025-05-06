#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hologres MCP Client测试脚本

该脚本用于测试Hologres MCP Server提供的资源和工具。
参考知乎文章：https://zhuanlan.zhihu.com/p/21166726702
"""

import asyncio
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import inspect
from typing import Any, Dict, List, Union

# 加载环境变量
env_path = Path(__file__).parent / ".test_mcp_client_env"
load_dotenv(dotenv_path=env_path)

# 设置服务器连接参数
def get_server_params():
    """
    获取服务器连接参数
    
    如果命令行参数提供了服务器脚本路径，则使用命令行参数
    否则使用默认路径
    """
    if len(sys.argv) > 1:
        server_script = sys.argv[1]
    else:
        # 默认路径，指向项目中的server.py
        server_script = str(Path(__file__).parent.parent / "src" / "hologres_mcp_server" / "server.py")
    
    # 获取Hologres数据库连接环境变量
    hologres_env = {
        "HOLOGRES_HOST": os.getenv("HOLOGRES_HOST"),
        "HOLOGRES_PORT": os.getenv("HOLOGRES_PORT"),
        "HOLOGRES_USER": os.getenv("HOLOGRES_USER"),
        "HOLOGRES_PASSWORD": os.getenv("HOLOGRES_PASSWORD"),
        "HOLOGRES_DATABASE": os.getenv("HOLOGRES_DATABASE")
    }
    
    # 确保所有必需的环境变量都已设置
    if not all(hologres_env.values()):
        print("错误：缺少必要的数据库配置环境变量，请检查.test_mcp_client_env文件")
        sys.exit(1)
        
    return StdioServerParameters(
        command="python",           # 运行命令
        args=[server_script],      # 服务器脚本路径
        env=hologres_env           # 传递Hologres数据库连接环境变量
    )

def to_serializable(obj: Any) -> Union[Dict, List, str, int, float, bool, None]:
    """
    将MCP对象转换为可JSON序列化的格式
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif hasattr(obj, '__dict__'):
        # 对于自定义对象，转换其属性为字典
        result = {}
        for key, value in obj.__dict__.items():
            # 跳过私有属性
            if not key.startswith('_'):
                result[key] = to_serializable(value)
        return result
    elif hasattr(obj, '_asdict'):  # 处理namedtuple
        return to_serializable(obj._asdict())
    else:
        # 尝试获取对象的所有公共属性
        result = {}
        for key in dir(obj):
            if not key.startswith('_') and not inspect.ismethod(getattr(obj, key)):
                try:
                    value = getattr(obj, key)
                    result[key] = to_serializable(value)
                except Exception:
                    pass  # 忽略无法序列化的属性
        return result if result else str(obj)

async def test_list_resources(session):
    """
    测试列出资源
    """
    print("\n===== 测试列出资源 =====")
    resources = await session.list_resources()
    serializable_resources = to_serializable(resources)
    print(f"资源列表: {json.dumps(serializable_resources, indent=2, ensure_ascii=False)}")
    return resources

async def test_list_resource_templates(session):
    """
    测试列出资源模板
    """
    print("\n===== 测试列出资源模板 =====")
    templates = await session.list_resource_templates()
    serializable_templates = to_serializable(templates)
    print(f"资源模板列表: {json.dumps(serializable_templates, indent=2, ensure_ascii=False)}")
    return templates

async def test_read_resource(session, uri):
    """
    测试读取资源
    """
    print(f"\n===== 测试读取资源: {uri} =====")
    try:
        content = await session.read_resource(uri)
        print(f"资源内容:\n{content}")
        return content
    except Exception as e:
        print(f"读取资源失败: {e}")
        return None

async def test_list_tools(session):
    """
    测试列出工具
    """
    print("\n===== 测试列出工具 =====")
    tools = await session.list_tools()
    serializable_tools = to_serializable(tools)
    print(f"工具列表: {json.dumps(serializable_tools, indent=2, ensure_ascii=False)}")
    return tools

async def test_call_tool(session, tool_name, args):
    """
    测试调用工具
    """
    print(f"\n===== 测试调用工具: {tool_name} =====")
    print(f"参数: {json.dumps(args, indent=2, ensure_ascii=False)}")
    try:
        result = await session.call_tool(tool_name, args)
        serializable_result = to_serializable(result)
        print(f"调用结果:\n{json.dumps(serializable_result, indent=2, ensure_ascii=False) if isinstance(serializable_result, (dict, list)) else serializable_result}")
        return result
    except Exception as e:
        print(f"调用工具失败: {e}")
        return None

async def run_tests():
    """
    运行所有测试
    """
    server_params = get_server_params()
    
    print("开始测试 Hologres MCP Client...")
    print(f"服务器脚本路径: {server_params.args[0]}")
    
    # 建立连接
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # 初始化连接
            await session.initialize()
            print("成功连接到 MCP 服务器")
            
            # 测试列出资源
            resources = await test_list_resources(session)
            
            # 测试列出资源模板
            templates = await test_list_resource_templates(session)
            
            # 测试读取资源 - 列出所有schema
            await test_read_resource(session, "hologres:///schemas")
            
            # 测试读取系统信息
            await test_read_resource(session, "system:///hg_instance_version")
            
            # 测试列出工具
            tools = await test_list_tools(session)
            
            # 测试调用工具 - 列出所有schema
            await test_call_tool(session, "list_hg_schemas", {})
            
            # 测试调用工具 - 执行SELECT查询
            await test_call_tool(session, "execute_hg_select_sql", {
                "query": "SELECT current_database() AS current_db, current_user AS current_user;"
            })
            
            # 测试调用工具 - 获取查询计划
            await test_call_tool(session, "get_hg_query_plan", {
                "query": "SELECT * FROM information_schema.tables LIMIT 5;"
            })
            
            print("\n===== 所有测试完成 =====")

def main():
    """
    主函数
    """
    asyncio.run(run_tests())

if __name__ == "__main__":
    main()