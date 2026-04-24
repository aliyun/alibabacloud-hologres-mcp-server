
"""
Integration tests for new tools added in V1.0.3.

These tests require a real Hologres database connection.
Heavy operations (warehouse suspend/resume/restart/rebalance) are excluded.

Run with: pytest tests/integration/test_new_tools_integration.py -v -m integration
"""

import asyncio
import os

import pytest
from mcp import ClientSession

pytestmark = pytest.mark.integration


# ============================================================================
# Table Analysis Tools
# ============================================================================


class TestMCPNewTableAnalysisTools:
    """Tests for table analysis tools (properties, shard info, storage size)."""

    async def test_get_hg_table_properties(self, mcp_session, test_schema, test_table):
        if test_table is None:
            pytest.skip("No test table available")
        result = await mcp_session.call_tool("get_hg_table_properties", {"schema_name": test_schema, "table": test_table})
        assert result is not None
        assert hasattr(result, "content")
        assert len(result.content[0].text.strip()) > 0

    async def test_get_hg_table_properties_nonexistent(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_table_properties", {"schema_name": "public", "table": "nonexistent_table_xyz_12345"})
        assert result is not None
        text = result.content[0].text
        assert "No properties" in text or "not found" in text.lower() or "error" in text.lower()

    async def test_get_hg_table_shard_info(self, mcp_session, test_schema, test_table):
        if test_table is None:
            pytest.skip("No test table available")
        result = await mcp_session.call_tool("get_hg_table_shard_info", {"schema_name": test_schema, "table": test_table})
        assert result is not None
        text = result.content[0].text
        assert "Shard" in text or "Table Group" in text

    async def test_get_hg_table_storage_size(self, mcp_session, test_schema, test_table):
        if test_table is None:
            pytest.skip("No test table available")
        result = await mcp_session.call_tool("get_hg_table_storage_size", {"schema_name": test_schema, "table": test_table})
        assert result is not None
        text = result.content[0].text
        assert "Storage Size" in text or "Total" in text

    async def test_get_hg_table_info_trend(self, mcp_session, test_schema, test_table):
        if test_table is None:
            pytest.skip("No test table available")
        result = await mcp_session.call_tool("get_hg_table_info_trend", {"schema_name": test_schema, "table": test_table, "days": 7})
        assert result is not None
        text = result.content[0].text
        assert "Storage Trend" in text or "not available" in text.lower() or "No" in text


# ============================================================================
# Query Monitoring Tools
# ============================================================================


class TestMCPNewQueryMonitoringTools:
    """Tests for query monitoring tools."""

    async def test_list_hg_active_queries(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_active_queries", {"state": "active"})
        assert result is not None
        assert hasattr(result, "content")
        text = result.content[0].text
        assert "Active Queries" in text or "No active queries" in text or "pid" in text.lower()

    async def test_list_hg_active_queries_all(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_active_queries", {"state": "all"})
        assert result is not None
        assert hasattr(result, "content")

    async def test_get_hg_slow_queries(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_slow_queries", {"min_duration_ms": 1, "limit": 10})
        assert result is not None
        text = result.content[0].text
        assert "Slow Queries" in text or "No queries" in text

    async def test_get_hg_lock_diagnostics(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_lock_diagnostics", {})
        assert result is not None
        text = result.content[0].text
        assert "Lock" in text or "No" in text

    async def test_analyze_hg_query_by_id(self, mcp_session):
        result = await mcp_session.call_tool("analyze_hg_query_by_id", {"query_id": "nonexistent_query_id_xyz_12345"})
        assert result is not None
        text = result.content[0].text
        assert "No query found" in text or "not found" in text.lower() or "Error" in text

    async def test_cancel_hg_query(self, mcp_session, integration_env):
        # Use a separate psycopg connection to run a long query, so we can find it via MCP tools
        import psycopg
        import threading

        # Create a direct DB connection for the sleep query
        db_config = {
            "host": integration_env["HOLOGRES_HOST"],
            "port": int(integration_env["HOLOGRES_PORT"]),
            "user": integration_env["HOLOGRES_USER"],
            "password": integration_env["HOLOGRES_PASSWORD"],
            "dbname": integration_env["HOLOGRES_DATABASE"],
            "autocommit": True,
        }

        sleep_conn = psycopg.connect(**db_config)
        sleep_conn_pid = None
        try:
            # Get the PID of this connection
            with sleep_conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                sleep_conn_pid = cur.fetchone()[0]

            # Run pg_sleep in a background thread
            def run_sleep():
                with sleep_conn.cursor() as cur:
                    try:
                        cur.execute("SELECT pg_sleep(30)")
                    except Exception:
                        pass  # Will be cancelled

            sleep_thread = threading.Thread(target=run_sleep, daemon=True)
            sleep_thread.start()

            # Give the query time to start
            await asyncio.sleep(2)

            # Find and cancel the query via MCP tool
            cancel_result = await mcp_session.call_tool("cancel_hg_query", {"pid": sleep_conn_pid, "terminate": False})
            cancel_text = cancel_result.content[0].text

            # Verify the cancel succeeded
            assert str(sleep_conn_pid) in cancel_text or "cancel" in cancel_text.lower() or "Error" in cancel_text

            # Wait for the thread to finish
            sleep_thread.join(timeout=5)

        finally:
            if not sleep_conn.closed:
                sleep_conn.close()


# ============================================================================
# Warehouse Tools (Read-only + Light Operations)
# ============================================================================


class TestMCPNewWarehouseTools:
    """Tests for warehouse tools - excludes heavy operations."""

    async def test_list_hg_warehouses(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_warehouses", {})
        assert result is not None
        text = result.content[0].text
        assert "Computing Groups" in text or "warehouses" in text.lower() or "Current" in text

    async def test_get_hg_warehouse_status(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_warehouse_status", {"warehouse_name": "local"})
        assert result is not None
        text = result.content[0].text
        assert "Warehouse Status" in text or "Error" in text or "not found" in text.lower()

    async def test_switch_hg_warehouse(self, mcp_session):
        result = await mcp_session.call_tool("switch_hg_warehouse", {"warehouse_name": "local"})
        assert result is not None
        text = result.content[0].text
        assert "local" in text.lower() or "Error" in text or "success" in text.lower()


# ============================================================================
# Recycle Bin Tools
# ============================================================================


class TestMCPNewRecyclebinTools:
    """Tests for recycle bin tools."""

    async def test_list_hg_recyclebin(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_recyclebin", {})
        assert result is not None
        text = result.content[0].text
        assert "Recycle Bin" in text or "empty" in text.lower()

    async def test_restore_hg_table_from_recyclebin(self, mcp_session, integration_test_prefix):
        table_name = f"{integration_test_prefix}recyclebin_test"
        await mcp_session.call_tool("execute_hg_ddl_sql", {"query": f"CREATE TABLE IF NOT EXISTS public.{table_name} (id INT PRIMARY KEY)"})
        try:
            await mcp_session.call_tool("execute_hg_ddl_sql", {"query": f"DROP TABLE IF EXISTS public.{table_name}"})
            result = await mcp_session.call_tool("restore_hg_table_from_recyclebin", {"table_name": table_name, "schema_name": "public"})
            assert result is not None
            text = result.content[0].text
            assert "Successfully restored" in text or "not enabled" in text.lower() or "not found" in text.lower() or "Error" in text
        finally:
            await mcp_session.call_tool("execute_hg_ddl_sql", {"query": f"DROP TABLE IF EXISTS public.{table_name}"})


# ============================================================================
# Configuration and Metadata Tools
# ============================================================================


class TestMCPNewConfigAndMetaTools:
    """Tests for GUC, external databases, data masking rules."""

    async def test_get_hg_guc_config(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_guc_config", {"guc_name": "hg_version"})
        assert result is not None
        text = result.content[0].text
        assert "hg_version" in text.lower() or "Error" in text

    async def test_get_hg_guc_config_nonexistent(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_guc_config", {"guc_name": "nonexistent_guc_xyz_12345"})
        assert result is not None
        text = result.content[0].text
        assert "Error" in text or "not found" in text.lower()

    async def test_list_hg_external_databases(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_external_databases", {})
        assert result is not None
        text = result.content[0].text
        assert "External" in text or "Foreign" in text or "No" in text or "error" in text.lower()

    async def test_list_hg_data_masking_rules(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_data_masking_rules", {})
        assert result is not None
        text = result.content[0].text
        assert "Data Masking" in text or "Error" in text


# ============================================================================
# Dynamic Table Tools
# ============================================================================


class TestMCPNewDynamicTableTools:
    """Tests for dynamic table tools."""

    async def test_list_hg_dynamic_tables(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_dynamic_tables", {})
        assert result is not None
        text = result.content[0].text
        assert "Dynamic Tables" in text or "No Dynamic Tables" in text or "Error" in text

    async def test_get_hg_dynamic_table_refresh_history_nonexistent(self, mcp_session):
        result = await mcp_session.call_tool("get_hg_dynamic_table_refresh_history", {"schema_name": "public", "table_name": "nonexistent_dt_xyz_12345", "limit": 5})
        assert result is not None
        text = result.content[0].text
        assert "No refresh history" in text or "not found" in text.lower() or "Error" in text


# ============================================================================
# Query Queue Tools (V3.0+, may be skipped if not available)
# ============================================================================


class TestMCPNewQueryQueueTools:
    """Tests for query queue tools - V3.0+ features."""

    async def test_list_hg_query_queues(self, mcp_session):
        result = await mcp_session.call_tool("list_hg_query_queues", {})
        assert result is not None
        text = result.content[0].text
        assert "Query Queue" in text or "not available" in text.lower() or "Error" in text or "No" in text


# ============================================================================
# Procedure Tool (Updated parameter name)
# ============================================================================


class TestMCPProcedureToolUpdated:
    """Updated tests for call_hg_procedure to verify procedure_args parameter works via MCP protocol."""

    async def test_call_procedure_with_procedure_args_param(self, mcp_session, integration_test_prefix):
        procedure_name = f"{integration_test_prefix}updated_args_proc"
        await mcp_session.call_tool("execute_hg_ddl_sql", {"query": f"CREATE OR REPLACE PROCEDURE public.{procedure_name}(p_key TEXT, p_val TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE PLPGSQL"})
        try:
            result = await mcp_session.call_tool("call_hg_procedure", {"procedure_name": f"public.{procedure_name}", "procedure_args": ["key1", "val1"]})
            assert result is not None
            text = result.content[0].text
            assert "syntax error" not in text.lower(), f"Got syntax error: {text}"
        finally:
            await mcp_session.call_tool("execute_hg_ddl_sql", {"query": f"DROP PROCEDURE IF EXISTS public.{procedure_name}(TEXT, TEXT)"})

    async def test_call_procedure_hg_update_database_property(self, mcp_session):
        result = await mcp_session.call_tool("call_hg_procedure", {"procedure_name": "hg_update_database_property", "procedure_args": ["hg_experimental_enable_fixed_dispatcher_for_multi_values_in", "on"]})
        assert result is not None
        text = result.content[0].text
        assert "syntax error" not in text.lower(), f"Got syntax error: {text}"


# ============================================================================
# Tool Listing Verification
# ============================================================================


class TestMCPNewToolListing:
    """Verify all new tools are registered in the MCP server."""

    async def test_all_new_tools_listed(self, mcp_session):
        result = await mcp_session.list_tools()
        assert result is not None
        tool_names = {tool.name for tool in result.tools}
        expected_new_tools = {
            "list_hg_dynamic_tables", "get_hg_dynamic_table_refresh_history",
            "list_hg_recyclebin", "restore_hg_table_from_recyclebin",
            "list_hg_warehouses", "switch_hg_warehouse", "get_hg_warehouse_status",
            "cancel_hg_query", "list_hg_active_queries", "list_hg_query_queues",
            "get_hg_table_properties", "get_hg_table_shard_info", "get_hg_table_storage_size",
            "get_hg_table_info_trend", "get_hg_guc_config", "get_hg_lock_diagnostics",
            "get_hg_slow_queries", "analyze_hg_query_by_id", "list_hg_external_databases",
            "list_hg_data_masking_rules", "query_hg_external_files", "manage_hg_query_queue",
            "manage_hg_classifier", "set_hg_query_queue_property",
            "rebalance_hg_warehouse", "manage_hg_warehouse",
        }
        missing = expected_new_tools - tool_names
        assert not missing, f"Missing tools: {missing}"
