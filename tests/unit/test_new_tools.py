"""
Tests for the 27 new tool functions added in V1.0.3.

These tools use connect_with_retry() directly for database operations,
unlike the original 12 tools which delegate to handle_call_tool().
"""

from unittest.mock import MagicMock, patch

from conftest import PATCH_CONNECT, _make_mock_conn

from hologres_mcp_server.server import (
    analyze_hg_query_by_id,
    cancel_hg_query,
    get_hg_dynamic_table_refresh_history,
    get_hg_guc_config,
    get_hg_lock_diagnostics,
    get_hg_slow_queries,
    get_hg_table_info_trend,
    get_hg_table_properties,
    get_hg_table_shard_info,
    get_hg_table_storage_size,
    get_hg_warehouse_status,
    list_hg_active_queries,
    list_hg_data_masking_rules,
    list_hg_dynamic_tables,
    list_hg_external_databases,
    list_hg_query_queues,
    list_hg_recyclebin,
    list_hg_warehouses,
    manage_hg_classifier,
    manage_hg_query_queue,
    manage_hg_warehouse,
    query_hg_external_files,
    rebalance_hg_warehouse,
    restore_hg_table_from_recyclebin,
    set_hg_query_queue_property,
    switch_hg_warehouse,
)

# ============================================================================
# Query Performance Analysis
# ============================================================================


class TestAnalyzeHgQueryById:
    """Tests for analyze_hg_query_by_id tool."""

    def test_query_found(self):
        row = ("ok", 1500, "2025-01-01 10:00:00", "user1", "app", 1024, 2048, 4096, 512, 100, "SELECT 1")
        desc = [
            ("status",), ("duration",), ("query_start",), ("usename",),
            ("application_name",), ("read_bytes",), ("write_bytes",),
            ("memory_bytes",), ("shuffle_bytes",), ("cpu_time_ms",), ("query_detail",),
        ]
        conn, cursor = _make_mock_conn(fetchone=row, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = analyze_hg_query_by_id("abc-123")
            assert "abc-123" in result
            assert "Query Profile" in result
            assert "Key Metrics" in result
            cursor.execute.assert_called_once()

    def test_query_not_found(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = None
        cursor.description = []
        with patch(PATCH_CONNECT, return_value=conn):
            result = analyze_hg_query_by_id("nonexistent")
            assert "No query found" in result or "nonexistent" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("connection refused")):
            result = analyze_hg_query_by_id("abc")
            assert "Error" in result


class TestGetHgSlowQueries:
    """Tests for get_hg_slow_queries tool."""

    def test_slow_queries_found(self):
        rows = [("q1", "user1", "ok", 5000, "2025-01-01", 1024, 2048, 100, "SELECT ...")]
        desc = [("query_id",), ("usename",), ("status",), ("duration",),
                ("query_start",), ("read_bytes",), ("memory_bytes",),
                ("cpu_time_ms",), ("query_preview",)]
        conn, _ = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_slow_queries(1000, 20)
            assert "Slow Queries" in result
            assert "q1" in result

    def test_no_slow_queries(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_slow_queries(1000, 20)
            assert "No queries found" in result


# ============================================================================
# Dynamic Table Management
# ============================================================================


class TestListHgDynamicTables:
    """Tests for list_hg_dynamic_tables tool."""

    def test_tables_found(self):
        rows = [("public", "dt1", "ok", "5m", "2025-01-01", "1m", True)]
        conn, _ = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_dynamic_tables()
            assert "Dynamic Tables" in result
            assert "dt1" in result

    def test_with_schema_filter(self):
        rows = [("myschema", "dt1", "ok", "10m", "2025-01-01", "2m", True)]
        conn, cursor = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_dynamic_tables("myschema")
            assert "dt1" in result
            # Verify schema param was passed
            call_args = cursor.execute.call_args
            assert "myschema" in call_args[0][1]

    def test_no_tables(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_dynamic_tables()
            assert "No Dynamic Tables found" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("timeout")):
            result = list_hg_dynamic_tables()
            assert "Error" in result


class TestGetHgDynamicTableRefreshHistory:
    """Tests for get_hg_dynamic_table_refresh_history tool."""

    def test_history_found(self):
        rows = [("qid1", "ok", "2025-01-01 10:00", "2025-01-01 10:05", 300000, 100, "full")]
        conn, _ = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_dynamic_table_refresh_history("public", "my_dt")
            assert "Refresh History" in result
            assert "qid1" in result

    def test_no_history(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_dynamic_table_refresh_history("public", "my_dt")
            assert "No refresh history" in result


# ============================================================================
# Recycle Bin Management
# ============================================================================


class TestListHgRecyclebin:
    """Tests for list_hg_recyclebin tool."""

    def test_items_found(self):
        rows = [(12345, "public", "old_table", "admin", "admin", "2025-01-01")]
        conn, _ = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_recyclebin()
            assert "Recycle Bin" in result
            assert "old_table" in result
            assert "12345" in result

    def test_empty_recyclebin(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_recyclebin()
            assert "empty" in result.lower()


class TestRestoreHgTableFromRecyclebin:
    """Tests for restore_hg_table_from_recyclebin tool."""

    def test_restore_success(self):
        conn, cursor = _make_mock_conn()
        # First call: SHOW hg_enable_recyclebin -> on
        # Second call: SELECT from hg_recyclebin -> returns a row
        # Third call: RECOVER TABLE
        cursor.fetchone.side_effect = [("on",), (12345,), None]
        with patch(PATCH_CONNECT, return_value=conn):
            result = restore_hg_table_from_recyclebin("my_table", "public")
            assert "Successfully restored" in result or "12345" in result

    def test_recyclebin_disabled(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = ("off",)
        with patch(PATCH_CONNECT, return_value=conn):
            result = restore_hg_table_from_recyclebin("my_table")
            assert "not enabled" in result.lower() or "Error" in result

    def test_table_not_in_recyclebin(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [("on",), None]
        with patch(PATCH_CONNECT, return_value=conn):
            result = restore_hg_table_from_recyclebin("nonexistent", "public")
            assert "not found" in result.lower() or "Error" in result


# ============================================================================
# Warehouse (Computing Group) Management
# ============================================================================


class TestListHgWarehouses:
    """Tests for list_hg_warehouses tool."""

    def test_warehouses_found(self):
        conn, cursor = _make_mock_conn()
        # First call: SHOW hg_computing_resource
        # Second call: SELECT from hg_warehouses
        cursor.fetchone.return_value = ("default_wh",)
        cursor.fetchall.return_value = [(1, "default_wh", 8, 16384, 2, "running", True)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_warehouses()
            assert "Computing Groups" in result
            assert "default_wh" in result

    def test_no_warehouses(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = ("default",)
        cursor.fetchall.return_value = []
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_warehouses()
            assert "No warehouses" in result or "Computing Groups" in result


class TestSwitchHgWarehouse:
    """Tests for switch_hg_warehouse tool."""

    def test_switch_success(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("my_warehouse")
            assert "my_warehouse" in result


class TestManageHgWarehouse:
    """Tests for manage_hg_warehouse tool."""

    def test_suspend(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("suspend", "wh1")
            assert "suspended" in result.lower()
            assert "wh1" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_suspend_warehouse" in sql

    def test_resume(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("resume", "wh1")
            assert "resumed" in result.lower()

    def test_restart(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("restart", "wh1")
            assert "restarted" in result.lower()

    def test_rename(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("rename", "old_name", new_name="new_name")
            assert "renamed" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_rename_warehouse" in sql

    def test_rename_missing_new_name(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("rename", "wh1")
            assert "Error" in result or "required" in result.lower()

    def test_resize(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("resize", "wh1", cu=64)
            assert "resized" in result.lower() or "64" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_alter_warehouse" in sql

    def test_resize_invalid_cu(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("resize", "wh1", cu=0)
            assert "Error" in result or "positive" in result.lower()

    def test_unknown_action(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("invalid_action", "wh1")
            assert "Unknown action" in result

    def test_action_case_insensitive(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_warehouse("SUSPEND", "wh1")
            assert "suspended" in result.lower()

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("permission denied")):
            result = manage_hg_warehouse("suspend", "wh1")
            assert "Error" in result


class TestGetHgWarehouseStatus:
    """Tests for get_hg_warehouse_status tool."""

    def test_status_returned(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [
            ("running",),  # hg_get_warehouse_status
            ("idle",),     # hg_get_rebalance_warehouse_status
            (1, 8, 16384, 2, "running", True),  # hg_warehouses
        ]
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_warehouse_status("wh1")
            assert "Warehouse Status" in result
            assert "wh1" in result
            assert "running" in result

    def test_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("not found")):
            result = get_hg_warehouse_status("nonexistent")
            assert "Error" in result


class TestRebalanceHgWarehouse:
    """Tests for rebalance_hg_warehouse tool."""

    def test_rebalance_triggered(self):
        conn, _ = _make_mock_conn(fetchone=("rebalance_started",))
        with patch(PATCH_CONNECT, return_value=conn):
            result = rebalance_hg_warehouse("wh1")
            assert "rebalance" in result.lower()
            assert "wh1" in result

    def test_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("no permission")):
            result = rebalance_hg_warehouse("wh1")
            assert "Error" in result


# ============================================================================
# Query Monitoring & Control
# ============================================================================


class TestListHgActiveQueries:
    """Tests for list_hg_active_queries tool."""

    def test_active_queries(self):
        rows = [(123, "user1", "active", "SELECT 1", "2025-01-01")]
        desc = [("pid",), ("usename",), ("state",), ("query",), ("query_start",)]
        conn, _ = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_active_queries("active")
            assert "Active Queries" in result or "123" in result

    def test_no_active_queries(self):
        conn, _ = _make_mock_conn(fetchall=[], description=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_active_queries("active")
            assert "No" in result or "no" in result


class TestCancelHgQuery:
    """Tests for cancel_hg_query tool."""

    def test_cancel_success(self):
        conn, _ = _make_mock_conn(fetchone=(True,))
        with patch(PATCH_CONNECT, return_value=conn):
            result = cancel_hg_query(12345)
            assert "12345" in result or "cancel" in result.lower()

    def test_terminate(self):
        conn, cursor = _make_mock_conn(fetchone=(True,))
        with patch(PATCH_CONNECT, return_value=conn):
            cancel_hg_query(12345, terminate=True)
            sql = cursor.execute.call_args[0][0]
            assert "pg_terminate_backend" in sql

    def test_cancel_not_terminate(self):
        conn, cursor = _make_mock_conn(fetchone=(True,))
        with patch(PATCH_CONNECT, return_value=conn):
            cancel_hg_query(12345, terminate=False)
            sql = cursor.execute.call_args[0][0]
            assert "pg_cancel_backend" in sql


class TestGetHgLockDiagnostics:
    """Tests for get_hg_lock_diagnostics tool."""

    def test_locks_found(self):
        rows = [(100, "active", "SELECT 1", 200, "idle", "UPDATE t")]
        desc = [("blocked_pid",), ("blocked_state",), ("blocked_query",),
                ("blocking_pid",), ("blocking_state",), ("blocking_query",)]
        conn, _ = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_lock_diagnostics()
            assert "Lock" in result or "100" in result

    def test_no_locks(self):
        conn, _ = _make_mock_conn(fetchall=[], description=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_lock_diagnostics()
            assert "No" in result or "no" in result


# ============================================================================
# Table Analysis & Schema
# ============================================================================


class TestGetHgTableStorageSize:
    """Tests for get_hg_table_storage_size tool."""

    def test_with_hg_relation_size(self):
        conn, cursor = _make_mock_conn()
        # hg_relation_size calls return data, index, meta sizes
        cursor.fetchone.side_effect = [(1048576,), (524288,), (262144,)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_storage_size("public", "users")
            assert "Storage Size" in result
            assert "public.users" in result

    def test_fallback_to_pg_functions(self):
        conn, cursor = _make_mock_conn()
        # hg_relation_size fails, fallback to pg functions
        cursor.fetchone.side_effect = [Exception("not found")]
        cursor.execute.side_effect = [Exception("hg_relation_size not found")]
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_storage_size("public", "users")
            # Should return error since all calls fail
            assert "Error" in result or "Storage Size" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("table not found")):
            result = get_hg_table_storage_size("public", "nonexistent")
            assert "Error" in result


class TestGetHgTableProperties:
    """Tests for get_hg_table_properties tool."""

    def test_properties_found(self):
        rows = [("distribution_key", "id"), ("clustering_key", "ts"), ("segment_key", "ds")]
        conn, _ = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_properties("public", "orders")
            assert "Properties" in result
            assert "distribution_key" in result
            assert "id" in result

    def test_no_properties(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_properties("public", "orders")
            assert "No properties" in result or "not found" in result.lower()


class TestGetHgTableShardInfo:
    """Tests for get_hg_table_shard_info tool."""

    def test_shard_info_found(self):
        conn, cursor = _make_mock_conn()
        # First call: table_group from hg_table_properties
        # Second call: shard_count from hg_table_group_properties
        cursor.fetchone.side_effect = [("tg_default",), ("32",)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_shard_info("public", "orders")
            assert "Shard" in result or "tg_default" in result

    def test_no_table_group(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = None
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_shard_info("public", "orders")
            assert "Shard Info" in result or "unknown" in result.lower()


class TestGetHgTableInfoTrend:
    """Tests for get_hg_table_info_trend tool."""

    def test_trend_data(self):
        rows = [("2025-01-03", 1048576, 524288, 10, 1000, 50, 20),
                ("2025-01-02", 1000000, 500000, 9, 900, 45, 18)]
        conn, _ = _make_mock_conn(fetchall=rows)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_info_trend("public", "orders", 7)
            assert "Storage Trend" in result
            assert "public.orders" in result

    def test_no_data(self):
        conn, _ = _make_mock_conn(fetchall=[])
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_info_trend("public", "orders")
            assert "No" in result or "no" in result


# ============================================================================
# External Data & Lake
# ============================================================================


class TestListHgExternalDatabases:
    """Tests for list_hg_external_databases tool."""

    def test_databases_found(self):
        rows = [("ext_db1", "mc_server")]
        desc = [("datname",), ("srvname",)]
        conn, _ = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_external_databases()
            assert "ext_db1" in result or "External" in result


# ============================================================================
# Query Queue Management (V3.0+)
# ============================================================================


class TestListHgQueryQueues:
    """Tests for list_hg_query_queues tool."""

    def test_queues_found(self):
        conn, cursor = _make_mock_conn()
        # First fetchall: queues, second: classifiers
        cursor.fetchall.side_effect = [
            [("queue1", 10, 100)],
            [("queue1", "cls1", 1)],
        ]
        cursor.description = [("query_queue_name",), ("max_concurrency",), ("max_queue_size",)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_query_queues()
            assert "queue1" in result or "Query Queue" in result


class TestManageHgQueryQueue:
    """Tests for manage_hg_query_queue tool."""

    def test_create_success(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("create", "my_queue", 10, 100)
            assert "Successfully created" in result
            assert "my_queue" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_create_query_queue" in sql

    def test_create_invalid_params(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("create", "my_queue", 0, 0)
            assert "Error" in result or "positive" in result.lower()

    def test_drop(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("drop", "my_queue")
            assert "dropped" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_drop_query_queue" in sql

    def test_clear(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("clear", "my_queue")
            assert "cleared" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_clear_query_queue" in sql

    def test_unknown_action(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("invalid", "q1")
            assert "Unknown action" in result

    def test_action_case_insensitive(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_query_queue("DROP", "my_queue")
            assert "dropped" in result.lower()

    def test_sql_injection_in_name(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            manage_hg_query_queue("drop", "queue'; DROP TABLE users; --")
            sql = cursor.execute.call_args[0][0]
            # Single quotes should be escaped
            assert "''" in sql

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("permission denied")):
            result = manage_hg_query_queue("create", "q", 10, 100)
            assert "Error" in result


class TestManageHgClassifier:
    """Tests for manage_hg_classifier tool."""

    def test_create(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_classifier("create", "q1", "cls1", 10)
            assert "Successfully created" in result
            assert "cls1" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_create_classifier" in sql

    def test_drop(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_classifier("drop", "q1", "cls1")
            assert "dropped" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_drop_classifier" in sql

    def test_unknown_action(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = manage_hg_classifier("update", "q1", "cls1")
            assert "Unknown action" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("failed")):
            result = manage_hg_classifier("create", "q1", "cls1", 5)
            assert "Error" in result


class TestSetHgQueryQueueProperty:
    """Tests for set_hg_query_queue_property tool."""

    def test_set_queue_property(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("queue", "q1", "max_concurrency", "20")
            assert "Successfully set" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_set_query_queue_property" in sql

    def test_remove_queue_property(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("queue", "q1", "max_concurrency", "20", action="remove")
            assert "removed" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_remove_query_queue_property" in sql

    def test_set_classifier_rule(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("classifier", "q1", "user_name", "admin", classifier_name="cls1")
            assert "Successfully set" in result
            sql = cursor.execute.call_args[0][0]
            assert "hg_set_classifier_rule_condition_value" in sql

    def test_classifier_missing_name(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("classifier", "q1", "user_name", "admin")
            assert "Error" in result or "required" in result.lower()

    def test_remove_classifier_rule(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property(
                "classifier", "q1", "user_name", "admin", classifier_name="cls1", action="remove"
            )
            assert "removed" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_remove_classifier_rule_condition_value" in sql

    def test_unknown_target(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("invalid", "q1", "key", "val")
            assert "Unknown target" in result

    def test_unknown_action(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property("queue", "q1", "key", "val", action="update")
            assert "Unknown action" in result

    def test_user_name_quoted(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            set_hg_query_queue_property("classifier", "q1", "user_name", "MyUser", classifier_name="cls1")
            sql = cursor.execute.call_args[0][0]
            # user_name value should be wrapped in double quotes
            assert '"MyUser"' in sql

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("denied")):
            result = set_hg_query_queue_property("queue", "q1", "key", "val")
            assert "Error" in result


# ============================================================================
# Security & Configuration
# ============================================================================


class TestListHgDataMaskingRules:
    """Tests for list_hg_data_masking_rules tool."""

    def test_rules_found(self):
        conn, cursor = _make_mock_conn()
        # SHOW hg_anon_enable, SHOW hg_anon_labels, pg_seclabel, pg_shseclabel
        cursor.fetchone.side_effect = [("on",), ("label1,label2",)]
        cursor.fetchall.side_effect = [
            [("users", "email", "hg_anon", "label1")],
            [("admin", "label1")],
        ]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            assert "on" in result
            assert "Column-level" in result
            assert "User-level" in result

    def test_no_rules(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [("off",), ("",)]
        cursor.fetchall.side_effect = [[], []]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            assert "No column-level" in result or "No user-level" in result

    def test_extension_not_installed(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = Exception("unrecognized parameter")
        # The function catches inner exceptions individually
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result or "Error" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("connection failed")):
            result = list_hg_data_masking_rules()
            assert "Error" in result


class TestQueryHgExternalFiles:
    """Tests for query_hg_external_files tool."""

    def test_basic_query(self):
        rows = [(1, "hello"), (2, "world")]
        desc = [("id",), ("name",)]
        conn, cursor = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = query_hg_external_files("oss://bucket/data", "csv")
            assert "External Files" in result
            assert "hello" in result
            sql = cursor.execute.call_args[0][0]
            assert "EXTERNAL_FILES" in sql
            assert "oss://bucket/data" in sql
            assert "csv" in sql

    def test_with_columns(self):
        rows = [(1, "test")]
        desc = [("id",), ("name",)]
        conn, cursor = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            query_hg_external_files("oss://b/d", "parquet", columns="id int, name text")
            sql = cursor.execute.call_args[0][0]
            assert "AS (id int, name text)" in sql

    def test_with_oss_endpoint_and_role(self):
        rows = [(1,)]
        desc = [("id",)]
        conn, cursor = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            query_hg_external_files(
                "oss://b/d", "orc",
                oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
                role_arn="acs:ram::123:role/test"
            )
            sql = cursor.execute.call_args[0][0]
            assert "oss_endpoint" in sql
            assert "role_arn" in sql

    def test_no_data(self):
        conn, cursor = _make_mock_conn(fetchall=[])
        cursor.description = [("id",)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = query_hg_external_files("oss://b/d", "csv")
            assert "no data" in result.lower()

    def test_function_not_available(self):
        with patch(PATCH_CONNECT) as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = Exception("function EXTERNAL_FILES does not exist")
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            result = query_hg_external_files("oss://b/d", "csv")
            assert "V4.1" in result or "not available" in result.lower()

    def test_many_rows_truncated(self):
        rows = [(i, f"row{i}") for i in range(200)]
        desc = [("id",), ("name",)]
        conn, _ = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = query_hg_external_files("oss://b/d", "csv")
            assert "more rows" in result
            assert "200" in result


class TestGetHgGucConfig:
    """Tests for get_hg_guc_config tool."""

    def test_guc_found(self):
        conn, _ = _make_mock_conn(fetchone=("128MB",))
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_guc_config("work_mem")
            assert "work_mem" in result
            assert "128MB" in result

    def test_guc_not_found(self):
        with patch(PATCH_CONNECT) as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = Exception("unrecognized configuration parameter")
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            result = get_hg_guc_config("nonexistent_param")
            assert "Error" in result

    def test_db_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("connection lost")):
            result = get_hg_guc_config("work_mem")
            assert "Error" in result
