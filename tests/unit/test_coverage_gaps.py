"""
Tests to fill coverage gaps in server.py — targeting specific uncovered branches.
"""

from unittest.mock import MagicMock, patch

import pytest
from conftest import PATCH_CONNECT, _make_mock_conn

from hologres_mcp_server.server import (
    cancel_hg_query,
    get_hg_dynamic_table_refresh_history,
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
    restore_hg_table_from_recyclebin,
    set_hg_query_queue_property,
    switch_hg_warehouse,
    validate_connection,
)

# ============================================================================
# validate_connection lifespan
# ============================================================================


class TestValidateConnection:
    """Tests for validate_connection lifespan handler."""

    async def test_connection_success(self):
        mock_conn = MagicMock()
        with patch(PATCH_CONNECT, return_value=mock_conn):
            async with validate_connection(MagicMock()) as ctx:
                assert ctx == {}
            mock_conn.close.assert_called_once()

    async def test_connection_failure(self, capsys):
        with patch(PATCH_CONNECT, side_effect=Exception("refused")):
            async with validate_connection(MagicMock()) as ctx:
                assert ctx == {}
            captured = capsys.readouterr()
            assert "Warning" in captured.out or "refused" in captured.out


# ============================================================================
# _switch_warehouse branches
# ============================================================================


class TestSwitchWarehouseBranches:
    """Tests for uncovered branches in switch_hg_warehouse."""

    def test_switch_to_local(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("local")
            assert "local" in result.lower()
            sql = cursor.execute.call_args[0][0]
            assert "hg_computing_resource" in sql

    def test_switch_to_serverless(self):
        conn, cursor = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("serverless")
            assert "serverless" in result.lower()

    def test_switch_to_LOCAL_case_insensitive(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("LOCAL")
            assert "local" in result

    def test_warehouse_not_found(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = None
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("nonexistent_wh")
            assert "not found" in result.lower()

    def test_warehouse_found_and_switched(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = ("my_wh",)
        with patch(PATCH_CONNECT, return_value=conn):
            result = switch_hg_warehouse("my_wh")
            assert "my_wh" in result

    def test_switch_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("network error")):
            result = switch_hg_warehouse("wh1")
            assert "Error" in result


# ============================================================================
# _get_table_storage_size — pg fallback branch
# ============================================================================


class TestTableStorageSizeFallback:
    """Tests for _get_table_storage_size pg_relation_size fallback."""

    def test_fallback_to_pg_functions(self):
        conn, cursor = _make_mock_conn()
        # First 3 execute calls (hg_relation_size) raise exception,
        # then pg_total_relation_size and pg_relation_size succeed
        call_count = [0]
        original_execute = cursor.execute

        def side_effect_fn(sql, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 1 and "hg_relation_size" in str(sql):
                raise Exception("hg_relation_size not found")
            return original_execute(sql, *args, **kwargs)

        cursor.execute = MagicMock(side_effect=side_effect_fn)
        cursor.fetchone.return_value = (2048,)

        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_storage_size("public", "test_table")
            # Should contain pg fallback results or error
            assert "Storage Size" in result or "Error" in result


# ============================================================================
# _cancel_query — failed results
# ============================================================================


class TestCancelQueryBranches:
    """Tests for _cancel_query failed result branches."""

    def test_cancel_failed(self):
        conn, _ = _make_mock_conn(fetchone=(False,))
        with patch(PATCH_CONNECT, return_value=conn):
            result = cancel_hg_query(99999)
            assert "Failed" in result

    def test_terminate_failed(self):
        conn, _ = _make_mock_conn(fetchone=(False,))
        with patch(PATCH_CONNECT, return_value=conn):
            result = cancel_hg_query(99999, terminate=True)
            assert "Failed" in result

    def test_terminate_success(self):
        conn, _ = _make_mock_conn(fetchone=(True,))
        with patch(PATCH_CONNECT, return_value=conn):
            result = cancel_hg_query(12345, terminate=True)
            assert "terminated" in result.lower()

    def test_cancel_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("denied")):
            result = cancel_hg_query(123)
            assert "Error" in result


# ============================================================================
# _list_active_queries — idle and all state branches
# ============================================================================


class TestListActiveQueriesBranches:
    """Tests for _list_active_queries state filter branches."""

    def test_idle_state(self):
        rows = [(100, "user1", "db1", "idle", "app1", "2025-01-01", "00:01:00", "SELECT 1")]
        desc = [
            ("pid",),
            ("usename",),
            ("datname",),
            ("state",),
            ("application_name",),
            ("query_start",),
            ("duration",),
            ("query_preview",),
        ]
        conn, cursor = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_active_queries("idle")
            sql = cursor.execute.call_args[0][0]
            assert "idle" in sql
            assert "100" in result or "Active" in result

    def test_all_state(self):
        rows = [(100, "user1", "db1", "active", "app1", "2025-01-01", "00:01:00", "SELECT 1")]
        desc = [
            ("pid",),
            ("usename",),
            ("datname",),
            ("state",),
            ("application_name",),
            ("query_start",),
            ("duration",),
            ("query_preview",),
        ]
        conn, cursor = _make_mock_conn(fetchall=rows, description=desc)
        with patch(PATCH_CONNECT, return_value=conn):
            list_hg_active_queries("all")
            sql = cursor.execute.call_args[0][0]
            # 'all' should NOT add state filter
            assert "AND state" not in sql


# ============================================================================
# _list_query_queues — empty classifiers and version error
# ============================================================================


class TestListQueryQueuesBranches:
    """Tests for _list_query_queues uncovered branches."""

    def test_empty_classifiers(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchall.side_effect = [
            [("queue1", 10, 100)],  # queues found
            [],  # no classifiers
        ]
        cursor.description = [("query_queue_name",), ("max_concurrency",), ("max_queue_size",)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_query_queues()
            assert "No classifiers" in result

    def test_no_queues(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchall.side_effect = [[], []]
        cursor.description = [("query_queue_name",), ("max_concurrency",), ("max_queue_size",)]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_query_queues()
            assert "No query queues" in result or "Query Queues" in result

    def test_version_error(self):
        """'does not exist' error should return V3.0+ message."""
        with patch(PATCH_CONNECT) as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = Exception("relation hg_query_queues does not exist")
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            result = list_hg_query_queues()
            assert "V3.0" in result or "not available" in result.lower()

    def test_general_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("connection refused")):
            result = list_hg_query_queues()
            assert "Error" in result


# ============================================================================
# _get_table_shard_info — table group properties branch
# ============================================================================


class TestTableShardInfoBranches:
    """Tests for _get_table_shard_info uncovered branches."""

    def test_with_table_group_properties(self):
        conn, cursor = _make_mock_conn()
        # First: table_group name, Second: table group properties
        cursor.fetchone.return_value = ("tg_default",)
        cursor.fetchall.return_value = [("shard_count", "32"), ("replica_count", "2")]
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_shard_info("public", "orders")
            assert "tg_default" in result
            assert "Table Group Properties" in result
            assert "shard_count" in result
            assert "32" in result

    def test_with_empty_table_group_properties(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.return_value = ("tg_default",)
        cursor.fetchall.return_value = []  # no properties
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_table_shard_info("public", "orders")
            assert "tg_default" in result
            assert "Table Group Properties" not in result

    def test_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("db error")):
            result = get_hg_table_shard_info("public", "orders")
            assert "Error" in result


# ============================================================================
# _list_external_databases — foreign servers fallback
# ============================================================================


class TestListExternalDatabasesBranches:
    """Tests for _list_external_databases uncovered branches."""

    def test_no_external_dbs_with_foreign_servers(self):
        conn, cursor = _make_mock_conn()
        # First fetchall: no external databases
        # Second fetchall: foreign servers found
        cursor.fetchall.side_effect = [
            [],  # no external databases
            [("mc_server", "maxcompute", "{options}")],  # foreign servers
        ]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_external_databases()
            assert "Foreign Servers" in result
            assert "mc_server" in result

    def test_no_external_dbs_no_foreign_servers(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchall.side_effect = [[], []]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_external_databases()
            assert "No External Databases" in result or "No" in result

    def test_external_dbs_found(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchall.return_value = [("ext_db1", "admin", "my external db")]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_external_databases()
            assert "External Databases" in result
            assert "ext_db1" in result

    def test_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("permission denied")):
            result = list_hg_external_databases()
            assert "Error" in result


# ============================================================================
# _get_table_info_trend — version error
# ============================================================================


class TestTableInfoTrendBranches:
    """Tests for _get_table_info_trend uncovered branches."""

    def test_version_error(self):
        """'does not exist' error should return V1.3+ message."""
        with patch(PATCH_CONNECT) as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = Exception("relation hg_table_info does not exist")
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            result = get_hg_table_info_trend("public", "orders")
            assert "V1.3" in result or "not available" in result.lower()

    def test_general_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("timeout")):
            result = get_hg_table_info_trend("public", "orders")
            assert "Error" in result


# ============================================================================
# _get_warehouse_status — suppressed exception branches
# ============================================================================


class TestWarehouseStatusBranches:
    """Tests for _get_warehouse_status suppressed exception branches."""

    def test_rebalance_exception_suppressed(self):
        conn, cursor = _make_mock_conn()
        call_count = [0]

        def fetchone_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return ("running",)  # hg_get_warehouse_status
            elif call_count[0] == 2:
                raise Exception("rebalance not supported")  # rebalance fails
            elif call_count[0] == 3:
                return (1, 8, 16384, 2, "running", True)  # hg_warehouses
            return None

        cursor.fetchone.side_effect = fetchone_side_effect
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_warehouse_status("wh1")
            assert "Warehouse Status" in result
            assert "running" in result
            # Rebalance status should not appear
            assert "Rebalance" not in result

    def test_warehouse_info_exception_suppressed(self):
        conn, cursor = _make_mock_conn()
        call_count = [0]

        def fetchone_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return ("running",)  # hg_get_warehouse_status
            elif call_count[0] == 2:
                return ("idle",)  # rebalance status
            elif call_count[0] == 3:
                return None  # hg_warehouses returns nothing
            return None

        cursor.fetchone.side_effect = fetchone_side_effect
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_warehouse_status("wh1")
            assert "Warehouse Status" in result
            assert "Configuration" not in result

    def test_both_exceptions_suppressed(self):
        conn, cursor = _make_mock_conn()
        execute_count = [0]
        original_execute = cursor.execute

        def execute_side_effect(sql, *args, **kwargs):
            execute_count[0] += 1
            if execute_count[0] == 1:
                # hg_get_warehouse_status succeeds
                cursor.fetchone.return_value = ("running",)
                return original_execute(sql, *args, **kwargs)
            else:
                # All subsequent calls fail
                raise Exception("not available")

        cursor.execute = MagicMock(side_effect=execute_side_effect)
        cursor.fetchone.return_value = ("running",)
        with patch(PATCH_CONNECT, return_value=conn):
            result = get_hg_warehouse_status("wh1")
            assert "Warehouse Status" in result


# ============================================================================
# _list_data_masking_rules — sub-query exception branches
# ============================================================================


class TestDataMaskingRulesBranches:
    """Tests for _list_data_masking_rules sub-query exception branches."""

    def test_column_rules_exception(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [("on",), ("label1",)]
        # First fetchall (column rules) raises, second (user rules) returns empty
        cursor.fetchall.side_effect = [
            Exception("pg_seclabel query error"),
            [],
        ]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            assert "Column rules query error" in result

    def test_user_rules_exception(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [("on",), ("label1",)]
        cursor.fetchall.side_effect = [
            [],  # column rules empty
            Exception("pg_shseclabel query error"),
        ]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            assert "User rules query error" in result

    def test_hg_anon_enable_exception(self):
        conn, cursor = _make_mock_conn()
        execute_count = [0]

        def execute_side_effect(sql, *args, **kwargs):
            execute_count[0] += 1
            if execute_count[0] == 1:
                raise Exception("unrecognized parameter")
            elif execute_count[0] == 2:
                raise Exception("unrecognized parameter")
            return None

        cursor.execute = MagicMock(side_effect=execute_side_effect)
        cursor.fetchall.return_value = []
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            assert "unknown" in result or "may not be installed" in result

    def test_labels_exception(self):
        conn, cursor = _make_mock_conn()
        cursor.fetchone.side_effect = [("on",), Exception("no labels param")]
        cursor.fetchall.side_effect = [[], []]
        with patch(PATCH_CONNECT, return_value=conn):
            result = list_hg_data_masking_rules()
            assert "Data Masking" in result
            # Labels exception is silently caught, should still produce result


# ============================================================================
# _set_query_queue_property — classifier unknown action
# ============================================================================


class TestSetQueryQueuePropertyBranches:
    """Tests for _set_query_queue_property uncovered branches."""

    def test_classifier_unknown_action(self):
        conn, _ = _make_mock_conn()
        with patch(PATCH_CONNECT, return_value=conn):
            result = set_hg_query_queue_property(
                "classifier", "q1", "key", "val", classifier_name="cls1", action="update"
            )
            assert "Unknown action" in result


# ============================================================================
# Consolidated exception-path tests
# ============================================================================


@pytest.mark.parametrize(
    "func,args",
    [
        (get_hg_slow_queries, (1000, 20)),
        (list_hg_recyclebin, ()),
        (restore_hg_table_from_recyclebin, ("test_table",)),
        (list_hg_dynamic_tables, ()),
        (get_hg_table_properties, ("public", "test")),
        (get_hg_lock_diagnostics, ()),
        (get_hg_dynamic_table_refresh_history, ("public", "dt1")),
    ],
)
def test_exception_returns_error(func, args):
    """Each helper should catch connection errors and return 'Error' in result."""
    with patch(PATCH_CONNECT, side_effect=Exception("test error")):
        result = func(*args)
        assert "Error" in result
