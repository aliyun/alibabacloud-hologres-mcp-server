"""
Tests for resource functions in server module.

Resources (11 total):
- list_schemas() -> hologres:///schemas
- list_tables_in_schema(schema) -> hologres:///{schema}/tables
- get_table_ddl(schema, table) -> hologres:///{schema}/{table}/ddl
- get_table_statistics(schema, table) -> hologres:///{schema}/{table}/statistic
- get_table_partitions(schema, table) -> hologres:///{schema}/{table}/partitions
- get_hg_instance_version() -> system:///hg_instance_version
- get_missing_stats_tables() -> system:///missing_stats_tables
- get_stat_activity() -> system:///stat_activity
- get_guc_value(guc_name) -> system:///guc_value/{guc_name}
- get_query_log_latest(row_limits) -> system:///query_log/latest/{row_limits}
- get_query_log_user(user_name, row_limits) -> system:///query_log/user/{user_name}/{row_limits}
- get_query_log_application(application_name, row_limits) -> system:///query_log/application/{application_name}/{row_limits}
- get_query_log_failed(interval, row_limits) -> system:///query_log/failed/{interval}/{row_limits}
"""

import pytest
from unittest.mock import patch, MagicMock

from hologres_mcp_server.server import (
    list_schemas,
    list_tables_in_schema,
    get_table_ddl,
    get_table_statistics,
    get_table_partitions,
    get_hg_instance_version,
    get_missing_stats_tables,
    get_stat_activity,
    get_guc_value,
    get_query_log_latest,
    get_query_log_user,
    get_query_log_application,
    get_query_log_failed,
)


class TestListSchemas:
    """Tests for list_schemas resource."""

    def test_basic_functionality(self):
        """Test schema listing returns formatted string."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("public",), ("analytics",)]):
            result = list_schemas()

            assert result == "public\nanalytics"

    def test_excludes_system_schemas(self):
        """Test query excludes system schemas."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("public",)]) as mock:
            list_schemas()

            query = mock.call_args[0][1]
            assert "pg_catalog" in query
            assert "information_schema" in query
            assert "hologres_statistic" in query

    def test_empty_result(self):
        """Test empty result handling."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
            result = list_schemas()

            assert result == ""

    def test_single_schema(self):
        """Test single schema result."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("public",)]):
            result = list_schemas()

            assert result == "public"


class TestListTablesInSchema:
    """Tests for list_tables_in_schema resource."""

    def test_basic_functionality(self):
        """Test table listing returns formatted string."""
        mock_result = [
            ("users", ""),
            ("orders", " (view)"),
            ("external_data", " (foreign table)"),
        ]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = list_tables_in_schema("public")

            # Each table should be quoted
            assert '"users"' in result
            assert '"orders" (view)' in result
            assert '"external_data" (foreign table)' in result

    def test_schema_parameter_in_query(self):
        """Test schema parameter is included in query."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]) as mock:
            list_tables_in_schema("analytics")

            query = mock.call_args[0][1]
            assert "analytics" in query

    def test_empty_result(self):
        """Test empty result handling."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
            result = list_tables_in_schema("empty_schema")

            assert result == ""

    def test_special_characters_in_table_name(self):
        """Test table names with special characters are quoted properly."""
        mock_result = [("table-with-dash", "")]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = list_tables_in_schema("public")

            assert '"table-with-dash"' in result

    def test_quotes_escaped(self):
        """Test quotes in table names are escaped."""
        mock_result = [('table"with"quotes', "")]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = list_tables_in_schema("public")

            # Quotes should be doubled for escaping
            assert '""' in result


class TestGetTableDdl:
    """Tests for get_table_ddl resource."""

    def test_basic_functionality(self):
        """Test DDL retrieval."""
        ddl = "CREATE TABLE public.users (id INT PRIMARY KEY);"
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[(ddl,)]):
            result = get_table_ddl("public", "users")

            assert ddl in result

    def test_view_ddl_handling(self):
        """Test VIEW DDL special handling."""
        view_ddl = "CREATE VIEW my_view AS SELECT * FROM t\n\nEND; Type: VIEW"
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[(view_ddl,)]):
            with patch("hologres_mcp_server.server.try_infer_view_comments", return_value="-- No comments"):
                result = get_table_ddl("public", "my_view")

                assert "CREATE VIEW" in result

    def test_view_ddl_with_comments(self):
        """Test VIEW DDL with inferred comments."""
        view_ddl = "CREATE VIEW my_view AS SELECT id, name FROM users\n\nEND; Type: VIEW"
        inferred_comments = "-- Inferred comments\nCOMMENT ON COLUMN public.my_view.id IS 'User ID';"
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[(view_ddl,)]):
            with patch("hologres_mcp_server.server.try_infer_view_comments", return_value=inferred_comments):
                result = get_table_ddl("public", "my_view")

                assert "CREATE VIEW" in result
                assert "COMMENT ON COLUMN" in result

    def test_not_found(self):
        """Test DDL not found handling."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
            result = get_table_ddl("public", "nonexistent")

            assert "No DDL found" in result

    def test_empty_ddl(self):
        """Test empty DDL handling."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("",)]):
            result = get_table_ddl("public", "empty_table")

            # Empty DDL should either return empty or indicate not found
            assert "No DDL found" in result or result == ""

    def test_schema_table_parameters(self):
        """Test schema and table are used in query."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("DDL",)]) as mock:
            get_table_ddl("my_schema", "my_table")

            query = mock.call_args[0][1]
            assert "my_schema" in query
            assert "my_table" in query


class TestGetTableStatistics:
    """Tests for get_table_statistics resource."""

    def test_basic_functionality(self):
        """Test statistics retrieval."""
        mock_result = [
            ("public", "users", 1, 1, 1000, "2024-01-01 00:00:00"),
        ]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = get_table_statistics("public", "users")

            assert "Schema" in result  # Header
            assert "public" in result
            assert "users" in result

    def test_not_found(self):
        """Test statistics not found handling."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
            result = get_table_statistics("public", "nonexistent")

            assert "No statistics found" in result

    def test_headers_included(self):
        """Test headers are included in output."""
        mock_result = [
            ("public", "users", 1, 1, 1000, "2024-01-01"),
        ]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = get_table_statistics("public", "users")

            # Check for header row
            assert "Schema" in result
            assert "Table" in result
            assert "Total Rows" in result

    def test_multiple_rows(self):
        """Test multiple statistics rows."""
        mock_result = [
            ("public", "users", 1, 1, 1000, "2024-01-01"),
            ("public", "users", 1, 2, 2000, "2024-01-02"),
        ]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = get_table_statistics("public", "users")

            # Should have header + 2 data rows
            lines = result.split("\n")
            assert len(lines) == 3


class TestGetTablePartitions:
    """Tests for get_table_partitions resource."""

    def test_basic_functionality(self):
        """Test partition listing."""
        mock_result = [
            ("users_2023_01",),
            ("users_2023_02",),
        ]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=mock_result):
            result = get_table_partitions("public", "users")

            assert "users_2023_01" in result
            assert "users_2023_02" in result

    def test_empty_result(self):
        """Test empty partition list."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
            result = get_table_partitions("public", "non_partitioned")

            assert result == ""

    def test_schema_table_in_query(self):
        """Test schema and table are used in query."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]) as mock:
            get_table_partitions("my_schema", "my_table")

            query = mock.call_args[0][1]
            assert "my_schema" in query
            assert "my_table" in query


class TestGetHgInstanceVersion:
    """Tests for get_hg_instance_version resource."""

    def test_basic_functionality(self):
        """Test Hologres instance version retrieval."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("Hologres 2.1.0",)]):
            result = get_hg_instance_version()

            assert "2.1.0" in result


class TestGetMissingStatsTables:
    """Tests for get_missing_stats_tables resource."""

    def test_empty_result(self):
        """Test missing stats tables when none exist."""
        mock_result = []
        mock_headers = ["schemaname", "tablename"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_missing_stats_tables()

            assert "No tables found" in result

    def test_with_data(self):
        """Test missing stats tables with results."""
        mock_result = [
            ("public", "users", None, None),
            ("analytics", "events", None, None),
        ]
        mock_headers = ["schemaname", "tablename", "column", "reason"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_missing_stats_tables()

            assert "public" in result
            assert "users" in result
            assert "analytics" in result


class TestGetStatActivity:
    """Tests for get_stat_activity resource."""

    def test_empty_result(self):
        """Test stat activity when no queries running."""
        mock_result = []
        mock_headers = ["pid", "query", "state"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_stat_activity()

            assert "No queries found" in result

    def test_with_data(self):
        """Test stat activity with running queries."""
        mock_result = [
            (1234, "SELECT 1", "active", None),
            (1235, "INSERT INTO t VALUES (1)", "idle", None),
        ]
        mock_headers = ["pid", "query", "state", "time"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_stat_activity()

            assert "1234" in result
            assert "SELECT 1" in result


class TestGetGucValue:
    """Tests for get_guc_value resource."""

    def test_basic_functionality(self):
        """Test GUC value retrieval."""
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("auto",)]):
            result = get_guc_value("hg_enable_scale_out")

            assert "hg_enable_scale_out" in result
            assert "auto" in result

    def test_empty_name(self):
        """Test GUC with empty name."""
        result = get_guc_value("")

        assert "cannot be empty" in result


class TestGetQueryLogLatest:
    """Tests for get_query_log_latest resource."""

    def test_basic_functionality(self):
        """Test latest query log retrieval."""
        mock_result = [
            (1, "SELECT 1", "SUCCESS", None),
            (2, "SELECT 2", "SUCCESS", None),
        ]
        mock_headers = ["id", "query", "status", "error"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_query_log_latest("2")

            assert "SELECT 1" in result

    def test_invalid_limit(self):
        """Test query log with invalid limit."""
        result = get_query_log_latest("abc")

        assert "Invalid row limits" in result

    def test_zero_limit(self):
        """Test query log with zero limit."""
        result = get_query_log_latest("0")

        assert "must be a positive integer" in result

    def test_negative_limit(self):
        """Test query log with negative limit."""
        result = get_query_log_latest("-1")

        assert "must be a positive integer" in result


class TestGetQueryLogUser:
    """Tests for get_query_log_user resource."""

    def test_basic_functionality(self):
        """Test query log by user."""
        mock_result = [
            (1, "SELECT 1", "test_user", None),
        ]
        mock_headers = ["id", "query", "user", "time"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_query_log_user("test_user", "10")

            assert "SELECT 1" in result

    def test_empty_username(self):
        """Test query log with empty username."""
        result = get_query_log_user("", "10")

        assert "cannot be empty" in result


class TestGetQueryLogApplication:
    """Tests for get_query_log_application resource."""

    def test_basic_functionality(self):
        """Test query log by application."""
        mock_result = [
            (1, "SELECT 1", "my_app", None),
        ]
        mock_headers = ["id", "query", "app", "time"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_query_log_application("my_app", "10")

            assert "SELECT 1" in result

    def test_empty_application_name(self):
        """Test query log with empty application name."""
        result = get_query_log_application("", "10")

        assert "cannot be empty" in result


class TestGetQueryLogFailed:
    """Tests for get_query_log_failed resource."""

    def test_basic_functionality(self):
        """Test failed query log."""
        mock_result = [
            (1, "SELECT bad", "FAILED", "syntax error"),
        ]
        mock_headers = ["id", "query", "status", "error"]
        with patch("hologres_mcp_server.server.handle_read_resource", return_value=(mock_result, mock_headers)):
            result = get_query_log_failed("1 day", "10")

            assert "SELECT bad" in result

    def test_empty_interval(self):
        """Test failed query log with empty interval."""
        result = get_query_log_failed("", "10")

        assert "cannot be empty" in result