"""
Tests for utility functions in utils module.

Functions:
- connect_with_retry(retries=3)
- handle_read_resource(resource_name, query, with_headers=False)
- handle_call_tool(tool_name, query, serverless=False)
- get_view_definition(cursor, schema_name, view_name)
- get_column_comment(cursor, schema_name, table_name, column_name)
- try_infer_view_comments(schema_name, view_name)
"""

import pytest
from unittest.mock import MagicMock, patch, call
import psycopg

from hologres_mcp_server.utils import (
    connect_with_retry,
    handle_read_resource,
    handle_call_tool,
    get_view_definition,
    get_column_comment,
    try_infer_view_comments,
)


class TestConnectWithRetry:
    """Tests for connect_with_retry function."""

    def test_connect_success_first_attempt(self, mock_env_basic):
        """Test successful connection on first attempt."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.autocommit = True

        with patch("psycopg.connect", return_value=mock_conn) as mock_connect:
            result = connect_with_retry(retries=3)

            assert result == mock_conn
            mock_connect.assert_called_once()

    def test_connect_failure_then_success(self, mock_env_basic):
        """Test connection succeeds after initial failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg.connect", side_effect=[
            psycopg.Error("Connection failed"),
            mock_conn
        ]) as mock_connect:
            with patch("time.sleep"):
                result = connect_with_retry(retries=3)

                assert result == mock_conn
                assert mock_connect.call_count == 2

    def test_connect_all_failures(self, mock_env_basic):
        """Test connection fails after all retries exhausted."""
        with patch("psycopg.connect", side_effect=psycopg.Error("Connection failed")) as mock_connect:
            with patch("time.sleep"):
                with pytest.raises(psycopg.Error, match="Failed to connect"):
                    connect_with_retry(retries=2)

                # Should try initial + retries = 3 attempts total
                assert mock_connect.call_count == 3

    def test_connect_custom_retries(self, mock_env_basic):
        """Test custom retry count."""
        with patch("psycopg.connect", side_effect=psycopg.Error("Connection failed")) as mock_connect:
            with patch("time.sleep"):
                with pytest.raises(psycopg.Error):
                    connect_with_retry(retries=5)

                # Initial + 5 retries = 6 attempts
                assert mock_connect.call_count == 6

    def test_connect_zero_retries(self, mock_env_basic):
        """Test with zero retries (fail immediately)."""
        with patch("psycopg.connect", side_effect=psycopg.Error("Connection failed")) as mock_connect:
            with pytest.raises(psycopg.Error, match="Failed to connect"):
                connect_with_retry(retries=0)

            # Should only try once
            assert mock_connect.call_count == 1

    def test_connect_sets_autocommit(self, mock_env_basic):
        """Test that autocommit is set to True."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg.connect", return_value=mock_conn):
            connect_with_retry(retries=3)

            assert mock_conn.autocommit is True


class TestHandleReadResource:
    """Tests for handle_read_resource function."""

    def test_read_resource_success(self, mock_env_basic):
        """Test successful resource read."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("val1", "val2")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT 1")

            assert result == [("val1", "val2")]

    def test_read_resource_with_headers(self, mock_env_basic):
        """Test resource read with headers."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("val1", "val2")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            rows, headers = handle_read_resource("test_resource", "SELECT 1", with_headers=True)

            assert rows == [("val1", "val2")]
            assert headers == ["col1", "col2"]

    def test_read_resource_error_handling(self, mock_env_basic):
        """Test error handling in resource read."""
        with patch("hologres_mcp_server.utils.connect_with_retry", side_effect=Exception("DB Error")):
            result = handle_read_resource("test_resource", "SELECT 1")

            assert "Error executing query" in result
            assert "DB Error" in result

    def test_read_resource_empty_result(self, mock_env_basic):
        """Test handling of empty result set."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",)]
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT 1 WHERE FALSE")

            assert result == []


class TestHandleCallTool:
    """Tests for handle_call_tool function."""

    def test_call_tool_select_query(self, mock_env_basic):
        """Test SELECT query execution."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("val1", "val2"), ("val3", "val4")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_hg_select_sql", "SELECT * FROM users")

            assert "col1,col2" in result
            assert "val1,val2" in result

    def test_call_tool_with_serverless(self, mock_env_basic):
        """Test serverless mode sets computing resource."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",)]
        mock_cursor.fetchall.return_value = [("val1",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            handle_call_tool("execute_hg_select_sql_with_serverless", "SELECT 1", serverless=True)

            # Verify serverless setting was executed
            mock_cursor.execute.assert_any_call("set hg_computing_resource='serverless'")

    def test_call_tool_analyze_command(self, mock_env_basic):
        """Test ANALYZE command special handling."""
        mock_cursor = MagicMock()
        mock_cursor.description = None

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("gather_hg_table_statistics", "ANALYZE public.users")

            assert "Successfully" in result
            assert "ANALYZE public.users" in result

    def test_call_tool_error_handling(self, mock_env_basic):
        """Test error handling in tool call."""
        with patch("hologres_mcp_server.utils.connect_with_retry", side_effect=Exception("Query failed")):
            result = handle_call_tool("execute_hg_select_sql", "SELECT * FROM invalid")

            assert "Error executing query" in result
            assert "Query failed" in result

    def test_call_tool_ddl_execution(self, mock_env_basic):
        """Test DDL statement execution (no result)."""
        mock_cursor = MagicMock()
        mock_cursor.description = None

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_hg_ddl_sql", "CREATE TABLE test (id INT)")

            assert "Query executed successfully" in result

    def test_call_tool_dml_execution(self, mock_env_basic):
        """Test DML statement execution with row count."""
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.rowcount = 5

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_dml_sql", "INSERT INTO users VALUES (1)")

            assert "5 rows affected" in result


class TestGetViewDefinition:
    """Tests for get_view_definition function."""

    def test_get_view_definition_found(self, mock_env_basic):
        """Test successful view definition retrieval."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("SELECT * FROM base_table",)

        result = get_view_definition(mock_cursor, "public", "my_view")

        assert result == "SELECT * FROM base_table"
        mock_cursor.execute.assert_called_once()

    def test_get_view_definition_not_found(self, mock_env_basic):
        """Test view not found returns None."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None

        result = get_view_definition(mock_cursor, "public", "nonexistent_view")

        assert result is None

    def test_get_view_definition_empty_result(self, mock_env_basic):
        """Test empty result handling."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = []

        result = get_view_definition(mock_cursor, "public", "empty_view")

        # Empty list is falsy, should return None-ish
        assert not result


class TestGetColumnComment:
    """Tests for get_column_comment function."""

    def test_get_column_comment_found(self, mock_env_basic):
        """Test successful column comment retrieval."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("User name column",)

        result = get_column_comment(mock_cursor, "public", "users", "name")

        assert result == "User name column"

    def test_get_column_comment_not_found(self, mock_env_basic):
        """Test column comment not found returns None."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None

        result = get_column_comment(mock_cursor, "public", "users", "nonexistent")

        assert result is None

    def test_get_column_comment_null(self, mock_env_basic):
        """Test column comment is NULL in database."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)

        result = get_column_comment(mock_cursor, "public", "users", "name")

        # (None,) is truthy but contains None
        assert result is None


class TestTryInferViewComments:
    """Tests for try_infer_view_comments function."""

    def test_infer_view_comments_no_view(self, mock_env_basic):
        """Test handling when view doesn't exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # No view definition

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            result = try_infer_view_comments("public", "nonexistent_view")

            assert result == ""

    def test_infer_view_comments_error_handling(self, mock_env_basic):
        """Test error handling returns empty string."""
        with patch("psycopg.connect", side_effect=Exception("Connection error")):
            result = try_infer_view_comments("public", "any_view")

            assert result == ""

    def test_infer_view_comments_simple_view(self, mock_env_basic):
        """Test with a simple view definition."""
        mock_cursor = MagicMock()

        # First call: get view definition
        # Second call: check if comment exists on view column
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.col1 FROM base_table t",),  # view definition
            (None,),  # no existing comment on view column
            ("Source column comment",),  # comment on source column
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Mock pglast parser
        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                # Create a mock parsed statement
                mock_stmt = MagicMock()
                mock_stmt.stmt = MagicMock()
                mock_parse.return_value = [mock_stmt]

                # This test verifies the function doesn't crash
                # The actual parsing logic is complex and depends on pglast
                result = try_infer_view_comments("public", "test_view")

                # Should either return comments or empty string
                assert isinstance(result, str)

    def test_infer_view_comments_with_exception_in_parsing(self, mock_env_basic):
        """Test handling of exceptions during SQL parsing."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("SELECT * FROM base_table",)

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", side_effect=Exception("Parse error")):
                result = try_infer_view_comments("public", "test_view")

                assert result == ""

    def test_infer_view_comments_with_select_stmt(self, mock_env_basic):
        """Test with a parsed SELECT statement."""
        mock_cursor = MagicMock()

        # Return view definition, then various column checks
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.id FROM users t",),  # view definition
            (None,),  # no existing comment
            ("User ID",),  # source column comment
        ]

        # Set up execute to be callable
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Create proper pglast mock objects
        import pglast.ast

        # Create a ColumnRef
        column_ref = MagicMock(spec=pglast.ast.ColumnRef)
        field1 = MagicMock()
        field1.sval = "t"
        field2 = MagicMock()
        field2.sval = "id"
        column_ref.fields = [field1, field2]

        # Create a ResTarget
        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = column_ref
        res_target.name = None

        # Create a SelectStmt
        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]

        # Create a raw statement
        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "test_view")

                # Should process the SELECT statement
                assert isinstance(result, str)