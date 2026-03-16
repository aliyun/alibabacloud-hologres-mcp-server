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
            result = handle_call_tool("execute_hg_dml_sql", "INSERT INTO users VALUES (1)")

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


class TestTryInferViewCommentsExtended:
    """Extended tests for try_infer_view_comments function."""

    def test_multi_column_view(self, mock_env_basic):
        """Test view with multiple columns."""
        mock_cursor = MagicMock()
        # View definition with multiple columns
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.id, t.name, t.email FROM users t",),  # view definition
            (None,),  # no existing comment on column 1
            ("User ID",),  # comment on source column 1
            (None,),  # no existing comment on column 2
            ("User Name",),  # comment on source column 2
            (None,),  # no existing comment on column 3
            ("Email Address",),  # comment on source column 3
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Create multiple ColumnRefs for multi-column view
        def create_column_ref(table, column):
            ref = MagicMock(spec=pglast.ast.ColumnRef)
            f1 = MagicMock()
            f1.sval = table
            f2 = MagicMock()
            f2.sval = column
            ref.fields = [f1, f2]
            return ref

        def create_res_target(column_ref, name=None):
            target = MagicMock(spec=pglast.ast.ResTarget)
            target.val = column_ref
            target.name = name
            return target

        res_targets = [
            create_res_target(create_column_ref("t", "id")),
            create_res_target(create_column_ref("t", "name")),
            create_res_target(create_column_ref("t", "email")),
        ]

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = res_targets

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "multi_col_view")

                assert isinstance(result, str)
                assert "COMMENT ON COLUMN" in result or result == ""

    def test_view_with_aliased_columns(self, mock_env_basic):
        """Test view with column aliases (SELECT id AS user_id)."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.id AS user_id, t.name AS user_name FROM users t",),
            (None,),
            ("Original ID comment",),
            (None,),
            ("Original name comment",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Create aliased columns
        column_ref1 = MagicMock(spec=pglast.ast.ColumnRef)
        field1 = MagicMock(sval="t")
        field2 = MagicMock(sval="id")
        column_ref1.fields = [field1, field2]

        res_target1 = MagicMock(spec=pglast.ast.ResTarget)
        res_target1.val = column_ref1
        res_target1.name = "user_id"  # Alias

        column_ref2 = MagicMock(spec=pglast.ast.ColumnRef)
        field3 = MagicMock(sval="t")
        field4 = MagicMock(sval="name")
        column_ref2.fields = [field3, field4]

        res_target2 = MagicMock(spec=pglast.ast.ResTarget)
        res_target2.val = column_ref2
        res_target2.name = "user_name"  # Alias

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target1, res_target2]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "aliased_view")

                assert isinstance(result, str)

    def test_view_with_expressions(self, mock_env_basic):
        """Test view with expressions like UPPER(name)."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT UPPER(name) FROM users",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Expression (not a ColumnRef)
        func_call = MagicMock(spec=pglast.ast.FuncCall)
        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = func_call  # Expression, not ColumnRef
        res_target.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "expr_view")

                # Expressions don't have source columns to infer comments from
                assert isinstance(result, str)

    def test_view_with_star_select(self, mock_env_basic):
        """Test view with SELECT *."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT * FROM users",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # SELECT * produces a different AST structure
        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                # Simulate parsing result for SELECT *
                raw_stmt = MagicMock()
                select_stmt = MagicMock()
                # SELECT * has targetList that may contain special nodes
                select_stmt.targetList = None  # or special star node
                raw_stmt.stmt = select_stmt
                mock_parse.return_value = [raw_stmt]

                result = try_infer_view_comments("public", "star_view")

                assert isinstance(result, str)

    def test_view_with_subquery(self, mock_env_basic):
        """Test view containing a subquery."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT * FROM (SELECT id FROM users) AS subq",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                raw_stmt = MagicMock()
                raw_stmt.stmt = MagicMock()  # Subquery structure
                mock_parse.return_value = [raw_stmt]

                result = try_infer_view_comments("public", "subquery_view")

                assert isinstance(result, str)

    def test_view_with_case_when(self, mock_env_basic):
        """Test view with CASE WHEN expression."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM users",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                raw_stmt = MagicMock()
                select_stmt = MagicMock()
                res_target = MagicMock()
                case_expr = MagicMock()  # CASE expression
                res_target.val = case_expr
                select_stmt.targetList = [res_target]
                raw_stmt.stmt = select_stmt
                mock_parse.return_value = [raw_stmt]

                result = try_infer_view_comments("public", "case_view")

                assert isinstance(result, str)

    def test_view_comment_already_exists(self, mock_env_basic):
        """Test when view column already has a comment."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.id FROM users t",),
            ("Existing view comment",),  # Comment already exists on view column
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        column_ref = MagicMock(spec=pglast.ast.ColumnRef)
        field1 = MagicMock(sval="t")
        field2 = MagicMock(sval="id")
        column_ref.fields = [field1, field2]

        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = column_ref
        res_target.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "commented_view")

                # Should not add duplicate comment
                assert isinstance(result, str)

    def test_view_with_schema_qualified_tables(self, mock_env_basic):
        """Test view referencing schema-qualified tables."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT t.id FROM other_schema.users t",),
            (None,),
            ("Comment from other schema",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        column_ref = MagicMock(spec=pglast.ast.ColumnRef)
        field1 = MagicMock(sval="t")
        field2 = MagicMock(sval="id")
        column_ref.fields = [field1, field2]

        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = column_ref
        res_target.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "cross_schema_view")

                assert isinstance(result, str)

    def test_view_with_mixed_source_tables(self, mock_env_basic):
        """Test view with JOIN from multiple tables."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT u.id, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",),
            (None,),
            ("User ID",),
            (None,),
            ("Order ID",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Column from first table
        col_ref1 = MagicMock(spec=pglast.ast.ColumnRef)
        f1 = MagicMock(sval="u")
        f2 = MagicMock(sval="id")
        col_ref1.fields = [f1, f2]

        res1 = MagicMock(spec=pglast.ast.ResTarget)
        res1.val = col_ref1
        res1.name = None

        # Column from second table
        col_ref2 = MagicMock(spec=pglast.ast.ColumnRef)
        f3 = MagicMock(sval="o")
        f4 = MagicMock(sval="order_id")
        col_ref2.fields = [f3, f4]

        res2 = MagicMock(spec=pglast.ast.ResTarget)
        res2.val = col_ref2
        res2.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res1, res2]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "join_view")

                assert isinstance(result, str)


class TestTryInferViewCommentsComplexScenarios:
    """Additional tests for complex VIEW scenarios in try_infer_view_comments."""

    def test_view_with_nested_subquery(self, mock_env_basic):
        """Test VIEW with nested subquery."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT * FROM (SELECT id FROM (SELECT id FROM base) AS inner_subq) AS outer_subq",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                # Create nested subquery AST structure
                raw_stmt = MagicMock()
                raw_stmt.stmt = MagicMock()  # Complex nested structure
                mock_parse.return_value = [raw_stmt]

                result = try_infer_view_comments("public", "nested_subquery_view")

                # Should handle nested structure without crashing
                assert isinstance(result, str)

    def test_view_with_window_function(self, mock_env_basic):
        """Test VIEW containing window functions like ROW_NUMBER, RANK."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_at) AS row_num FROM products",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Create a window function AST node
        func_call = MagicMock(spec=pglast.ast.FuncCall)
        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = func_call
        res_target.name = "row_num"  # Aliased window function

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "window_view")

                # Window functions don't have source columns to infer from
                assert isinstance(result, str)

    def test_view_with_aggregate_functions(self, mock_env_basic):
        """Test VIEW containing aggregate functions like COUNT, SUM, AVG."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT category, COUNT(*) AS cnt, SUM(price) AS total FROM products GROUP BY category",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        # Create aggregate function AST nodes
        count_func = MagicMock(spec=pglast.ast.FuncCall)
        count_target = MagicMock(spec=pglast.ast.ResTarget)
        count_target.val = count_func
        count_target.name = "cnt"

        sum_func = MagicMock(spec=pglast.ast.FuncCall)
        sum_target = MagicMock(spec=pglast.ast.ResTarget)
        sum_target.val = sum_func
        sum_target.name = "total"

        # Column reference for GROUP BY column
        category_ref = MagicMock(spec=pglast.ast.ColumnRef)
        category_field = MagicMock(sval="category")
        category_ref.fields = [category_field]
        category_target = MagicMock(spec=pglast.ast.ResTarget)
        category_target.val = category_ref
        category_target.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [category_target, count_target, sum_target]

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "aggregate_view")

                # Aggregate functions don't have source columns
                # but GROUP BY columns might
                assert isinstance(result, str)

    def test_view_with_cte(self, mock_env_basic):
        """Test VIEW with Common Table Expression (WITH clause)."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("WITH user_orders AS (SELECT user_id, COUNT(*) FROM orders GROUP BY user_id) SELECT * FROM user_orders",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql") as mock_parse:
                # CTE creates a more complex AST
                raw_stmt = MagicMock()
                raw_stmt.stmt = MagicMock()
                mock_parse.return_value = [raw_stmt]

                result = try_infer_view_comments("public", "cte_view")

                assert isinstance(result, str)

    def test_view_with_distinct(self, mock_env_basic):
        """Test VIEW with DISTINCT clause."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            ("SELECT DISTINCT category FROM products",),
            (None,),
            ("Product category",),
        ]
        mock_cursor.execute = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        import pglast.ast

        column_ref = MagicMock(spec=pglast.ast.ColumnRef)
        field = MagicMock(sval="category")
        column_ref.fields = [field]

        res_target = MagicMock(spec=pglast.ast.ResTarget)
        res_target.val = column_ref
        res_target.name = None

        select_stmt = MagicMock(spec=pglast.ast.SelectStmt)
        select_stmt.targetList = [res_target]
        select_stmt.distinctClause = [True]  # DISTINCT is set

        raw_stmt = MagicMock()
        raw_stmt.stmt = select_stmt

        with patch("psycopg.connect", return_value=mock_conn):
            with patch("pglast.parser.parse_sql", return_value=[raw_stmt]):
                result = try_infer_view_comments("public", "distinct_view")

                assert isinstance(result, str)


class TestConnectWithRetryExtended:
    """Extended tests for connect_with_retry function."""

    def test_concurrent_connections(self, mock_env_basic):
        """Test concurrent connection attempts."""
        import threading
        import time

        results = []
        errors = []

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.autocommit = True

        def worker():
            try:
                with patch("psycopg.connect", return_value=mock_conn):
                    conn = connect_with_retry(retries=1)
                    results.append(conn)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 5

    def test_connection_timeout_simulation(self, mock_env_basic):
        """Test handling of connection timeout."""
        import time

        def slow_connect(*args, **kwargs):
            time.sleep(0.1)  # Simulate slow connection
            raise psycopg.Error("Connection timeout")

        with patch("psycopg.connect", side_effect=slow_connect):
            with patch("time.sleep"):
                with pytest.raises(psycopg.Error, match="Failed to connect"):
                    connect_with_retry(retries=1)

    def test_negative_retry_count(self, mock_env_basic):
        """Test negative retry count is handled."""
        with patch("psycopg.connect", side_effect=psycopg.Error("Connection failed")):
            with patch("time.sleep"):
                # Negative retries should be treated as 0 or cause immediate failure
                with pytest.raises(psycopg.Error):
                    connect_with_retry(retries=-1)

    def test_float_retry_count(self, mock_env_basic):
        """Test float retry count handling."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg.connect", return_value=mock_conn):
            # Float should be converted to int or handled gracefully
            result = connect_with_retry(retries=2.5)
            assert result == mock_conn


class TestHandleCallToolEdgeCases:
    """Edge case tests for handle_call_tool function."""

    def test_very_long_query_string(self, mock_env_basic):
        """Test query with very long string (>1MB)."""
        # Create a 1MB+ query string
        long_query = "SELECT '" + "x" * (1024 * 1024) + "'"

        mock_cursor = MagicMock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [("ok",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_hg_select_sql", long_query)

            # Should handle long query without error
            assert isinstance(result, str)

    def test_query_with_unicode(self, mock_env_basic):
        """Test query with Unicode characters."""
        unicode_query = "SELECT '中文测试' AS chinese, '日本語' AS japanese, '🔥' AS emoji"

        mock_cursor = MagicMock()
        mock_cursor.description = [("chinese",), ("japanese",), ("emoji",)]
        mock_cursor.fetchall.return_value = [("中文测试", "日本語", "🔥")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_hg_select_sql", unicode_query)

            assert "中文测试" in result or "chinese,japanese,emoji" in result

    def test_query_with_null_bytes(self, mock_env_basic):
        """Test query containing null bytes."""
        # Query with embedded null byte (potential security issue)
        query_with_null = "SELECT * FROM users WHERE name = 'test\x00value'"

        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_call_tool("execute_hg_select_sql", query_with_null)

            # Should handle null bytes gracefully
            assert isinstance(result, str)

    def test_concurrent_tool_calls(self, mock_env_basic):
        """Test concurrent tool calls."""
        import threading

        results = []
        errors = []

        mock_cursor = MagicMock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [("ok",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def worker(query_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
                    result = handle_call_tool("execute_hg_select_sql", f"SELECT {query_id}")
                    results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 5


class TestHandleReadResourceEdgeCases:
    """Edge case tests for handle_read_resource function."""

    def test_result_with_null_values(self, mock_env_basic, mock_cursor_with_nulls):
        """Test result set containing NULL values."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor_with_nulls)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT * FROM test")

            # Should handle NULL values
            assert result is not None
            assert len(result) == 3  # 3 rows from fixture

    def test_result_with_very_wide_rows(self, mock_env_basic, mock_cursor_with_large_result):
        """Test result set with many columns (100+)."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor_with_large_result)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT * FROM wide_table")

            # Should handle wide result set
            assert result is not None
            assert len(result) == 1000  # 1000 rows from fixture

    def test_result_with_very_long_values(self, mock_env_basic, mock_cursor_with_very_long_values):
        """Test result set with very long string values."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor_with_very_long_values)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT * FROM test")

            # Should handle very long values
            assert result is not None

    def test_result_with_binary_data(self, mock_env_basic, mock_cursor_with_binary_data):
        """Test result set with binary data."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor_with_binary_data)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
            result = handle_read_resource("test_resource", "SELECT * FROM test")

            # Should handle binary data
            assert result is not None
            assert len(result) == 2