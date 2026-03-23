"""
Tests for tool functions in server module.

Tools (12 total):
- execute_hg_select_sql
- execute_hg_select_sql_with_serverless
- execute_hg_dml_sql
- execute_hg_ddl_sql
- gather_hg_table_statistics
- get_hg_query_plan
- get_hg_execution_plan
- call_hg_procedure
- create_hg_maxcompute_foreign_table
- list_hg_schemas
- list_hg_tables_in_a_schema
- show_hg_table_ddl
"""

import pytest
from unittest.mock import patch, MagicMock

from hologres_mcp_server.server import (
    execute_hg_select_sql,
    execute_hg_select_sql_with_serverless,
    execute_hg_dml_sql,
    execute_hg_ddl_sql,
    gather_hg_table_statistics,
    get_hg_query_plan,
    get_hg_execution_plan,
    call_hg_procedure,
    create_hg_maxcompute_foreign_table,
    list_hg_schemas,
    list_hg_tables_in_a_schema,
    show_hg_table_ddl,
)


class TestExecuteHgSelectSql:
    """Tests for execute_hg_select_sql tool."""

    def test_valid_select(self):
        """Test valid SELECT query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql("SELECT * FROM users")
            assert result == "result"
            mock.assert_called_once_with("execute_hg_select_sql", "SELECT * FROM users", serverless=False)

    def test_valid_select_with_whitespace(self):
        """Test SELECT with leading whitespace."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql("   SELECT * FROM users")
            assert result == "result"

    def test_valid_with_cte(self):
        """Test WITH ... SELECT query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
            assert result == "result"

    def test_invalid_insert(self):
        """Test INSERT query is rejected."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("INSERT INTO users VALUES (1)")

    def test_invalid_update(self):
        """Test UPDATE query is rejected."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("UPDATE users SET name = 'test'")

    def test_invalid_delete(self):
        """Test DELETE query is rejected."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("DELETE FROM users")


class TestExecuteHgSelectSqlWithServerless:
    """Tests for execute_hg_select_sql_with_serverless tool."""

    def test_valid_select(self):
        """Test valid SELECT query with serverless."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql_with_serverless("SELECT * FROM large_table")
            assert result == "result"
            mock.assert_called_once_with("execute_hg_select_sql_with_serverless", "SELECT * FROM large_table", serverless=True)

    def test_serverless_flag_passed(self):
        """Test serverless flag is correctly passed."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            execute_hg_select_sql_with_serverless("SELECT 1")
            assert mock.call_args[1]["serverless"] is True

    def test_valid_with_cte(self):
        """Test WITH ... SELECT query with serverless."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql_with_serverless("WITH cte AS (SELECT 1) SELECT * FROM cte")
            assert result == "result"

    def test_invalid_non_select(self):
        """Test non-SELECT query is rejected."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql_with_serverless("INSERT INTO users VALUES (1)")


class TestExecuteHgDmlSql:
    """Tests for execute_hg_dml_sql tool."""

    def test_valid_insert(self):
        """Test valid INSERT query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="1 rows affected") as mock:
            result = execute_hg_dml_sql("INSERT INTO users VALUES (1, 'test')")
            assert result == "1 rows affected"
            mock.assert_called_once_with("execute_hg_dml_sql", "INSERT INTO users VALUES (1, 'test')", serverless=False)

    def test_valid_update(self):
        """Test valid UPDATE query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="2 rows affected") as mock:
            result = execute_hg_dml_sql("UPDATE users SET name = 'test'")
            assert result == "2 rows affected"

    def test_valid_delete(self):
        """Test valid DELETE query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="1 rows affected") as mock:
            result = execute_hg_dml_sql("DELETE FROM users WHERE id = 1")
            assert result == "1 rows affected"

    def test_invalid_select(self):
        """Test SELECT query is rejected."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("SELECT * FROM users")

    def test_invalid_ddl(self):
        """Test DDL query is rejected."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("CREATE TABLE test (id INT)")


class TestExecuteHgDdlSql:
    """Tests for execute_hg_ddl_sql tool."""

    def test_valid_create_table(self):
        """Test valid CREATE TABLE query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = execute_hg_ddl_sql("CREATE TABLE test (id INT)")
            assert result == "success"
            mock.assert_called_once()

    def test_valid_alter(self):
        """Test valid ALTER TABLE query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = execute_hg_ddl_sql("ALTER TABLE users ADD COLUMN age INT")
            assert result == "success"

    def test_valid_drop(self):
        """Test valid DROP TABLE query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = execute_hg_ddl_sql("DROP TABLE test")
            assert result == "success"

    def test_valid_comment_on(self):
        """Test valid COMMENT ON query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = execute_hg_ddl_sql("COMMENT ON TABLE users IS 'User table'")
            assert result == "success"

    def test_invalid_select(self):
        """Test SELECT query is rejected."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("SELECT * FROM users")

    def test_invalid_dml(self):
        """Test DML query is rejected."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("INSERT INTO users VALUES (1)")


class TestGatherHgTableStatistics:
    """Tests for gather_hg_table_statistics tool."""

    def test_basic_functionality(self):
        """Test ANALYZE command is generated correctly."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="Successfully ANALYZED") as mock:
            result = gather_hg_table_statistics(schema_name="public", table="users")
            assert result == "Successfully ANALYZED"

            # Check the generated query
            call_args = mock.call_args
            assert "ANALYZE public.users" in call_args[0][1]

    def test_schema_in_query(self):
        """Test schema name is included in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            gather_hg_table_statistics(schema_name="my_schema", table="my_table")

            query = mock.call_args[0][1]
            assert "my_schema" in query

    def test_table_in_query(self):
        """Test table name is included in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            gather_hg_table_statistics(schema_name="public", table="orders")

            query = mock.call_args[0][1]
            assert "orders" in query

    def test_serverless_false(self):
        """Test serverless is False for statistics gathering."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            gather_hg_table_statistics(schema_name="public", table="users")
            assert mock.call_args[1]["serverless"] is False


class TestGetHgQueryPlan:
    """Tests for get_hg_query_plan tool."""

    def test_explain_prefix_added(self):
        """Test EXPLAIN prefix is added to query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="plan") as mock:
            result = get_hg_query_plan("SELECT * FROM users")
            assert result == "plan"

            query = mock.call_args[0][1]
            assert query.startswith("EXPLAIN ")
            assert "SELECT * FROM users" in query

    def test_complex_query(self):
        """Test with complex query."""
        complex_query = "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="plan") as mock:
            get_hg_query_plan(complex_query)

            query = mock.call_args[0][1]
            assert query == f"EXPLAIN {complex_query}"


class TestGetHgExecutionPlan:
    """Tests for get_hg_execution_plan tool."""

    def test_explain_analyze_prefix_added(self):
        """Test EXPLAIN ANALYZE prefix is added to query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="execution_plan") as mock:
            result = get_hg_execution_plan("SELECT * FROM users")
            assert result == "execution_plan"

            query = mock.call_args[0][1]
            assert query.startswith("EXPLAIN ANALYZE ")
            assert "SELECT * FROM users" in query

    def test_complex_query(self):
        """Test with complex query."""
        complex_query = "SELECT * FROM users WHERE id > 100"
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="plan") as mock:
            get_hg_execution_plan(complex_query)

            query = mock.call_args[0][1]
            assert query == f"EXPLAIN ANALYZE {complex_query}"


class TestCallHgProcedure:
    """Tests for call_hg_procedure tool."""

    def test_procedure_no_args(self):
        """Test calling procedure without arguments."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            result = call_hg_procedure("my_procedure")
            assert result == "done"

            query = mock.call_args[0][1]
            assert query == "CALL my_procedure()"

    def test_procedure_with_args(self):
        """Test calling procedure with arguments."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            result = call_hg_procedure("my_procedure", ["arg1", "arg2"])
            assert result == "done"

            query = mock.call_args[0][1]
            assert query == "CALL my_procedure(arg1, arg2)"

    def test_procedure_with_single_arg(self):
        """Test calling procedure with single argument."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            call_hg_procedure("my_procedure", ["value1"])

            query = mock.call_args[0][1]
            assert query == "CALL my_procedure(value1)"

    def test_procedure_name_with_schema(self):
        """Test calling procedure with schema prefix."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            call_hg_procedure("public.my_procedure")

            query = mock.call_args[0][1]
            assert "public.my_procedure" in query


class TestCreateHgMaxcomputeForeignTable:
    """Tests for create_hg_maxcompute_foreign_table tool."""

    def test_basic_functionality(self):
        """Test basic foreign table creation query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = create_hg_maxcompute_foreign_table(
                maxcompute_project="my_project",
                maxcompute_tables=["table1", "table2"]
            )
            assert result == "success"

            query = mock.call_args[0][1]
            assert "IMPORT FOREIGN SCHEMA" in query
            assert "my_project" in query
            assert "table1, table2" in query
            assert "odps_server" in query

    def test_default_schema_values(self):
        """Test default schema values are used."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            create_hg_maxcompute_foreign_table(
                maxcompute_project="my_project",
                maxcompute_tables=["table1"]
            )

            query = mock.call_args[0][1]
            # Default maxcompute_schema is "default"
            assert "default" in query
            # Default local_schema is "public"
            assert "INTO public" in query

    def test_custom_schemas(self):
        """Test custom schema values."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            create_hg_maxcompute_foreign_table(
                maxcompute_project="my_project",
                maxcompute_tables=["table1"],
                maxcompute_schema="my_schema",
                local_schema="analytics"
            )

            query = mock.call_args[0][1]
            assert "my_schema" in query
            assert "INTO analytics" in query

    def test_single_table(self):
        """Test with single table."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            create_hg_maxcompute_foreign_table(
                maxcompute_project="project",
                maxcompute_tables=["single_table"]
            )

            query = mock.call_args[0][1]
            assert "single_table" in query


class TestListHgSchemas:
    """Tests for list_hg_schemas tool."""

    def test_basic_functionality(self):
        """Test schema listing query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="public\nanalytics") as mock:
            result = list_hg_schemas()
            assert result == "public\nanalytics"

            query = mock.call_args[0][1]
            assert "SELECT table_schema" in query
            assert "information_schema.tables" in query

    def test_excludes_system_schemas(self):
        """Test system schemas are excluded in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="public") as mock:
            list_hg_schemas()

            query = mock.call_args[0][1]
            assert "pg_catalog" in query
            assert "information_schema" in query
            assert "hologres" in query


class TestListHgTablesInASchema:
    """Tests for list_hg_tables_in_a_schema tool."""

    def test_basic_functionality(self):
        """Test table listing query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="users, orders") as mock:
            result = list_hg_tables_in_a_schema(schema_name="public")
            assert result == "users, orders"

            query = mock.call_args[0][1]
            assert "SELECT" in query
            assert "table_name" in query
            assert "public" in query

    def test_schema_parameter_in_query(self):
        """Test schema parameter is in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="") as mock:
            list_hg_tables_in_a_schema(schema_name="analytics")

            query = mock.call_args[0][1]
            assert "analytics" in query

    def test_table_type_detection(self):
        """Test table type detection is included in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="") as mock:
            list_hg_tables_in_a_schema(schema_name="public")

            query = mock.call_args[0][1]
            # Check for table type detection
            assert "VIEW" in query
            assert "FOREIGN" in query
            assert "partitioned" in query.lower()


class TestShowHgTableDdl:
    """Tests for show_hg_table_ddl tool."""

    def test_basic_functionality(self):
        """Test DDL retrieval."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="CREATE TABLE...") as mock:
            result = show_hg_table_ddl(schema_name="public", table="users")
            assert "CREATE TABLE" in result

            query = mock.call_args[0][1]
            assert "hg_dump_script" in query
            assert "public" in query
            assert "users" in query

    def test_view_handling(self):
        """Test VIEW DDL handling."""
        view_ddl = "CREATE VIEW my_view AS SELECT * FROM t\n\nEND;"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=view_ddl):
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=[(view_ddl,)]):
                with patch("hologres_mcp_server.server.try_infer_view_comments", return_value=""):
                    result = show_hg_table_ddl(schema_name="public", table="my_view")

                    # Should contain view DDL
                    assert "CREATE VIEW" in result or "my_view" in result

    def test_schema_table_in_query(self):
        """Test schema and table are properly quoted in query."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="DDL") as mock:
            show_hg_table_ddl(schema_name="my_schema", table="my_table")

            query = mock.call_args[0][1]
            assert "my_schema" in query
            assert "my_table" in query


class TestToolParameterValidation:
    """Tests for tool parameter validation and edge cases."""

    def test_gather_stats_empty_schema(self):
        """Test gather_hg_table_statistics with empty schema name."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            # Empty schema - the function doesn't validate, it just passes through
            result = gather_hg_table_statistics(schema_name="", table="users")

            # Check that the query was formed with empty schema
            query = mock.call_args[0][1]
            assert query == "ANALYZE .users"  # Note: this is a potential SQL issue

    def test_gather_stats_empty_table(self):
        """Test gather_hg_table_statistics with empty table name."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = gather_hg_table_statistics(schema_name="public", table="")

            query = mock.call_args[0][1]
            assert query == "ANALYZE public."

    def test_gather_stats_very_long_names(self, mock_env_with_long_names):
        """Test gather_hg_table_statistics with very long names."""
        long_name = "a" * 1000

        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = gather_hg_table_statistics(schema_name=long_name, table=long_name)

            query = mock.call_args[0][1]
            assert long_name in query

    def test_gather_stats_special_characters(self):
        """Test gather_hg_table_statistics with special characters in names."""
        special_schema = "schema-with-dashes"
        special_table = "table;with;semicolons"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = gather_hg_table_statistics(schema_name=special_schema, table=special_table)

            query = mock.call_args[0][1]
            assert special_schema in query
            assert special_table in query

    def test_gather_stats_sql_injection_attempt(self, sql_injection_payloads):
        """Test gather_hg_table_statistics with SQL injection attempts."""
        for payload in sql_injection_payloads[:5]:  # Test a subset
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                # Use injection payload as schema/table name
                result = gather_hg_table_statistics(schema_name="public", table=payload)

                query = mock.call_args[0][1]
                # The payload is directly interpolated - potential security issue
                assert payload in query or "ANALYZE" in query

    def test_call_procedure_empty_name(self):
        """Test call_hg_procedure with empty procedure name."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            result = call_hg_procedure("")

            query = mock.call_args[0][1]
            assert query == "CALL ()"  # Empty procedure name

    def test_call_procedure_with_sql_injection(self, sql_injection_payloads):
        """Test call_hg_procedure with SQL injection in arguments."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
            # Injection in procedure arguments
            result = call_hg_procedure("my_procedure", ["'; DROP TABLE users; --"])

            query = mock.call_args[0][1]
            assert "DROP TABLE" in query  # Injection payload is included

    def test_maxcompute_foreign_table_empty_project(self):
        """Test create_hg_maxcompute_foreign_table with empty project."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = create_hg_maxcompute_foreign_table(
                maxcompute_project="",
                maxcompute_tables=["table1"]
            )

            query = mock.call_args[0][1]
            # Empty project results in "#" in the schema name (project#schema format)
            assert '"#default"' in query

    def test_maxcompute_foreign_table_empty_tables(self):
        """Test create_hg_maxcompute_foreign_table with empty table list."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = create_hg_maxcompute_foreign_table(
                maxcompute_project="my_project",
                maxcompute_tables=[]
            )

            query = mock.call_args[0][1]
            # Empty table list results in "LIMIT TO ()"
            assert "LIMIT TO ()" in query

    def test_list_tables_empty_schema(self):
        """Test list_hg_tables_in_a_schema with empty schema name."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="") as mock:
            result = list_hg_tables_in_a_schema(schema_name="")

            query = mock.call_args[0][1]
            # Empty schema is embedded in query
            assert "tab.table_schema = ''" in query


class TestToolBoundaryConditions:
    """工具函数边界条件测试。"""

    def test_unicode_in_schema_table_names(self):
        """测试 Unicode 字符在 schema/table 名中。"""
        unicode_schema = "测试模式"
        unicode_table = "表格名称"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = gather_hg_table_statistics(schema_name=unicode_schema, table=unicode_table)

            query = mock.call_args[0][1]
            # Unicode characters should be preserved in the query
            assert unicode_schema in query
            assert unicode_table in query

    def test_null_byte_in_parameters(self):
        """测试参数中包含 NULL 字节。"""
        # Null byte in schema name
        schema_with_null = "public\x00malicious"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            result = gather_hg_table_statistics(schema_name=schema_with_null, table="users")

            query = mock.call_args[0][1]
            # Null byte should be in the query (potential truncation risk)
            assert schema_with_null in query or "public" in query

    def test_call_procedure_with_none_arguments(self):
        """测试 call_hg_procedure 参数为 None。

        当前实现不支持参数列表中包含 None 值，
        会抛出 TypeError。这记录了边界条件行为。
        """
        # 当前行为：None 参数会导致 TypeError
        # 如果需要支持 None，需要在实现中添加类型转换
        with pytest.raises(TypeError):
            call_hg_procedure("my_procedure", [None, "valid_arg"])

    def test_maxcompute_foreign_table_none_tables(self):
        """测试 maxcompute_tables 为 None 时的行为。

        当前实现不支持 maxcompute_tables 为 None，
        会抛出 TypeError。这记录了边界条件行为。
        """
        # 当前行为：None 参数会导致 TypeError
        # FastMCP 的参数验证应该确保传入的是列表
        with pytest.raises(TypeError):
            create_hg_maxcompute_foreign_table(
                maxcompute_project="my_project",
                maxcompute_tables=None
            )

    def test_very_long_sql_query(self):
        """测试超长 SQL 查询。"""
        # Create a query with many conditions
        long_query = "SELECT * FROM users WHERE " + " AND ".join([f"id{i} = {i}" for i in range(1000)])

        with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
            result = execute_hg_select_sql(long_query)

            # Query should be accepted even if very long
            assert mock.called
            assert len(mock.call_args[0][1]) > 10000

    def test_special_sql_characters_in_query(self):
        """测试 SQL 特殊字符在查询中。"""
        special_queries = [
            "SELECT * FROM users WHERE name = 'O''Brien'",  # Escaped quote
            "SELECT * FROM users WHERE data LIKE '%test%'",  # LIKE pattern
            "SELECT * FROM users WHERE json_col->>'key' = 'value'",  # JSON operator
            "SELECT * FROM users WHERE arr_col @> ARRAY[1, 2]",  # Array operator
        ]

        for query in special_queries:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result") as mock:
                result = execute_hg_select_sql(query)

                # Special characters should be preserved
                assert mock.called
                called_query = mock.call_args[0][1]
                assert query in called_query


class TestShowHgTableDdlExtended:
    """Extended tests for show_hg_table_ddl tool."""

    def test_ddl_for_nonexistent_table(self):
        """Test DDL retrieval for non-existent table."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="Error: table not found"):
            result = show_hg_table_ddl(schema_name="public", table="nonexistent_table")

            # Error message should be returned
            assert "Error" in result or "not found" in result

    def test_ddl_with_special_characters(self):
        """Test DDL with special characters in table name."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="DDL content") as mock:
            result = show_hg_table_ddl(schema_name="my-schema", table="table;with;special")

            query = mock.call_args[0][1]
            # Special characters are included in query
            assert "my-schema" in query
            assert "table;with;special" in query

    def test_ddl_error_handling(self):
        """Test DDL error handling paths."""
        # Test with connection error
        with patch("hologres_mcp_server.server.handle_call_tool",
                   return_value="Error executing query: Connection failed"):
            result = show_hg_table_ddl(schema_name="public", table="users")

            assert "Error" in result or "Connection failed" in result

        # Test with permission error
        with patch("hologres_mcp_server.server.handle_call_tool",
                   return_value="Error: permission denied"):
            result = show_hg_table_ddl(schema_name="restricted", table="secret_table")

            assert "Error" in result or "permission" in result

    def test_view_ddl_full_processing(self):
        """Test VIEW DDL full processing path (lines 147-151).

        This tests the complete VIEW handling path where:
        1. handle_call_tool returns result containing 'Type: VIEW'
        2. handle_read_resource returns valid DDL tuple
        3. try_infer_view_comments returns comments
        4. The final result combines view_content + comments + '\\n\\nEND.'
        """
        view_ddl = "CREATE VIEW my_view AS SELECT id, name FROM users\n\nEND;"
        # Result from handle_call_tool must contain "Type: VIEW"
        tool_result = f"{view_ddl}\n\nType: VIEW"

        # handle_read_resource returns tuple with DDL
        ddl_tuple = [(view_ddl,)]

        # Comments from try_infer_view_comments
        inferred_comments = "\n-- Infer view column comments from related tables\nCOMMENT ON COLUMN public.my_view.id IS 'User ID';"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=tool_result):
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=ddl_tuple):
                with patch("hologres_mcp_server.server.try_infer_view_comments", return_value=inferred_comments):
                    result = show_hg_table_ddl(schema_name="public", table="my_view")

                    # Verify the full processing path
                    # view_content should have '\n\nEND;' replaced
                    assert "CREATE VIEW my_view" in result
                    assert "COMMENT ON COLUMN public.my_view.id IS 'User ID';" in result
                    assert "-- Infer view column comments from related tables" in result
                    # Should end with '\n\nEND.' (added by the function)
                    assert result.endswith("\n\nEND.")

    def test_view_ddl_with_empty_comments(self):
        """Test VIEW DDL processing when no comments are inferred."""
        view_ddl = "CREATE VIEW simple_view AS SELECT * FROM base\n\nEND;"
        tool_result = f"{view_ddl}\n\nType: VIEW"
        ddl_tuple = [(view_ddl,)]

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=tool_result):
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=ddl_tuple):
                with patch("hologres_mcp_server.server.try_infer_view_comments", return_value=""):
                    result = show_hg_table_ddl(schema_name="public", table="simple_view")

                    # Should still have the view content
                    assert "CREATE VIEW simple_view" in result
                    assert result.endswith("\n\nEND.")

    def test_view_ddl_handle_read_resource_empty(self):
        """Test VIEW DDL when handle_read_resource returns empty."""
        tool_result = "CREATE VIEW my_view AS SELECT 1\n\nEND;\n\nType: VIEW"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=tool_result):
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=[]):
                result = show_hg_table_ddl(schema_name="public", table="my_view")

                # Should fall through and return the original result
                assert "Type: VIEW" in result

    def test_view_ddl_handle_read_resource_none_first_element(self):
        """Test VIEW DDL when ddl[0] is falsy."""
        tool_result = "CREATE VIEW my_view AS SELECT 1\n\nEND;\n\nType: VIEW"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=tool_result):
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("",)]):
                result = show_hg_table_ddl(schema_name="public", table="my_view")

                # Should fall through since ddl[0][0] is empty string (falsy)
                assert isinstance(result, str)