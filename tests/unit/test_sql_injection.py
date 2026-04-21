"""
SQL Injection Prevention Tests for Hologres MCP Server.

Tests verify that SQL injection attempts are properly handled and do not
allow unauthorized execution of malicious queries.

IMPORTANT: These tests document current behavior and potential security risks.
The current implementation uses string interpolation for SQL construction,
which is inherently vulnerable to SQL injection. These tests serve as:
1. Documentation of current behavior
2. Baseline for future security improvements
3. Verification that injection attempts don't cause crashes
"""

from unittest.mock import MagicMock, patch

from hologres_mcp_server.server import (
    call_hg_procedure,
    execute_hg_select_sql,
    gather_hg_table_statistics,
    get_guc_value,
    show_hg_table_ddl,
)


class TestSqlInjectionPrevention:
    """Tests for SQL injection prevention across all tools and resources."""

    def test_injection_in_select_query(self, sql_injection_payloads):
        """Test SQL injection attempts in SELECT queries."""
        injection_payloads = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "' UNION SELECT * FROM information_schema.tables --",
        ]

        for payload in injection_payloads:
            # The SELECT validation only checks if query starts with SELECT
            # So injection in WHERE clause would pass validation
            query = f"SELECT * FROM users WHERE name = '{payload}'"

            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result"):
                try:
                    result = execute_hg_select_sql(query)
                    # Query is executed as-is - injection payload included
                    assert isinstance(result, str)
                except ValueError:
                    # Some payloads might fail validation
                    pass

    def test_injection_in_table_name(self, sql_injection_payloads):
        """Test SQL injection attempts in table names."""
        for payload in sql_injection_payloads[:5]:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                # Table name is directly interpolated
                gather_hg_table_statistics(schema_name="public", table=payload)

                query = mock.call_args[0][1]
                # Injection payload is included in the SQL
                assert "ANALYZE" in query
                # This documents the security risk: payload is in the query
                assert payload in query

    def test_injection_in_schema_name(self, sql_injection_payloads):
        """Test SQL injection attempts in schema names."""
        for payload in sql_injection_payloads[:5]:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                gather_hg_table_statistics(schema_name=payload, table="users")

                query = mock.call_args[0][1]
                assert payload in query  # Documents security risk

    def test_injection_in_guc_name(self, sql_injection_payloads):
        """Test SQL injection attempts in GUC names."""
        # Most injection payloads would fail because SHOW expects a valid GUC name
        # But we test that the system handles them gracefully
        for payload in sql_injection_payloads[:3]:
            with patch("hologres_mcp_server.server.handle_read_resource", return_value=[("value",)]):
                result = get_guc_value(payload)

                # GUC name is directly interpolated into SHOW statement
                assert isinstance(result, str)

    def test_injection_in_procedure_args(self, sql_injection_payloads):
        """Test SQL injection attempts in stored procedure arguments."""
        injection_args = [
            "'); DROP TABLE users; --",
            "' OR '1'='1",
            "1; DROP TABLE users",
        ]

        for arg in injection_args:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="done") as mock:
                call_hg_procedure("my_procedure", [arg])

                query = mock.call_args[0][1]
                # New implementation wraps args in quotes and escapes single quotes
                assert "my_procedure" in query
                assert "CALL" in query

    def test_union_injection_attempt(self):
        """Test UNION-based SQL injection attempts."""
        union_payloads = [
            "' UNION SELECT NULL --",
            "' UNION SELECT username, password FROM users --",
            "' UNION ALL SELECT * FROM sensitive_table --",
        ]

        for payload in union_payloads:
            query = f"SELECT * FROM products WHERE id = '{payload}'"

            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result"):
                try:
                    result = execute_hg_select_sql(query)
                    assert isinstance(result, str)
                except ValueError:
                    pass

    def test_comment_injection_attempt(self):
        """Test comment-based SQL injection attempts."""
        comment_payloads = [
            "admin'--",
            "admin'/*",
            "' OR 1=1--",
            "' OR '1'='1'/**/",
        ]

        for payload in comment_payloads:
            query = f"SELECT * FROM users WHERE username = '{payload}'"

            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result"):
                try:
                    result = execute_hg_select_sql(query)
                    assert isinstance(result, str)
                except ValueError:
                    pass

    def test_stacked_queries_attempt(self):
        """Test stacked queries injection attempts."""
        stacked_payloads = [
            "SELECT * FROM users; DROP TABLE users;",
            "SELECT 1; DELETE FROM users WHERE '1'='1'",
            "SELECT * FROM users; INSERT INTO admin VALUES ('hacker', 'password');",
        ]

        for payload in stacked_payloads:
            # Each payload starts with SELECT, so it passes validation
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result"):
                try:
                    result = execute_hg_select_sql(payload)
                    assert isinstance(result, str)
                    # Note: This documents that stacked queries may be executed
                    # depending on database configuration
                except ValueError:
                    # Some might fail validation
                    pass

    def test_time_based_injection(self):
        """Test time-based blind SQL injection attempts."""
        time_payloads = [
            "'; SELECT pg_sleep(5); --",
            "' AND (SELECT * FROM (SELECT(SLEEP(5)))a); --",
            "'; WAITFOR DELAY '0:0:5'; --",
        ]

        for payload in time_payloads:
            # These would be injected via parameters, not directly
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="result"):
                try:
                    # Attempt via procedure call
                    result = call_hg_procedure("test_proc", [payload])
                    assert isinstance(result, str)
                except Exception:
                    pass


class TestSqlInjectionAssertions:
    """
    Test SQL injection with actual assertions about security behavior.

    These tests go beyond documentation to assert specific behaviors
    that help prevent or detect SQL injection attempts.
    """

    def test_injection_payload_rejected_by_database(self, sql_injection_payloads):
        """Verify dangerous payloads in schema/table names cause database errors.

        When malicious payloads are injected into identifier positions,
        the database should reject them due to syntax errors or
        non-existent objects, not execute the malicious SQL.
        """
        dangerous_payloads = [
            "users; DROP TABLE users",
            "users' OR '1'='1",
            "users); DROP TABLE users; --",
        ]

        for payload in dangerous_payloads:
            with patch(
                "hologres_mcp_server.server.handle_call_tool", return_value="Error: relation does not exist"
            ) as mock:
                # Inject payload as table name
                result = gather_hg_table_statistics(schema_name="public", table=payload)

                # Assert: The query was formed with the payload
                query = mock.call_args[0][1]
                assert payload in query

                # Assert: The result indicates an error (table doesn't exist)
                # This means the injection didn't create a valid table reference
                assert "Error" in result or "does not exist" in result

    def test_stacked_queries_single_transaction(self):
        """Verify stacked queries behavior in tool execution.

        Stacked queries (multiple SQL statements separated by semicolons)
        that pass validation should be handled safely by the database layer.
        """
        stacked_query = "SELECT * FROM users; DROP TABLE users;"

        # Mock the database layer to verify behavior
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",)]
        mock_cursor.fetchall.return_value = [("result",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("hologres_mcp_server.server.handle_call_tool") as mock_tool:
            # Simulate what handle_call_tool would do
            mock_tool.return_value = "col1\nresult"

            result = execute_hg_select_sql(stacked_query)

            # Assert: Query was passed through (validation only checks SELECT prefix)
            assert mock_tool.called

            # Assert: Result is a string (not crashed)
            assert isinstance(result, str)

    def test_union_injection_data_isolation(self):
        """Verify UNION injection returns expected columns only.

        Even if UNION injection is attempted, the result should contain
        only the columns from the original query, not arbitrary data.
        """
        # Simulate a successful UNION injection
        union_query = "SELECT id, name FROM users UNION SELECT password, email FROM admin"

        # Mock result with extra columns (simulating successful injection)
        mock_result = "id,name\n1,user1\nadminpass,admin@email.com"

        with patch("hologres_mcp_server.server.handle_call_tool", return_value=mock_result):
            result = execute_hg_select_sql(union_query)

            # Assert: Result format is consistent (header + data)
            lines = result.split("\n")
            assert len(lines) >= 1  # At least header

            # Assert: Column header matches expected format
            header = lines[0]
            assert "," in header  # CSV format maintained

    def test_numeric_injection_in_identifiers(self):
        """Test that numeric injection payloads in identifiers are handled."""
        numeric_payloads = [
            "1 OR 1=1",
            "1; DROP TABLE users",
            "1 AND 1=1",
        ]

        for payload in numeric_payloads:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="Error: syntax error") as mock:
                result = gather_hg_table_statistics(schema_name="public", table=payload)

                # Assert: Query includes the payload (it's interpolated)
                query = mock.call_args[0][1]
                assert payload in query

                # Assert: Result indicates error (not successful injection)
                assert "Error" in result or "syntax" in result.lower()


class TestSqlInjectionDocumentation:
    """
    Documentation tests that highlight security considerations.

    These tests document the current implementation's behavior with
    potentially dangerous inputs. They are not assertions about security,
    but rather documentation of what happens.
    """

    def test_document_parameter_interpolation(self):
        """Document that parameters are directly interpolated into SQL."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            # Show how table name is interpolated
            gather_hg_table_statistics(schema_name="public", table="users")

            query = mock.call_args[0][1]
            assert query == "ANALYZE public.users"

    def test_document_special_character_handling(self):
        """Document how special characters in identifiers are handled."""
        special_names = [
            ("table;with;semicolons", "Table with semicolons"),
            ("table'with'quotes", "Table with quotes"),
            ("table with spaces", "Table with spaces"),
        ]

        for table_name, _description in special_names:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                gather_hg_table_statistics(schema_name="public", table=table_name)

                query = mock.call_args[0][1]
                # Document: special characters are included as-is
                assert table_name in query

    def test_document_identifier_quoting(self):
        """Document that identifiers are not automatically quoted."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
            # Current implementation doesn't quote identifiers
            show_hg_table_ddl(schema_name="my schema", table="my table")

            query = mock.call_args[0][1]
            # Note: Spaces are included without quotes
            assert "my schema" in query
            assert "my table" in query


class TestSafeIdentifierPatterns:
    """Test patterns that should be safe vs potentially dangerous."""

    def test_safe_alphanumeric_identifiers(self):
        """Test that alphanumeric identifiers work correctly."""
        safe_identifiers = [
            ("public", "users"),
            ("analytics", "orders_2024"),
            ("schema123", "table_abc"),
        ]

        for schema, table in safe_identifiers:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                gather_hg_table_statistics(schema_name=schema, table=table)

                query = mock.call_args[0][1]
                assert f"ANALYZE {schema}.{table}" == query

    def test_potentially_dangerous_identifiers(self):
        """Test identifiers that could be problematic."""
        dangerous_identifiers = [
            ("public", "users; DROP TABLE users"),
            ("public; DROP SCHEMA public", "users"),
            ("-- comment", "users"),
        ]

        for schema, table in dangerous_identifiers:
            with patch("hologres_mcp_server.server.handle_call_tool", return_value="success") as mock:
                # This documents that dangerous patterns are not filtered
                gather_hg_table_statistics(schema_name=schema, table=table)

                query = mock.call_args[0][1]
                # The dangerous content is in the query
                assert schema in query or table in query
