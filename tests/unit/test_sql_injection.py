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

import pytest
from unittest.mock import patch, MagicMock

from hologres_mcp_server.server import (
    execute_hg_select_sql,
    execute_hg_dml_sql,
    execute_hg_ddl_sql,
    gather_hg_table_statistics,
    call_hg_procedure,
    get_guc_value,
    show_hg_table_ddl,
    list_hg_tables_in_a_schema,
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

            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="result"):
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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="success") as mock:
                # Table name is directly interpolated
                result = gather_hg_table_statistics("public", payload)

                query = mock.call_args[0][1]
                # Injection payload is included in the SQL
                assert "ANALYZE" in query
                # This documents the security risk: payload is in the query
                assert payload in query

    def test_injection_in_schema_name(self, sql_injection_payloads):
        """Test SQL injection attempts in schema names."""
        for payload in sql_injection_payloads[:5]:
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="success") as mock:
                result = gather_hg_table_statistics(payload, "users")

                query = mock.call_args[0][1]
                assert payload in query  # Documents security risk

    def test_injection_in_guc_name(self, sql_injection_payloads):
        """Test SQL injection attempts in GUC names."""
        # Most injection payloads would fail because SHOW expects a valid GUC name
        # But we test that the system handles them gracefully
        for payload in sql_injection_payloads[:3]:
            with patch("hologres_mcp_server.server.handle_read_resource",
                       return_value=[("value",)]):
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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="done") as mock:
                result = call_hg_procedure("my_procedure", [arg])

                query = mock.call_args[0][1]
                # Arguments are concatenated without sanitization
                assert arg in query  # Documents security risk

    def test_union_injection_attempt(self):
        """Test UNION-based SQL injection attempts."""
        union_payloads = [
            "' UNION SELECT NULL --",
            "' UNION SELECT username, password FROM users --",
            "' UNION ALL SELECT * FROM sensitive_table --",
        ]

        for payload in union_payloads:
            query = f"SELECT * FROM products WHERE id = '{payload}'"

            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="result"):
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

            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="result"):
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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="result"):
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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="result") as mock:
                try:
                    # Attempt via procedure call
                    result = call_hg_procedure("test_proc", [payload])
                    assert isinstance(result, str)
                except Exception:
                    pass


class TestSqlInjectionDocumentation:
    """
    Documentation tests that highlight security considerations.

    These tests document the current implementation's behavior with
    potentially dangerous inputs. They are not assertions about security,
    but rather documentation of what happens.
    """

    def test_document_parameter_interpolation(self):
        """Document that parameters are directly interpolated into SQL."""
        with patch("hologres_mcp_server.server.handle_call_tool",
                   return_value="success") as mock:
            # Show how table name is interpolated
            gather_hg_table_statistics("public", "users")

            query = mock.call_args[0][1]
            assert query == "ANALYZE public.users"

    def test_document_special_character_handling(self):
        """Document how special characters in identifiers are handled."""
        special_names = [
            ("table;with;semicolons", "Table with semicolons"),
            ("table'with'quotes", "Table with quotes"),
            ("table with spaces", "Table with spaces"),
        ]

        for table_name, description in special_names:
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="success") as mock:
                gather_hg_table_statistics("public", table_name)

                query = mock.call_args[0][1]
                # Document: special characters are included as-is
                assert table_name in query

    def test_document_identifier_quoting(self):
        """Document that identifiers are not automatically quoted."""
        with patch("hologres_mcp_server.server.handle_call_tool",
                   return_value="success") as mock:
            # Current implementation doesn't quote identifiers
            show_hg_table_ddl("my schema", "my table")

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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="success") as mock:
                gather_hg_table_statistics(schema, table)

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
            with patch("hologres_mcp_server.server.handle_call_tool",
                       return_value="success") as mock:
                # This documents that dangerous patterns are not filtered
                gather_hg_table_statistics(schema, table)

                query = mock.call_args[0][1]
                # The dangerous content is in the query
                assert schema in query or table in query