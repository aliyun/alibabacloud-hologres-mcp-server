"""
Tests for SQL validation logic in server tools.

SQL validation is embedded in tool functions:
- execute_hg_select_sql: SELECT or WITH...SELECT
- execute_hg_select_sql_with_serverless: SELECT only
- execute_hg_dml_sql: INSERT, UPDATE, DELETE
- execute_hg_ddl_sql: CREATE, ALTER, DROP, COMMENT ON
"""

import pytest
from unittest.mock import patch

# Import tool functions for testing validation
from hologres_mcp_server.server import (
    execute_hg_select_sql,
    execute_hg_select_sql_with_serverless,
    execute_hg_dml_sql,
    execute_hg_ddl_sql,
)


class TestSelectValidation:
    """Tests for SELECT statement validation in execute_hg_select_sql."""

    # === Valid SELECT Statements ===

    def test_select_validation_simple(self):
        """Test simple SELECT statement passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("SELECT * FROM users")
            assert result == "success"

    def test_select_validation_with_leading_whitespace(self):
        """Test SELECT with leading whitespace passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("   SELECT * FROM users")
            assert result == "success"

    def test_select_validation_with_tabs(self):
        """Test SELECT with leading tabs passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("\t\tSELECT * FROM users")
            assert result == "success"

    def test_select_validation_with_newlines(self):
        """Test SELECT with leading newlines passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("\n\nSELECT * FROM users")
            assert result == "success"

    def test_select_validation_with_cte(self):
        """Test WITH ... SELECT statement passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            # Use single-line CTE to avoid DOTALL flag issues in regex
            result = execute_hg_select_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
            assert result == "success"

    def test_select_validation_with_cte_multiline(self):
        """Test multi-line WITH ... SELECT statement passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            # Multi-line CTE with actual newlines between WITH and SELECT
            result = execute_hg_select_sql("""WITH
    users AS (SELECT 1 AS id),
    orders AS (SELECT 2 AS order_id)
SELECT * FROM users""")
            assert result == "success"

    def test_select_validation_with_cte_leading_whitespace(self):
        """Test WITH ... SELECT with leading whitespace passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("   WITH cte AS (SELECT 1) SELECT * FROM cte")
            assert result == "success"

    def test_select_validation_case_insensitive(self):
        """Test case insensitivity for SELECT keyword."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("select * from users")
            assert result == "success"

    def test_select_validation_mixed_case(self):
        """Test mixed case SELECT passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("sElEcT * FROM users")
            assert result == "success"

    def test_select_validation_with_cte_case_insensitive(self):
        """Test case insensitivity for WITH keyword."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("with cte as (select 1) select * from cte")
            assert result == "success"

    # === Invalid Statements (should raise ValueError) ===

    def test_select_validation_invalid_insert(self):
        """Test INSERT fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("INSERT INTO users VALUES (1, 'test')")

    def test_select_validation_invalid_update(self):
        """Test UPDATE fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("UPDATE users SET name = 'test'")

    def test_select_validation_invalid_delete(self):
        """Test DELETE fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("DELETE FROM users")

    def test_select_validation_invalid_create(self):
        """Test CREATE fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("CREATE TABLE test (id INT)")

    def test_select_validation_invalid_alter(self):
        """Test ALTER fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("ALTER TABLE users ADD COLUMN age INT")

    def test_select_validation_invalid_drop(self):
        """Test DROP fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("DROP TABLE users")

    def test_select_validation_invalid_truncate(self):
        """Test TRUNCATE fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("TRUNCATE TABLE users")


class TestSelectServerlessValidation:
    """Tests for SELECT statement validation in execute_hg_select_sql_with_serverless."""

    # Note: Serverless version has stricter validation (no WITH support)
    def test_serverless_select_simple(self):
        """Test simple SELECT passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql_with_serverless("SELECT * FROM users")
            assert result == "success"

    def test_serverless_select_with_whitespace(self):
        """Test SELECT with whitespace passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql_with_serverless("   SELECT * FROM users")
            assert result == "success"

    def test_serverless_select_case_insensitive(self):
        """Test case insensitivity for SELECT keyword."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql_with_serverless("select * from users")
            assert result == "success"

    def test_serverless_select_with_cte_rejected(self):
        """Test WITH ... SELECT is rejected (serverless doesn't support CTE prefix)."""
        # Note: The serverless validation is stricter - it only checks if query starts with SELECT
        # after stripping whitespace, so WITH...SELECT would be rejected
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql_with_serverless("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_serverless_select_invalid_insert(self):
        """Test INSERT fails validation."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql_with_serverless("INSERT INTO users VALUES (1)")


class TestDmlValidation:
    """Tests for DML statement validation in execute_hg_dml_sql."""

    # === Valid DML Statements ===

    def test_dml_validation_insert(self):
        """Test INSERT passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("INSERT INTO users VALUES (1, 'test')")
            assert result == "success"

    def test_dml_validation_update(self):
        """Test UPDATE passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("UPDATE users SET name = 'test'")
            assert result == "success"

    def test_dml_validation_delete(self):
        """Test DELETE passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("DELETE FROM users")
            assert result == "success"

    def test_dml_validation_with_whitespace(self):
        """Test DML with leading whitespace passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("   INSERT INTO users VALUES (1)")
            assert result == "success"

    def test_dml_validation_case_insensitive(self):
        """Test case insensitivity for DML keywords."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("insert into users values (1)")
            assert result == "success"

    # === Invalid Statements (should raise ValueError) ===

    def test_dml_validation_invalid_select(self):
        """Test SELECT fails validation."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("SELECT * FROM users")

    def test_dml_validation_invalid_create(self):
        """Test CREATE fails validation."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("CREATE TABLE test (id INT)")

    def test_dml_validation_invalid_alter(self):
        """Test ALTER fails validation."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("ALTER TABLE users ADD COLUMN age INT")

    def test_dml_validation_invalid_drop(self):
        """Test DROP fails validation."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("DROP TABLE users")


class TestDdlValidation:
    """Tests for DDL statement validation in execute_hg_ddl_sql."""

    # === Valid DDL Statements ===

    def test_ddl_validation_create_table(self):
        """Test CREATE TABLE passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("CREATE TABLE test (id INT)")
            assert result == "success"

    def test_ddl_validation_create_view(self):
        """Test CREATE VIEW passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("CREATE VIEW test_view AS SELECT * FROM users")
            assert result == "success"

    def test_ddl_validation_alter(self):
        """Test ALTER passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("ALTER TABLE users ADD COLUMN age INT")
            assert result == "success"

    def test_ddl_validation_drop(self):
        """Test DROP passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("DROP TABLE users")
            assert result == "success"

    def test_ddl_validation_comment_on(self):
        """Test COMMENT ON passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("COMMENT ON TABLE users IS 'User table'")
            assert result == "success"

    def test_ddl_validation_with_whitespace(self):
        """Test DDL with leading whitespace passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("   CREATE TABLE test (id INT)")
            assert result == "success"

    def test_ddl_validation_case_insensitive(self):
        """Test case insensitivity for DDL keywords."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("create table test (id int)")
            assert result == "success"

    def test_ddl_validation_comment_on_case_insensitive(self):
        """Test case insensitivity for COMMENT ON."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_ddl_sql("comment on table users is 'test'")
            assert result == "success"

    # === Invalid Statements (should raise ValueError) ===

    def test_ddl_validation_invalid_select(self):
        """Test SELECT fails validation."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("SELECT * FROM users")

    def test_ddl_validation_invalid_insert(self):
        """Test INSERT fails validation."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("INSERT INTO users VALUES (1)")

    def test_ddl_validation_invalid_update(self):
        """Test UPDATE fails validation."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("UPDATE users SET name = 'test'")

    def test_ddl_validation_invalid_delete(self):
        """Test DELETE fails validation."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("DELETE FROM users")

    def test_ddl_validation_invalid_truncate(self):
        """Test TRUNCATE fails validation (not in allowed list)."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("TRUNCATE TABLE users")


class TestEdgeCases:
    """Tests for edge cases in SQL validation."""

    def test_empty_query_select(self):
        """Test empty query raises ValueError for SELECT tool."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("")

    def test_whitespace_only_query_select(self):
        """Test whitespace-only query raises ValueError for SELECT tool."""
        with pytest.raises(ValueError, match="must be a SELECT statement"):
            execute_hg_select_sql("   ")

    def test_empty_query_dml(self):
        """Test empty query raises ValueError for DML tool."""
        with pytest.raises(ValueError, match="must be a DML statement"):
            execute_hg_dml_sql("")

    def test_empty_query_ddl(self):
        """Test empty query raises ValueError for DDL tool."""
        with pytest.raises(ValueError, match="must be a DDL statement"):
            execute_hg_ddl_sql("")

    def test_select_with_subquery(self):
        """Test SELECT with subquery passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("SELECT * FROM (SELECT 1) AS t")
            assert result == "success"

    def test_select_with_union(self):
        """Test SELECT with UNION passes validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_select_sql("SELECT 1 UNION SELECT 2")
            assert result == "success"

    def test_insert_with_select(self):
        """Test INSERT...SELECT passes DML validation."""
        with patch("hologres_mcp_server.server.handle_call_tool", return_value="success"):
            result = execute_hg_dml_sql("INSERT INTO users SELECT * FROM temp_users")
            assert result == "success"