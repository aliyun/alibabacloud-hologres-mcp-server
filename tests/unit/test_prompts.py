"""
Tests for prompt functions in server module.

Prompts:
- analyze_table_performance(schema, table)
- optimize_query(query)
- explore_schema(schema="public")
"""

from hologres_mcp_server.server import (
    analyze_table_performance,
    explore_schema,
    optimize_query,
)


class TestAnalyzeTablePerformance:
    """Tests for analyze_table_performance prompt."""

    def test_basic_functionality(self):
        """Test basic prompt generation."""
        result = analyze_table_performance("public", "users")

        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_schema_table(self):
        """Test prompt contains schema.table reference."""
        result = analyze_table_performance("public", "users")

        assert "public.users" in result

    def test_contains_schema_parameter(self):
        """Test prompt contains schema name."""
        result = analyze_table_performance("my_schema", "my_table")

        assert "my_schema" in result

    def test_contains_table_parameter(self):
        """Test prompt contains table name."""
        result = analyze_table_performance("public", "my_table")

        assert "my_table" in result

    def test_contains_analysis_keywords(self):
        """Test prompt contains analysis-related keywords."""
        result = analyze_table_performance("public", "users")

        # Should contain DDL reference
        assert "DDL" in result
        # Should mention statistics
        assert "statistics" in result.lower()
        # Should mention partition if applicable
        assert "partition" in result.lower()
        # Should contain analysis steps
        assert "analyze" in result.lower()

    def test_contains_tool_references(self):
        """Test prompt contains references to tools and resources."""
        result = analyze_table_performance("public", "users")

        # Should reference the DDL tool
        assert "show_hg_table_ddl" in result
        # Should reference the statistic resource pattern
        assert "statistic" in result

    def test_with_special_characters_in_schema(self):
        """Test handling of special characters in schema name."""
        result = analyze_table_performance("my-schema", "users")

        assert "my-schema" in result

    def test_with_special_characters_in_table(self):
        """Test handling of special characters in table name."""
        result = analyze_table_performance("public", "user_data_2024")

        assert "user_data_2024" in result

    def test_with_uppercase_names(self):
        """Test handling of uppercase names."""
        result = analyze_table_performance("PUBLIC", "USERS")

        assert "PUBLIC.USERS" in result

    def test_with_underscore_prefix(self):
        """Test handling of underscore prefix (common for internal tables)."""
        result = analyze_table_performance("_internal", "_metadata")

        assert "_internal._metadata" in result

    def test_contains_optimization_focus_areas(self):
        """Test prompt contains optimization focus areas."""
        result = analyze_table_performance("public", "users")

        # These are key Hologres optimization areas
        assert "Distribution key" in result
        assert "Clustering key" in result


class TestOptimizeQuery:
    """Tests for optimize_query prompt."""

    def test_basic_functionality(self):
        """Test basic prompt generation."""
        result = optimize_query("SELECT * FROM users")

        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_query(self):
        """Test prompt contains the original query."""
        query = "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
        result = optimize_query(query)

        assert query in result

    def test_with_simple_query(self):
        """Test with simple SELECT query."""
        result = optimize_query("SELECT * FROM users")

        assert "SELECT * FROM users" in result

    def test_with_complex_query(self):
        """Test with complex query containing joins and subqueries."""
        complex_query = """
        WITH user_orders AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.name, uo.order_count
        FROM users u
        JOIN user_orders uo ON u.id = uo.user_id
        WHERE uo.order_count > 10
        ORDER BY uo.order_count DESC
        """
        result = optimize_query(complex_query)

        assert "WITH" in result
        assert "user_orders" in result

    def test_contains_tool_references(self):
        """Test prompt contains references to tools."""
        result = optimize_query("SELECT * FROM users")

        assert "get_hg_query_plan" in result
        assert "get_hg_execution_plan" in result

    def test_contains_optimization_keywords(self):
        """Test prompt contains optimization-related keywords."""
        result = optimize_query("SELECT * FROM users")

        assert "optimize" in result.lower()

    def test_contains_optimization_considerations(self):
        """Test prompt contains optimization considerations."""
        result = optimize_query("SELECT * FROM users")

        # Key optimization considerations for Hologres
        assert "Join order" in result
        assert "Filter selectivity" in result
        assert "Memory usage" in result
        assert "Serverless" in result

    def test_with_special_sql_characters(self):
        """Test with special SQL characters."""
        query = 'SELECT * FROM "user-table" WHERE "col-name" = \'value\''
        result = optimize_query(query)

        assert query in result

    def test_with_multiline_query(self):
        """Test with multiline query."""
        query = """
        SELECT
            u.id,
            u.name,
            COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
        """
        result = optimize_query(query)

        assert "users u" in result
        assert "orders o" in result


class TestExploreSchema:
    """Tests for explore_schema prompt."""

    def test_basic_functionality(self):
        """Test basic prompt generation."""
        result = explore_schema("public")

        assert isinstance(result, str)
        assert len(result) > 0

    def test_default_parameter(self):
        """Test default schema parameter is 'public'."""
        result = explore_schema()

        assert "public" in result

    def test_custom_schema(self):
        """Test with custom schema name."""
        result = explore_schema("analytics")

        assert "analytics" in result

    def test_contains_schema_name(self):
        """Test prompt contains schema name in title."""
        result = explore_schema("my_schema")

        # Should have the schema name prominently displayed
        assert "my_schema" in result

    def test_contains_tool_references(self):
        """Test prompt contains references to tools."""
        result = explore_schema("public")

        assert "list_hg_tables_in_a_schema" in result
        assert "show_hg_table_ddl" in result

    def test_contains_exploration_steps(self):
        """Test prompt contains exploration steps."""
        result = explore_schema("public")

        # Should mention exploring/listing tables
        assert "table" in result.lower()
        # Should mention DDL
        assert "DDL" in result
        # Should mention statistics
        assert "statistic" in result.lower()

    def test_contains_table_type_detection(self):
        """Test prompt mentions table type detection."""
        result = explore_schema("public")

        # Should mention different table types
        assert "table" in result.lower() or "view" in result.lower()

    def test_with_special_characters(self):
        """Test handling of special characters in schema name."""
        result = explore_schema("my-analytics-schema")

        assert "my-analytics-schema" in result

    def test_with_uppercase(self):
        """Test handling of uppercase schema name."""
        result = explore_schema("ANALYTICS")

        assert "ANALYTICS" in result

    def test_with_underscore_prefix(self):
        """Test handling of underscore prefix."""
        result = explore_schema("_internal")

        assert "_internal" in result

    def test_identifies_missing_statistics(self):
        """Test prompt identifies missing statistics check."""
        result = explore_schema("public")

        assert "missing statistics" in result.lower()

    def test_mentions_schema_structure_summary(self):
        """Test prompt mentions summarizing schema structure."""
        result = explore_schema("public")

        assert "summar" in result.lower()  # matches "summarize" or "summary"
