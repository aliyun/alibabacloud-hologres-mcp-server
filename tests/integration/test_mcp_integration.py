"""
Integration tests for Hologres MCP Server.

These tests require a real Hologres database connection.
Configure connection settings in tests/integration/.test_mcp_client_env file.

Run with: pytest tests/integration/test_mcp_integration.py -v -m integration
"""

import pytest
from mcp import ClientSession


# ============================================================================
# Test Markers
# ============================================================================

pytestmark = pytest.mark.integration


# ============================================================================
# MCP Connection Tests
# ============================================================================

class TestMCPConnection:
    """Tests for MCP server connection and basic functionality."""

    async def test_initialize_session(self, mcp_session: ClientSession):
        """Test that MCP session initializes successfully."""
        assert mcp_session is not None
        # Session is already initialized in the fixture

    async def test_list_tools(self, mcp_session: ClientSession):
        """Test that we can list all available tools."""
        result = await mcp_session.list_tools()

        assert result is not None
        assert hasattr(result, "tools")
        assert len(result.tools) > 0

        # Verify expected tool names
        tool_names = {tool.name for tool in result.tools}
        expected_tools = {
            "execute_hg_select_sql",
            "execute_hg_select_sql_with_serverless",
            "execute_hg_dml_sql",
            "execute_hg_ddl_sql",
            "gather_hg_table_statistics",
            "get_hg_query_plan",
            "get_hg_execution_plan",
            "call_hg_procedure",
            "create_hg_maxcompute_foreign_table",
            "list_hg_schemas",
            "list_hg_tables_in_a_schema",
            "show_hg_table_ddl",
        }
        assert expected_tools.issubset(tool_names), f"Missing tools: {expected_tools - tool_names}"

    async def test_list_resources(self, mcp_session: ClientSession):
        """Test that we can list all available resources."""
        result = await mcp_session.list_resources()

        assert result is not None
        assert hasattr(result, "resources")
        # Resources list might be empty if no dynamic resources are registered

    async def test_list_resource_templates(self, mcp_session: ClientSession):
        """Test that we can list all resource templates."""
        result = await mcp_session.list_resource_templates()

        assert result is not None
        assert hasattr(result, "resourceTemplates")
        assert len(result.resourceTemplates) > 0

        # Verify expected resource template URIs (templates have parameters like {schema})
        template_uris = {t.uriTemplate for t in result.resourceTemplates}
        expected_templates = {
            "hologres:///{schema}/tables",
            "hologres:///{schema}/{table}/ddl",
            "hologres:///{schema}/{table}/statistic",
            "hologres:///{schema}/{table}/partitions",
            "system:///guc_value/{guc_name}",
            "system:///query_log/latest/{row_limits}",
        }
        assert expected_templates.issubset(template_uris), f"Missing templates: {expected_templates - template_uris}"

    async def test_list_prompts(self, mcp_session: ClientSession):
        """Test that we can list all available prompts."""
        result = await mcp_session.list_prompts()

        assert result is not None
        assert hasattr(result, "prompts")
        assert len(result.prompts) > 0

        # Verify expected prompt names
        prompt_names = {p.name for p in result.prompts}
        expected_prompts = {
            "analyze_table_performance",
            "optimize_query",
            "explore_schema",
        }
        assert expected_prompts.issubset(prompt_names), f"Missing prompts: {expected_prompts - prompt_names}"


# ============================================================================
# MCP Resources Tests
# ============================================================================

class TestMCPResources:
    """Tests for MCP resource reading functionality."""

    async def test_list_schemas(self, mcp_session: ClientSession):
        """Test reading the schemas resource."""
        result = await mcp_session.read_resource("hologres:///schemas")

        assert result is not None
        assert hasattr(result, "contents")
        content = result.contents[0]
        assert content.text is not None
        # Should contain at least one schema (typically 'public')
        assert len(content.text.strip()) > 0

    async def test_list_tables_in_schema(
        self, mcp_session: ClientSession, test_schema: str
    ):
        """Test reading the tables resource for a specific schema."""
        result = await mcp_session.read_resource(
            f"hologres:///{test_schema}/tables"
        )

        assert result is not None
        assert hasattr(result, "contents")
        # The result might be empty if no tables exist in the schema

    async def test_get_table_ddl(
        self, mcp_session: ClientSession, test_schema: str, test_table: str
    ):
        """Test reading the DDL resource for a specific table."""
        if test_table is None:
            pytest.skip("No test table available")

        result = await mcp_session.read_resource(
            f"hologres:///{test_schema}/{test_table}/ddl"
        )

        assert result is not None
        assert hasattr(result, "contents")
        content = result.contents[0]
        assert content.text is not None
        # DDL should contain CREATE statement
        assert "CREATE" in content.text.upper() or "No DDL found" in content.text

    async def test_get_table_statistics(
        self, mcp_session: ClientSession, test_schema: str, test_table: str
    ):
        """Test reading the statistics resource for a specific table."""
        if test_table is None:
            pytest.skip("No test table available")

        result = await mcp_session.read_resource(
            f"hologres:///{test_schema}/{test_table}/statistic"
        )

        assert result is not None
        assert hasattr(result, "contents")
        # Statistics might not exist for all tables

    async def test_get_system_info_hg_instance_version(
        self, mcp_session: ClientSession
    ):
        """Test reading system resource for Hologres instance version."""
        result = await mcp_session.read_resource(
            "system:///hg_instance_version"
        )

        assert result is not None
        assert hasattr(result, "contents")
        content = result.contents[0]
        assert content.text is not None
        # Version should be a version string
        assert len(content.text.strip()) > 0

    async def test_get_system_info_missing_stats_tables(
        self, mcp_session: ClientSession
    ):
        """Test reading system resource for missing statistics tables."""
        result = await mcp_session.read_resource(
            "system:///missing_stats_tables"
        )

        assert result is not None
        assert hasattr(result, "contents")

    async def test_get_system_info_stat_activity(
        self, mcp_session: ClientSession
    ):
        """Test reading system resource for current activity."""
        result = await mcp_session.read_resource(
            "system:///stat_activity"
        )

        assert result is not None
        assert hasattr(result, "contents")

    async def test_get_system_info_guc_value(
        self, mcp_session: ClientSession
    ):
        """Test reading system resource for GUC value."""
        result = await mcp_session.read_resource(
            "system:///guc_value/hg_version"
        )

        assert result is not None
        assert hasattr(result, "contents")
        content = result.contents[0]
        assert content.text is not None
        assert "hg_version" in content.text

    async def test_get_system_info_query_log(
        self, mcp_session: ClientSession
    ):
        """Test reading system resource for query log."""
        result = await mcp_session.read_resource(
            "system:///query_log/latest/5"
        )

        assert result is not None
        assert hasattr(result, "contents")


# ============================================================================
# MCP Tools Tests (Read-Only)
# ============================================================================

class TestMCPTools:
    """Tests for MCP tool calls (read-only operations)."""

    async def test_list_hg_schemas(self, mcp_session: ClientSession):
        """Test list_hg_schemas tool."""
        result = await mcp_session.call_tool("list_hg_schemas", {})

        assert result is not None
        assert hasattr(result, "content")
        assert len(result.content) > 0
        # Result should be text content
        text_content = result.content[0].text
        assert text_content is not None
        # Should contain at least 'public' schema
        assert "public" in text_content.lower() or len(text_content.strip()) > 0

    async def test_list_hg_tables(
        self, mcp_session: ClientSession, test_schema: str
    ):
        """Test list_hg_tables_in_a_schema tool."""
        result = await mcp_session.call_tool(
            "list_hg_tables_in_a_schema",
            {"schema": test_schema}
        )

        assert result is not None
        assert hasattr(result, "content")
        # Tables list might be empty

    async def test_execute_select(self, mcp_session: ClientSession):
        """Test execute_hg_select_sql tool with a simple query."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql",
            {"query": "SELECT current_database() AS db, current_user AS usr"}
        )

        assert result is not None
        assert hasattr(result, "content")
        text_content = result.content[0].text
        assert text_content is not None
        # Should contain headers and data
        assert "db" in text_content.lower() or "database" in text_content.lower()

    async def test_execute_select_with_cte(self, mcp_session: ClientSession):
        """Test execute_hg_select_sql tool with a CTE query."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql",
            {
                "query": """
                WITH test_data AS (
                    SELECT 1 AS id, 'test' AS name
                )
                SELECT * FROM test_data
                """
            }
        )

        assert result is not None
        assert hasattr(result, "content")
        text_content = result.content[0].text
        assert text_content is not None

    async def test_execute_select_serverless(self, mcp_session: ClientSession):
        """Test execute_hg_select_sql_with_serverless tool."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql_with_serverless",
            {"query": "SELECT current_database() AS db"}
        )

        assert result is not None
        assert hasattr(result, "content")

    async def test_get_query_plan(self, mcp_session: ClientSession):
        """Test get_hg_query_plan tool."""
        result = await mcp_session.call_tool(
            "get_hg_query_plan",
            {"query": "SELECT * FROM information_schema.tables LIMIT 5"}
        )

        assert result is not None
        assert hasattr(result, "content")
        text_content = result.content[0].text
        assert text_content is not None
        # Query plan should contain EXPLAIN output
        assert len(text_content.strip()) > 0

    async def test_get_execution_plan(self, mcp_session: ClientSession):
        """Test get_hg_execution_plan tool."""
        result = await mcp_session.call_tool(
            "get_hg_execution_plan",
            {"query": "SELECT 1 AS test"}
        )

        assert result is not None
        assert hasattr(result, "content")
        text_content = result.content[0].text
        assert text_content is not None

    async def test_show_table_ddl(
        self, mcp_session: ClientSession, test_schema: str, test_table: str
    ):
        """Test show_hg_table_ddl tool."""
        if test_table is None:
            pytest.skip("No test table available")

        result = await mcp_session.call_tool(
            "show_hg_table_ddl",
            {"schema": test_schema, "table": test_table}
        )

        assert result is not None
        assert hasattr(result, "content")
        text_content = result.content[0].text
        assert text_content is not None

    async def test_execute_select_invalid_sql(self, mcp_session: ClientSession):
        """Test that invalid SQL returns an error."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql",
            {"query": "SELECT * FROM nonexistent_table_xyz_12345"}
        )

        assert result is not None
        assert hasattr(result, "content")
        # Should contain an error message
        text_content = result.content[0].text
        assert "error" in text_content.lower() or "Error" in text_content

    async def test_execute_select_non_select_rejected(
        self, mcp_session: ClientSession
    ):
        """Test that non-SELECT statements are rejected."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql",
            {"query": "INSERT INTO test VALUES (1)"}
        )

        assert result is not None
        assert hasattr(result, "isError")
        assert result.isError is True


# ============================================================================
# MCP DDL Tools Tests (Modifies Database)
# ============================================================================

class TestMCPDDLTools:
    """Tests for MCP DDL tool calls (modifies database objects)."""

    async def test_create_and_drop_table(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test CREATE and DROP table operations."""
        table_name = f"{integration_test_prefix}table"

        # Create table
        create_result = await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id INT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            }
        )

        assert create_result is not None
        assert hasattr(create_result, "content")

        # Verify table exists by listing tables
        list_result = await mcp_session.call_tool(
            "list_hg_tables_in_a_schema",
            {"schema": "public"}
        )
        assert table_name in list_result.content[0].text

        # Drop table
        drop_result = await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {"query": f"DROP TABLE IF EXISTS public.{table_name}"}
        )

        assert drop_result is not None
        assert hasattr(drop_result, "content")

    async def test_create_and_drop_view(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test CREATE and DROP view operations."""
        view_name = f"{integration_test_prefix}view"

        # Create view
        create_result = await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE OR REPLACE VIEW public.{view_name} AS
                SELECT current_database() AS db_name
                """
            }
        )

        assert create_result is not None
        assert hasattr(create_result, "content")

        # Drop view
        drop_result = await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {"query": f"DROP VIEW IF EXISTS public.{view_name}"}
        )

        assert drop_result is not None
        assert hasattr(drop_result, "content")

    async def test_alter_table(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test ALTER table operations."""
        table_name = f"{integration_test_prefix}alter_test"

        # Create table first
        await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id INT
                )
                """
            }
        )

        try:
            # Alter table - add column
            alter_result = await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS new_col TEXT"}
            )

            assert alter_result is not None
            assert hasattr(alter_result, "content")

        finally:
            # Cleanup
            await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"DROP TABLE IF EXISTS public.{table_name}"}
            )

    async def test_comment_on_table(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test COMMENT ON table operations."""
        table_name = f"{integration_test_prefix}comment_test"

        # Create table first
        await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id INT,
                    name TEXT
                )
                """
            }
        )

        try:
            # Add comment
            comment_result = await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"COMMENT ON TABLE public.{table_name} IS 'Test table for integration tests'"}
            )

            assert comment_result is not None
            assert hasattr(comment_result, "content")

        finally:
            # Cleanup
            await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"DROP TABLE IF EXISTS public.{table_name}"}
            )

    async def test_ddl_non_ddl_rejected(self, mcp_session: ClientSession):
        """Test that non-DDL statements are rejected."""
        result = await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {"query": "SELECT 1"}
        )

        assert result is not None
        assert hasattr(result, "isError")
        assert result.isError is True


# ============================================================================
# MCP DML Tools Tests (Modifies Data)
# ============================================================================

class TestMCPDMLTools:
    """Tests for MCP DML tool calls (modifies data)."""

    async def test_insert_update_delete(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test INSERT, UPDATE, and DELETE operations in sequence."""
        table_name = f"{integration_test_prefix}dml_test"

        # Create table first
        await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id INT PRIMARY KEY,
                    name TEXT,
                    value INT
                )
                """
            }
        )

        try:
            # INSERT
            insert_result = await mcp_session.call_tool(
                "execute_hg_dml_sql",
                {
                    "query": f"""
                    INSERT INTO public.{table_name} (id, name, value)
                    VALUES (1, 'test1', 100), (2, 'test2', 200)
                    """
                }
            )

            assert insert_result is not None
            assert hasattr(insert_result, "content")
            assert "2" in insert_result.content[0].text  # 2 rows affected

            # Verify insert
            select_result = await mcp_session.call_tool(
                "execute_hg_select_sql",
                {"query": f"SELECT * FROM public.{table_name} ORDER BY id"}
            )
            assert "test1" in select_result.content[0].text

            # UPDATE
            update_result = await mcp_session.call_tool(
                "execute_hg_dml_sql",
                {
                    "query": f"""
                    UPDATE public.{table_name}
                    SET value = 150
                    WHERE id = 1
                    """
                }
            )

            assert update_result is not None
            assert hasattr(update_result, "content")

            # Verify update
            verify_result = await mcp_session.call_tool(
                "execute_hg_select_sql",
                {"query": f"SELECT value FROM public.{table_name} WHERE id = 1"}
            )
            assert "150" in verify_result.content[0].text

            # DELETE
            delete_result = await mcp_session.call_tool(
                "execute_hg_dml_sql",
                {"query": f"DELETE FROM public.{table_name} WHERE id = 2"}
            )

            assert delete_result is not None
            assert hasattr(delete_result, "content")

            # Verify delete
            count_result = await mcp_session.call_tool(
                "execute_hg_select_sql",
                {"query": f"SELECT COUNT(*) AS cnt FROM public.{table_name}"}
            )
            assert "1" in count_result.content[0].text  # Only 1 row left

        finally:
            # Cleanup
            await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"DROP TABLE IF EXISTS public.{table_name}"}
            )

    async def test_gather_statistics(
        self, mcp_session: ClientSession, integration_test_prefix: str
    ):
        """Test gather_hg_table_statistics tool."""
        table_name = f"{integration_test_prefix}stats_test"

        # Create table with some data
        await mcp_session.call_tool(
            "execute_hg_ddl_sql",
            {
                "query": f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id INT,
                    name TEXT
                )
                """
            }
        )

        try:
            # Insert some data
            await mcp_session.call_tool(
                "execute_hg_dml_sql",
                {
                    "query": f"INSERT INTO public.{table_name} VALUES (1, 'test'), (2, 'test2')"
                }
            )

            # Gather statistics
            stats_result = await mcp_session.call_tool(
                "gather_hg_table_statistics",
                {"schema": "public", "table": table_name}
            )

            assert stats_result is not None
            assert hasattr(stats_result, "content")

        finally:
            # Cleanup
            await mcp_session.call_tool(
                "execute_hg_ddl_sql",
                {"query": f"DROP TABLE IF EXISTS public.{table_name}"}
            )

    async def test_dml_non_dml_rejected(self, mcp_session: ClientSession):
        """Test that non-DML statements are rejected."""
        result = await mcp_session.call_tool(
            "execute_hg_dml_sql",
            {"query": "SELECT 1"}
        )

        assert result is not None
        assert hasattr(result, "isError")
        assert result.isError is True


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling and edge cases."""

    async def test_invalid_resource_uri(self, mcp_session: ClientSession):
        """Test reading an invalid resource URI."""
        from mcp.shared.exceptions import McpError

        with pytest.raises(McpError) as exc_info:
            await mcp_session.read_resource("invalid:///uri")

        # Should raise an McpError with "Unknown resource" message
        assert "Unknown resource" in str(exc_info.value) or "unknown" in str(exc_info.value).lower()

    async def test_tool_with_missing_required_params(
        self, mcp_session: ClientSession
    ):
        """Test calling a tool without required parameters."""
        result = await mcp_session.call_tool(
            "execute_hg_select_sql",
            {}  # Missing 'query' parameter
        )

        assert result is not None
        assert hasattr(result, "isError")
        assert result.isError is True

    async def test_system_info_invalid_guc(self, mcp_session: ClientSession):
        """Test reading an invalid GUC value."""
        result = await mcp_session.read_resource(
            "system:///guc_value/nonexistent_guc_xyz_12345"
        )

        assert result is not None
        # Should handle the error gracefully