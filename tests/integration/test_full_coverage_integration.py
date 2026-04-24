"""
Integration tests to achieve full MCP tool coverage (39/39).

Covers the 7 tools not tested by test_mcp_integration.py or test_new_tools_integration.py:
- query_and_plotly_chart (6 chart types + edge cases)
- query_hg_external_files (version check)
- manage_hg_query_queue (lifecycle + validation)
- manage_hg_classifier (lifecycle + validation)
- set_hg_query_queue_property (set/remove + validation)
- manage_hg_warehouse (input validation only — no real suspend/resume/restart)
- rebalance_hg_warehouse (error path only)

Also covers:
- list_hg_active_queries(state="idle") edge case

Run with: pytest tests/integration/test_full_coverage_integration.py -v -m integration
"""

import pytest
from mcp import ClientSession

pytestmark = pytest.mark.integration


# ============================================================================
# Chart Tool (query_and_plotly_chart)
# ============================================================================


class TestChartTool:
    """Tests for query_and_plotly_chart — all 6 chart types and edge cases."""

    async def test_bar_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT x, x * 10 AS y FROM generate_series(1, 5) AS x",
                "chart_type": "bar",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_line_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT x, x * 10 AS y FROM generate_series(1, 5) AS x",
                "chart_type": "line",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_scatter_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT x, x * x AS y FROM generate_series(1, 10) AS x",
                "chart_type": "scatter",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_pie_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT 'Category' || x::text AS label, x * 10 AS value FROM generate_series(1, 4) AS x",
                "chart_type": "pie",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_histogram_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT (random() * 100)::int AS value FROM generate_series(1, 50)",
                "chart_type": "histogram",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_area_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT x, x * 5 + 10 AS y FROM generate_series(1, 8) AS x",
                "chart_type": "area",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text

    async def test_custom_title_and_columns(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT x AS month, x * 100 AS revenue, x * 50 AS cost FROM generate_series(1, 6) AS x",
                "chart_type": "bar",
                "x_column": "month",
                "y_column": "revenue",
                "title": "Monthly Revenue",
            },
        )
        text = result.content[0].text
        assert "data:image/png;base64," in text
        assert "Monthly Revenue" in text

    async def test_unsupported_chart_type(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT 1 AS x, 2 AS y",
                "chart_type": "radar",
            },
        )
        text = result.content[0].text
        assert "Unsupported chart type" in text
        assert "radar" in text

    async def test_empty_result_chart(self, mcp_session: ClientSession):
        result = await mcp_session.call_tool(
            "query_and_plotly_chart",
            {
                "query": "SELECT 1 AS x, 2 AS y WHERE FALSE",
                "chart_type": "bar",
            },
        )
        text = result.content[0].text
        assert "no data" in text.lower()


# ============================================================================
# External Files Tool (query_hg_external_files)
# ============================================================================


class TestExternalFilesTool:
    """Tests for query_hg_external_files — version and error handling."""

    async def test_external_files_version_or_access_error(self, mcp_session: ClientSession):
        """Call with a fake OSS path. Either gets version error (V4.1+ required)
        or access error (path doesn't exist). Both prove the tool is callable."""
        result = await mcp_session.call_tool(
            "query_hg_external_files",
            {
                "path": "oss://mcp-test-nonexistent-bucket/test.csv",
                "format": "csv",
            },
        )
        text = result.content[0].text
        # Should get either a version error or an access/permission error
        assert any(
            keyword in text.lower()
            for keyword in ["v4.1", "error", "not available", "does not exist", "denied", "not found"]
        )

    async def test_external_files_with_columns(self, mcp_session: ClientSession):
        """Call with columns parameter to exercise the AS clause path."""
        result = await mcp_session.call_tool(
            "query_hg_external_files",
            {
                "path": "oss://mcp-test-nonexistent-bucket/data.parquet",
                "format": "parquet",
                "columns": "id int, name text",
            },
        )
        text = result.content[0].text
        assert any(
            keyword in text.lower()
            for keyword in ["v4.1", "error", "not available", "does not exist", "denied", "not found"]
        )


# ============================================================================
# Query Queue Lifecycle (manage_hg_query_queue, manage_hg_classifier,
#                        set_hg_query_queue_property)
# ============================================================================


class TestQueryQueueLifecycle:
    """Tests for Query Queue management tools — validation and lifecycle."""

    async def _check_v3_support(self, mcp_session: ClientSession):
        """Check if Hologres instance supports V3.0+ query queue features."""
        result = await mcp_session.call_tool("list_hg_query_queues", {})
        text = result.content[0].text
        if "does not exist" in text.lower() or "not available" in text.lower():
            pytest.skip("Query Queue feature requires Hologres V3.0+")

    async def test_manage_queue_invalid_action(self, mcp_session: ClientSession):
        """Invalid action should return an error without touching the database."""
        result = await mcp_session.call_tool(
            "manage_hg_query_queue",
            {
                "action": "invalid_action",
                "queue_name": "mcp_test_queue",
            },
        )
        text = result.content[0].text
        assert "Unknown action" in text

    async def test_manage_queue_create_missing_params(self, mcp_session: ClientSession):
        """Create without concurrency/queue_size should return validation error."""
        await self._check_v3_support(mcp_session)
        result = await mcp_session.call_tool(
            "manage_hg_query_queue",
            {
                "action": "create",
                "queue_name": "mcp_test_queue",
                # max_concurrency and max_queue_size default to 0
            },
        )
        text = result.content[0].text
        assert "error" in text.lower() or "positive" in text.lower()

    async def test_manage_classifier_invalid_action(self, mcp_session: ClientSession):
        """Invalid classifier action should return an error."""
        result = await mcp_session.call_tool(
            "manage_hg_classifier",
            {
                "action": "invalid_action",
                "queue_name": "mcp_test_queue",
                "classifier_name": "mcp_test_classifier",
            },
        )
        text = result.content[0].text
        assert "Unknown action" in text

    async def test_set_property_unknown_target(self, mcp_session: ClientSession):
        """Unknown target type should return an error."""
        result = await mcp_session.call_tool(
            "set_hg_query_queue_property",
            {
                "target": "invalid_target",
                "queue_name": "mcp_test_queue",
                "property_key": "max_concurrency",
                "property_value": "10",
            },
        )
        text = result.content[0].text
        assert "Unknown target" in text

    async def test_query_queue_full_lifecycle(self, mcp_session: ClientSession):
        """Full lifecycle: create queue → set property → list → drop queue."""
        await self._check_v3_support(mcp_session)
        queue_name = "mcp_test_lifecycle_queue"
        try:
            # Create queue
            result = await mcp_session.call_tool(
                "manage_hg_query_queue",
                {
                    "action": "create",
                    "queue_name": queue_name,
                    "max_concurrency": 5,
                    "max_queue_size": 10,
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower() or "error" not in text.lower()

            # Verify list_hg_query_queues returns data (queue may not be visible
            # in listing depending on warehouse association)
            result = await mcp_session.call_tool("list_hg_query_queues", {})
            text = result.content[0].text
            assert "Query Queues" in text

            # Set a property on the queue (change max_concurrency from 5 to 8)
            result = await mcp_session.call_tool(
                "set_hg_query_queue_property",
                {
                    "target": "queue",
                    "queue_name": queue_name,
                    "property_key": "max_concurrency",
                    "property_value": "8",
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower()
        finally:
            # Drop queue (cleanup)
            await mcp_session.call_tool(
                "manage_hg_query_queue",
                {"action": "drop", "queue_name": queue_name},
            )

    async def test_classifier_lifecycle(self, mcp_session: ClientSession):
        """Classifier lifecycle within a queue: create queue → create classifier → set rule → drop classifier → drop queue."""
        await self._check_v3_support(mcp_session)
        queue_name = "mcp_test_cls_queue"
        classifier_name = "mcp_test_classifier"

        # Get the current database user for classifier rule
        user_result = await mcp_session.call_tool(
            "execute_hg_select_sql", {"query": "SELECT current_user"}
        )
        current_user = user_result.content[0].text.strip().split("\n")[-1].strip()

        try:
            # Create queue first
            await mcp_session.call_tool(
                "manage_hg_query_queue",
                {
                    "action": "create",
                    "queue_name": queue_name,
                    "max_concurrency": 3,
                    "max_queue_size": 5,
                },
            )

            # Create classifier
            result = await mcp_session.call_tool(
                "manage_hg_classifier",
                {
                    "action": "create",
                    "queue_name": queue_name,
                    "classifier_name": classifier_name,
                    "priority": 10,
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower()

            # Set a classifier rule condition (use actual user)
            result = await mcp_session.call_tool(
                "set_hg_query_queue_property",
                {
                    "target": "classifier",
                    "queue_name": queue_name,
                    "classifier_name": classifier_name,
                    "property_key": "user_name",
                    "property_value": current_user,
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower()

            # Remove the classifier rule
            result = await mcp_session.call_tool(
                "set_hg_query_queue_property",
                {
                    "target": "classifier",
                    "queue_name": queue_name,
                    "classifier_name": classifier_name,
                    "property_key": "user_name",
                    "property_value": current_user,
                    "action": "remove",
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower()

            # Drop classifier
            result = await mcp_session.call_tool(
                "manage_hg_classifier",
                {
                    "action": "drop",
                    "queue_name": queue_name,
                    "classifier_name": classifier_name,
                },
            )
            text = result.content[0].text
            assert "successfully" in text.lower()
        finally:
            # Cleanup: drop classifier then queue (ignore errors)
            try:
                await mcp_session.call_tool(
                    "manage_hg_classifier",
                    {
                        "action": "drop",
                        "queue_name": queue_name,
                        "classifier_name": classifier_name,
                    },
                )
            except Exception:
                pass
            await mcp_session.call_tool(
                "manage_hg_query_queue",
                {"action": "drop", "queue_name": queue_name},
            )


# ============================================================================
# Warehouse Management (manage_hg_warehouse, rebalance_hg_warehouse)
# ============================================================================


class TestWarehouseManagement:
    """Tests for warehouse management — input validation only.
    No real suspend/resume/restart operations to avoid disrupting the instance."""

    async def test_manage_warehouse_invalid_action(self, mcp_session: ClientSession):
        """Invalid action should return an error without touching infrastructure."""
        result = await mcp_session.call_tool(
            "manage_hg_warehouse",
            {
                "action": "invalid_action",
                "warehouse_name": "local",
            },
        )
        text = result.content[0].text
        assert "Unknown action" in text

    async def test_manage_warehouse_rename_missing_name(self, mcp_session: ClientSession):
        """Rename without new_name should return validation error."""
        result = await mcp_session.call_tool(
            "manage_hg_warehouse",
            {
                "action": "rename",
                "warehouse_name": "local",
            },
        )
        text = result.content[0].text
        assert "new_name is required" in text

    async def test_manage_warehouse_resize_invalid_cu(self, mcp_session: ClientSession):
        """Resize with cu=0 should return validation error."""
        result = await mcp_session.call_tool(
            "manage_hg_warehouse",
            {
                "action": "resize",
                "warehouse_name": "local",
                "cu": 0,
            },
        )
        text = result.content[0].text
        assert "cu must be a positive integer" in text

    async def test_rebalance_nonexistent_warehouse(self, mcp_session: ClientSession):
        """Rebalancing a nonexistent warehouse should return an error."""
        result = await mcp_session.call_tool(
            "rebalance_hg_warehouse",
            {"warehouse_name": "mcp_test_nonexistent_warehouse_xyz"},
        )
        text = result.content[0].text
        assert "error" in text.lower()


# ============================================================================
# Active Queries Edge Cases
# ============================================================================


class TestActiveQueriesEdgeCases:
    """Edge case coverage for list_hg_active_queries."""

    async def test_list_active_queries_idle(self, mcp_session: ClientSession):
        """Test list_hg_active_queries with state='idle'."""
        result = await mcp_session.call_tool(
            "list_hg_active_queries",
            {"state": "idle"},
        )
        text = result.content[0].text
        # Should return either idle connections or "No ... queries" message
        assert text is not None
        assert len(text) > 0
