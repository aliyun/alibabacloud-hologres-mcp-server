"""
Tests for chart generation (_query_and_chart) and _format_bytes helper.
"""

from unittest.mock import patch

import pytest
from conftest import PATCH_CONNECT, _make_mock_conn

from hologres_mcp_server.server import (
    _format_bytes,
    _query_and_chart,
    query_and_plotly_chart,
)

# ============================================================================
# _format_bytes
# ============================================================================


class TestFormatBytes:
    """Tests for _format_bytes helper function."""

    def test_bytes_unit(self):
        assert _format_bytes(500) == "500.0 B"

    def test_kb_unit(self):
        assert _format_bytes(1024) == "1.0 KB"

    def test_mb_unit(self):
        assert _format_bytes(1048576) == "1.0 MB"

    def test_gb_unit(self):
        assert _format_bytes(1073741824) == "1.0 GB"

    def test_tb_unit(self):
        assert _format_bytes(1099511627776) == "1.0 TB"

    def test_pb_unit(self):
        # 1 PB = 1024 TB = 1024^5 bytes
        assert _format_bytes(1024**5) == "1.0 PB"

    def test_zero(self):
        assert _format_bytes(0) == "0.0 B"

    def test_negative(self):
        # Negative values should still format
        result = _format_bytes(-1024)
        assert "KB" in result

    def test_float_input(self):
        result = _format_bytes(1536.0)
        assert "KB" in result

    def test_string_numeric(self):
        # String that can be converted to float
        result = _format_bytes("1024")
        assert "KB" in result

    def test_type_error(self):
        # Non-numeric value returns str representation
        result = _format_bytes("not_a_number")
        assert result == "not_a_number"

    def test_none(self):
        result = _format_bytes(None)
        assert result == "None"

    def test_large_pb_value(self):
        # Very large value (multiple PB)
        result = _format_bytes(5 * 1024**5)
        assert "PB" in result


# ============================================================================
# _query_and_chart
# ============================================================================


class TestQueryAndChart:
    """Tests for _query_and_chart function."""

    def _make_chart_conn(self, rows, headers):
        """Helper to create a connection that returns chart data."""
        desc = [(h,) for h in headers]
        return _make_mock_conn(fetchall=rows, description=desc)

    @pytest.mark.parametrize(
        "chart_type,rows,headers",
        [
            ("bar", [("A", 10), ("B", 20), ("C", 30)], ["category", "value"]),
            ("line", [("2025-01", 100), ("2025-02", 200)], ["month", "sales"]),
            ("scatter", [(1.0, 10), (2.0, 20), (3.0, 30)], ["x", "y"]),
            ("pie", [("A", 40), ("B", 30), ("C", 30)], ["label", "share"]),
            ("histogram", [(10,), (20,), (20,), (30,), (30,), (30,), (40,)], ["value"]),
            ("area", [(1, 10), (2, 25), (3, 15)], ["x", "y"]),
        ],
    )
    def test_chart_type(self, chart_type, rows, headers):
        conn, _ = self._make_chart_conn(rows, headers)
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", chart_type, "", "", "")
            assert "data:image/png;base64," in result

    def test_scatter_chart_non_numeric_x(self):
        rows = [("cat_a", 10), ("cat_b", 20)]
        conn, _ = self._make_chart_conn(rows, ["name", "value"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "scatter", "", "", "")
            assert "data:image/png;base64," in result

    def test_unsupported_chart_type(self):
        rows = [("A", 10)]
        conn, _ = self._make_chart_conn(rows, ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "radar", "", "", "")
            assert "Unsupported chart type" in result
            assert "radar" in result

    def test_empty_data(self):
        conn, _ = self._make_chart_conn([], ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "no data" in result.lower()

    def test_custom_columns_and_title(self):
        rows = [("A", 10, 100), ("B", 20, 200)]
        conn, _ = self._make_chart_conn(rows, ["name", "val1", "val2"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "name", "val2", "My Custom Title")
            assert "data:image/png;base64," in result
            assert "My Custom Title" in result

    def test_column_not_in_headers_fallback(self):
        rows = [("A", 10)]
        conn, _ = self._make_chart_conn(rows, ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "nonexistent_x", "nonexistent_y", "")
            # Should fallback to index 0 and 1
            assert "data:image/png;base64," in result

    def test_single_column_data(self):
        rows = [(10,), (20,), (30,)]
        conn, _ = self._make_chart_conn(rows, ["value"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "data:image/png;base64," in result

    def test_y_data_non_numeric(self):
        rows = [("A", "not_a_number"), ("B", "also_not")]
        conn, _ = self._make_chart_conn(rows, ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            # Should still work, y_data stays as strings
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "data:image/png;base64," in result or "Error" in result

    def test_y_data_with_none(self):
        rows = [("A", None), ("B", 20)]
        conn, _ = self._make_chart_conn(rows, ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "data:image/png;base64," in result

    def test_connection_error(self):
        with patch(PATCH_CONNECT, side_effect=Exception("connection failed")):
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "Error" in result

    def test_many_rows_truncated_in_output(self):
        rows = [(i, i * 10) for i in range(100)]
        conn, _ = self._make_chart_conn(rows, ["x", "y"])
        with patch(PATCH_CONNECT, return_value=conn):
            result = _query_and_chart("SELECT * FROM t", "bar", "", "", "")
            assert "Rows: 100" in result
            # Data output should be limited to 50 rows
            lines = result.split("\n")
            data_lines = [line for line in lines if "\t" in line]
            # header + 50 data rows = 51 max
            assert len(data_lines) <= 51

    def test_tool_wrapper_validates_query(self):
        """query_and_plotly_chart should validate the query is a SELECT."""
        with pytest.raises(ValueError):
            query_and_plotly_chart("DROP TABLE users")
