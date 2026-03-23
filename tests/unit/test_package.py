"""
Tests for package entry point in __init__.py.
"""

import pytest
from unittest.mock import patch, MagicMock


class TestPackageMain:
    """Tests for package main() function."""

    def test_main_calls_server_main(self):
        """Test that package.main() calls server.main()."""
        from hologres_mcp_server import main

        with patch("hologres_mcp_server.server.main") as mock_server_main:
            main()
            mock_server_main.assert_called_once()

    def test_main_propagates_exceptions(self):
        """Test that exceptions from server.main() are propagated."""
        from hologres_mcp_server import main

        with patch("hologres_mcp_server.server.main", side_effect=RuntimeError("Server error")):
            with pytest.raises(RuntimeError, match="Server error"):
                main()

    def test_main_module_all_exports(self):
        """Test that __all__ exports are correct."""
        import hologres_mcp_server

        assert "main" in hologres_mcp_server.__all__
        assert "server" in hologres_mcp_server.__all__
