"""
Tests for server.main() function.
"""

from unittest.mock import patch

import pytest


class TestServerMain:
    """Tests for server.main() function."""

    def test_main_calls_app_run(self):
        """Test that server.main() calls app.run()."""
        from hologres_mcp_server.server import main

        with patch("hologres_mcp_server.server.app") as mock_app:
            main()
            mock_app.run.assert_called_once()

    def test_main_propagates_exceptions(self):
        """Test that exceptions from app.run() are propagated."""
        from hologres_mcp_server.server import main

        with patch("hologres_mcp_server.server.app") as mock_app:
            mock_app.run.side_effect = RuntimeError("App failed to start")
            with pytest.raises(RuntimeError, match="App failed to start"):
                main()

    def test_main_no_arguments(self):
        """Test that main() can be called without arguments."""
        from hologres_mcp_server.server import main

        with patch("hologres_mcp_server.server.app") as mock_app:
            # Should not raise any errors
            main()
            mock_app.run.assert_called_once_with()
