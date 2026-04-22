"""
Tests for server.main() function.
"""

from unittest.mock import patch

import pytest


class TestServerMain:
    """Tests for server.main() function."""

    def test_main_default_stdio(self):
        """Test that server.main() defaults to stdio transport."""
        from hologres_mcp_server.server import main

        with patch("hologres_mcp_server.server.app") as mock_app, patch("sys.argv", ["hologres-mcp-server"]):
            main()
            mock_app.run.assert_called_once_with(transport="stdio")

    def test_main_streamable_http(self):
        """Test that --transport streamable-http passes correct args."""
        from hologres_mcp_server.server import main

        with (
            patch("hologres_mcp_server.server.app") as mock_app,
            patch("sys.argv", ["hologres-mcp-server", "--transport", "streamable-http"]),
        ):
            main()
            mock_app.run.assert_called_once_with(transport="streamable-http", host="127.0.0.1", port=8000)

    def test_main_streamable_http_custom_host_port(self):
        """Test that custom host and port are passed through."""
        from hologres_mcp_server.server import main

        with (
            patch("hologres_mcp_server.server.app") as mock_app,
            patch(
                "sys.argv",
                ["hologres-mcp-server", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "9000"],
            ),
        ):
            main()
            mock_app.run.assert_called_once_with(transport="streamable-http", host="0.0.0.0", port=9000)

    def test_main_sse_transport(self):
        """Test that --transport sse passes correct args."""
        from hologres_mcp_server.server import main

        with (
            patch("hologres_mcp_server.server.app") as mock_app,
            patch("sys.argv", ["hologres-mcp-server", "--transport", "sse"]),
        ):
            main()
            mock_app.run.assert_called_once_with(transport="sse", host="127.0.0.1", port=8000)

    def test_main_invalid_transport(self):
        """Test that an invalid transport value raises SystemExit."""
        from hologres_mcp_server.server import main

        with patch("sys.argv", ["hologres-mcp-server", "--transport", "invalid"]):
            with pytest.raises(SystemExit):
                main()

    def test_main_propagates_exceptions(self):
        """Test that exceptions from app.run() are propagated."""
        from hologres_mcp_server.server import main

        with patch("hologres_mcp_server.server.app") as mock_app, patch("sys.argv", ["hologres-mcp-server"]):
            mock_app.run.side_effect = RuntimeError("App failed to start")
            with pytest.raises(RuntimeError, match="App failed to start"):
                main()
