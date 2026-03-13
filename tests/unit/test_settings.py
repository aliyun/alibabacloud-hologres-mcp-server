"""
Tests for settings module.
"""

import os
import pytest
from unittest.mock import patch

from hologres_mcp_server.settings import get_db_config, SERVER_VERSION


class TestServerVersion:
    """Tests for SERVER_VERSION constant."""

    def test_server_version_is_string(self):
        """SERVER_VERSION should be a string."""
        assert isinstance(SERVER_VERSION, str)

    def test_server_version_format(self):
        """SERVER_VERSION should follow semantic versioning format."""
        import re
        assert re.match(r"^\d+\.\d+\.\d+$", SERVER_VERSION)


class TestGetDbConfig:
    """Tests for get_db_config function."""

    # === Normal Scenarios ===

    def test_get_db_config_normal(self, mock_env_basic):
        """Test normal environment variable configuration."""
        config = get_db_config()

        assert config["host"] == "test-host.hologres.aliyuncs.com"
        assert config["port"] == "80"
        assert config["user"] == "test_user"
        assert config["password"] == "test_password"
        assert config["dbname"] == "test_db"
        assert config["options"] is None
        assert "hologres-mcp-server" in config["application_name"]

    def test_get_db_config_with_sts_token(self, mock_env_with_sts_token):
        """Test STS Token configuration using Alibaba Cloud credentials."""
        config = get_db_config()

        assert config["user"] == "test_access_key"
        assert config["password"] == "test_secret"
        assert config["options"] == "sts_token=test_sts_token"

    def test_get_db_config_alibaba_cloud_credentials(self, mock_env_alibaba_cloud):
        """Test using Alibaba Cloud AK/SK credentials without STS token."""
        config = get_db_config()

        assert config["user"] == "test_access_key"
        assert config["password"] == "test_secret"
        assert config["options"] is None

    # === Exception Scenarios ===

    def test_get_db_config_missing_user(self, mock_env_missing_user):
        """Test missing user raises ValueError."""
        with pytest.raises(ValueError, match="Missing required database configuration"):
            get_db_config()

    def test_get_db_config_missing_password(self, mock_env_missing_password):
        """Test missing password raises ValueError."""
        with pytest.raises(ValueError, match="Missing required database configuration"):
            get_db_config()

    def test_get_db_config_missing_database(self, mock_env_missing_database):
        """Test missing database name raises ValueError."""
        with pytest.raises(ValueError, match="Missing required database configuration"):
            get_db_config()

    def test_get_db_config_missing_all_required(self):
        """Test missing all required configuration raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing required database configuration"):
                get_db_config()

    # === Boundary Cases ===

    def test_get_db_config_default_values(self, mock_env_minimal):
        """Test default values for host (localhost) and port (5432)."""
        config = get_db_config()

        assert config["host"] == "localhost"
        assert config["port"] == "5432"

    def test_get_db_config_custom_port(self):
        """Test custom port configuration."""
        env = {
            "HOLOGRES_HOST": "custom-host.hologres.aliyuncs.com",
            "HOLOGRES_PORT": "5433",
            "HOLOGRES_USER": "test_user",
            "HOLOGRES_PASSWORD": "test_password",
            "HOLOGRES_DATABASE": "test_db",
        }
        with patch.dict(os.environ, env, clear=True):
            config = get_db_config()
            assert config["port"] == "5433"

    def test_get_db_config_priority_hologres_credentials(self):
        """Test that HOLOGRES_* credentials take priority over ALIBABA_CLOUD_*."""
        env = {
            "HOLOGRES_HOST": "hologres-host",
            "HOLOGRES_PORT": "80",
            "HOLOGRES_USER": "hologres_user",
            "HOLOGRES_PASSWORD": "hologres_password",
            "HOLOGRES_DATABASE": "test_db",
            "ALIBABA_CLOUD_ACCESS_KEY_ID": "alibaba_key",
            "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "alibaba_secret",
        }
        with patch.dict(os.environ, env, clear=True):
            config = get_db_config()
            assert config["user"] == "hologres_user"
            assert config["password"] == "hologres_password"
            assert config["options"] is None

    def test_get_db_config_application_name_format(self, mock_env_basic):
        """Test application_name format includes version."""
        config = get_db_config()

        assert config["application_name"] == f"hologres-mcp-server-{SERVER_VERSION}"

    def test_get_db_config_empty_string_values(self):
        """Test that empty string values are treated as missing."""
        env = {
            "HOLOGRES_HOST": "",
            "HOLOGRES_PORT": "",
            "HOLOGRES_USER": "",
            "HOLOGRES_PASSWORD": "",
            "HOLOGRES_DATABASE": "",
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="Missing required database configuration"):
                get_db_config()

    def test_get_db_config_whitespace_values(self):
        """Test handling of whitespace-only values."""
        env = {
            "HOLOGRES_HOST": "  ",
            "HOLOGRES_PORT": "  ",
            "HOLOGRES_USER": "  ",
            "HOLOGRES_PASSWORD": "  ",
            "HOLOGRES_DATABASE": "test_db",
        }
        with patch.dict(os.environ, env, clear=True):
            # Whitespace values are truthy, so this should pass
            config = get_db_config()
            assert config["user"] == "  "