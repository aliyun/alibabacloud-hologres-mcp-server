"""
Pytest configuration and shared fixtures for Hologres MCP Server tests.
"""

import os
import sys
import pytest
import pytest_asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch
from dotenv import load_dotenv


# ============================================================================
# Async Support Configuration
# ============================================================================

# Ensure pytest-asyncio works correctly
pytest_plugins = ('pytest_asyncio',)


# ============================================================================
# Environment Fixtures (for Unit Tests)
# ============================================================================

@pytest.fixture
def mock_env_basic():
    """Basic environment variables for testing."""
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_USER": "test_user",
        "HOLOGRES_PASSWORD": "test_password",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_with_sts_token():
    """Environment variables with STS token for testing."""
    env = {
        "ALIBABA_CLOUD_ACCESS_KEY_ID": "test_access_key",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "test_secret",
        "ALIBABA_CLOUD_SECURITY_TOKEN": "test_sts_token",
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_alibaba_cloud():
    """Environment variables with Alibaba Cloud credentials (no STS token)."""
    env = {
        "ALIBABA_CLOUD_ACCESS_KEY_ID": "test_access_key",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "test_secret",
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_missing_user():
    """Environment variables missing user credential."""
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_PASSWORD": "test_password",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_missing_password():
    """Environment variables missing password."""
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_USER": "test_user",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_missing_database():
    """Environment variables missing database name."""
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_USER": "test_user",
        "HOLOGRES_PASSWORD": "test_password",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_minimal():
    """Minimal environment variables (using defaults)."""
    env = {
        "HOLOGRES_USER": "test_user",
        "HOLOGRES_PASSWORD": "test_password",
        "HOLOGRES_DATABASE": "test_db",
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


# ============================================================================
# Database Connection Mocks
# ============================================================================

@pytest.fixture
def mock_cursor():
    """Create a mock database cursor."""
    cursor = MagicMock()
    cursor.description = [
        ("column1",),
        ("column2",),
    ]
    cursor.fetchall.return_value = [
        ("value1", "value2"),
        ("value3", "value4"),
    ]
    cursor.rowcount = 2
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """Create a mock database connection."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.autocommit = True
    return conn


@pytest.fixture
def mock_psycopg_connect(mock_connection):
    """Mock psycopg.connect to return mock connection."""
    with patch("psycopg.connect", return_value=mock_connection) as mock:
        yield mock


@pytest.fixture
def mock_connect_with_retry(mock_connection):
    """Mock connect_with_retry function."""
    with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_connection) as mock:
        yield mock


# ============================================================================
# Query Result Fixtures
# ============================================================================

@pytest.fixture
def sample_select_result():
    """Sample SELECT query result."""
    return [
        ("row1_col1", "row1_col2"),
        ("row2_col1", "row2_col2"),
    ]


@pytest.fixture
def sample_headers():
    """Sample column headers."""
    return ["col1", "col2"]


@pytest.fixture
def sample_schema_list():
    """Sample schema list result."""
    return [("public",), ("test_schema",)]


@pytest.fixture
def sample_table_list():
    """Sample table list result."""
    return [
        ("users", " (table)"),
        ("orders", " (view)"),
        ("external_data", " (foreign table)"),
    ]


# ============================================================================
# Tool Result Fixtures
# ============================================================================

@pytest.fixture
def expected_select_result_format():
    """Expected format for SELECT tool result."""
    return "column1,column2\nvalue1,value2\nvalue3,value4"


@pytest.fixture
def expected_dml_result_format():
    """Expected format for DML tool result."""
    return "Query executed successfully. 2 rows affected."


# ============================================================================
# Integration Test Fixtures
# ============================================================================

@pytest.fixture
def integration_env():
    """
    Load integration test environment variables.
    Skip tests if required variables are missing.

    Required environment variables:
    - HOLOGRES_HOST: Hologres instance host
    - HOLOGRES_PORT: Hologres instance port
    - HOLOGRES_USER: Database user
    - HOLOGRES_PASSWORD: Database password
    - HOLOGRES_DATABASE: Database name
    """
    # Try to load from .test_mcp_client_env file
    env_path = Path(__file__).parent / "integration" / ".test_mcp_client_env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    required_vars = [
        "HOLOGRES_HOST",
        "HOLOGRES_PORT",
        "HOLOGRES_USER",
        "HOLOGRES_PASSWORD",
        "HOLOGRES_DATABASE",
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        pytest.skip(
            f"Missing required environment variables for integration tests: {', '.join(missing_vars)}. "
            f"Create tests/integration/.test_mcp_client_env file with the required variables."
        )

    return {var: os.getenv(var) for var in required_vars}


@pytest.fixture
def mcp_server_params(integration_env):
    """
    MCP server connection parameters for integration tests.
    Returns StdioServerParameters configured with Hologres credentials.
    """
    from mcp import ClientSession, StdioServerParameters

    # Get the server script path
    server_script = str(
        Path(__file__).parent.parent / "src" / "hologres_mcp_server" / "server.py"
    )

    # Hologres database connection environment variables
    hologres_env = {
        "HOLOGRES_HOST": integration_env["HOLOGRES_HOST"],
        "HOLOGRES_PORT": integration_env["HOLOGRES_PORT"],
        "HOLOGRES_USER": integration_env["HOLOGRES_USER"],
        "HOLOGRES_PASSWORD": integration_env["HOLOGRES_PASSWORD"],
        "HOLOGRES_DATABASE": integration_env["HOLOGRES_DATABASE"],
    }

    return StdioServerParameters(
        command=sys.executable,  # Use current Python interpreter
        args=[server_script],
        env=hologres_env,
    )


@pytest_asyncio.fixture
async def mcp_session(mcp_server_params):
    """
    Async MCP client session fixture for integration tests.
    Automatically handles session initialization and cleanup.

    Note: This fixture manually manages context manager cleanup to avoid the
    "Attempted to exit cancel scope in a different task than it was entered in"
    error that can occur with pytest-asyncio and anyio compatibility issues.
    """
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client

    # Enter the context managers manually
    cm1 = stdio_client(mcp_server_params)
    read, write = await cm1.__aenter__()
    cm2 = ClientSession(read, write)
    session = await cm2.__aenter__()
    await session.initialize()

    yield session

    # Manual cleanup with error handling
    try:
        await cm2.__aexit__(None, None, None)
    except RuntimeError as e:
        if "cancel scope" not in str(e).lower():
            raise
    except Exception:
        pass  # Ignore other teardown errors

    try:
        await cm1.__aexit__(None, None, None)
    except RuntimeError as e:
        if "cancel scope" not in str(e).lower():
            raise
    except Exception:
        pass  # Ignore other teardown errors


@pytest.fixture
def test_schema():
    """Test schema name for integration tests, can be overridden via environment variable."""
    return os.getenv("HOLOGRES_TEST_SCHEMA", "public")


@pytest.fixture
def test_table():
    """Test table name for integration tests, can be overridden via environment variable."""
    return os.getenv("HOLOGRES_TEST_TABLE")


@pytest.fixture
def integration_test_prefix():
    """Prefix for test objects created during integration tests.
    Used to identify and clean up test tables, views, etc.
    """
    return "mcp_test_"


# ============================================================================
# Edge Case Fixtures for Extended Unit Tests
# ============================================================================

@pytest.fixture
def mock_env_with_long_names():
    """Environment variables with very long names for testing edge cases."""
    long_name = "a" * 1000  # 1000 character name
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_USER": "test_user",
        "HOLOGRES_PASSWORD": "test_password",
        "HOLOGRES_DATABASE": "test_db",
        "HOLOGRES_TEST_LONG_SCHEMA": long_name,
        "HOLOGRES_TEST_LONG_TABLE": long_name,
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_env_with_unicode():
    """Environment variables with Unicode characters for testing."""
    env = {
        "HOLOGRES_HOST": "test-host.hologres.aliyuncs.com",
        "HOLOGRES_PORT": "80",
        "HOLOGRES_USER": "测试用户",  # Chinese characters
        "HOLOGRES_PASSWORD": "パスワード123",  # Japanese characters
        "HOLOGRES_DATABASE": "test_db_αβγ",  # Greek letters
    }
    with patch.dict(os.environ, env, clear=True):
        yield env


@pytest.fixture
def mock_cursor_with_nulls():
    """Create a mock cursor that returns NULL values in results."""
    cursor = MagicMock()
    cursor.description = [
        ("col1",),
        ("col2",),
        ("col3",),
    ]
    cursor.fetchall.return_value = [
        ("value1", None, "value3"),
        (None, None, None),
        ("value1", "value2", None),
    ]
    cursor.rowcount = 3
    return cursor


@pytest.fixture
def mock_cursor_with_large_result():
    """Create a mock cursor that returns a large number of rows and columns."""
    # Create 100 columns
    columns = [f"col{i}" for i in range(100)]
    cursor = MagicMock()
    cursor.description = [(col,) for col in columns]

    # Create 1000 rows with 100 columns each
    rows = [tuple(f"value_{i}_{j}" for j in range(100)) for i in range(1000)]
    cursor.fetchall.return_value = rows
    cursor.rowcount = 1000
    return cursor


@pytest.fixture
def sql_injection_payloads():
    """Common SQL injection payloads for security testing."""
    return [
        # Basic injection attempts
        "'; DROP TABLE users; --",
        "1; DROP TABLE users",
        "' OR '1'='1",
        "' OR '1'='1' --",
        "1' OR '1'='1",
        "admin'--",
        "' UNION SELECT * FROM users --",

        # Comment-based injection
        "/* comment */",
        "-- comment",
        "# comment",

        # Stacked queries
        "SELECT * FROM users; DROP TABLE users;",

        # Time-based injection
        "'; WAITFOR DELAY '0:0:5' --",
        "'; SELECT SLEEP(5) --",
        "1; SELECT pg_sleep(5);",

        # Union-based injection
        "' UNION SELECT NULL --",
        "' UNION SELECT NULL, NULL --",
        "' UNION ALL SELECT NULL --",

        # Boolean-based injection
        "' AND 1=1 --",
        "' AND 1=2 --",
        "' OR 1=1 --",

        # Function-based injection
        "'; EXEC xp_cmdshell('dir') --",
        "'; SELECT * FROM information_schema.tables --",

        # Schema/table name injection
        "users; DROP TABLE users",
        "users' OR '1'='1",
        "users; DELETE FROM users WHERE '1'='1",

        # Numeric injection
        "1 OR 1=1",
        "1; DROP TABLE users",
        "1 AND 1=1",
    ]


@pytest.fixture
def edge_case_schema_names():
    """Edge case schema names for boundary testing."""
    return [
        "",  # Empty string
        "   ",  # Whitespace only
        "a",  # Single character
        "a" * 63,  # Maximum PostgreSQL identifier length
        "a" * 64,  # Exceeds maximum length
        "schema-with-dashes",
        "schema_with_underscores",
        "schema.with.dots",
        "schema\"with\"quotes",
        "schema'with'apostrophes",
        "schema with spaces",
        "schema;with;semicolons",
        "UPPERCASE",
        "MixedCase",
        "123numbers",
        "_underscore_start",
        "测试schema",  # Unicode
        "📝emoji",  # Emoji
    ]


@pytest.fixture
def mock_cursor_with_very_long_values():
    """Create a mock cursor that returns very long string values."""
    long_value = "x" * (1024 * 1024)  # 1MB string
    cursor = MagicMock()
    cursor.description = [("col1",), ("col2",)]
    cursor.fetchall.return_value = [
        (long_value, "normal_value"),
        ("normal_value", long_value),
    ]
    cursor.rowcount = 2
    return cursor


@pytest.fixture
def mock_cursor_with_binary_data():
    """Create a mock cursor that returns binary data."""
    cursor = MagicMock()
    cursor.description = [("binary_col",), ("text_col",)]
    # Binary data with null bytes
    binary_data = bytes([0, 1, 2, 3, 0, 255, 254, 253])
    cursor.fetchall.return_value = [
        (binary_data, "text"),
        (b"\x00\x01\x02", "more text"),
    ]
    cursor.rowcount = 2
    return cursor