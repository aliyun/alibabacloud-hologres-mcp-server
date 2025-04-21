"""
Settings module for Hologres MCP Server.
"""

import os


SERVER_VERSION = "0.1.9"


def get_db_config():
    """Get database configuration from environment variables."""
    user = os.getenv("HOLOGRES_USER")
    password = os.getenv("HOLOGRES_PASSWORD")
    options = None
    if user is None or password is None:
        user = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
        password = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        sts_token = os.getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
        if sts_token:
            options = f"sts_token={sts_token}"

    config = {
        "host": os.getenv("HOLOGRES_HOST", "localhost"),
        "port": os.getenv("HOLOGRES_PORT", "5432"),
        "user": user,
        "password": password,
        "options": options,
        "dbname": os.getenv("HOLOGRES_DATABASE"),
        "application_name": f"hologres-mcp-server-{SERVER_VERSION}"
    }
    if not all([config["user"], config["password"], config["dbname"]]):
        raise ValueError("Missing required database configuration.")
    
    return config