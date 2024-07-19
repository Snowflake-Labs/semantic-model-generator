import os

DEFAULT_SESSION_TIMEOUT_SEC = int(os.environ.get("SNOWFLAKE_SESSION_TIMEOUT_SEC", 120))
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")  # optional per README docs
SNOWFLAKE_AUTHENTICATOR = os.getenv("SNOWFLAKE_AUTHENTICATOR")
SNOWFLAKE_ACCOUNT_LOCATOR = os.getenv("SNOWFLAKE_ACCOUNT_LOCATOR")


def assert_required_env_vars() -> list[str]:
    """
    Ensures that the required environment variables are set before proceeding.
    Returns: list of missing required environment variables

    """

    missing_env_vars = []
    if not SNOWFLAKE_ROLE:
        missing_env_vars.append("SNOWFLAKE_ROLE")
    if not SNOWFLAKE_WAREHOUSE:
        missing_env_vars.append("SNOWFLAKE_WAREHOUSE")
    if not SNOWFLAKE_USER:
        missing_env_vars.append("SNOWFLAKE_USER")
    if not SNOWFLAKE_ACCOUNT_LOCATOR:
        missing_env_vars.append("SNOWFLAKE_ACCOUNT_LOCATOR")
    if not SNOWFLAKE_PASSWORD and not SNOWFLAKE_AUTHENTICATOR:
        missing_env_vars.append("SNOWFLAKE_PASSWORD/SNOWFLAKE_AUTHENTICATOR")

    return missing_env_vars
