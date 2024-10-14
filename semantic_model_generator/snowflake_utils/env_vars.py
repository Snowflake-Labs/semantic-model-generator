import os

from dotenv import load_dotenv

load_dotenv(override=True)
DEFAULT_SESSION_TIMEOUT_SEC = int(os.environ.get("SNOWFLAKE_SESSION_TIMEOUT_SEC", 120))
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_AUTHENTICATOR = os.getenv("SNOWFLAKE_AUTHENTICATOR")
SNOWFLAKE_ACCOUNT_LOCATOR = os.getenv("SNOWFLAKE_ACCOUNT_LOCATOR")

# Optional MFA environment variables
SNOWFLAKE_MFA_PASSCODE = os.getenv("SNOWFLAKE_MFA_PASSCODE")
SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD = os.getenv("SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD")


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
    if not SNOWFLAKE_HOST:
        missing_env_vars.append("SNOWFLAKE_HOST")
    if not SNOWFLAKE_PASSWORD and not SNOWFLAKE_AUTHENTICATOR:
        missing_env_vars.append("SNOWFLAKE_PASSWORD/SNOWFLAKE_AUTHENTICATOR")

    # Assert that SNOWFLAKE_PASSWORD is required unless the user is using the externalbrowser authenticator
    if (
        SNOWFLAKE_AUTHENTICATOR
        and SNOWFLAKE_AUTHENTICATOR.lower() != "externalbrowser"
        and not SNOWFLAKE_PASSWORD
    ):
        missing_env_vars.append("SNOWFLAKE_PASSWORD")

    return missing_env_vars
