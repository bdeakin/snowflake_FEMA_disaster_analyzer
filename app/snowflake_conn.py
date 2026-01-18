import os

from dotenv import load_dotenv
import snowflake.connector
try:
    from snowflake.snowpark.context import get_active_session
    _SNOWPARK_AVAILABLE = True
except Exception:
    get_active_session = None
    _SNOWPARK_AVAILABLE = False


REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
]

OPTIONAL_FLAGS = [
    "SNOWFLAKE_OCSP_FAIL_OPEN",
    "SNOWFLAKE_DISABLE_OCSP_CHECKS",
]


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _flag_enabled(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_connection():
    if _SNOWPARK_AVAILABLE:
        try:
            # Streamlit in Snowflake uses the active session; no env vars needed.
            return get_active_session()._conn._conn  # type: ignore[attr-defined]
        except Exception:
            # Fall through to env-based connection for local runs.
            pass
    load_dotenv()
    ocsp_fail_open = _flag_enabled("SNOWFLAKE_OCSP_FAIL_OPEN")
    disable_ocsp_checks = _flag_enabled("SNOWFLAKE_DISABLE_OCSP_CHECKS")
    return snowflake.connector.connect(
        account=_get_env("SNOWFLAKE_ACCOUNT"),
        user=_get_env("SNOWFLAKE_USER"),
        password=_get_env("SNOWFLAKE_PASSWORD"),
        role=_get_env("SNOWFLAKE_ROLE"),
        warehouse=_get_env("SNOWFLAKE_WAREHOUSE"),
        database=_get_env("SNOWFLAKE_DATABASE"),
        schema=_get_env("SNOWFLAKE_SCHEMA"),
        ocsp_fail_open=ocsp_fail_open,
        disable_ocsp_checks=disable_ocsp_checks,
    )
