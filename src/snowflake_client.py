import os
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import snowflake.connector

DEBUG_LOG_PATH = "/Users/bdeakin/Documents/Vibe Coding/snowflake_FEMA_disaster_analyzer/.cursor/debug.log"


def _debug_log(message: str, data: Dict[str, object], location: str, hypothesis_id: str) -> None:
    payload = {
        "sessionId": "debug-session",
        "runId": "post-fix-11",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(pd.Timestamp.utcnow().timestamp() * 1000),
    }


def _get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value or ""


def get_connection():
    return snowflake.connector.connect(
        account=_get_env("SNOWFLAKE_ACCOUNT"),
        user=_get_env("SNOWFLAKE_USER"),
        password=_get_env("SNOWFLAKE_PASSWORD"),
        role=_get_env("SNOWFLAKE_ROLE"),
        warehouse=_get_env("SNOWFLAKE_WAREHOUSE"),
        database=_get_env("SNOWFLAKE_DATABASE"),
        schema=_get_env("SNOWFLAKE_SCHEMA"),
    )


def fetch_dataframe(sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params=params)
            return cur.fetch_pandas_all()
        finally:
            cur.close()


def execute_scalar(sql: str, params: Optional[Iterable[Any]] = None) -> Any:
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params=params)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()


def call_cortex_complete(prompt: str, model: str) -> str:
    sql = "SELECT snowflake.cortex.complete(%(model)s, %(prompt)s) AS response"
    try:
        result = execute_scalar(sql, params={"model": model, "prompt": prompt})
    except Exception as exc:
        raise
    if result is None:
        raise RuntimeError("Cortex returned no response.")
    return str(result)


def describe_relation(relation_fqn: str, relation_type: str = "TABLE") -> pd.DataFrame:
    sql = f"DESC {relation_type} {relation_fqn}"
    try:
        return fetch_dataframe(sql)
    except Exception as exc:
        raise


def show_tables_like(pattern: str, database: str, schema: str) -> pd.DataFrame:
    sql = f"SHOW TABLES LIKE '{pattern}' IN SCHEMA {database}.{schema}"
    return fetch_dataframe(sql)


def show_views_like(pattern: str, database: str, schema: str) -> pd.DataFrame:
    sql = f"SHOW VIEWS LIKE '{pattern}' IN SCHEMA {database}.{schema}"
    return fetch_dataframe(sql)


def show_views_in_schema(database: str, schema: str) -> pd.DataFrame:
    sql = (
        "SELECT TABLE_NAME AS name "
        "FROM INFORMATION_SCHEMA.VIEWS "
        "WHERE TABLE_CATALOG = %(db)s AND TABLE_SCHEMA = %(schema)s"
    )
    return fetch_dataframe(sql, params={"db": database, "schema": schema})
