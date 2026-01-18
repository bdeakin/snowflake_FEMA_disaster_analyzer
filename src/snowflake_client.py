import json
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
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(payload) + "\n")
    except Exception:
        pass


def _get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value or ""


def _get_env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_connection(use_arrow: bool = True):
    # #region agent log
    _debug_log(
        "get_connection flags",
        {
            "ocsp_fail_open": _get_env_bool("SNOWFLAKE_OCSP_FAIL_OPEN", False),
            "disable_ocsp_checks": _get_env_bool("SNOWFLAKE_DISABLE_OCSP_CHECKS", False),
            "use_arrow_result_format": use_arrow,
        },
        "snowflake_client.py:get_connection",
        "OCSP2",
    )
    # #endregion agent log
    return snowflake.connector.connect(
        account=_get_env("SNOWFLAKE_ACCOUNT"),
        user=_get_env("SNOWFLAKE_USER"),
        password=_get_env("SNOWFLAKE_PASSWORD"),
        role=_get_env("SNOWFLAKE_ROLE"),
        warehouse=_get_env("SNOWFLAKE_WAREHOUSE"),
        database=_get_env("SNOWFLAKE_DATABASE"),
        schema=_get_env("SNOWFLAKE_SCHEMA"),
        ocsp_fail_open=_get_env_bool("SNOWFLAKE_OCSP_FAIL_OPEN", False),
        disable_ocsp_checks=_get_env_bool("SNOWFLAKE_DISABLE_OCSP_CHECKS", False),
        use_arrow_result_format=use_arrow,
    )


def fetch_dataframe(sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            # #region agent log
            _debug_log(
                "fetch_dataframe execute",
                {
                    "sql_preview": str(sql)[:300],
                    "params_type": type(params).__name__,
                },
                "snowflake_client.py:fetch_dataframe",
                "OCSP3",
            )
            # #endregion agent log
            cur.execute(sql, params=params)
            return cur.fetch_pandas_all()
        except Exception as exc:
            # #region agent log
            _debug_log(
                "fetch_dataframe error",
                {"error": str(exc)},
                "snowflake_client.py:fetch_dataframe",
                "OCSP3",
            )
            # #endregion agent log
            raise
        finally:
            cur.close()


def fetch_dataframe_plain(sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    with get_connection(use_arrow=False) as conn:
        cur = conn.cursor()
        try:
            # #region agent log
            _debug_log(
                "fetch_dataframe_plain execute",
                {
                    "sql_preview": str(sql)[:300],
                    "params_type": type(params).__name__,
                },
                "snowflake_client.py:fetch_dataframe_plain",
                "OCSP3",
            )
            # #endregion agent log
            cur.execute(sql, params=params)
            # #region agent log
            _debug_log(
                "fetch_dataframe_plain executed",
                {"sfqid": getattr(cur, "sfqid", None)},
                "snowflake_client.py:fetch_dataframe_plain",
                "OCSP4",
            )
            # #endregion agent log
            # #region agent log
            try:
                batches = cur.get_result_batches()
                batch_meta = []
                for batch in batches[:3]:
                    batch_meta.append(
                        {
                            "rowcount": getattr(batch, "rowcount", None),
                            "compressed_size": getattr(batch, "compressed_size", None),
                            "uncompressed_size": getattr(batch, "uncompressed_size", None),
                            "has_remote_url": bool(getattr(batch, "remote_url", None)),
                        }
                    )
                _debug_log(
                    "fetch_dataframe_plain batches",
                    {
                        "sfqid": getattr(cur, "sfqid", None),
                        "batch_count": len(batches),
                        "batches": batch_meta,
                    },
                    "snowflake_client.py:fetch_dataframe_plain",
                    "OCSP4",
                )
            except Exception as exc:
                _debug_log(
                    "fetch_dataframe_plain batches error",
                    {"error": str(exc)},
                    "snowflake_client.py:fetch_dataframe_plain",
                    "OCSP4",
                )
            # #endregion agent log
            rows = []
            fetched = 0
            batch_size = 500
            while True:
                chunk = cur.fetchmany(batch_size)
                if not chunk:
                    break
                rows.extend(chunk)
                fetched += len(chunk)
                if fetched == len(chunk):
                    # #region agent log
                    _debug_log(
                        "fetch_dataframe_plain first chunk",
                        {"fetched_rows": fetched, "batch_size": batch_size},
                        "snowflake_client.py:fetch_dataframe_plain",
                        "OCSP4",
                    )
                    # #endregion agent log
            # #region agent log
            _debug_log(
                "fetch_dataframe_plain complete",
                {"fetched_rows": fetched, "batch_size": batch_size},
                "snowflake_client.py:fetch_dataframe_plain",
                "OCSP4",
            )
            # #endregion agent log
            columns = [col[0] for col in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=columns)
        except Exception as exc:
            # #region agent log
            _debug_log(
                "fetch_dataframe_plain error",
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "errno": getattr(exc, "errno", None),
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "sfqid": getattr(exc, "sfqid", None),
                    "cursor_sfqid": getattr(cur, "sfqid", None),
                    "fetched_rows": locals().get("fetched", None),
                },
                "snowflake_client.py:fetch_dataframe_plain",
                "OCSP3",
            )
            # #endregion agent log
            raise
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
