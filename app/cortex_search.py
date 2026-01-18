from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

import pandas as pd
import snowflake.connector

try:
    import _snowflake  # type: ignore
    from snowflake.snowpark.context import get_active_session
    _SNOWFLAKE_AVAILABLE = True
except Exception:
    _snowflake = None
    get_active_session = None
    _SNOWFLAKE_AVAILABLE = False


SEMANTIC_VIEW = "ANALYTICS.SILVER.EXPLORER"


def init_context() -> None:
    if _SNOWFLAKE_AVAILABLE:
        session = get_active_session()
        session.sql("USE ROLE ACCOUNTADMIN").collect()
        session.sql("USE WAREHOUSE COMPUTE_WH").collect()
        session.sql("USE DATABASE ANALYTICS").collect()
        session.sql("USE SCHEMA SILVER").collect()


def _normalize_response(response: Any) -> Dict[str, Any]:
    if isinstance(response, str):
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return {"message": {"content": [{"type": "text", "text": response}]}}
    if isinstance(response, dict):
        if "data" in response:
            data = response["data"]
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return {"message": {"content": [{"type": "text", "text": data}]}}
            if isinstance(data, dict):
                return data
        return response
    return {"message": {"content": [{"type": "text", "text": str(response)}]}}


def _extract_blocks(response: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    content = None
    if isinstance(response.get("message"), dict):
        content = response["message"].get("content")
    if content is None and isinstance(response.get("messages"), list):
        last = response["messages"][-1] if response["messages"] else {}
        if isinstance(last, dict):
            content = last.get("content")
    if content is None and isinstance(response.get("content"), list):
        content = response.get("content")

    text_blocks: List[str] = []
    sql_blocks: List[str] = []
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text" and isinstance(block.get("text"), str):
                text_blocks.append(block["text"])
            if block_type == "sql" and isinstance(block.get("sql"), str):
                sql_blocks.append(block["sql"])
    return text_blocks, sql_blocks


def call_analyst(messages: List[Dict[str, str]]) -> Tuple[List[str], List[str]]:
    body = {"messages": messages, "semantic_view": SEMANTIC_VIEW, "stream": False}
    if _SNOWFLAKE_AVAILABLE:
        response = _snowflake.send_snow_api_request(
            "POST",
            "/api/v2/cortex/analyst/message",
            {},
            body,
        )
        normalized = _normalize_response(response)
        return _extract_blocks(normalized)
    response = _call_analyst_sql(body)
    normalized = _normalize_response(response)
    return _extract_blocks(normalized)


def _is_single_select(sql: str) -> bool:
    stripped = sql.strip().strip(";")
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        return False
    forbidden = [" insert ", " update ", " delete ", " merge ", " create ", " drop ", " alter "]
    if any(token in lowered for token in forbidden):
        return False
    parts = [p for p in stripped.split(";") if p.strip()]
    return len(parts) == 1


def _ensure_limit(sql: str) -> str:
    lowered = sql.lower()
    if " limit " in lowered:
        return sql
    return f"{sql.rstrip().rstrip(';')} LIMIT 1000"


def run_sql(sql: str):
    if not _is_single_select(sql):
        raise ValueError("Only single SELECT statements are allowed.")
    safe_sql = _ensure_limit(sql)
    if _SNOWFLAKE_AVAILABLE:
        session = get_active_session()
        return session.sql(safe_sql).to_pandas()
    conn = _get_local_connection()
    try:
        cur = conn.cursor()
        cur.execute(safe_sql)
        try:
            return cur.fetch_pandas_all()
        except Exception:
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def snowflake_available() -> bool:
    return _SNOWFLAKE_AVAILABLE or _has_local_creds()


def _has_local_creds() -> bool:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]
    return all(os.getenv(k) for k in required)


def _get_local_connection():
    if not _has_local_creds():
        raise RuntimeError("Missing Snowflake credentials for local Cortex Search.")
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="ANALYTICS",
        schema="SILVER",
    )


def _call_analyst_sql(body: Dict[str, Any]) -> Any:
    conn = _get_local_connection()
    try:
        cur = conn.cursor()
        cur.execute("SHOW FUNCTIONS LIKE 'SYSTEM$CORTEX_ANALYST_QUERY' IN ACCOUNT")
        rows = cur.fetchall()
        if len(rows) == 0:
            raise RuntimeError("SYSTEM$CORTEX_ANALYST_QUERY is not available in this account.")

        def _exec_payload(payload_dict: Dict[str, Any], attempt_id: str) -> Any:
            payload = json.dumps(payload_dict)
            escaped = payload.replace("'", "''")
            sql = f"SELECT SYSTEM$CORTEX_ANALYST_QUERY('{escaped}') AS response"
            cur.execute(sql)
            return cur.fetchone()[0]

        messages = body.get("messages", [])
        # Attempt 1: semantic_view
        try:
            return _exec_payload(
                {"messages": messages, "semantic_view": SEMANTIC_VIEW, "stream": False},
                "H2",
            )
        except Exception as exc:
            last_error = exc

        # Attempt 2: semantic_model as view name
        try:
            return _exec_payload(
                {"messages": messages, "semantic_model": SEMANTIC_VIEW, "stream": False},
                "H5",
            )
        except Exception as exc:
            last_error = exc

        # Attempt 3: semantic_model as YAML from semantic view
        try:
            cur.execute(
                "SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW(%(sv)s)",
                {"sv": SEMANTIC_VIEW},
            )
            yaml_text = cur.fetchone()[0]
            return _exec_payload(
                {"messages": messages, "semantic_model": yaml_text, "stream": False},
                "H6",
            )
        except Exception as exc:
            last_error = exc
            raise last_error
    finally:
        conn.close()
