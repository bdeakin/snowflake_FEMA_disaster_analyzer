from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

try:
    import streamlit as st
    _STREAMLIT_AVAILABLE = True
except Exception:
    st = None
    _STREAMLIT_AVAILABLE = False
# #region agent log
_LOG_PATH = "/Users/bdeakin/Documents/Vibe Coding/snowflake_FEMA_disaster_analyzer/.cursor/debug.log"


def _log_debug(hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    payload = {
        "sessionId": "debug-session",
        "runId": "pre-fix",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(__import__("time").time() * 1000),
    }
    with open(_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")
# #endregion

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
    response = _call_analyst_rest_local(body)
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
    return _SNOWFLAKE_AVAILABLE or (_has_local_creds() and _has_local_token())


def _has_local_creds() -> bool:
    load_dotenv("config/secrets.env")
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]
    if all(os.getenv(k) for k in required):
        return True
    if _STREAMLIT_AVAILABLE:
        try:
            return all(st.secrets.get(k) for k in required)
        except Exception:
            return False
    return False


def _has_local_token() -> bool:
    load_dotenv("config/secrets.env")
    if os.getenv("SNOWFLAKE_TOKEN") and os.getenv("SNOWFLAKE_ACCOUNT"):
        return True
    if _STREAMLIT_AVAILABLE:
        try:
            return bool(st.secrets.get("SNOWFLAKE_TOKEN")) and bool(
                st.secrets.get("SNOWFLAKE_ACCOUNT")
            )
        except Exception:
            return False
    return False


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


def _call_analyst_rest_local(body: Dict[str, Any]) -> Any:
    load_dotenv("config/secrets.env")
    token = os.getenv("SNOWFLAKE_TOKEN")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    if _STREAMLIT_AVAILABLE:
        token = token or st.secrets.get("SNOWFLAKE_TOKEN")
        account = account or st.secrets.get("SNOWFLAKE_ACCOUNT")
    if not token:
        raise RuntimeError("SNOWFLAKE_TOKEN is required for local Cortex Search.")
    if not account:
        raise RuntimeError("SNOWFLAKE_ACCOUNT is required for local Cortex Search.")

    account_host = account.lower()
    base_url = f"https://{account_host}.snowflakecomputing.com"
    url = f"{base_url}/api/v2/cortex/analyst/message"
    token_type = os.getenv("SNOWFLAKE_TOKEN_TYPE", "PAT")
    # #region agent log
    _log_debug(
        "H1",
        "cortex_search.py:_call_analyst_rest_local",
        "Preparing REST call",
        {
            "has_token": bool(token),
            "account": account,
            "account_host": account_host,
            "token_type": token_type,
            "token_len": len(token) if token else 0,
            "has_role": bool(os.getenv("SNOWFLAKE_ROLE")),
            "has_wh": bool(os.getenv("SNOWFLAKE_WAREHOUSE")),
            "url": url,
        },
    )
    # #endregion
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": token_type,
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    if _STREAMLIT_AVAILABLE:
        role = role or st.secrets.get("SNOWFLAKE_ROLE")
        warehouse = warehouse or st.secrets.get("SNOWFLAKE_WAREHOUSE")
    if role:
        headers["X-Snowflake-Role"] = role
    if warehouse:
        headers["X-Snowflake-Warehouse"] = warehouse
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    # #region agent log
    _log_debug(
        "H2",
        "cortex_search.py:_call_analyst_rest_local",
        "REST response status",
        {
            "status_code": resp.status_code,
            "reason": resp.reason,
            "text_snippet": resp.text[:200],
        },
    )
    # #endregion
    # #region agent log
    _log_debug(
        "H3",
        "cortex_search.py:_call_analyst_rest_local",
        "REST response headers (subset)",
        {
            "www_authenticate": resp.headers.get("www-authenticate"),
            "request_id": resp.headers.get("x-snowflake-request-id"),
            "content_type": resp.headers.get("content-type"),
        },
    )
    # #endregion
    if resp.status_code == 401:
        auth_hint = resp.headers.get("www-authenticate")
        request_id = resp.headers.get("x-snowflake-request-id")
        raise RuntimeError(
            "Cortex Analyst REST unauthorized (PAT mode). Check the PAT value, "
            "its expiration, and role grants. "
            f"request_id={request_id} auth_hint={auth_hint}"
        )
    resp.raise_for_status()
    return resp.json()
