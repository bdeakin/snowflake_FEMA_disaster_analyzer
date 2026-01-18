from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from snowflake_conn import get_connection


@dataclass
class QueryResult:
    df: pd.DataFrame
    sql: str
    params: Dict[str, Any]


def fetch_df(sql: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
    params = params or {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        try:
            df = cur.fetch_pandas_all()
        except Exception as exc:
            if "254007" in str(exc):
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description] if cur.description else []
                df = pd.DataFrame(rows, columns=cols)
            else:
                raise
        df.columns = [str(c).lower() for c in df.columns]
        return QueryResult(df=df, sql=sql, params=params)
    except Exception as exc:
        raise
    finally:
        conn.close()


def _in_clause(param_base: str, values: Optional[list[str]]) -> tuple[str, Dict[str, Any]]:
    if not values:
        return "", {}
    placeholders = []
    params: Dict[str, Any] = {}
    for idx, value in enumerate(values):
        key = f"{param_base}{idx}"
        placeholders.append(f"%({key})s")
        params[key] = value
    clause = "AND disaster_type IN (" + ", ".join(placeholders) + ")"
    return clause, params


def get_distinct_disaster_types() -> QueryResult:
    sql = """
        SELECT DISTINCT disaster_type AS disaster_type
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE disaster_type IS NOT NULL
        ORDER BY disaster_type
    """
    return fetch_df(sql)


def get_state_choropleth(
    start_date: str,
    end_date: str,
    disaster_types: Optional[list[str]] = None,
) -> QueryResult:
    type_clause, type_params = _in_clause("dtype", disaster_types)
    params: Dict[str, Any] = {"start_date": start_date, "end_date": end_date, **type_params}
    sql = """
        SELECT
          state AS state,
          COUNT(*) AS disaster_count
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE disaster_declaration_date BETWEEN %(start_date)s AND %(end_date)s
          {type_clause}
        GROUP BY state
    """.format(type_clause=type_clause)
    return fetch_df(sql, params)


def get_cube_summary(
    state: str,
    start_date: str,
    end_date: str,
    grain: str,
    disaster_types: Optional[list[str]] = None,
) -> QueryResult:
    if grain == "year":
        bucket_expr = "DATE_TRUNC('year', disaster_declaration_date)"
    elif grain == "month":
        bucket_expr = "DATE_TRUNC('month', disaster_declaration_date)"
    else:
        bucket_expr = "DATE_TRUNC('week', disaster_declaration_date)"

    type_clause, type_params = _in_clause("dtype", disaster_types)
    params: Dict[str, Any] = {
        "state": state,
        "start_date": start_date,
        "end_date": end_date,
        **type_params,
    }

    sql = f"""
        SELECT
          disaster_type AS disaster_type,
          {bucket_expr} AS period_bucket,
          COUNT(*) AS disaster_count
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE state = %(state)s
          AND disaster_declaration_date BETWEEN %(start_date)s AND %(end_date)s
          {type_clause}
        GROUP BY disaster_type, {bucket_expr}
    """
    return fetch_df(sql, params)


def get_drilldown(
    state: str,
    disaster_type: str,
    period_bucket: str,
    grain: str,
) -> QueryResult:
    if grain == "year":
        bucket_expr = "DATE_TRUNC('year', disaster_declaration_date)"
    elif grain == "month":
        bucket_expr = "DATE_TRUNC('month', disaster_declaration_date)"
    else:
        bucket_expr = "DATE_TRUNC('week', disaster_declaration_date)"

    sql = f"""
        SELECT
          disaster_id AS disaster_id,
          disaster_declaration_date AS disaster_declaration_date,
          disaster_type AS disaster_type,
          county_name AS county_name,
          state AS state,
          declaration_name AS declaration_name,
          centroid_lat AS centroid_lat,
          centroid_lon AS centroid_lon
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE state = %(state)s
          AND disaster_type = %(disaster_type)s
          AND {bucket_expr} = %(period_bucket)s
          AND centroid_lat IS NOT NULL
          AND centroid_lon IS NOT NULL
        LIMIT 5000
    """
    return fetch_df(
        sql,
        {
            "state": state,
            "disaster_type": disaster_type,
            "period_bucket": period_bucket,
        },
    )
