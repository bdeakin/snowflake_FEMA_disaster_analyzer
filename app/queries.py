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
        df = cur.fetch_pandas_all()
        df.columns = [str(c).lower() for c in df.columns]
        return QueryResult(df=df, sql=sql, params=params)
    finally:
        conn.close()


def get_state_choropleth(year_start: int, year_end: int) -> QueryResult:
    sql = """
        SELECT
          state AS state,
          SUM(disaster_count) AS disaster_count
        FROM ANALYTICS.GOLD.DISASTERS_BY_STATE
        WHERE YEAR(period_bucket) BETWEEN %(year_start)s AND %(year_end)s
        GROUP BY state
    """
    return fetch_df(sql, {"year_start": year_start, "year_end": year_end})


def get_cube_summary(state: str, year_start: int, year_end: int, grain: str) -> QueryResult:
    if grain == "year":
        table = "ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_YEAR"
    elif grain == "month":
        table = "ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_MONTH"
    else:
        table = "ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_WEEK"

    sql = f"""
        SELECT
          disaster_type AS disaster_type,
          period_bucket AS period_bucket,
          disaster_count AS disaster_count
        FROM {table}
        WHERE state = %(state)s
          AND YEAR(period_bucket) BETWEEN %(year_start)s AND %(year_end)s
    """
    return fetch_df(sql, {"state": state, "year_start": year_start, "year_end": year_end})


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
