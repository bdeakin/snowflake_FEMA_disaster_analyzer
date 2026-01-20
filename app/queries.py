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
            fallback_allowed = "254007" in str(exc) or type(exc).__name__ == "NotSupportedError"
            if fallback_allowed:
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


def execute_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    params = params or {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
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


def get_disaster_date_bounds() -> QueryResult:
    sql = """
        SELECT
          MIN(disaster_declaration_date) AS min_date,
          MAX(disaster_declaration_date) AS max_date
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE disaster_declaration_date IS NOT NULL
    """
    return fetch_df(sql)




def get_sankey_rows(
    start_date: str,
    end_date: str,
    disaster_types: Optional[list[str]] = None,
) -> QueryResult:
    type_clause, type_params = _in_clause("dtype", disaster_types)
    sql = """
        SELECT DISTINCT
          CAST(disaster_id AS STRING) AS record_id,
          disaster_id AS disaster_id,
          county_fips AS county_fips,
          state AS state,
          disaster_type AS disaster_type,
          disaster_declaration_date AS disaster_declaration_date,
          disaster_begin_date AS disaster_begin_date,
          disaster_end_date AS disaster_end_date,
          declaration_name AS declaration_name
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE COALESCE(disaster_declaration_date, disaster_begin_date, disaster_end_date)
          >= %(start_date)s
          AND COALESCE(disaster_declaration_date, disaster_begin_date, disaster_end_date)
          < %(end_date)s
          {type_clause}
    """.format(type_clause=type_clause)
    return fetch_df(
        sql,
        {"start_date": start_date, "end_date": end_date, **type_params},
    )




def get_name_grouping_cache(record_ids: list[str]) -> QueryResult:
    if not record_ids:
        return QueryResult(df=pd.DataFrame(), sql="", params={})
    placeholders = []
    params: Dict[str, Any] = {}
    for idx, record_id in enumerate(record_ids):
        key = f"rid{idx}"
        placeholders.append(f"%({key})s")
        params[key] = record_id
    sql = f"""
        SELECT
          record_id AS record_id,
          source_text_hash AS source_text_hash,
          is_named_event AS is_named_event,
          canonical_event_name AS canonical_event_name,
          name_group AS name_group,
          theme_group AS theme_group,
          theme_confidence AS theme_confidence,
          confidence AS confidence,
          llm_model AS llm_model,
          created_at AS created_at,
          updated_at AS updated_at
        FROM ANALYTICS.MONITORING.DISASTER_NAME_GROUPING_CACHE
        WHERE record_id IN ({", ".join(placeholders)})
    """
    return fetch_df(sql, params)




def upsert_name_grouping_cache(
    rows: list[dict[str, Any]],
    batch_size: int = 200,
) -> None:
    if not rows:
        return

    columns = [
        "record_id",
        "source_text_hash",
        "is_named_event",
        "canonical_event_name",
        "name_group",
        "theme_group",
        "theme_confidence",
        "confidence",
        "llm_model",
    ]
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        values_sql = []
        params: Dict[str, Any] = {}
        for row_idx, row in enumerate(chunk):
            placeholders = []
            for col in columns:
                key = f"{col}_{start}_{row_idx}"
                placeholders.append(f"%({key})s")
                params[key] = row.get(col)
            values_sql.append("(" + ", ".join(placeholders) + ")")

        sql = f"""
            MERGE INTO ANALYTICS.MONITORING.DISASTER_NAME_GROUPING_CACHE AS target
            USING (
                SELECT
                  column1 AS record_id,
                  column2 AS source_text_hash,
                  column3 AS is_named_event,
                  column4 AS canonical_event_name,
                  column5 AS name_group,
                  column6 AS theme_group,
                  column7 AS theme_confidence,
                  column8 AS confidence,
                  column9 AS llm_model
                FROM VALUES {", ".join(values_sql)}
            ) AS source
            ON target.record_id = source.record_id
            WHEN MATCHED AND target.source_text_hash <> source.source_text_hash THEN
              UPDATE SET
                source_text_hash = source.source_text_hash,
                is_named_event = source.is_named_event,
                canonical_event_name = source.canonical_event_name,
                name_group = source.name_group,
                theme_group = source.theme_group,
                theme_confidence = source.theme_confidence,
                confidence = source.confidence,
                llm_model = source.llm_model,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (
                record_id,
                source_text_hash,
                is_named_event,
                canonical_event_name,
                name_group,
                theme_group,
                theme_confidence,
                confidence,
                llm_model,
                created_at,
                updated_at
              )
              VALUES (
                source.record_id,
                source.source_text_hash,
                source.is_named_event,
                source.canonical_event_name,
                source.name_group,
                source.theme_group,
                source.theme_confidence,
                source.confidence,
                source.llm_model,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
              )
        """
        execute_sql(sql, params)


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
        WHERE disaster_declaration_date >= %(start_date)s
          AND disaster_declaration_date < %(end_date)s
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
          AND disaster_declaration_date >= %(start_date)s
          AND disaster_declaration_date < %(end_date)s
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
          disaster_begin_date AS disaster_begin_date,
          disaster_end_date AS disaster_end_date,
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


def get_sunburst_rows(
    start_date: str,
    end_date: str,
    disaster_types: Optional[list[str]] = None,
) -> QueryResult:
    type_clause, type_params = _in_clause("dtype", disaster_types)
    params: Dict[str, Any] = {"start_date": start_date, "end_date": end_date, **type_params}
    sql = """
        SELECT
          disaster_type AS disaster_type,
          declaration_name AS declaration_name,
          state AS state,
          county_fips AS county_fips,
          disaster_declaration_date AS disaster_declaration_date
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE disaster_declaration_date >= %(start_date)s
          AND disaster_declaration_date < %(end_date)s
          {type_clause}
          AND state IS NOT NULL
          AND county_fips IS NOT NULL
    """.format(type_clause=type_clause)
    return fetch_df(sql, params)


def get_trends_bump_ranks(
    binning: str,
    start_date: str,
    end_date: str,
    top_n: int,
) -> QueryResult:
    if binning == "months":
        bucket_expr = "DATE_TRUNC('month', disaster_declaration_date)"
    elif binning == "years":
        bucket_expr = "DATE_TRUNC('year', disaster_declaration_date)"
    else:
        bucket_expr = (
            "DATE_FROM_PARTS(FLOOR(EXTRACT(year FROM disaster_declaration_date) / 10) * 10, 1, 1)"
        )

    sql = f"""
        WITH bucket_counts AS (
            SELECT
              {bucket_expr} AS period_bucket,
              disaster_type AS disaster_type,
              COUNT(*) AS disaster_count
            FROM ANALYTICS.SILVER.FCT_DISASTERS
            WHERE disaster_declaration_date >= %(start_date)s
              AND disaster_declaration_date < %(end_date)s
              AND disaster_type IS NOT NULL
            GROUP BY {bucket_expr}, disaster_type
        )
        SELECT
          period_bucket,
          disaster_type,
          disaster_count,
          DENSE_RANK() OVER (
            PARTITION BY period_bucket
            ORDER BY disaster_count DESC, disaster_type
          ) AS rank
        FROM bucket_counts
        QUALIFY rank <= %(top_n)s
        ORDER BY period_bucket, rank, disaster_type
    """
    return fetch_df(
        sql,
        {"start_date": start_date, "end_date": end_date, "top_n": top_n},
    )


def get_bump_drilldown_state_summary(
    binning: str,
    period_bucket: str,
    disaster_type: str,
) -> QueryResult:
    if binning == "months":
        date_filter = (
            "disaster_declaration_date >= %(period_start)s "
            "AND disaster_declaration_date < DATEADD('month', 1, %(period_start)s)"
        )
    elif binning == "years":
        date_filter = (
            "disaster_declaration_date >= %(period_start)s "
            "AND disaster_declaration_date < DATEADD('year', 1, %(period_start)s)"
        )
    else:
        date_filter = (
            "disaster_declaration_date >= %(period_start)s "
            "AND disaster_declaration_date < DATEADD('year', 10, %(period_start)s)"
        )

    sql = f"""
        SELECT
          state AS state,
          COUNT(*) AS disaster_count,
          LISTAGG(
            DISTINCT NULLIF(TRIM(declaration_name), ''),
            ', '
          ) WITHIN GROUP (ORDER BY NULLIF(TRIM(declaration_name), '')) AS specific_disasters
        FROM ANALYTICS.SILVER.FCT_DISASTERS
        WHERE disaster_type = %(disaster_type)s
          AND {date_filter}
        GROUP BY state
        ORDER BY disaster_count DESC, state
    """
    return fetch_df(
        sql,
        {"period_start": period_bucket, "disaster_type": disaster_type},
    )


def get_consistency_runs(
    window_start: Optional[str],
    window_end: Optional[str],
    status_filters: Optional[list[str]],
    limit_rows: int = 100,
) -> QueryResult:
    date_clause = ""
    params: Dict[str, Any] = {"limit_rows": limit_rows}
    if window_start and window_end:
        date_clause = (
            "AND window_end_date >= %(window_start)s "
            "AND window_start_date <= %(window_end)s"
        )
        params["window_start"] = window_start
        params["window_end"] = window_end

    status_clause = ""
    if status_filters:
        placeholders = []
        for idx, value in enumerate(status_filters):
            key = f"status{idx}"
            placeholders.append(f"%({key})s")
            params[key] = value
        status_clause = (
            "AND (silver_vs_public_status IN (" + ", ".join(placeholders) + ") "
            "OR gold_vs_silver_status IN (" + ", ".join(placeholders) + ") "
            "OR gold_vs_public_status IN (" + ", ".join(placeholders) + "))"
        )

    table_name = "ANALYTICS.MONITORING.CONSISTENCY_CHECK_RUNS"
    sql = f"""
        SELECT *
        FROM {table_name}
        WHERE 1=1
          {date_clause}
          {status_clause}
        ORDER BY run_ts DESC
        LIMIT %(limit_rows)s
    """
    return fetch_df(sql, params)


def get_dynamic_table_metadata(table_fqns: list[str]) -> QueryResult:
    if not table_fqns:
        return QueryResult(df=pd.DataFrame(), sql="", params={})
    table_names = [name.split(".")[-1].upper() for name in table_fqns]
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SHOW DYNAMIC TABLES IN DATABASE ANALYTICS")
        cur.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        df = cur.fetch_pandas_all()
        df.columns = [str(c).lower() for c in df.columns]
        if "name" in df.columns:
            df = df[df["name"].str.upper().isin(table_names)]
        return QueryResult(df=df, sql="SHOW DYNAMIC TABLES IN DATABASE ANALYTICS", params={})
    finally:
        conn.close()


def get_task_status(task_fqns: list[str]) -> QueryResult:
    if not task_fqns:
        return QueryResult(df=pd.DataFrame(), sql="", params={})
    task_names = [name.split(".")[-1].upper() for name in task_fqns]
    clause, params = _in_clause("tsk", task_names)
    sql = f"""
        SELECT
          name,
          database_name,
          schema_name,
          state,
          schedule,
          warehouse,
          comment,
          next_scheduled_time,
          last_successful_run_time,
          last_suspended_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS
        WHERE database_name = 'ANALYTICS'
          {clause.replace('disaster_type', 'name')}
        ORDER BY name
    """
    try:
        return fetch_df(sql, params)
    except Exception:
        return QueryResult(df=pd.DataFrame(), sql=sql, params=params)


def get_task_history(task_fqns: list[str], limit_rows: int = 5) -> QueryResult:
    if not task_fqns:
        return QueryResult(df=pd.DataFrame(), sql="", params={})
    task_names = [name.split(".")[-1].upper() for name in task_fqns]
    clause, params = _in_clause("tsk", task_names)
    params["limit_rows"] = limit_rows
    sql = f"""
        SELECT
          name,
          database_name,
          schema_name,
          state,
          scheduled_time,
          completed_time,
          error_message
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE database_name = 'ANALYTICS'
          {clause.replace('disaster_type', 'name')}
        ORDER BY scheduled_time DESC
        LIMIT %(limit_rows)s
    """
    try:
        return fetch_df(sql, params)
    except Exception:
        return QueryResult(df=pd.DataFrame(), sql=sql, params=params)


