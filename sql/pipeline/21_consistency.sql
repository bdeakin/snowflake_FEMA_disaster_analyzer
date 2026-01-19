-- Consistency checker: MONITORING schema, results table, stored procedure, task

CREATE SCHEMA IF NOT EXISTS ANALYTICS.MONITORING;

CREATE OR REPLACE TABLE ANALYTICS.MONITORING.CONSISTENCY_CHECK_RUNS (
  run_id STRING,
  run_ts TIMESTAMP_NTZ,
  window_start_date DATE,
  window_end_date DATE,
  public_row_count NUMBER,
  public_distinct_id_count NUMBER,
  public_min_start_date DATE,
  public_max_start_date DATE,
  public_id_signature STRING,
  silver_row_count NUMBER,
  silver_distinct_id_count NUMBER,
  silver_min_start_date DATE,
  silver_max_start_date DATE,
  silver_id_signature STRING,
  silver_last_refresh_ts TIMESTAMP_NTZ,
  silver_target_lag STRING,
  gold_total_count NUMBER,
  gold_dim_row_count NUMBER,
  gold_last_refresh_ts TIMESTAMP_NTZ,
  gold_target_lag STRING,
  silver_vs_public_status STRING,
  silver_vs_public_reason STRING,
  gold_vs_silver_status STRING,
  gold_vs_silver_reason STRING,
  gold_vs_public_status STRING,
  gold_vs_public_reason STRING,
  task_name STRING,
  notes STRING
);

DROP TABLE IF EXISTS ANALYTICS.MONITORING.CONSISTENCY_CHECK_DEBUG;


CREATE OR REPLACE PROCEDURE ANALYTICS.MONITORING.SP_RUN_CONSISTENCY_CHECK(
  window_start_date DATE,
  window_end_date DATE
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_run_id STRING;
  v_run_ts TIMESTAMP_NTZ;
  v_start DATE;
  v_end DATE;
  v_silver_refresh TIMESTAMP_NTZ;
  v_gold_refresh TIMESTAMP_NTZ;
  v_silver_lag STRING;
  v_gold_lag STRING;
  v_silver_lag_minutes NUMBER;
  v_gold_lag_minutes NUMBER;
  v_show_id STRING;
  v_err_id STRING;
  v_err_ts TIMESTAMP_NTZ;
  v_err_msg STRING;
  v_failed_query_id STRING;
  v_err_step STRING;
BEGIN
  v_run_id := UUID_STRING();
  v_run_ts := CURRENT_TIMESTAMP();
  v_start := COALESCE(window_start_date, DATEADD('day', -30, CURRENT_DATE()));
  v_end := COALESCE(window_end_date, CURRENT_DATE());
  v_err_step := 'init';

  BEGIN
    v_err_step := 'dynamic_table_metadata';
    EXECUTE IMMEDIATE 'SHOW DYNAMIC TABLES IN DATABASE ANALYTICS';
    v_show_id := (SELECT LAST_QUERY_ID());

    SELECT last_refresh, target_lag
      INTO :v_silver_refresh, :v_silver_lag
    FROM TABLE(RESULT_SCAN(:v_show_id))
    WHERE schema_name = 'SILVER'
      AND name = 'FCT_DISASTERS'
    LIMIT 1;

    SELECT last_refresh, target_lag
      INTO :v_gold_refresh, :v_gold_lag
    FROM TABLE(RESULT_SCAN(:v_show_id))
    WHERE schema_name = 'GOLD'
      AND name = 'DISASTERS_BY_STATE'
    LIMIT 1;
  EXCEPTION
    WHEN OTHER THEN
      v_silver_refresh := NULL;
      v_silver_lag := NULL;
      v_gold_refresh := NULL;
      v_gold_lag := NULL;
  END;

  v_silver_lag_minutes := CASE
    WHEN v_silver_lag ILIKE '%day%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_silver_lag, '\\d+')) * 1440
    WHEN v_silver_lag ILIKE '%hour%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_silver_lag, '\\d+')) * 60
    WHEN v_silver_lag ILIKE '%min%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_silver_lag, '\\d+'))
    ELSE NULL
  END;
  v_gold_lag_minutes := CASE
    WHEN v_gold_lag ILIKE '%day%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_gold_lag, '\\d+')) * 1440
    WHEN v_gold_lag ILIKE '%hour%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_gold_lag, '\\d+')) * 60
    WHEN v_gold_lag ILIKE '%min%' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(v_gold_lag, '\\d+'))
    ELSE NULL
  END;

  v_err_step := 'metrics_insert';
  INSERT INTO ANALYTICS.MONITORING.CONSISTENCY_CHECK_RUNS
    WITH public_base AS (
      SELECT
        d.disaster_id,
        d.disaster_declaration_date,
        LPAD(REPLACE(a.county_geo_id, 'geoId/', ''), 5, '0') AS county_fips
      FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX d
      JOIN SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX a
        ON d.disaster_id = a.disaster_id
      WHERE d.disaster_declaration_date BETWEEN :v_start AND :v_end
    ),
    public_with_state AS (
      SELECT
        p.disaster_id,
        p.disaster_declaration_date,
        p.county_fips,
        c.state_abbr AS state
      FROM public_base p
      LEFT JOIN ANALYTICS.REF.COUNTY_CENTROIDS c
        ON p.county_fips = c.county_fips
    ),
    public_metrics AS (
      SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT disaster_id || '|' || COALESCE(county_fips, '')) AS distinct_id_count,
        MIN(disaster_declaration_date) AS min_date,
        MAX(disaster_declaration_date) AS max_date,
        HASH_AGG(TO_VARCHAR(disaster_id) || '|' || COALESCE(county_fips, '') || '|' || TO_VARCHAR(disaster_declaration_date)) AS id_signature
      FROM public_with_state
    ),
    silver_metrics AS (
      SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT disaster_id || '|' || COALESCE(county_fips, '')) AS distinct_id_count,
        MIN(disaster_declaration_date) AS min_date,
        MAX(disaster_declaration_date) AS max_date,
        HASH_AGG(TO_VARCHAR(disaster_id) || '|' || COALESCE(county_fips, '') || '|' || TO_VARCHAR(disaster_declaration_date)) AS id_signature
      FROM ANALYTICS.SILVER.FCT_DISASTERS
      WHERE disaster_declaration_date BETWEEN :v_start AND :v_end
    ),
    gold_actual AS (
      SELECT
        COUNT(*) AS dim_row_count,
        SUM(disaster_count) AS total_count
      FROM ANALYTICS.GOLD.DISASTERS_BY_STATE
      WHERE period_bucket BETWEEN :v_start AND :v_end
    ),
    expected_gold_silver AS (
      SELECT
        state,
        DATE_TRUNC('year', disaster_declaration_date) AS period_bucket,
        COUNT(*) AS disaster_count
      FROM ANALYTICS.SILVER.FCT_DISASTERS
      WHERE disaster_declaration_date BETWEEN :v_start AND :v_end
      GROUP BY state, DATE_TRUNC('year', disaster_declaration_date)
    ),
    expected_gold_public AS (
      SELECT
        state,
        DATE_TRUNC('year', disaster_declaration_date) AS period_bucket,
        COUNT(*) AS disaster_count
      FROM public_with_state
      WHERE state IS NOT NULL
      GROUP BY state, DATE_TRUNC('year', disaster_declaration_date)
    ),
    gold_mismatch_silver AS (
      SELECT
        COUNT(*) AS mismatched_rows,
        SUM(ABS(COALESCE(g.disaster_count,0) - COALESCE(e.disaster_count,0))) AS count_diff
      FROM ANALYTICS.GOLD.DISASTERS_BY_STATE g
      FULL OUTER JOIN expected_gold_silver e
        ON g.state = e.state AND g.period_bucket = e.period_bucket
      WHERE COALESCE(g.period_bucket, e.period_bucket) BETWEEN :v_start AND :v_end
        AND COALESCE(g.disaster_count, -1) <> COALESCE(e.disaster_count, -1)
    ),
    gold_mismatch_public AS (
      SELECT
        COUNT(*) AS mismatched_rows,
        SUM(ABS(COALESCE(g.disaster_count,0) - COALESCE(e.disaster_count,0))) AS count_diff
      FROM ANALYTICS.GOLD.DISASTERS_BY_STATE g
      FULL OUTER JOIN expected_gold_public e
        ON g.state = e.state AND g.period_bucket = e.period_bucket
      WHERE COALESCE(g.period_bucket, e.period_bucket) BETWEEN :v_start AND :v_end
        AND COALESCE(g.disaster_count, -1) <> COALESCE(e.disaster_count, -1)
    )
    SELECT
      :v_run_id,
      :v_run_ts,
      :v_start,
      :v_end,
      public_metrics.row_count,
      public_metrics.distinct_id_count,
      public_metrics.min_date,
      public_metrics.max_date,
      public_metrics.id_signature,
      silver_metrics.row_count,
      silver_metrics.distinct_id_count,
      silver_metrics.min_date,
      silver_metrics.max_date,
      silver_metrics.id_signature,
      :v_silver_refresh,
      :v_silver_lag,
      gold_actual.total_count,
      gold_actual.dim_row_count,
      :v_gold_refresh,
      :v_gold_lag,
      CASE
        WHEN public_metrics.row_count = silver_metrics.row_count
         AND public_metrics.min_date = silver_metrics.min_date
         AND public_metrics.max_date = silver_metrics.max_date
         AND public_metrics.id_signature = silver_metrics.id_signature
        THEN 'IN_SYNC'
        WHEN :v_silver_refresh IS NOT NULL
         AND :v_silver_lag_minutes IS NOT NULL
         AND DATEDIFF('minute', :v_silver_refresh, CURRENT_TIMESTAMP()) <= :v_silver_lag_minutes
        THEN 'STALE_OK'
        ELSE 'OUT_OF_SYNC'
      END AS silver_vs_public_status,
      CASE
        WHEN public_metrics.id_signature = silver_metrics.id_signature THEN 'All key metrics match'
        ELSE 'Mismatch in counts/min/max/signature'
      END AS silver_vs_public_reason,
      CASE
        WHEN gold_mismatch_silver.mismatched_rows = 0 THEN 'IN_SYNC'
        WHEN :v_gold_refresh IS NOT NULL
         AND :v_gold_lag_minutes IS NOT NULL
         AND DATEDIFF('minute', :v_gold_refresh, CURRENT_TIMESTAMP()) <= :v_gold_lag_minutes
        THEN 'STALE_OK'
        ELSE 'OUT_OF_SYNC'
      END AS gold_vs_silver_status,
      CASE
        WHEN gold_mismatch_silver.mismatched_rows = 0 THEN 'Gold matches expected from Silver'
        ELSE 'Gold differs from expected Silver aggregation'
      END AS gold_vs_silver_reason,
      CASE
        WHEN gold_mismatch_public.mismatched_rows = 0 THEN 'IN_SYNC'
        WHEN :v_gold_refresh IS NOT NULL
         AND :v_gold_lag_minutes IS NOT NULL
         AND DATEDIFF('minute', :v_gold_refresh, CURRENT_TIMESTAMP()) <= :v_gold_lag_minutes
        THEN 'STALE_OK'
        ELSE 'OUT_OF_SYNC'
      END AS gold_vs_public_status,
      CASE
        WHEN gold_mismatch_public.mismatched_rows = 0 THEN 'Gold matches expected from Public'
        ELSE 'Gold differs from expected Public aggregation'
      END AS gold_vs_public_reason,
      'ANALYTICS.MONITORING.TASK_RUN_CONSISTENCY_CHECK_12H',
      NULL
    FROM public_metrics, silver_metrics, gold_actual, gold_mismatch_silver, gold_mismatch_public;

  RETURN 'OK';
EXCEPTION
  WHEN OTHER THEN
    v_err_id := UUID_STRING();
    v_err_ts := CURRENT_TIMESTAMP();
    v_failed_query_id := (SELECT LAST_QUERY_ID());
    v_err_msg := NULL;
    BEGIN
      EXECUTE IMMEDIATE 'SHOW QUERIES';
      v_show_id := (SELECT LAST_QUERY_ID());
      SELECT error_message
        INTO :v_err_msg
      FROM TABLE(RESULT_SCAN(:v_show_id))
      WHERE query_id = :v_failed_query_id
      LIMIT 1;
    EXCEPTION
      WHEN OTHER THEN
        v_err_msg := NULL;
    END;
    INSERT INTO ANALYTICS.MONITORING.CONSISTENCY_CHECK_RUNS (
      run_id, run_ts, window_start_date, window_end_date,
      silver_vs_public_status, silver_vs_public_reason,
      gold_vs_silver_status, gold_vs_silver_reason,
      gold_vs_public_status, gold_vs_public_reason,
      task_name, notes
    )
    SELECT
      :v_err_id, :v_err_ts, :v_start, :v_end,
      'ERROR', 'Stored procedure failed',
      'ERROR', 'Stored procedure failed',
      'ERROR', 'Stored procedure failed',
      'ANALYTICS.MONITORING.TASK_RUN_CONSISTENCY_CHECK_12H',
      COALESCE(
        NULLIF(:v_err_msg, ''),
        'Stored procedure failed at step ' || COALESCE(:v_err_step, 'unknown')
      );
    RETURN 'ERROR';
END;
$$;

CREATE OR REPLACE TASK ANALYTICS.MONITORING.TASK_RUN_CONSISTENCY_CHECK_12H
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */12 * * * UTC'
AS
  CALL ANALYTICS.MONITORING.SP_RUN_CONSISTENCY_CHECK(NULL, NULL);
