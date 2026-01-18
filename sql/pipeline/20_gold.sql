-- Gold layer aggregates for fast UI rendering

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.GOLD.DISASTERS_BY_STATE
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  state,
  period_year AS period_bucket,
  COUNT(*) AS disaster_count
FROM ANALYTICS.SILVER.FCT_DISASTERS
GROUP BY state, period_year;

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_YEAR
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  state,
  disaster_type,
  period_year AS period_bucket,
  COUNT(*) AS disaster_count
FROM ANALYTICS.SILVER.FCT_DISASTERS
GROUP BY state, disaster_type, period_year;

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_MONTH
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  state,
  disaster_type,
  period_month AS period_bucket,
  COUNT(*) AS disaster_count
FROM ANALYTICS.SILVER.FCT_DISASTERS
GROUP BY state, disaster_type, period_month;

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_WEEK
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  state,
  disaster_type,
  period_week AS period_bucket,
  COUNT(*) AS disaster_count
FROM ANALYTICS.SILVER.FCT_DISASTERS
GROUP BY state, disaster_type, period_week;
