-- Silver layer: one row per disaster + county
-- NOTE: Column names should be validated by discovery queries.

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.SILVER.FCT_DISASTERS
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  d.disaster_id AS disaster_id,
  c.state_abbr AS state,
  a.fema_designated_area AS county_name,
  LPAD(REPLACE(a.county_geo_id, 'geoId/', ''), 5, '0') AS county_fips,
  c.centroid_lat AS centroid_lat,
  c.centroid_lon AS centroid_lon,
  d.disaster_type AS disaster_type,
  d.disaster_declaration_date AS disaster_declaration_date,
  d.disaster_begin_date AS disaster_begin_date,
  d.disaster_end_date AS disaster_end_date,
  d.disaster_declaration_name AS declaration_name,
  DATE_TRUNC('year', d.disaster_declaration_date) AS period_year,
  DATE_TRUNC('month', d.disaster_declaration_date) AS period_month,
  DATE_TRUNC('week', d.disaster_declaration_date) AS period_week
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX d
JOIN SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX a
  ON d.disaster_id = a.disaster_id
LEFT JOIN ANALYTICS.REF.COUNTY_CENTROIDS c
  ON LPAD(REPLACE(a.county_geo_id, 'geoId/', ''), 5, '0') = c.county_fips
WHERE d.disaster_declaration_date IS NOT NULL;
