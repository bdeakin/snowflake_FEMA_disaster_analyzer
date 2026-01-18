-- Identify candidate join keys between declaration and areas
SELECT
  COUNT(*) AS rows_decl,
  COUNT(DISTINCT disaster_id) AS distinct_disaster_id
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX;

SELECT
  COUNT(*) AS rows_areas,
  COUNT(DISTINCT disaster_id) AS distinct_disaster_id
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX;

-- Check join multiplicity on disaster_id
SELECT
  d.disaster_id,
  COUNT(*) AS decl_rows,
  COUNT(a.*) AS area_rows
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX d
LEFT JOIN SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX a
  ON d.disaster_id = a.disaster_id
GROUP BY d.disaster_id
ORDER BY area_rows DESC
LIMIT 50;
