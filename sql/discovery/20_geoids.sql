-- Find county GeoID / FIPS-like fields in the areas table
SELECT column_name
FROM SNOWFLAKE_PUBLIC_DATA_PAID.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'PUBLIC_DATA'
  AND table_name = 'FEMA_DISASTER_DECLARATION_AREAS_INDEX'
  AND (column_name ILIKE '%FIPS%' OR column_name ILIKE '%GEO%' OR column_name ILIKE '%COUNTY%');

-- Sample potential county identifiers
SELECT
  *
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX
LIMIT 20;

-- Check state GeoID distribution
SELECT
  state_geo_id,
  COUNT(*) AS row_count
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX
GROUP BY state_geo_id
ORDER BY row_count DESC;
