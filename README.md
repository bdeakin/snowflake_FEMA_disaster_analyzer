# FEMA Disasters Explorer

Streamlit app that visualizes FEMA disaster data from Snowflake Public Data using a choropleth overview, cube summaries, and county-centroid drilldowns.

For the core architecture and data flow diagram, see `ARCHITECTURE.md`.

## WARNING: Destructive Setup
The setup script **drops and recreates** the `ANALYTICS` database:

```
DROP DATABASE IF EXISTS ANALYTICS;
```

Only run the pipeline setup in environments where this is safe.

## Requirements
- Python 3.9+
- Snowflake account with access to `SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA`

## Environment
Copy `config/env.example` to `config/secrets.env` and fill in credentials.

Required variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
Optional (use for OCSP/certificate issues):
- `SNOWFLAKE_OCSP_FAIL_OPEN` (true/false)
- `SNOWFLAKE_DISABLE_OCSP_CHECKS` (true/false)
Optional (for bump chart LLM summaries):
- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default: `gpt-4o-mini`)
Optional (local Cortex Analyst REST, PAT only):
- `SNOWFLAKE_TOKEN` (PAT token)
Note: OCSP errors were resolved by upgrading `snowflake-connector-python` to the version pinned in `requirements.txt`.

## Setup Runbook
1. Install deps:
   ```
   pip install -r requirements.txt
   ```
2. Run discovery SQL in `sql/discovery/` and update the join map summary below.
3. Run `sql/pipeline/00_setup.sql` to recreate `ANALYTICS`.
4. Download county centroids:
   ```
   python scripts/download_county_centroids.py
   ```
5. Load county centroids to Snowflake:
   ```
   python scripts/load_county_centroids_to_snowflake.py
   ```
6. Run pipeline SQL:
   - `sql/pipeline/10_silver.sql`
   - `sql/pipeline/20_gold.sql`
7. Run the app:
   ```
   streamlit run app/app.py
   ```

## Join Map Summary (Discovery)
- Base tables (INDEX only, PIT ignored in v1):
  - `FEMA_DISASTER_DECLARATION_INDEX`
  - `FEMA_DISASTER_DECLARATION_AREAS_INDEX`
- Join keys:
  - `DISASTER_ID` (one declaration joins to many areas)
- County GeoID source field:
  - `FEMA_DISASTER_DECLARATION_AREAS_INDEX.COUNTY_GEO_ID`
  - Format observed: `geoId/36013` (strip prefix, left-pad to 5 for county FIPS)
- State mapping:
  - `STATE_GEO_ID` present in areas table
  - Choropleth uses `state_abbr` from `ANALYTICS.REF.COUNTY_CENTROIDS`
- PIT tables: Not used (v1); row counts are orders of magnitude larger than INDEX

## Data Model
- `ANALYTICS.REF.COUNTY_CENTROIDS`
- `ANALYTICS.SILVER.FCT_DISASTERS`
- `ANALYTICS.GOLD.DISASTERS_BY_STATE`
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_YEAR`
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_MONTH`
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_WEEK`
