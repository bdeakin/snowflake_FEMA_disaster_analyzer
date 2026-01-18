# FEMA Disaster Analyzer (Snowflake + Streamlit)

This app connects to the FEMA disaster declarations dataset in Snowflake, renders results on a US map with Plotly, and supports a Mapbox-based map view.

## Setup

1. Create a virtual environment and install dependencies:

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy the environment template into the secrets file and fill in your Snowflake details:

```
cp config/env.example config/secrets.env
```

3. Set the FEMA table and column mappings if your schema differs:
   - `FEMA_TABLE` or `FEMA_TABLE_FQN`
   - `FEMA_COL_STATE`, `FEMA_COL_INCIDENT_TYPE`, `FEMA_COL_DECLARATION_DATE`
   - `FEMA_COL_LATITUDE`, `FEMA_COL_LONGITUDE`, `FEMA_COL_DISASTER_ID`

4. Provide a Mapbox token for the map view:
   - `MAPBOX_ACCESS_TOKEN` in `config/secrets.env`
5. Optional OCSP fallback (use only if your environment blocks OCSP checks):
   - `SNOWFLAKE_OCSP_FAIL_OPEN=true` in `config/secrets.env`
   - If the error persists, try `SNOWFLAKE_DISABLE_OCSP_CHECKS=true`

## Unified FEMA View

This app expects a unified FEMA view that joins disaster declarations, designated areas, mission assignments, and regions. Create the view in a database you can write to (not the shared `SNOWFLAKE_PUBLIC_DATA_PAID` database), then set `FEMA_JOINED_VIEW_FQN` in your `.env`.

Example (run in Snowflake, adjust target database/schema):

```
CREATE OR REPLACE VIEW YOUR_DB.PUBLIC.FEMA_DISASTER_UNIFIED_VIEW AS
WITH areas AS (
    SELECT
        DISASTER_ID,
        MIN(STATE_GEO_ID) AS STATE_GEO_ID,
        ARRAY_AGG(DISTINCT STATE_GEO_ID) AS STATE_GEO_IDS,
        MIN(COUNTY_GEO_ID) AS COUNTY_GEO_ID,
        ARRAY_AGG(DISTINCT COUNTY_GEO_ID) AS COUNTY_GEO_IDS,
        MIN(FEMA_REGION_ID) AS FEMA_REGION_ID,
        ARRAY_AGG(DISTINCT FEMA_REGION_ID) AS FEMA_REGION_IDS,
        ARRAY_AGG(DISTINCT FEMA_DESIGNATED_AREA) AS DESIGNATED_AREAS,
        ARRAY_AGG(DISTINCT FEMA_PLACE_CODE) AS FEMA_PLACE_CODES,
        MAX(DESIGNATED_DATE) AS DESIGNATED_DATE
    FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_AREAS_INDEX
    GROUP BY DISASTER_ID
),
missions AS (
    SELECT
        DISASTER_ID,
        COUNT(*) AS MISSION_ASSIGNMENT_COUNT,
        ARRAY_AGG(DISTINCT MISSION_ASSIGNMENT_TYPE) AS MISSION_ASSIGNMENT_TYPES,
        ARRAY_AGG(DISTINCT REQUESTING_AGENCY) AS REQUESTING_AGENCIES,
        MAX(OBLIGATION_DATE) AS MISSION_LAST_OBLIGATION_DATE,
        SUM(OBLIGATION_AMOUNT) AS MISSION_OBLIGATION_AMOUNT
    FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_MISSION_ASSIGNMENT_INDEX
    GROUP BY DISASTER_ID
)
SELECT
    d.DISASTER_ID,
    d.FEMA_DISASTER_DECLARATION_ID,
    d.DISASTER_DECLARATION_NAME,
    d.DISASTER_DECLARATION_TYPE,
    d.DISASTER_DECLARATION_DATE,
    d.DISASTER_TYPE AS INCIDENT_TYPE,
    d.DISASTER_BEGIN_DATE,
    d.DISASTER_END_DATE,
    d.DECLARED_PROGRAMS,
    a.STATE_GEO_ID,
    a.STATE_GEO_IDS,
    a.COUNTY_GEO_ID,
    a.COUNTY_GEO_IDS,
    a.FEMA_REGION_ID,
    a.FEMA_REGION_IDS,
    a.DESIGNATED_AREAS,
    a.FEMA_PLACE_CODES,
    a.DESIGNATED_DATE,
    r.FEMA_REGION_NAME,
    r.LATITUDE AS REGION_LATITUDE,
    r.LONGITUDE AS REGION_LONGITUDE,
    m.MISSION_ASSIGNMENT_COUNT,
    m.MISSION_ASSIGNMENT_TYPES,
    m.REQUESTING_AGENCIES,
    m.MISSION_LAST_OBLIGATION_DATE,
    m.MISSION_OBLIGATION_AMOUNT
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX d
LEFT JOIN areas a ON d.DISASTER_ID = a.DISASTER_ID
LEFT JOIN missions m ON d.DISASTER_ID = m.DISASTER_ID
LEFT JOIN SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_REGION_INDEX r ON a.FEMA_REGION_ID = r.FEMA_REGION_ID;
```

## Run

```
streamlit run app.py
```

## Cortex Notes

- Ensure your role has access to `snowflake.cortex.complete` and that Cortex is enabled for your account.
- The app restricts generated SQL to `SELECT` statements on the configured FEMA table.
- If your account uses a different model name, set `SNOWFLAKE_CORTEX_MODEL`.

## Troubleshooting

- If filters fail to load, check that the column mappings match your table.
- If geospatial columns are not available, the map will fall back to state-level counts.
