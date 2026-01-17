# FEMA Disaster Analyzer (Snowflake + Streamlit)

This app connects to the FEMA disaster declarations dataset in Snowflake, renders results on a US map with Plotly, and supports natural-language queries via Snowflake Cortex.

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
