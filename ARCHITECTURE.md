# Architecture Overview

This application provides a fast “overview → investigate → drilldown” workflow on FEMA
disaster data hosted in Snowflake. The pipeline builds curated Silver and aggregated Gold
datasets, and the Streamlit app renders a choropleth, a period summary, and a drilldown
map with county-centroid jitter. A bump chart tab surfaces top disaster types by decade,
with drilldown summaries and optional LLM context.

## Core Components
- **Snowflake Public Data**: Source FEMA tables in `SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA`.
- **Reference Data**: County centroids loaded into `ANALYTICS.REF.COUNTY_CENTROIDS`.
- **Silver Layer**: `ANALYTICS.SILVER.FCT_DISASTERS` (one row per disaster + county, derived
  periods, state derivation via `STATE_GEO_ID` and centroid reference).
- **Gold Layer**: Aggregate dynamic tables used for fast queries by state and period.
- **Streamlit App**: `app/app.py` drives UI and calls query helpers to render Plotly charts.
- **LLM Summary**: Optional OpenAI summary for bump chart drilldown selections.

## Data & UI Flow
\n```mermaid
flowchart LR
  sourceFEMA[SnowflakePublicData] --> silverFCT[SilverFCT_DISASTERS]
  refCentroids[CountyCentroids] --> silverFCT
  silverFCT --> goldAgg[GoldAggregates]
  goldAgg --> appUI[StreamlitApp]
  silverFCT --> appUI
  appUI --> bumpTab[BumpChartTab]
  bumpTab --> drilldown[DrilldownTable]
  drilldown --> llm[OpenAI_Summary]
\n```

## Notes
- Gold dynamic tables provide responsive aggregation for the main view.
- Drilldown uses county centroids; overlapping incidents are jittered for visibility.
- LLM summaries are cached per decade/type and shown in a modal on bump selection.
