# Architecture Overview

This application provides a fast “overview → investigate → drilldown” workflow on FEMA
disaster data hosted in Snowflake. The pipeline builds curated Silver and aggregated Gold
datasets, and the Streamlit app renders a choropleth, a period summary, and a drilldown
map with county-centroid jitter. The UI is organized into Map View, Change in Disaster Types
Over Time, Annual Disaster Themes, Disaster Impact Assessment, and Consistency Checker tabs,
with an About tab that documents narrative, architecture, and integrations.

## Core Components
- **Snowflake Public Data**: Source FEMA tables in `SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA`.
- **Reference Data**: County centroids loaded into `ANALYTICS.REF.COUNTY_CENTROIDS`.
- **Silver Layer**: `ANALYTICS.SILVER.FCT_DISASTERS` (one row per disaster + county, derived
  periods, state derivation via `STATE_GEO_ID` and centroid reference).
- **Gold Layer**: Aggregate dynamic tables used for fast queries by state and period.
- **Streamlit App**: `app/app.py` drives UI and calls query helpers to render Plotly charts.
- **Cortex Assistant**: Map View includes a chat pane powered by the Analyst semantic view
  `ANALYTICS.SILVER.DISASTER_EXPLORER` via the Cortex Analyst REST API.
- **LLM Summary**: Optional OpenAI summary for bump chart drilldown selections.
- **Sankey Cache**: LLM name grouping cache stored in `ANALYTICS.MONITORING`.

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
- Explore cube chart uses log-scaled bubble sizes to surface small counts.
- LLM summaries are cached per decade/type and shown in a modal on bump selection.
- Active tab selection controls which sidebar filters are rendered to keep state isolated.
- Sunburst narratives render inline below the chart based on selection (year, event, state).
- Sunburst selection filters the displayed subtree to keep focus on the active event path.
- Sunburst drilldown shows breadcrumbs and locks filters while focused.
- Sunburst year colors are assigned from a stable, session-level map.
- Sunburst filtering diagnostics track node subsets during drill-in.
- Filtered sunburst totals are recomputed to keep full-circle rendering.
- Sunburst selection updates trigger a rerun to apply the new subtree immediately.
- Sunburst reset clears selection state and restarts from full hierarchy.
- Sunburst ignores the first event after rerun to prevent unintended auto-selection.
- Sunburst drill-in re-roots the chart at the selected node to hide prior rings.
- Release summaries are tracked in `VERSION_HISTORY.md`.
- About tab narrative is sourced from `config/app_metadata.py` for centralized edits.
- Consistency Checker writes to `ANALYTICS.MONITORING` via a 12-hour Snowflake Task.
- Snowflake connection supports key-pair auth for environments with MFA.
- Streamlit deployments require Snowpark in `requirements.txt` to access active sessions.
- Consistency Checker displays scheduled task metadata and the next scheduled run time.
- Consistency Checker results view is refreshed on demand without triggering a run.
- Consistency Checker failure notes record the last query error message or step name.
- Development narrative is tracked in `DEVELOPMENT_NARRATIVE.md`.
- Sankey uses a cached LLM grouping of declaration names per disaster record.
- Sankey aggregates county counts in SQL to reduce row volume before rendering.
