# Dynamic Tables Narrative

This document explains how the FEMA Disasters Explorer uses Snowflake Dynamic Tables to
transform public FEMA data into curated Silver and aggregated Gold layers, and how each
tab in the app uses those layers.

## Data Source → Silver Dynamic Table

**Source data** comes from Snowflake Public Data in:
`SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FEMA_DISASTER_DECLARATION_INDEX` and
`FEMA_DISASTER_DECLARATION_AREAS_INDEX`.

The Silver layer is built by the dynamic table:
`ANALYTICS.SILVER.FCT_DISASTERS` (see `sql/pipeline/10_silver.sql`). It:

- Normalizes one row per **disaster + county**.
- Joins declaration metadata to county-level areas.
- Adds county centroids to support map plotting.
- Derives period buckets (year/month/week) for downstream aggregation.

Because it is a **dynamic table**, it refreshes on a target lag (`TARGET_LAG='1 hour'`),
so downstream tables and the app see consistent, near‑fresh data without manual ETL runs.

## Silver → Gold Dynamic Tables

Gold dynamic tables aggregate the Silver table for fast UI rendering
(`sql/pipeline/20_gold.sql`). These are:

- `ANALYTICS.GOLD.DISASTERS_BY_STATE` (state + year totals)
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_YEAR`
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_MONTH`
- `ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_WEEK`

These keep the UI responsive by serving pre-aggregated counts rather than raw rows.

## How Each Tab Uses Dynamic Tables

### Map View
- **Choropleth** uses `ANALYTICS.GOLD.DISASTERS_BY_STATE` for state‑level totals.
- **Cube grid** uses Gold cube tables (year/month/week) depending on the selected time range.
- **Drilldown map** uses the Silver table to display county‑level points with centroids.

### Change in Disaster Types Over Time
- Uses Gold cube tables to power the bump chart (fast, aggregated counts).
- Drilldowns can use Silver for detailed county‑level context.

### Annual Disaster Themes (Sankey)
- Uses the Silver table for declaration names, types, states, and dates.
- The Sankey query now aggregates **county counts in SQL** to reduce row volume and cost.

### Disaster Impact Assessment (Sunburst)
- Uses the Silver table to build the sunburst hierarchy (type → year → event → state).
- LLM summaries use Silver data for narrative context (top states, top events).

### Consistency Checker
- Shows **dynamic table metadata** (lag, refresh) for Silver/Gold tables.
- Uses the consistency results table (not a dynamic table) plus task metadata to report
  health and scheduled refresh behavior.

## Why Dynamic Tables Are a Good Fit

- **Consistency**: Silver/Gold layers refresh together under the same lag target.
- **Freshness without orchestration**: No external scheduler needed; Snowflake manages it.
- **Performance**: Gold aggregates drastically reduce UI query load.
- **Governance**: Clear lineage from public data → Silver → Gold for audits and debugging.
- **Predictable cost**: Refreshes are incremental and bounded by target lag/warehouse.

In short, dynamic tables give the app reliable, low‑latency aggregates while preserving
access to detailed Silver rows for drilldowns and narratives.
