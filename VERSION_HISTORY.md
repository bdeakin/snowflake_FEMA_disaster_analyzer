# Version History

This file summarizes notable changes to the FEMA Disasters Explorer over time. It is
intended as a high-level narrative; see `git log` for exhaustive details.

## v1.10
- Sankey now uses single-year filtering with theme-first flow.

## v1.11
- Sunburst drilldown stabilized with locked filters, breadcrumbs, and stable year colors.
- Sunburst name grouping now uses an incremental cache to avoid repeated LLM calls.

## v1.9
- Explore cube chart now uses log-scaled bubble sizes for small counts.
- Explore drilldown resets on filter changes to keep results in sync.
- Sankey filters support multi-select defaults and year formatting improvements.

## v1.8
- Added a Sankey tab with year/type filters and D3-based rendering.
- Implemented LLM name grouping with a persistent cache in `ANALYTICS.MONITORING`.
- Added Snowflake DDL for the name grouping cache table.

## v1.7
- Consistency Checker now read-only in-app with manual refresh and next run time display.
- Consistency checks run as caller to avoid task-owner permission gaps.
- Monitoring SQL cleaned of debug scaffolding and sync error reporting improved.

## v1.6
- Sunburst drilldown behavior stabilized: re-rooted views, subtree filtering, and inline narratives.
- Sunburst now supports named event narratives with LLM summaries.
- Per-tab filters standardized; Trends adds binning (decades/years/months) and Top-N.
- Cleanup: removed unused LLM helpers and empty directories.

## v1.5
- Replaced the Sankey experiment with a Sunburst chart for named storms and states.
- Added reset control for Sunburst navigation.

## v1.4
- Added Cortex Search and local/SiS compatibility work (later removed).
- Improved Snowflake credential handling for Streamlit contexts.

## v1.3
- Added LLM summaries for Disaster Type Trends drilldowns.
- Improved narrative formatting and state-focused summaries.

## v1.2
- Added Disaster Type Trends tab (bump chart) with drilldown table and state summaries.
- Derived missing state values in Silver layer.

## v1.1
- Stabilized drilldown behavior and improved point rendering per disaster.
- Updated Snowflake connector version to address OCSP issues.

## v1.0
- Initial working release: choropleth + period summary + county drilldown.
