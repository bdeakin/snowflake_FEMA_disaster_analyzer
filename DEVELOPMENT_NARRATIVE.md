# Development Narrative

This document summarizes the development activity for the FEMA Disasters Explorer,
with emphasis on plan-based build phases and the bug-fix work that occurred along
the way. Counts are approximate, based on the project prompt history.

## Plan-Based Build Phases

### 1) Initial Plan: FEMA Explorer Core
- Goal: Build a Streamlit app with a fast "overview -> investigate -> drilldown" flow.
- Outputs: Choropleth, cube summary, drilldown map, Silver + Gold dynamic tables,
  county centroid reference data, and default 2023–2025 time filters.

### 2) Sankey Plan (Later Retired)
- Goal: Year -> hurricane -> state flow with LLM damage estimates and click modals.
- Outcome: Persistent mapping/ordering issues led to deprecation of the Sankey approach.

### 3) Sunburst Plan (Replacement)
- Goal: Asymmetric Sunburst: incident type -> year -> named event -> state.
- Outputs: LLM grouping and summaries, drilldown re-rooting, reset control, inline
  narrative panel.

### 4) Named Storms Plan
- Goal: Include named Tropical Storms alongside Hurricanes.
- Outcome: Top-level grouping expanded to "Named Hurricanes" + "Tropical Storms" and
  event/state narratives.

### 5) Consistency Checker Plan
- Goal: 12-hour consistency checks between Public, Silver, and Gold layers using
  Snowflake Tasks + Stored Procedures.
- Outputs: Monitoring schema, results table, stored procedure, task, and a UI tab for
  results and task metadata.

### 6) Read-Only Consistency Checker UI
- Goal: Remove in-app "Run Now" and rely on Snowflake task/manual procedure calls.
- Outputs: Read-only results view, "Refresh Results" button, and next run metadata.

### 7) Sankey (Observable Template)
- Goal: Add a Sankey view with LLM-grouped declaration names and cached outputs.
- Outputs: Sankey tab with year/type filters, LLM name grouping cache table, and
  D3-based rendering via Streamlit HTML.

### 8) Explore Filter Sync + Log Scale
- Goal: Ensure small-count disasters are visible and drilldown respects filters.
- Outputs: Log-scaled cube bubbles and filter-driven drilldown resets.


## Bug-Fix Narrative (By Category)

### Visualization Interaction / Mapping (≈8–10)
- Sankey mapping inversions, hover mismatches, and ordering issues.
- Sunburst drilldown bugs: partial arcs, reappearing rings, incorrect re-rooting, reset
  behavior.
- Sunburst drilldown stabilized with breadcrumb trail, subtree focus, and stable year colors.

### UI State & Filtering (≈5–7)
- Sidebar filters not switching per tab.
- Consistency Checker date filtering excluding valid runs.
- Rerun/selection state quirks during Sunburst drilldown.
- Drilldown locks filters to prevent cross-filter interference.
- Annual Disaster Themes defaults adjusted to focus initial view.
- Annual Disaster Themes defaults updated to 2024 with Fire, Flood, and Hurricane.

### LLM + News Integrations (≈4–6)
- LLM summarization error handling and modal issues.

### Snowflake SQL / Stored Procedures (≈10–14)
- SQL scripting syntax and variable binding errors.
- Dynamic table metadata field mismatch (last_refresh_time vs last_refresh).
- Procedure execution privileges and compilation fixes.

### Permissions & Metadata Access (≈4–6)
- INFORMATION_SCHEMA / ACCOUNT_USAGE access errors.
- Task metadata access and safe fallbacks.
- Procedure execution role vs caller privileges.

### Instrumentation / Cleanup (≈3–5)
- Temporary debug logging during investigations, later removed.
- Removal of dead code and empty directories.
- About tab narrative expanded to clarify intent and technical hurdles.

## Current State

- Visualization: Sunburst is the primary exploratory visualization with inline
  narratives.
- Visualization: Sankey provides Year → Type → Name → State flow views.
- News/LLM: Summaries derived from FEMA metadata.
- Consistency Checker: Read-only UI with manual refresh and scheduled task metadata.
- Monitoring: Procedure runs as caller to avoid ownership privilege gaps.

## Approximate Fix Counts (Summary)

- Visualization: ~8–10
- UI state/filtering: ~5–7
- LLM/news: ~4–6
- Snowflake SQL/SP: ~10–14
- Permissions/metadata: ~4–6
- Instrumentation/cleanup: ~3–5
