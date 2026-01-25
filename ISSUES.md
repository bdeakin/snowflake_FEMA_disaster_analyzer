# Issues and Fixes

This list summarizes notable bugs encountered during development and the fixes applied.

## Map View
- Year range failed to include 2025 when declaration dates were missing. Fixed by using an
  effective date (`COALESCE(declaration, begin, end)`) for Map View filters.
- Selected state cleared on filter changes and drilldown persisted unexpectedly. Fixed by
  persisting selected state across filter changes and resetting drilldown until a new cube
  selection is made.
- Drilldown legend layout caused nested column errors. Fixed by moving legend into an expander.

## Disaster Summary by Period (Cube)
- X-axis showed fractional year labels (e.g., `2,024.2`). Fixed by forcing a categorical
  axis ordered by period buckets.
- Circles were hard to see on some backgrounds. Added a black marker outline for contrast.

## Annual Disaster Themes (Sankey)
- No-records for 2025 caused by null declaration dates. Fixed by filtering on effective date.
- Cache warming mismatch with aggregated query caused missing cache hits. Fixed record_id
  generation and cache logic to align with aggregation.
- Cache status visibility requested; added per-year cached/uncached display in filter pane.

## Disaster Impact Assessment (Sunburst)
- Infinite rerenders and auto-advancing drilldowns. Stabilized click handling, added
  guarded reruns, and locked filters during drilldown.
- Drilldown filter state desynced with selection. Ensured focused disaster type remains
  selected while filters are locked.
- “Deselect all” still showed a chart. Added a clear “No data selected” state.
- Missing data on render caused `_build_sunburst_nodes` errors. Guarded render path.

## Cortex Assistant
- Initial implementation expected a UDF and failed. Switched to Cortex Analyst REST API.
- Assistant returned only interpretation text. Added SQL execution and a result preview table.
- Context was missing (years/types/state). Added prompt context and send-time progress cues.
- Year formatting appeared with thousands separators. Normalized year-like columns for display.

## Snowflake Auth and Deployment
- MFA/TOTP prevented password auth. Added key-pair auth support and made password optional.
- DER key formatting errors. Documented conversion to unencrypted DER and base64 encoding.

## Cache Warming / LLM
- Cache warmer `record_id` mismatch after query aggregation. Fixed ID hashing logic.
- LLM grouping reran unnecessarily. Added incremental cache and reuse of grouped names.
