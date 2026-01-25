# Tasks Narrative

This document explains how the FEMA Disasters Explorer uses Snowflake Tasks to run a
scheduled consistency checker, how the results are produced, and how the app surfaces
them in the Consistency Checker tab.

## Why Tasks

The app needs a reliable, scheduled way to verify that Silver and Gold layers remain in
sync with the public FEMA source data. Snowflake Tasks provide:

- **Scheduling** without external cron or orchestration.
- **Reliable execution** inside Snowflake.
- **Auditability** through task history and result tables.

## How the Task Is Configured

The consistency checker task is defined in the pipeline SQL (see `sql/pipeline/21_consistency.sql`)
and is scheduled to run every 12 hours. The task calls a stored procedure that computes
row counts, distinct counts, and date bounds across the Public, Silver, and Gold layers.

Key configuration elements:

- **Schedule**: 12-hour cadence.
- **Warehouse**: Uses a dedicated warehouse for consistent performance.
- **Procedure call**: The task calls a stored procedure that writes a summary row into
  a monitoring results table.

## How the Data Is Produced

Each task run executes the stored procedure, which:

1. Queries the public FEMA datasets for reference counts and date ranges.
2. Queries the Silver table for the same metrics.
3. Queries the Gold aggregates for totals and timing metadata.
4. Compares the results and produces status columns such as `IN_SYNC`, `STALE_OK`,
   or `OUT_OF_SYNC`.
5. Writes a single run record into the monitoring results table, including timestamps,
   counts, and explanatory notes.

## How the App Presents Task Results

On the **Consistency Checker** tab, the app:

- Shows **task status** and next scheduled run using task metadata.
- Displays **task history** (last 5 runs).
- Presents **consistency results** with filters for date range and status.

This gives users a clear view of whether the data pipeline is current and trustworthy,
and when the next automated validation will occur.
