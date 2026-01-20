# Prompts Log

Format:
- timestamp: YYYY-MM-DD HH:MM
  goal: <short goal>
  prompt: |
    <verbatim prompt>
  outcome: <what happened>
  next: <next action>

- timestamp: 2026-01-18 00:00
  goal: Define new FEMA explorer plan
  prompt: |
    Cursor Plan Mode: FEMA Disasters Explorer (Snowflake Public Data → Dynamic Tables → Choropleth + Cube Summary + Drilldown)
    Goals
    
    Build a Streamlit application that visualizes FEMA disaster data sourced from Snowflake Public Data.
    
    Provide a fast “overview → investigate → drilldown” experience:
    
    Viewport 1: US state choropleth colored by disaster count
    
    Viewport 2: State-selected cube summary grid by disaster type and adaptive time period
    
    Drilldown: Cube-selected map of individual disasters as icons with hover details
    
    Use Snowflake Dynamic Tables to create reproducible, performant Silver (curated) and Gold (aggregated) datasets.
    
    Support performant rendering by defaulting the time filter to 2023–2025.
    
    Non-Goals
    
    No Cortex AI, LLM features, RAG, embeddings, etc.
    
    No real-time ingestion / streaming pipelines.
    
    No production-grade auth / user management.
    
    No high-resolution incident coordinates: FEMA lacks direct lat/lon; we will use county centroids.
    
    No full GIS polygon overlays beyond state choropleths.
    
    Constraints / Hard Requirements
    
    Snowflake connection must use environment variables:
    
    SNOWFLAKE_ACCOUNT=BIUHBZF-WFC89039
    
    SNOWFLAKE_ROLE=ACCOUNTADMIN
    
    SNOWFLAKE_WAREHOUSE=COMPUTE_WH
    
    SNOWFLAKE_DATABASE=SNOWFLAKE_PUBLIC_DATA_PAID
    
    SNOWFLAKE_SCHEMA=PUBLIC_DATA
    
    FEMA source tables are in SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA and include:
    
    FEMA_DISASTER_DECLARATION_INDEX
    
    FEMA_DISASTER_DECLARATION_INDEX_PIT
    
    FEMA_DISASTER_DECLARATION_AREAS_INDEX
    
    FEMA_DISASTER_DECLARATION_AREAS_INDEX_PIT
    
    FEMA_MISSION_ASSIGNMENT_INDEX
    
    FEMA_MISSION_ASSIGNMENT_INDEX_PIT
    
    FEMA_NATIONAL_FLOOD_INSURANCE_PROGRAM_CLAIM_INDEX
    
    FEMA_NATIONAL_FLOOD_INSURANCE_PROGRAM_CLAIM_INDEX_PIT
    
    FEMA_NATIONAL_FLOOD_INSURANCE_PROGRAM_POLICY_INDEX
    
    FEMA_NATIONAL_FLOOD_INSURANCE_PROGRAM_POLICY_INDEX_PIT
    
    FEMA_REGION_INDEX
    
    FEMA_REGION_INDEX_PIT
    
    Join discovery is mandatory. Do not assume keys.
    
    Incidents do not have lat/long; they have county GeoIDs (likely county FIPS/GEOID). Map drilldown points must be plotted using county centroids.
    
    The build process must download a county centroid CSV and load it into Snowflake as reference data.
    
    The project must drop and recreate the ANALYTICS database as part of setup:
    
    DROP DATABASE IF EXISTS ANALYTICS;
    
    recreate schemas ANALYTICS.REF, ANALYTICS.SILVER, ANALYTICS.GOLD
    
    must be clearly warned/documented in README
    
    Must maintain prompts.md with all prompts used during build.
    
    Success Criteria (Definition of Done)
    
    Running setup SQL + scripts produces a clean environment from scratch (idempotent):
    
    ANALYTICS recreated
    
    county centroids downloaded + loaded
    
    Silver + Gold objects created
    
    Streamlit app loads and works end-to-end:
    
    Year range filter defaults to 2023–2025
    
    Choropleth renders quickly and correctly
    
    Clicking a state shows cube summary grid
    
    Clicking a cube switches map to point view and shows hover details
    
    Drilldown view uses county GeoIDs mapped to centroids from reference dataset.
    
    Repo contains all required deliverables including prompts.md, discovery SQL, pipeline SQL, and scripts.
    
    Application Requirements (UI and Interaction)
    Global Filter
    
    Year range slider
    
    Default: start year 2023, end year 2025
    
    Filters all queries to control data volume/performance
    
    Viewport 1 — State Choropleth
    
    US state choropleth
    
    Metric: total number of disasters in filtered dataset
    
    Color scale: light red → bright red by count
    
    On click: sets selected_state
    
    Viewport 2 — Cube Summary (below map)
    
    Shown only when a state is selected.
    
    Grouped by:
    
    Disaster type (incident type)
    
    Period bucket (grain selected dynamically)
    
    Adaptive period logic
    
    multi-year selection → yearly
    
    multi-month selection → monthly
    
    multi-week selection → weekly
    
    Visualization:
    
    table-like “cube grid”
    
    cube size proportional to count
    
    cube color proportional to count (light red → bright red)
    
    On click: sets selected_disaster_type and selected_period_bucket
    
    Drilldown Mode — County-Centroid Icons
    
    Triggered when a cube is selected.
    
    Map switches from choropleth to a scatter-icon plot
    
    Each point plotted at:
    
    county centroid lat/lon from reference dataset
    
    Hover tooltip shows:
    
    disaster id / number
    
    declaration/incident date
    
    disaster type
    
    county name
    
    state
    
    title/name (if available)
    
    Handle overlap:
    
    aggregate by county (count) OR jitter
    
    Data Model and Pipeline
    Discovery Requirements
    
    All join/key inference must be done through discovery. Must output:
    
    selected base tables
    
    join keys
    
    which table contains county GeoIDs
    
    how PIT tables work, whether/how used
    
    All discovery SQL stored under:
    
    /sql/discovery/*.sql
    
    Reference Data
    
    Download county centroids CSV during build
    
    Script: scripts/download_county_centroids.py
    
    Output: data/county_centroids.csv
    
    Load into:
    
    ANALYTICS.REF.COUNTY_CENTROIDS
    
    Minimum columns:
    
    county_fips (string 5-digit)
    
    county_name
    
    state_fips
    
    optionally state_abbr
    
    centroid_lat float
    
    centroid_lon float
    
    Silver Layer (curated, drilldown-friendly)
    
    Create:
    
    ANALYTICS.SILVER.FCT_DISASTERS (DT or view)
    
    Grain:
    
    one row per (disaster, county)
    
    Must include:
    
    disaster id
    
    state
    
    county FIPS/GEOID
    
    centroid lat/lon (join to REF)
    
    disaster type
    
    relevant dates
    
    derived period buckets: year/month/week
    
    Gold Layer (fast aggregates)
    
    Create DTs for:
    
    Choropleth:
    
    ANALYTICS.GOLD.DISASTERS_BY_STATE
    
    Cube summaries at multiple grains:
    
    ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_YEAR
    
    ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_MONTH
    
    ANALYTICS.GOLD.CUBES_BY_STATE_TYPE_WEEK
    
    App chooses the correct one based on selection span.
    
    Architecture Overview
    Snowflake
    
    Source: SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA
    
    Derived: ANALYTICS
    
    REF (county centroids)
    
    SILVER (curated fact)
    
    GOLD (aggregates)
    
    Repo Structure
    prompts.md
    .env.example
    requirements.txt
    README.md
    sql/
      discovery/
      pipeline/
    scripts/
      download_county_centroids.py
      load_county_centroids_to_snowflake.py
    app/
      app.py
      snowflake_conn.py
      queries.py
      viz.py
    data/
      county_centroids.csv   (generated)
    
    Implementation Phases
    Phase 0 — Project scaffolding + prompts discipline
    
    Create repo structure
    
    Create .env.example
    
    Create prompts.md
    
    Acceptance
    
    Repo skeleton exists
    
    prompts.md format established
    
    Phase 1 — Discovery (keys, joins, PIT interpretation, county GeoID)
    
    Inspect columns and sample data
    
    Determine join keys and validate cardinality
    
    Identify county GeoID fields
    
    Decide whether PIT is needed or ignore for v1
    
    Acceptance
    
    Discovery SQL saved
    
    README includes join map summary
    
    Phase 2 — Setup pipeline (destructive ANALYTICS rebuild)
    
    Create /sql/pipeline/00_setup.sql:
    
    drop + recreate ANALYTICS
    
    create schemas REF/SILVER/GOLD
    
    Document WARNING in README
    
    Acceptance
    
    Running setup script resets everything cleanly
    
    Phase 3 — County centroid download + Snowflake load
    
    scripts/download_county_centroids.py
    
    download, validate, save to data/
    
    scripts/load_county_centroids_to_snowflake.py
    
    create table and load CSV
    
    Acceptance
    
    ANALYTICS.REF.COUNTY_CENTROIDS populated and queryable
    
    Phase 4 — Silver curated dataset
    
    Build ANALYTICS.SILVER.FCT_DISASTERS with:
    
    FEMA data joined to areas
    
    county GeoID extracted
    
    join to county centroids
    
    Store derived buckets
    
    Acceptance
    
    Filters 2023–2025 return manageable row counts
    
    county centroid join coverage documented
    
    Phase 5 — Gold DTs
    
    Build gold aggregates for:
    
    choropleth
    
    cubes at year/month/week grains
    
    Acceptance
    
    Fast aggregate queries
    
    Results reconcile with Silver spot checks
    
    Phase 6 — Streamlit app UI
    
    Implement:
    
    year range filter default 2023–2025
    
    choropleth map
    
    state click selection
    
    cube grid (adaptive grain)
    
    cube click drilldown (county centroid scatter)
    
    Acceptance
    
    End-to-end interactions function correctly
    
    Drilldown hover shows required fields
    
    Risks & Mitigations
    
    GeoID not county FIPS (could be another code)
    
    discovery must confirm
    
    centroid mapping logic must normalize GeoID → county_fips
    
    County join yields multiple counties per disaster
    
    treat as one row per disaster+county (expected)
    
    PIT tables cause ambiguity
    
    v1 may ignore PIT and use INDEX only
    
    document in README
    
    Data volume large
    
    default year range 2023–2025 mandatory
    
    additional LIMITs in drilldown
    
    State choropleth requires state code normalization
    
    ensure 2-letter state abbreviations used for mapping
    
    Notes on Prompts Discipline
    
    All prompts used in Cursor must be logged in prompts.md with:
    
    timestamp
    
    goal
    
    prompt text
    
    outcome
    
    next
  outcome: Plan accepted for implementation
  next: Implement plan tasks

- timestamp: 2026-01-18 00:05
  goal: Implement the approved plan
  prompt: |
    Implement the plan as specified, it is attached for your reference. Do NOT edit the plan file itself.
    
    To-do's from the plan have already been created. Do not create them again. Mark them as in_progress as you work, starting with the first one. Don't stop until you have completed all the to-dos.
  outcome: Work in progress
  next: Continue tasks

- timestamp: 2026-01-18 00:20
  goal: Run discovery SQL
  prompt: |
    Run the discovery.
  outcome: Discovery executed with column/key updates
  next: Update silver/gold queries based on discovery

- timestamp: 2026-01-18 00:25
  goal: Handle OCSP issues
  prompt: |
    It may be necessary to switch to insecure mode to avoid OCSP errors.
  outcome: Added optional OCSP flags support
  next: Retry discovery with flags

- timestamp: 2026-01-18 00:30
  goal: Retry discovery
  prompt: |
    Retry discovery.
  outcome: Discovery completed with OCSP flags
  next: Update data model fields

- timestamp: 2026-01-18 00:40
  goal: Run setup + pipeline SQL
  prompt: |
    Confirmed it is safe to drop and recreate analytics. Run the setup and pipeline SQL.
  outcome: Setup ran; centroids loaded; silver/gold DTs created
  next: Validate app with sample queries

- timestamp: 2026-01-18 00:45
  goal: Run discovery with OCSP flags
  prompt: |
    Retry discovery.
  outcome: Discovery completed using OCSP flags
  next: Update silver/gold fields

- timestamp: 2026-01-18 01:10
  goal: Enhance filters and cube view
  prompt: |
    Enhance the filter pane to include incident type. Also enhance the filter pane to include disaster start and end dates which can be explicitly selected. Replace the existing cube view which shows circles with a tabular view having incident types as rows and bins as columns. The bins should be sized based on the period of the returned record set (years for multi-year record sets, months for multi-month record sets, weeks for multi-week record sets). The representation of the data should be an actual cube rather than a circle. When clicking on one of the cubes, the drilldown should automatically focus on the map location for that drilldown.
  outcome: Implemented filters and cube heatmap with square markers
  next: User validation

- timestamp: 2026-01-18 10:30
  goal: Add NLQ pane with Cortex Analyst
  prompt: |
    Add a new “Natural Language Map Query” pane without changing the existing app...
  outcome: NLQ pane added with Analyst-driven SQL and map rendering
  next: Validate Analyst SQL generation

- timestamp: 2026-01-18 11:05
  goal: Rollback NLQ pane
  prompt: |
    For now, let's roll back the NLQ functionality. Eliminate the NLQ pane. Preserve the existing functionality on the main viewport. Eliminate dead and redundant code paths as a result of the roll back.
  outcome: Removed NLQ tab, deleted NLQ modules and semantic model, cleaned imports
  next: Verify Explore view only

- timestamp: 2026-01-18 11:20
  goal: Add bump chart pane
  prompt: |
    Create an additional pane within the application which has a bump chart showing the change in most frequently occurring disaster types by year. When clicking on an individual entry in the bump chart, produce a drilldown table view of the disasters within that entry. Summarize the rows in the drilldown to the incident type with detail about the count occurring in each state.
  outcome: Added bump chart tab with drilldown table
  next: Fix chart/query issues

- timestamp: 2026-01-18 11:35
  goal: Adjust bump chart UX
  prompt: |
    Make the bump chart significantly taller so that the visualization is easier to read, and limit the chart to the Top-5 most common disasters by year instead of Top-10.
  outcome: Chart height increased and top-5 enforced
  next: Enhance drilldown columns

- timestamp: 2026-01-18 11:40
  goal: Add declared incident names
  prompt: |
    On the bump chart in the drilldown area, add a column to the table with a distinct list of the declared disasters in each row.
  outcome: Added distinct declared incident list column
  next: Ensure correct naming

- timestamp: 2026-01-18 11:45
  goal: Use incident names
  prompt: |
    The table in the drilldown is showing the names of the disaster types. Instead, it should show the name of the declared incident - for instance "Hurricane Helene" or "Hurricane Irma".
  outcome: Drilldown list uses declaration_name for specific incidents
  next: Resolve LISTAGG order error

- timestamp: 2026-01-18 12:05
  goal: Decade bump chart + state derivation
  prompt: |
    Change the bump chart to be by decade instead of by year. Also, there are a number of disasters which appear to have no assigned state... derive the state from other information available and update the dynamic tables appropriately.
  outcome: Bump chart uses decade buckets; silver DT derives state via STATE_GEO_ID
  next: Rebuild silver DT

- timestamp: 2026-01-18 12:20
  goal: Bump chart styling
  prompt: |
    Rather than the bump chart being displayed as lines, re-factor to look more like this... circles with consistent colors.
  outcome: Chart uses labeled circles with stable colors
  next: Improve contrast/ordering

- timestamp: 2026-01-18 12:25
  goal: Bump chart readability
  prompt: |
    In the bump chart, the text inside the ball needs to be higher contrast... add bars that connect the disaster types... decades ordered logically.
  outcome: Added connecting lines, white labels, chronological decade axis
  next: Refine drilldown columns

- timestamp: 2026-01-18 12:30
  goal: Drilldown specific disasters column
  prompt: |
    In the drilldown section, create a separate column with the names of specific disasters.
  outcome: Added specific_disasters column
  next: Ship release

- timestamp: 2026-01-18 12:45
  goal: Add LLM summary
  prompt: |
    On the bump chart, when the user clicks on an entry, query an external LLM to summarize the causes... and embed as modal.
  outcome: Added OpenAI summary modal with caching
  next: Handle API key/quota

- timestamp: 2026-01-18 12:55
  goal: LLM summary format update
  prompt: |
    Update the LLM summarization to provide a broad thematic overview... summarize states in bullets... embed in a modal pop-up.
  outcome: Prompt updated for overview + bullets; modal dialog used
  next: Validate with quota

- timestamp: 2026-01-18 13:20
  goal: Cortex Analyst via REST
  prompt: |
    The code needs to be re-factored to ensure it is using the Cortex Analyst REST API, not relying on SQL system functions.
  outcome: Planned REST-only paths for SiS + local
  next: Implement REST client and update docs

- timestamp: 2026-01-18 13:35
  goal: PAT-only Cortex Analyst
  prompt: |
    Rebuild the application to use programmatic access tokens (PAT) for Cortex Analyst.
  outcome: PAT-only mode implemented with secrets.env loading and clearer 401 messaging
  next: Validate PAT behavior

- timestamp: 2026-01-18 13:50
  goal: Cortex REST troubleshooting
  prompt: |
    Can you design a curl request to test the endpoint from the command line against this REST endpoint?
  outcome: Provided curl template for /api/v2/cortex/analyst/message
  next: Test endpoint permissions

- timestamp: 2026-01-18 14:10
  goal: Streamlit Cloud secrets support
  prompt: |
    When attempting to open Cortex Analyst on Streamlit in Snowflake, this error is displayed: Cortex Search requires Snowflake credentials or Streamlit in Snowflake.
  outcome: Added st.secrets fallback for SNOWFLAKE_* and updated UI guidance
  next: Verify secrets setup in Cloud

- timestamp: 2026-01-18 14:25
  goal: Remove debug logging errors
  prompt: |
    This error occurred: Cortex Analyst call failed: [Errno 2] No such file or directory: '/.../.cursor/debug.log'
  outcome: Removed debug logger and hardcoded log path
  next: Commit cleanup

- timestamp: 2026-01-18 14:40
  goal: New account rebuild
  prompt: |
    I have to setup a new Snowflake Enterprise account... regenerate ANALYTICS and county centroids... update code for new env vars.
  outcome: Rebuilt ANALYTICS, reloaded centroids, rebuilt Silver/Gold in new account
  next: Validate app UI flow

- timestamp: 2026-01-18 14:55
  goal: Run command
  prompt: |
    Give me the fully qualified command line to run the application.
  outcome: Provided command with cd + python3 + streamlit run
  next: Run app and verify

- timestamp: 2026-01-18 15:05
  goal: Prompts log update
  prompt: |
    Update the prompts.md file based on prompts entered so far.
  outcome: Appended recent prompts and outcomes
  next: Commit if requested


- timestamp: 2026-01-19 02:00
  goal: Implement 12-hour Consistency Checker
  prompt: |
    Implement the 12-hour Consistency Checker with ANALYTICS.MONITORING and no disaster type filter.
  outcome: Replaced monitoring schema/task, updated UI, SQL, and docs
  next: Re-run 21_consistency.sql and resume task

- timestamp: 2026-01-19 02:20
  goal: Fix consistency SQL insert syntax
  prompt: |
    SQL compilation error: syntax error line 169 at position 2 unexpected 'INSERT'.
  outcome: Reordered INSERT to precede CTE and bound date variables
  next: Re-run 21_consistency.sql

- timestamp: 2026-01-19 02:40
  goal: Fix consistency results filter columns
  prompt: |
    SQL compilation error: invalid identifier 'WINDOW_START' in Consistency Checker results query.
  outcome: Updated results query to use window_start_date/window_end_date
  next: Reload Consistency Checker tab

- timestamp: 2026-01-19 02:55
  goal: Fix consistency status filter column
  prompt: |
    SQL compilation error: invalid identifier 'PUBLIC_VS_SILVER_STATUS'.
  outcome: Updated status filter to use silver_vs_public_status
  next: Reload Consistency Checker tab

- timestamp: 2026-01-19 03:10
  goal: Fix Run Now variable binding error
  prompt: |
    Failed to run consistency check: invalid identifier 'V_START'.
  outcome: Bound v_start/v_end with colon in exception insert
  next: Re-run 21_consistency.sql and try Run Now

- timestamp: 2026-01-19 03:25
  goal: Fix SQLERRM reference in consistency SP
  prompt: |
    Failed to run consistency check: invalid identifier 'SQLERRM'.
  outcome: Replaced SQLERRM with static error text in exception handler
  next: Re-run 21_consistency.sql and try Run Now

- timestamp: 2026-01-19 03:40
  goal: Fix UUID_STRING error in exception insert
  prompt: |
    Failed to run consistency check: Invalid expression [UUID_STRING()] in VALUES clause.
  outcome: Generate error UUID/timestamp in variables and insert via SELECT
  next: Re-run 21_consistency.sql and try Run Now

- timestamp: 2026-01-19 03:55
  goal: Handle Run Now unknown error
  prompt: |
    When I click "Run now" the following error occurs: Failed to run consistency check: Unknown error. However, it appears that a row is created in consistency results.
  outcome: Downgraded Run Now errors to warning and continue to show results
  next: Retry Run Now and verify results row

- timestamp: 2026-01-19 04:10
  goal: Debug Run Now stored procedure failure
  prompt: |
    When I click "Run Now" the following error occurs: Run Now returned an error, but a results row may still have been recorded. Error: Unknown error. The results rows show Stored procedure failed.
  outcome: Added runtime debug logging around Run Now and consistency check execution
  next: Reproduce Run Now and inspect debug logs

- timestamp: 2026-01-19 18:30
  goal: Stabilize sunburst drilldown UX
  prompt: |
    Stabilize sunburst drilldown behavior: lock filters during drilldown,
    add breadcrumb trail, prevent stale click replays, keep year colors
    stable across drilldowns, and avoid repeated name grouping calls.
  outcome: Implemented filter lock + breadcrumbs, stable year color map, and
    incremental name grouping cache; removed debug instrumentation after fix.
  next: Monitor drilldown behavior for any remaining rerender regressions.

- timestamp: 2026-01-19 19:10
  goal: Update About tab narrative
  prompt: |
    Expand the About tab development narrative and key technical challenges
    to better explain intent and the purpose of recent functionality.
  outcome: Expanded project phases and challenge descriptions in app metadata
    and updated documentation references.
  next: Refresh About tab to validate content display.

- timestamp: 2026-01-20 00:15
  goal: Default Annual Disaster Themes to Fire
  prompt: |
    Default the Annual Disaster Themes filter to Fire only.
  outcome: Default selection now sets Fire and deselects other types.
  next: Verify initial filter state in the app.

- timestamp: 2026-01-20 00:25
  goal: Update Annual Disaster Themes defaults
  prompt: |
    Default Annual Disaster Themes to Year 2024 and select Fire, Flood, Hurricane.
  outcome: Default year set to 2024 and type selection updated to Fire/Flood/Hurricane.
  next: Verify initial filter state in the app.
