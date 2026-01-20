GITHUB_REPO_URL = "https://github.com/bdeakin/snowflake_FEMA_disaster_analyzer.git"

# Estimated based on debugging operations in Cursor prompt history.
BUGS_RESOLVED_COUNT = 20

PROJECT_PHASES = [
    "Phase 1: Core FEMA Explorer (choropleth, cube summary, drilldown) to deliver a fast "
    "overview-to-investigation path and validate the Silver/Gold data model.",
    "Phase 2: Disaster Type Trends + LLM summaries to help users interpret shifts in "
    "incident patterns and add narrative context to the quantitative trends.",
    "Phase 3: Sankey experiment to visualize year to type to state flow; later removed "
    "after usability testing showed labeling and interaction issues at scale.",
    "Phase 4: Sunburst impact assessment with narratives to provide a compact hierarchy "
    "of named events and a drillable path from type to year to event to state.",
    "Phase 5: Consistency Checker with Snowflake Tasks to operationalize trust in the "
    "pipeline by monitoring Silver/Gold alignment on a scheduled cadence.",
    "Phase 6: Sankey reintroduced as Annual Disaster Themes to explain the story of a year "
    "and connect declaration names to broader thematic groupings.",
]

KEY_CHALLENGES = [
    "Missing incident lat/lon required building a reliable county GEOID centroid lookup, "
    "plus jittering logic to prevent overlapping points from hiding important clusters.",
    "Interactive Plotly selection demanded careful session-state orchestration to avoid "
    "rerun loops, stale click replays, and filter interactions that could desync charts.",
    "Dynamic table refresh semantics needed to be communicated clearly in the UI while "
    "balancing timeliness with compute cost and predictable refresh behavior.",
    "LLM grouping accuracy required prompt tuning, training hints, and caching strategies "
    "that avoid repeat calls while keeping classifications consistent across views.",
    "Performance tuning required aggressive query scoping, pre-aggregation, and cache "
    "warming to keep interactive filters responsive across decades of data.",
    "Snowflake permissions and metadata visibility often differed by environment, so task "
    "and dynamic table status needed safe fallbacks and resilient queries.",
]

MERMAID_DIAGRAMS = {
    "Architecture": """
flowchart LR
  PublicData --> SilverDT
  SilverDT --> GoldDT
  GoldDT --> StreamlitUI
  SilverDT --> StreamlitUI
""",
    "Pipeline": """
flowchart LR
  PublicData --> SilverDT
  SilverDT --> GoldDT
  GoldDT --> Views
""",
    "LLMEnrichment": """
flowchart LR
  Records --> OpenAI
  OpenAI --> Cache
  Cache --> ThemeGrouping
""",
    "ConsistencyChecker": """
flowchart LR
  Task --> StoredProc
  StoredProc --> ResultsTable
  ResultsTable --> UI
""",
}

PLAN_LINKS = [
    {"label": "Finalize Demo UI + About Tab", "filename": "finalize_demo_ui_fc98329f.plan.md"},
    {"label": "Sankey Cache Warming", "filename": "sankey_cache_warming_b1e148cb.plan.md"},
]
