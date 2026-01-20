GITHUB_REPO_URL = "https://github.com/bdeakin/snowflake_FEMA_disaster_analyzer.git"

# Estimated based on debugging operations in Cursor prompt history.
BUGS_RESOLVED_COUNT = 20

PROJECT_PHASES = [
    "Phase 1: Core FEMA Explorer (choropleth, cube summary, drilldown).",
    "Phase 2: Disaster Type Trends + LLM summaries.",
    "Phase 3: Sankey experiment (later removed due to UX issues).",
    "Phase 4: Sunburst impact assessment with narratives.",
    "Phase 5: Consistency Checker with Snowflake Tasks.",
    "Phase 6: Sankey reintroduced as Annual Disaster Themes.",
]

KEY_CHALLENGES = [
    "Missing incident lat/lon → county GEOID centroid mapping.",
    "Interactive Plotly selection and rerun state management.",
    "Dynamic table refresh semantics vs. materialized views.",
    "LLM grouping accuracy with caching and invalidation.",
    "Performance tuning via query scoping and aggregation.",
    "Snowflake permissions and metadata visibility.",
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
