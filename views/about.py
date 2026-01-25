import hashlib
import textwrap
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from config.app_metadata import (
    BUGS_RESOLVED_COUNT,
    GITHUB_REPO_URL,
    KEY_CHALLENGES,
    MERMAID_DIAGRAMS,
    PLAN_LINKS,
    PROJECT_PHASES,
)


def _render_mermaid(diagram: str, height: int = 260) -> None:
    diagram = textwrap.dedent(diagram).strip()
    node_id = f"mmd-{hashlib.md5(diagram.encode('utf-8')).hexdigest()}"
    html = f"""
    <div id="{node_id}" class="mermaid">
    {diagram}
    </div>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <script>
      mermaid.initialize({{ startOnLoad: false }});
      const el = document.getElementById("{node_id}");
      if (el) {{
        mermaid.run({{ nodes: [el] }});
      }}
    </script>
    """
    components.html(html, height=height, scrolling=True)


def _resolve_plan_links() -> list[tuple[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    candidates = [
        repo_root / ".cursor" / "plans",
        Path.home() / ".cursor" / "plans",
    ]
    resolved = []
    for plan in PLAN_LINKS:
        label = plan.get("label", "Plan")
        filename = plan.get("filename", "")
        path = None
        for base in candidates:
            candidate = base / filename
            if candidate.exists():
                path = candidate
                break
        if path:
            resolved.append((label, path.as_uri()))
        else:
            resolved.append((label, ""))
    return resolved


def render_about() -> None:
    st.subheader("Snowflake features in this demo")
    repo_root = Path(__file__).resolve().parents[1]
    feature_docs = [
        ("Dynamic tables narrative", repo_root / "DYNAMIC_TABLES.md"),
        ("Tasks narrative", repo_root / "TASKS_NARRATIVE.md"),
        ("Cortex narrative", repo_root / "CORTEX_NARRATIVE.md"),
    ]
    for label, path in feature_docs:
        if path.exists():
            text = path.read_text(encoding="utf-8")
            with st.expander(f"View {label.lower()}"):
                st.markdown(text)
        else:
            st.caption(f"{label} not found.")

    st.subheader("What this app is")
    st.write(
        "FEMA Disaster Explorer is a demo analytics experience that surfaces FEMA disaster "
        "declarations through a fast map-first workflow, drilldowns, and narrative summaries. "
        "It showcases Snowflake Dynamic Tables, Tasks, and LLM-assisted thematic grouping to "
        "highlight patterns, named events, and state-level impacts."
    )

    st.subheader("GitHub repository")
    st.link_button("Open repository", GITHUB_REPO_URL)

    st.subheader("Development narrative")
    for phase in PROJECT_PHASES:
        st.markdown(f"- {phase}")

    st.subheader("Cursor plan stages")
    for label, link in _resolve_plan_links():
        if link:
            st.markdown(f"- [{label}]({link})")
        else:
            st.markdown(f"- {label} (plan file not found locally)")

    st.subheader("Key technical challenges")
    for challenge in KEY_CHALLENGES:
        st.markdown(f"- {challenge}")
    st.caption(f"Total bugs identified and resolved: {BUGS_RESOLVED_COUNT}")

    st.subheader("Architecture diagrams")
    for title, diagram in MERMAID_DIAGRAMS.items():
        st.markdown(f"**{title}**")
        try:
            _render_mermaid(diagram)
        except Exception:
            st.code(diagram, language="mermaid")

    st.subheader("Snowflake features highlighted")
    st.markdown(
        "- **Dynamic tables** for Silver/Gold layers with lag-based refresh semantics.\n"
        "- **Tasks** for 12-hour consistency checks with results stored in monitoring tables.\n"
        "- **Cortex Analyst** for the Map View assistant powered by a semantic view."
    )

    st.subheader("Integrations")
    st.markdown(
        "- **OpenAI** performs thematic grouping and impact assessment narratives.\n"
        "- **Cortex Analyst** provides natural-language SQL for the Map View assistant.\n"
        "- **Data safety note**: summaries are derived from FEMA metadata."
    )
