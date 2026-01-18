import datetime as dt
import importlib.util
from pathlib import Path
import sys

import streamlit as st
import plotly.express as px

app_dir = Path(__file__).resolve().parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

def _load_module(module_name: str, file_name: str):
    file_path = app_dir / file_name
    unique_name = f"{module_name}_{int(file_path.stat().st_mtime)}"
    spec = importlib.util.spec_from_file_location(unique_name, file_path)
    module = importlib.util.module_from_spec(spec)
    if spec and spec.loader:
        sys.modules[unique_name] = module
        spec.loader.exec_module(module)
    return module


try:
    from queries import (
        get_bump_drilldown_state_summary,
        get_cube_summary,
        get_disaster_type_bump_ranks,
        get_distinct_disaster_types,
        get_drilldown,
        get_state_choropleth,
    )
    from llm import summarize_bump_entry
    from viz import build_bump_chart, build_choropleth, build_cube_grid, build_drilldown
except ImportError:
    queries = _load_module("app_queries", "queries.py")
    llm = _load_module("app_llm", "llm.py")
    viz = _load_module("app_viz", "viz.py")
    get_bump_drilldown_state_summary = queries.get_bump_drilldown_state_summary
    get_cube_summary = queries.get_cube_summary
    get_disaster_type_bump_ranks = queries.get_disaster_type_bump_ranks
    get_distinct_disaster_types = queries.get_distinct_disaster_types
    get_drilldown = queries.get_drilldown
    get_state_choropleth = queries.get_state_choropleth
    summarize_bump_entry = llm.summarize_bump_entry
    build_bump_chart = viz.build_bump_chart
    build_choropleth = viz.build_choropleth
    build_cube_grid = viz.build_cube_grid
    build_drilldown = viz.build_drilldown


st.set_page_config(page_title="FEMA Disasters Explorer", layout="wide")

st.title("FEMA Disasters Explorer")


explore_tab, bump_tab = st.tabs(["Explore", "Disaster Type Trends"])

with explore_tab:
    st.sidebar.header("Filters")
    default_start = dt.date(2023, 1, 1)
    default_end = dt.date(2025, 12, 31)
    start_date = st.sidebar.date_input("Disaster declaration start date", value=default_start)
    end_date = st.sidebar.date_input("Disaster declaration end date", value=default_end)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    type_result = get_distinct_disaster_types()
    type_options = type_result.df["disaster_type"].dropna().tolist()
    selected_types = st.sidebar.multiselect(
        "Disaster types",
        options=type_options,
        default=type_options,
    )
    if not selected_types:
        selected_types = None

    def _select_grain(start: dt.date, end: dt.date) -> str:
        if end.year - start.year >= 1:
            return "year"
        if (end.year * 12 + end.month) - (start.year * 12 + start.month) >= 1:
            return "month"
        return "week"


    grain = _select_grain(start_date, end_date)
    st.caption(f"Using period grain: {grain}")

    selected_state = st.session_state.get("selected_state")
    selected_cube = st.session_state.get("selected_cube")

    with st.spinner("Loading choropleth..."):
        choropleth_result = get_state_choropleth(
            start_date.isoformat(),
            end_date.isoformat(),
            selected_types,
        )
        choropleth_fig = build_choropleth(choropleth_result.df)
        choropleth_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        state_event = st.plotly_chart(
            choropleth_fig,
            use_container_width=True,
            on_select="rerun",
            selection_mode="points",
        )

    if state_event and state_event.selection and state_event.selection.points:
        selected_state = state_event.selection.points[0].get("location")
        st.session_state["selected_state"] = selected_state
        st.session_state.pop("selected_cube", None)

    if selected_state:
        st.subheader(f"Disaster Summary by Period: {selected_state}")
        cube_result = get_cube_summary(
            selected_state,
            start_date.isoformat(),
            end_date.isoformat(),
            grain,
            selected_types,
        )
        if not cube_result.df.empty:
            cube_fig = build_cube_grid(cube_result.df, grain)
            cube_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
            cube_event = st.plotly_chart(
                cube_fig,
                use_container_width=True,
                on_select="rerun",
                selection_mode="points",
            )
            if cube_event and cube_event.selection and cube_event.selection.points:
                point = cube_event.selection.points[0]
                period_bucket = point.get("customdata")[0] if point.get("customdata") else None
                if hasattr(period_bucket, "isoformat"):
                    period_bucket = period_bucket.isoformat()
                selected_cube = {
                    "disaster_type": point.get("y"),
                    "period_bucket": period_bucket or point.get("x"),
                }
                st.session_state["selected_cube"] = selected_cube
        else:
            st.info("No cube data for selection.")

    if selected_state and selected_cube:
        st.subheader("Drilldown")
        drilldown_result = get_drilldown(
            selected_state,
            selected_cube["disaster_type"],
            selected_cube["period_bucket"],
            grain,
        )
        if drilldown_result.df.empty:
            st.info("No drilldown data returned.")
        else:
            color_map = st.session_state.get("declaration_color_map", {})
            color_index = st.session_state.get("declaration_color_index", 0)
            palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
            for name in drilldown_result.df["declaration_name"].dropna().unique():
                if name not in color_map:
                    color_map[name] = palette[color_index % len(palette)]
                    color_index += 1
            st.session_state["declaration_color_map"] = color_map
            st.session_state["declaration_color_index"] = color_index
            drilldown_fig = build_drilldown(drilldown_result.df, color_map=color_map)
            drilldown_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
            st.plotly_chart(drilldown_fig, use_container_width=True)

with bump_tab:
    st.subheader("Top Disaster Types by Decade (Top 5)")
    bump_result = get_disaster_type_bump_ranks(limit_per_decade=5)
    if bump_result.df.empty:
        st.info("No bump chart data available.")
    else:
        bump_fig = build_bump_chart(bump_result.df)
        bump_fig.update_layout(margin={"r": 0, "t": 20, "l": 0, "b": 0}, height=700)
        bump_event = st.plotly_chart(
            bump_fig,
            use_container_width=True,
            on_select="rerun",
            selection_mode="points",
        )

        if bump_event and bump_event.selection and bump_event.selection.points:
            point = bump_event.selection.points[0]
            custom = point.get("customdata") or []
            period_decade = custom[0] if len(custom) > 0 else None
            disaster_type = custom[1] if len(custom) > 1 else point.get("legendgroup")
            if hasattr(period_decade, "isoformat"):
                period_decade = period_decade.isoformat()
            st.session_state["bump_selected"] = {
                "period_decade": period_decade,
                "disaster_type": disaster_type,
            }
            st.session_state["show_bump_llm_modal"] = True

        selected_bump = st.session_state.get("bump_selected")
        if selected_bump and selected_bump.get("period_decade") and selected_bump.get("disaster_type"):
            st.subheader(
                f"Drilldown: {selected_bump['disaster_type']} in {str(selected_bump['period_decade'])[:4]}s"
            )
            drilldown_summary = get_bump_drilldown_state_summary(
                selected_bump["period_decade"],
                selected_bump["disaster_type"],
            )
            if drilldown_summary.df.empty:
                st.info("No drilldown data returned.")
            else:
                st.dataframe(drilldown_summary.df, use_container_width=True)
                top_states = (
                    drilldown_summary.df[["state", "disaster_count"]]
                    .dropna(subset=["state"])
                    .head(5)
                    .itertuples(index=False, name=None)
                )
                decade_year = str(selected_bump["period_decade"])[:4]
                decade_label = f"{decade_year}s"
                cache = st.session_state.setdefault("bump_llm_cache", {})
                cache_key = f"{decade_label}|{selected_bump['disaster_type']}"
                show_modal = st.session_state.get("show_bump_llm_modal")
                if show_modal:
                    @st.dialog("LLM Summary")
                    def _show_llm_summary():
                        if cache_key in cache:
                            st.write(cache[cache_key])
                        else:
                            with st.spinner("Summarizing with OpenAI..."):
                                try:
                                    summary = summarize_bump_entry(
                                        decade_label=decade_label,
                                        disaster_type=selected_bump["disaster_type"],
                                        top_states=top_states,
                                    )
                                    cache[cache_key] = summary
                                    st.write(summary)
                                except Exception as exc:
                                    st.error(f"LLM summary failed: {exc}")
                    _show_llm_summary()
                    st.session_state["show_bump_llm_modal"] = False
        else:
            st.caption("Select a point in the bump chart to view drilldown details.")
