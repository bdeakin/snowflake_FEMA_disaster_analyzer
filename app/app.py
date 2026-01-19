import datetime as dt
import hashlib
import os
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_plotly_events import plotly_events
import requests

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
        get_distinct_disaster_types,
        get_drilldown,
        get_state_choropleth,
        get_sunburst_rows,
        get_trends_bump_ranks,
    )
    from llm import (
        summarize_bump_entry,
        group_declaration_names,
        summarize_year_events,
        summarize_named_event,
        summarize_unnamed_events,
        summarize_event_state,
    )
    from viz import (
        build_bump_chart,
        build_choropleth,
        build_cube_grid,
        build_drilldown,
        build_sunburst,
    )
except ImportError:
    queries = _load_module("app_queries", "queries.py")
    llm = _load_module("app_llm", "llm.py")
    viz = _load_module("app_viz", "viz.py")
    get_bump_drilldown_state_summary = queries.get_bump_drilldown_state_summary
    get_cube_summary = queries.get_cube_summary
    get_distinct_disaster_types = queries.get_distinct_disaster_types
    get_drilldown = queries.get_drilldown
    get_state_choropleth = queries.get_state_choropleth
    get_sunburst_rows = queries.get_sunburst_rows
    get_trends_bump_ranks = queries.get_trends_bump_ranks
    summarize_bump_entry = llm.summarize_bump_entry
    group_declaration_names = llm.group_declaration_names
    summarize_year_events = llm.summarize_year_events
    summarize_named_event = llm.summarize_named_event
    summarize_unnamed_events = llm.summarize_unnamed_events
    summarize_event_state = llm.summarize_event_state
    build_bump_chart = viz.build_bump_chart
    build_choropleth = viz.build_choropleth
    build_cube_grid = viz.build_cube_grid
    build_drilldown = viz.build_drilldown
    build_sunburst = viz.build_sunburst


st.set_page_config(page_title="FEMA Disasters Explorer", layout="wide")

st.title("FEMA Disasters Explorer")

type_result = get_distinct_disaster_types()
type_options = type_result.df["disaster_type"].dropna().tolist()

active_tab = st.radio(
    "View",
    ["Explore", "Disaster Type Trends", "Sunburst"],
    horizontal=True,
    key="active_tab",
)

if active_tab == "Explore":
    with st.sidebar:
        st.subheader("Explore Filters")
        explore_years = st.slider(
            "Year range",
            min_value=2000,
            max_value=2025,
            value=(2023, 2025),
            key="filters_explore_year_range",
        )
        default_types = {"Hurricane", "Tropical Storm", "Coastal Storm"}
        explore_types = []
        for dtype in type_options:
            checked = st.checkbox(
                dtype,
                value=dtype in default_types,
                key=f"filters_explore_type_{dtype}",
            )
            if checked:
                explore_types.append(dtype)
        if not explore_types:
            explore_types = None

    def _select_grain(start: dt.date, end: dt.date) -> str:
        if end.year - start.year >= 1:
            return "year"
        if (end.year * 12 + end.month) - (start.year * 12 + start.month) >= 1:
            return "month"
        return "week"


    start_date = dt.date(explore_years[0], 1, 1)
    end_date = dt.date(explore_years[1], 12, 31)
    selected_types = explore_types

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

elif active_tab == "Disaster Type Trends":
    with st.sidebar:
        st.subheader("Trends Filters")
        binning = st.selectbox(
            "Time binning",
            options=["decades", "years", "months"],
            index=0,
            key="filters_trends_binning",
        )
        top_n = st.selectbox(
            "Top-N disaster types",
            options=[5, 10, 15],
            index=0,
            key="filters_trends_top_n",
        )
        if binning in {"decades", "years"}:
            trends_years = st.slider(
                "Year range",
                min_value=2000,
                max_value=2025,
                value=(2000, 2025),
                key="filters_trends_year_range",
            )
            trends_start = dt.date(trends_years[0], 1, 1)
            trends_end = dt.date(trends_years[1], 12, 31)
        else:
            trends_start = st.date_input(
                "Start date",
                value=dt.date(2023, 1, 1),
                key="filters_trends_start_date",
            )
            trends_end = st.date_input(
                "End date",
                value=dt.date(2025, 12, 31),
                key="filters_trends_end_date",
            )
            if trends_start > trends_end:
                st.error("Start date must be before end date.")
                st.stop()

    chart_title = f"Top Disaster Types by {binning.title()} (Top {top_n})"
    st.subheader(chart_title)
    bump_result = get_trends_bump_ranks(
        binning,
        trends_start.isoformat(),
        trends_end.isoformat(),
        top_n,
    )
    if bump_result.df.empty:
        st.info("No bump chart data available.")
    else:
        bump_fig = build_bump_chart(bump_result.df, binning=binning)
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
            period_bucket = custom[0] if len(custom) > 0 else None
            disaster_type = custom[1] if len(custom) > 1 else point.get("legendgroup")
            if hasattr(period_bucket, "isoformat"):
                period_bucket = period_bucket.isoformat()
            st.session_state["bump_selected"] = {
                "period_bucket": period_bucket,
                "disaster_type": disaster_type,
                "binning": binning,
            }
            st.session_state["show_bump_llm_modal"] = True

        selected_bump = st.session_state.get("bump_selected")
        if selected_bump and selected_bump.get("period_bucket") and selected_bump.get("disaster_type"):
            period_dt = pd.to_datetime(selected_bump["period_bucket"], errors="coerce")
            if selected_bump.get("binning") == "months" and pd.notna(period_dt):
                period_label = period_dt.strftime("%b %Y")
            elif selected_bump.get("binning") == "years" and pd.notna(period_dt):
                period_label = period_dt.strftime("%Y")
            elif pd.notna(period_dt):
                period_label = f"{period_dt.strftime('%Y')}s"
            else:
                period_label = str(selected_bump["period_bucket"])
            st.subheader(
                f"Drilldown: {selected_bump['disaster_type']} in {period_label}"
            )
            drilldown_summary = get_bump_drilldown_state_summary(
                selected_bump.get("binning", "decades"),
                selected_bump["period_bucket"],
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
                decade_label = period_label
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

elif active_tab == "Sunburst":
    with st.sidebar:
        st.subheader("Sunburst Filters")
        sunburst_years = st.slider(
            "Year range",
            min_value=2000,
            max_value=2025,
            value=(2023, 2025),
            key="filters_sunburst_year_range",
        )
        default_sunburst = {"Hurricane", "Tropical Storm"}
        sunburst_types = []
        for dtype in type_options:
            checked = st.checkbox(
                dtype,
                value=dtype in default_sunburst,
                key=f"filters_sunburst_type_{dtype}",
            )
            if checked:
                sunburst_types.append(dtype)
        if not sunburst_types:
            sunburst_types = None

    st.subheader("Named Events → Year → Event → State")
    if st.button("Reset Sunburst view"):
        st.session_state["sunburst_reset_key"] = st.session_state.get("sunburst_reset_key", 0) + 1
        st.session_state.pop("sunburst_selected_node", None)
    sunburst_start = dt.date(sunburst_years[0], 1, 1)
    sunburst_end = dt.date(sunburst_years[1], 12, 31)
    sunburst_result = get_sunburst_rows(
        sunburst_start.isoformat(),
        sunburst_end.isoformat(),
        sunburst_types,
    )
    if sunburst_result.df.empty:
        st.info("No named event data available for the selected filters.")
    else:
        df = sunburst_result.df.copy()
        df["disaster_declaration_date"] = pd.to_datetime(df["disaster_declaration_date"])
        df["year"] = df["disaster_declaration_date"].dt.year.astype(int).astype(str)
        names = df["declaration_name"].fillna("").astype(str).str.strip()
        unique_names = sorted({name for name in names.tolist() if name})
        hash_input = "|".join(unique_names).encode("utf-8")
        cache_key = f"sunburst_name_map_{hashlib.md5(hash_input).hexdigest()}"
        name_map = st.session_state.get(cache_key)
        if name_map is None:
            with st.spinner("Grouping named events..."):
                try:
                    name_map = group_declaration_names(unique_names)
                except Exception as exc:
                    st.error(f"LLM grouping failed: {exc}")
                    name_map = {}
            st.session_state[cache_key] = name_map
        df["event"] = names.apply(
            lambda value: name_map.get(value, value) if value else "Other/Unnamed"
        )
        df.loc[df["event"].eq(""), "event"] = "Other/Unnamed"
        df["state"] = df["state"].fillna("Unknown")
        @st.cache_data(show_spinner=False)
        def _build_sunburst_nodes(input_df: pd.DataFrame) -> pd.DataFrame:
            sunburst_df = (
                input_df.groupby(["disaster_type", "year", "event", "state"])["county_fips"]
                .nunique()
                .reset_index(name="value")
            )
            nodes = []
            palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
            nodes.append(
                {
                    "id": "root",
                    "label": "Named Events",
                    "parent": "",
                    "value": int(sunburst_df["value"].sum()),
                    "customdata": {"node_type": "root"},
                    "color": "#ffffff",
                }
            )

            category_totals = (
                sunburst_df.groupby("disaster_type")["value"].sum().reset_index()
            )
            year_totals = (
                sunburst_df.groupby(["disaster_type", "year"])["value"]
                .sum()
                .reset_index()
            )
            year_list = year_totals["year"].tolist()
            year_colors = {
                year: palette[idx % len(palette)] for idx, year in enumerate(sorted(year_list))
            }
            for row in category_totals.itertuples(index=False):
                nodes.append(
                    {
                        "id": f"category:{row.disaster_type}",
                        "label": row.disaster_type,
                        "parent": "root",
                        "value": int(row.value),
                        "customdata": {"node_type": "category", "category": row.disaster_type},
                        "color": "#dddddd",
                    }
                )
            for row in year_totals.itertuples(index=False):
                nodes.append(
                    {
                        "id": f"category:{row.disaster_type}|year:{row.year}",
                        "label": str(row.year),
                        "parent": f"category:{row.disaster_type}",
                        "value": int(row.value),
                        "customdata": {
                            "node_type": "year",
                            "year": str(row.year),
                            "category": row.disaster_type,
                        },
                        "color": year_colors.get(row.year, "#cccccc"),
                    }
                )

            year_event = (
                sunburst_df.groupby(["disaster_type", "year", "event"])["value"]
                .sum()
                .reset_index()
            )
            for row in year_event.itertuples(index=False):
                nodes.append(
                    {
                        "id": f"category:{row.disaster_type}|year:{row.year}|event:{row.event}",
                        "label": row.event,
                        "parent": f"category:{row.disaster_type}|year:{row.year}",
                        "value": int(row.value),
                        "customdata": {
                            "node_type": "event",
                            "year": str(row.year),
                            "event": row.event,
                            "category": row.disaster_type,
                        },
                        "color": year_colors.get(row.year, "#cccccc"),
                    }
                )

            for row in sunburst_df.itertuples(index=False):
                nodes.append(
                    {
                        "id": (
                            f"category:{row.disaster_type}|year:{row.year}|"
                            f"event:{row.event}|state:{row.state}"
                        ),
                        "label": row.state,
                        "parent": f"category:{row.disaster_type}|year:{row.year}|event:{row.event}",
                        "value": int(row.value),
                        "customdata": {
                            "node_type": "state",
                            "year": str(row.year),
                            "event": row.event,
                            "state": row.state,
                            "category": row.disaster_type,
                        },
                        "color": year_colors.get(row.year, "#cccccc"),
                    }
                )

            return pd.DataFrame(nodes)

        nodes_df = _build_sunburst_nodes(df)
        st.session_state["sunburst_nodes_df"] = nodes_df
        selected_node = st.session_state.get("sunburst_selected_node")
        filtered_nodes = nodes_df
        selected_id = None
        if isinstance(selected_node, dict):
            node_type = selected_node.get("node_type")
            category = selected_node.get("category")
            year = selected_node.get("year")
            event_name = selected_node.get("event")
            state = selected_node.get("state")
            if node_type == "category" and category:
                selected_id = f"category:{category}"
            elif node_type == "year" and category and year:
                selected_id = f"category:{category}|year:{year}"
            elif node_type == "event" and category and year and event_name:
                selected_id = f"category:{category}|year:{year}|event:{event_name}"
            elif node_type == "state" and category and year and event_name and state:
                selected_id = (
                    f"category:{category}|year:{year}|event:{event_name}|state:{state}"
                )

        if selected_id:
            filtered_nodes = nodes_df[
                (nodes_df["id"] == selected_id)
                | (nodes_df["id"].str.startswith(f"{selected_id}|"))
            ]
            filtered_nodes = filtered_nodes.copy()
            subtree_ids = set(filtered_nodes["id"].tolist())
            filtered_nodes["parent"] = filtered_nodes["parent"].apply(
                lambda parent: parent if parent in subtree_ids else ""
            )
        if len(filtered_nodes) < len(nodes_df):
            nodes_map = {
                row["id"]: {
                    "id": row["id"],
                    "parent": row["parent"],
                    "value": row["value"],
                }
                for row in filtered_nodes.to_dict(orient="records")
            }
            children_map: dict[str, list[str]] = {}
            for node_id, node in nodes_map.items():
                parent_id = node["parent"]
                if parent_id:
                    children_map.setdefault(parent_id, []).append(node_id)

            def _sum_node(node_id: str) -> int:
                children = children_map.get(node_id)
                if not children:
                    return int(nodes_map[node_id]["value"])
                total = 0
                for child_id in children:
                    total += _sum_node(child_id)
                nodes_map[node_id]["value"] = total
                return total

            for node_id in list(nodes_map.keys()):
                _sum_node(node_id)
            filtered_nodes = filtered_nodes.copy()
            filtered_nodes["value"] = filtered_nodes["id"].map(
                lambda value: nodes_map.get(value, {}).get("value", 0)
            )
        st.session_state["sunburst_filtered_nodes_df"] = filtered_nodes
        sunburst_fig = build_sunburst(filtered_nodes)
        sunburst_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 80})
        reset_key = st.session_state.get("sunburst_reset_key", 0)
        sunburst_events = plotly_events(
            sunburst_fig,
            click_event=True,
            select_event=False,
            hover_event=False,
            key=f"sunburst_{reset_key}",
        )
        if sunburst_events:
            event = sunburst_events[0]
            cd = event.get("customdata") if isinstance(event, dict) else None
            if cd is None and isinstance(event, dict) and "pointNumber" in event:
                filtered_df = st.session_state.get("sunburst_filtered_nodes_df")
                if isinstance(filtered_df, pd.DataFrame):
                    idx = int(event["pointNumber"])
                    if 0 <= idx < len(filtered_df):
                        cd = filtered_df.iloc[idx]["customdata"]
            if st.session_state.get("sunburst_skip_event_once"):
                st.session_state["sunburst_skip_event_once"] = False
            elif isinstance(cd, dict):
                prev = st.session_state.get("sunburst_selected_node")
                st.session_state["sunburst_selected_node"] = cd
                if prev != cd:
                    st.session_state["sunburst_skip_event_once"] = True
                    st.rerun()

        def _fetch_gnews_articles(query: str, max_results: int = 3) -> list[dict]:
            gnews_key = os.getenv("GNEWS_API_KEY")
            if not gnews_key:
                return []
            url = (
                "https://gnews.io/api/v4/search"
                f"?q={requests.utils.quote(query)}&lang=en&max={max_results}"
                f"&token={requests.utils.quote(gnews_key)}"
            )
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            return [
                {
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "source": item.get("source", {}).get("name"),
                }
                for item in data.get("articles", [])
            ]

        st.subheader("Sunburst Narrative")
        selected_node = st.session_state.get("sunburst_selected_node")
        if not selected_node:
            st.caption("Click a year, named event, or state to see a narrative summary.")
        else:
            cache = st.session_state.setdefault("sunburst_summary_cache", {})
            node_type = selected_node.get("node_type")
            year_str = selected_node.get("year")
            year_int = int(year_str) if year_str and str(year_str).isdigit() else None
            event_name = selected_node.get("event")
            state = selected_node.get("state")

            if node_type == "year" and year_int is not None:
                cache_key = f"sunburst:year:{year_int}"
                if cache_key not in cache:
                    year_df = df[df["year"] == str(year_int)]
                    top_types = year_df["disaster_type"].value_counts().head(8).items()
                    top_events = (
                        year_df[year_df["event"] != "Other/Unnamed"]["event"]
                        .value_counts()
                        .head(8)
                        .items()
                    )
                    with st.spinner("Summarizing year events..."):
                        cache[cache_key] = summarize_year_events(
                            year_int,
                            top_types,
                            top_events,
                        )
                st.write(cache[cache_key])

            elif node_type == "event" and year_int is not None:
                if event_name == "Other/Unnamed":
                    cache_key = f"sunburst:unnamed:{year_int}"
                    if cache_key not in cache:
                        unnamed_df = df[(df["year"] == str(year_int)) & (df["event"] == "Other/Unnamed")]
                        top_types = unnamed_df["disaster_type"].value_counts().head(8).items()
                        with st.spinner("Summarizing unnamed events..."):
                            cache[cache_key] = summarize_unnamed_events(year_int, top_types)
                    st.write(cache[cache_key])
                else:
                    cache_key = f"sunburst:event:{year_int}:{event_name}"
                    if cache_key not in cache:
                        event_df = df[(df["year"] == str(year_int)) & (df["event"] == event_name)]
                        top_states = event_df["state"].value_counts().head(8).items()
                        query = f"\"{event_name}\" {year_int}"
                        with st.spinner("Fetching named event headlines..."):
                            try:
                                headlines = _fetch_gnews_articles(query)
                            except Exception as exc:
                                st.error(f"News lookup failed: {exc}")
                                headlines = []
                        with st.spinner("Summarizing named event..."):
                            cache[cache_key] = summarize_named_event(
                                event_name,
                                year_int,
                                top_states,
                                headlines,
                            )
                    st.write(cache[cache_key])

            elif node_type == "state" and year_int is not None and event_name:
                if event_name == "Other/Unnamed":
                    cache_key = f"sunburst:unnamed:{year_int}"
                    if cache_key not in cache:
                        unnamed_df = df[(df["year"] == str(year_int)) & (df["event"] == "Other/Unnamed")]
                        top_types = unnamed_df["disaster_type"].value_counts().head(8).items()
                        with st.spinner("Summarizing unnamed events..."):
                            cache[cache_key] = summarize_unnamed_events(year_int, top_types)
                    st.write(cache[cache_key])
                else:
                    cache_key = f"sunburst:state:{year_int}:{event_name}:{state}"
                    if cache_key not in cache:
                        query = f"\"{event_name}\" {state} {year_int}"
                        with st.spinner("Fetching state headlines..."):
                            try:
                                headlines = _fetch_gnews_articles(query)
                            except Exception as exc:
                                st.error(f"News lookup failed: {exc}")
                                headlines = []
                        with st.spinner("Summarizing event in state..."):
                            cache[cache_key] = summarize_event_state(
                                event_name,
                                state,
                                year_int,
                                headlines,
                            )
                    st.write(cache[cache_key])
            else:
                st.caption("Select a year or named event to see a summary.")

