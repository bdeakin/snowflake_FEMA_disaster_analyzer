import datetime as dt
import hashlib
import json
import os
import time
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

# #region agent log
_LOG_PATH = "/Users/bdeakin/Documents/Vibe Coding/snowflake_FEMA_disaster_analyzer/.cursor/debug.log"


def _log_debug(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    payload = {
        "sessionId": "debug-session",
        "runId": "pre-fix",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
# #endregion

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
        get_sankey_rows,
        get_state_choropleth,
    )
    from llm import (
        summarize_bump_entry,
        group_declaration_names,
        estimate_hurricane_damage,
        estimate_hurricane_year_damage,
        summarize_gdelt_headlines,
    )
    from viz import (
        build_bump_chart,
        build_choropleth,
        build_cube_grid,
        build_drilldown,
        build_sankey,
        build_sunburst,
    )
except ImportError:
    queries = _load_module("app_queries", "queries.py")
    llm = _load_module("app_llm", "llm.py")
    viz = _load_module("app_viz", "viz.py")
    get_bump_drilldown_state_summary = queries.get_bump_drilldown_state_summary
    get_cube_summary = queries.get_cube_summary
    get_disaster_type_bump_ranks = queries.get_disaster_type_bump_ranks
    get_distinct_disaster_types = queries.get_distinct_disaster_types
    get_drilldown = queries.get_drilldown
    get_sankey_rows = queries.get_sankey_rows
    get_state_choropleth = queries.get_state_choropleth
    summarize_bump_entry = llm.summarize_bump_entry
    group_declaration_names = llm.group_declaration_names
    estimate_hurricane_damage = llm.estimate_hurricane_damage
    estimate_hurricane_year_damage = llm.estimate_hurricane_year_damage
    summarize_gdelt_headlines = llm.summarize_gdelt_headlines
    build_bump_chart = viz.build_bump_chart
    build_choropleth = viz.build_choropleth
    build_cube_grid = viz.build_cube_grid
    build_drilldown = viz.build_drilldown
    build_sankey = viz.build_sankey
    build_sunburst = viz.build_sunburst


st.set_page_config(page_title="FEMA Disasters Explorer", layout="wide")

st.title("FEMA Disasters Explorer")

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

explore_tab, bump_tab, sankey_tab, sunburst_tab = st.tabs(
    ["Explore", "Disaster Type Trends", "Sankey", "Sunburst"]
)

with explore_tab:

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

with sankey_tab:
    st.subheader("Incident Type → Disaster → State Counties")
    sankey_filters = st.container()
    with sankey_filters:
        sankey_start = st.date_input(
            "Sankey start date",
            value=start_date,
            key="sankey_start_date",
        )
        sankey_end = st.date_input(
            "Sankey end date",
            value=end_date,
            key="sankey_end_date",
        )
        if sankey_start > sankey_end:
            st.error("Sankey start date must be before end date.")
            st.stop()
        sankey_type_default = "Hurricane" if "Hurricane" in type_options else type_options[0]
        sankey_type = st.selectbox(
            "Sankey incident type",
            options=type_options,
            index=type_options.index(sankey_type_default),
            key="sankey_type",
        )

    sankey_result = get_sankey_rows(
        sankey_start.isoformat(),
        sankey_end.isoformat(),
        [sankey_type],
    )
    if sankey_result.df.empty:
        st.info("No data available for the selected filters.")
    else:
        df = sankey_result.df.copy()
        df["disaster_declaration_date"] = pd.to_datetime(df["disaster_declaration_date"])
        df["hurricane_year"] = df["disaster_declaration_date"].dt.year.astype(int)
        names = df["declaration_name"].fillna("").astype(str).str.strip()
        unique_names = sorted({name for name in names.tolist() if name})
        hash_input = "|".join(unique_names).encode("utf-8")
        cache_key = f"sankey_name_map_{hashlib.md5(hash_input).hexdigest()}"
        name_map = st.session_state.get(cache_key)
        if name_map is None:
            with st.spinner("Grouping disaster names..."):
                try:
                    name_map = group_declaration_names(unique_names)
                except Exception as exc:
                    st.error(f"LLM grouping failed: {exc}")
                    name_map = {}
            st.session_state[cache_key] = name_map
        df["grouped_name"] = names.apply(
            lambda value: name_map.get(value, value) if value else "Other/Unnamed"
        )
        df.loc[df["grouped_name"].eq(""), "grouped_name"] = "Other/Unnamed"

        year_group = (
            df.groupby(["hurricane_year", "grouped_name"])["county_fips"]
            .nunique()
            .reset_index(name="value")
        )
        group_state = (
            df.groupby(["grouped_name", "state"])["county_fips"]
            .nunique()
            .reset_index(name="value")
        )
        links = pd.concat(
            [
                year_group.rename(columns={"hurricane_year": "source", "grouped_name": "target"}),
                group_state.rename(columns={"grouped_name": "source", "state": "target"}),
            ],
            ignore_index=True,
        )
        left_nodes = (
            year_group["hurricane_year"]
            .dropna()
            .astype(int)
            .sort_values()
            .astype(str)
            .unique()
            .tolist()
        )
        middle_nodes = sorted(year_group["grouped_name"].dropna().unique().tolist())
        right_nodes = sorted(group_state["state"].dropna().unique().tolist())
        links["source"] = links["source"].astype(str)
        links["target"] = links["target"].astype(str)
        # #region agent log
        _log_debug(
            "H6",
            "app.py:sankey_tab",
            "Sankey link snapshot",
            {
                "link_count": len(links),
                "links_head": links.head(15).to_dict(orient="records"),
                "left_nodes": left_nodes[:10],
                "middle_nodes": middle_nodes[:10],
                "right_nodes": right_nodes[:10],
            },
        )
        # #endregion
        sankey_fig = build_sankey(links, left_nodes, middle_nodes, right_nodes)
        node_labels = list(dict.fromkeys(left_nodes + middle_nodes + right_nodes))
        st.session_state["sankey_node_labels"] = node_labels
        sankey_events = plotly_events(
            sankey_fig,
            click_event=True,
            select_event=False,
            hover_event=True,
        )
        # #region agent log
        _log_debug(
            "H1",
            "app.py:sankey_tab",
            "Sankey click events",
            {
                "event_count": len(sankey_events),
                "event_keys": [list(ev.keys()) for ev in sankey_events[:3]],
            },
        )
        # #endregion
        selected_label = None
        link_pair = None
        if sankey_events:
            point = None
            for ev in sankey_events:
                if (isinstance(ev, dict) and (
                    "customdata" in ev or ("source" in ev and "target" in ev)
                )):
                    point = ev
                    break
            if point is None:
                point = sankey_events[0]
            selected_label = point.get("label")
            labels = st.session_state.get("sankey_node_labels", list(dict.fromkeys(left_nodes + middle_nodes + right_nodes)))
            point_idx = point.get("pointNumber")
            if not selected_label and isinstance(point_idx, int) and 0 <= point_idx < len(labels):
                selected_label = labels[point_idx]
            link_pair = None
            if isinstance(point, dict):
                customdata = point.get("customdata")
                if isinstance(customdata, list) and len(customdata) == 2:
                    link_pair = {"source": customdata[0], "target": customdata[1]}
                if point.get("source") is not None and point.get("target") is not None:
                    try:
                        source_idx = int(point.get("source"))
                        target_idx = int(point.get("target"))
                        if 0 <= source_idx < len(labels) and 0 <= target_idx < len(labels):
                            link_pair = {"source": labels[source_idx], "target": labels[target_idx]}
                    except Exception:
                        pass
                if link_pair is None and isinstance(point_idx, int):
                    try:
                        link_source = sankey_fig.data[0]["link"]["source"][point_idx]
                        link_target = sankey_fig.data[0]["link"]["target"][point_idx]
                        if 0 <= link_source < len(labels) and 0 <= link_target < len(labels):
                            link_pair = {"source": labels[link_source], "target": labels[link_target]}
                    except Exception:
                        pass
            # #region agent log
            link_info = None
            if isinstance(point_idx, int) and 0 <= point_idx < len(links):
                link_row = links.iloc[point_idx]
                link_info = {
                    "source": link_row.get("source"),
                    "target": link_row.get("target"),
                    "value": int(link_row.get("value")) if "value" in link_row else None,
                }
            _log_debug(
                "H1",
                "app.py:sankey_tab",
                "Sankey event data",
                {
                    "point": point,
                    "selected_label": selected_label,
                    "point_index": point_idx,
                    "link_info": link_info,
                    "link_pair": link_pair,
                    "point_keys": list(point.keys()) if isinstance(point, dict) else None,
                },
            )
            # #endregion
        # #region agent log
        _log_debug(
            "H2",
            "app.py:sankey_tab",
            "Sankey selection",
            {
                "selected_label": selected_label,
                "left_count": len(left_nodes),
                "middle_count": len(middle_nodes),
                "right_count": len(right_nodes),
            },
        )
        # #endregion

        if link_pair:
            if link_pair["source"] in middle_nodes and link_pair["target"] in right_nodes:
                st.session_state["sankey_selected_hurricane"] = link_pair["source"]
                st.session_state["sankey_selected_state"] = link_pair["target"]
            elif link_pair["source"] in left_nodes and link_pair["target"] in middle_nodes:
                st.session_state["sankey_selected_hurricane"] = link_pair["target"]
                st.session_state["sankey_selected_year"] = link_pair["source"]
        elif selected_label:
            if selected_label in middle_nodes:
                st.session_state["sankey_selected_hurricane"] = selected_label
            elif selected_label in right_nodes:
                st.session_state["sankey_selected_state"] = selected_label
            elif selected_label in left_nodes:
                st.session_state["sankey_selected_year"] = selected_label

        if sankey_type == "Hurricane":
            cache = st.session_state.setdefault("sankey_damage_cache", {})
            selected_hurricane = st.session_state.get("sankey_selected_hurricane")
            selected_state = st.session_state.get("sankey_selected_state")
            selected_year = st.session_state.get("sankey_selected_year")

            if link_pair and link_pair["source"] in left_nodes and link_pair["target"] in middle_nodes:
                st.session_state["sankey_damage_modal"] = {
                    "kind": "hurricane",
                    "hurricane": selected_hurricane,
                    "state": None,
                    "year": None,
                }
            elif link_pair and link_pair["source"] in middle_nodes and link_pair["target"] in right_nodes:
                st.session_state["sankey_damage_modal"] = {
                    "kind": "state",
                    "hurricane": selected_hurricane,
                    "state": selected_state,
                    "year": None,
                }
            elif selected_label in left_nodes:
                st.session_state["sankey_damage_modal"] = {
                    "kind": "year",
                    "year": int(selected_year) if selected_year else None,
                    "hurricane": None,
                    "state": None,
                }
            elif selected_label in middle_nodes:
                st.session_state["sankey_damage_modal"] = {
                    "kind": "hurricane",
                    "hurricane": selected_hurricane,
                    "state": None,
                    "year": None,
                }
            elif selected_label in right_nodes:
                if not selected_hurricane:
                    st.info("Select a hurricane name before requesting state damage estimates.")
                else:
                    st.session_state["sankey_damage_modal"] = {
                        "kind": "state",
                        "hurricane": selected_hurricane,
                        "state": selected_state,
                        "year": None,
                    }

            modal_payload = st.session_state.get("sankey_damage_modal")
            # #region agent log
            _log_debug(
                "H3",
                "app.py:sankey_tab",
                "Modal payload",
                {"modal_payload": modal_payload},
            )
            # #endregion
            if modal_payload:
                title = "Estimated Hurricane Damages"
                if modal_payload["kind"] == "state":
                    title = f"Estimated Damages: {modal_payload['state']}"
                if modal_payload["kind"] == "year":
                    title = f"Estimated Damages: {modal_payload['year']}"
                # #region agent log
                _log_debug(
                    "H5",
                    "app.py:sankey_tab",
                    "Opening damage modal",
                    {"payload": modal_payload},
                )
                # #endregion

                @st.dialog(title)
                def _show_damage_modal():
                    if modal_payload["kind"] == "year":
                        cache_key = f"hurricane_year:{modal_payload['year']}"
                    elif modal_payload["kind"] == "hurricane":
                        cache_key = f"hurricane:{modal_payload['hurricane']}"
                    else:
                        cache_key = f"hurricane:{modal_payload['hurricane']}:state:{modal_payload['state']}"
                    if cache_key in cache:
                        st.write(cache[cache_key])
                    else:
                        with st.spinner("Estimating hurricane damages..."):
                            try:
                                if modal_payload["kind"] == "year":
                                    cache[cache_key] = estimate_hurricane_year_damage(
                                        int(modal_payload["year"])
                                    )
                                else:
                                    # #region agent log
                                    _log_debug(
                                        "H4",
                                        "app.py:sankey_tab",
                                        "Calling estimate_hurricane_damage",
                                        {"cache_key": cache_key},
                                    )
                                    # #endregion
                                    cache[cache_key] = estimate_hurricane_damage(
                                        modal_payload["hurricane"],
                                        state=modal_payload["state"],
                                    )
                                st.write(cache[cache_key])
                            except Exception as exc:
                                st.error(f"Damage estimate failed: {exc}")

                _show_damage_modal()
                st.session_state["sankey_damage_modal"] = None
        st.caption("Counts represent distinct counties affected.")

with sunburst_tab:
    st.subheader("Hurricane → Year → Hurricane → State")
    if st.button("Reset Sunburst view"):
        st.session_state["sunburst_reset_key"] = st.session_state.get("sunburst_reset_key", 0) + 1
    sunburst_result = get_sankey_rows(
        start_date.isoformat(),
        end_date.isoformat(),
        ["Hurricane", "Tropical Storm"],
    )
    if sunburst_result.df.empty:
        st.info("No hurricane data available for the selected filters.")
    else:
        df = sunburst_result.df.copy()
        df["disaster_declaration_date"] = pd.to_datetime(df["disaster_declaration_date"])
        df["year"] = df["disaster_declaration_date"].dt.year.astype(int).astype(str)
        df["storm_category"] = df["disaster_type"].map(
            {"Hurricane": "Named Hurricanes", "Tropical Storm": "Tropical Storms"}
        ).fillna("Other Storms")
        names = df["declaration_name"].fillna("").astype(str).str.strip()
        unique_names = sorted({name for name in names.tolist() if name})
        hash_input = "|".join(unique_names).encode("utf-8")
        cache_key = f"sunburst_name_map_{hashlib.md5(hash_input).hexdigest()}"
        name_map = st.session_state.get(cache_key)
        if name_map is None:
            with st.spinner("Grouping hurricane names..."):
                try:
                    name_map = group_declaration_names(unique_names)
                except Exception as exc:
                    st.error(f"LLM grouping failed: {exc}")
                    name_map = {}
            st.session_state[cache_key] = name_map
        df["hurricane"] = names.apply(
            lambda value: name_map.get(value, value) if value else "Other/Unnamed"
        )
        df.loc[df["hurricane"].eq(""), "hurricane"] = "Other/Unnamed"
        df["state"] = df["state"].fillna("Unknown")
        sunburst_df = (
            df.groupby(["storm_category", "year", "hurricane", "state"])["county_fips"]
            .nunique()
            .reset_index(name="value")
        )
        # #region agent log
        helene_states = (
            sunburst_df[sunburst_df["hurricane"].str.contains("Helene", case=False, na=False)]
            .groupby("state")["value"]
            .sum()
            .sort_values(ascending=False)
            .head(20)
            .to_dict()
        )
        _log_debug(
            "H11",
            "app.py:sunburst_tab",
            "Helene state counts in sunburst",
            {"states": helene_states},
        )
        # #endregion

        nodes = []
        palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
        nodes.append(
            {
                "id": "root",
                "label": "Named Storms",
                "parent": "",
                "value": int(sunburst_df["value"].sum()),
                "customdata": {"node_type": "root"},
                "color": "#ffffff",
            }
        )

        category_totals = (
            sunburst_df.groupby("storm_category")["value"].sum().reset_index()
        )
        year_totals = (
            sunburst_df.groupby(["storm_category", "year"])["value"]
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
                    "id": f"category:{row.storm_category}",
                    "label": row.storm_category,
                    "parent": "root",
                    "value": int(row.value),
                    "customdata": {"node_type": "category", "category": row.storm_category},
                    "color": "#dddddd",
                }
            )
        for row in year_totals.itertuples(index=False):
            nodes.append(
                {
                    "id": f"category:{row.storm_category}|year:{row.year}",
                    "label": str(row.year),
                    "parent": f"category:{row.storm_category}",
                    "value": int(row.value),
                    "customdata": {
                        "node_type": "year",
                        "year": str(row.year),
                        "category": row.storm_category,
                    },
                    "color": year_colors.get(row.year, "#cccccc"),
                }
            )

        year_hurricane = (
            sunburst_df.groupby(["storm_category", "year", "hurricane"])["value"]
            .sum()
            .reset_index()
        )
        for row in year_hurricane.itertuples(index=False):
            nodes.append(
                {
                    "id": f"category:{row.storm_category}|year:{row.year}|hurricane:{row.hurricane}",
                    "label": row.hurricane,
                    "parent": f"category:{row.storm_category}|year:{row.year}",
                    "value": int(row.value),
                    "customdata": {
                        "node_type": "hurricane",
                        "year": str(row.year),
                        "hurricane": row.hurricane,
                        "category": row.storm_category,
                    },
                    "color": year_colors.get(row.year, "#cccccc"),
                }
            )

        for row in sunburst_df.itertuples(index=False):
            nodes.append(
                {
                    "id": (
                        f"category:{row.storm_category}|year:{row.year}|"
                        f"hurricane:{row.hurricane}|state:{row.state}"
                    ),
                    "label": row.state,
                    "parent": f"category:{row.storm_category}|year:{row.year}|hurricane:{row.hurricane}",
                    "value": int(row.value),
                    "customdata": {
                        "node_type": "state",
                        "year": str(row.year),
                        "hurricane": row.hurricane,
                        "state": row.state,
                        "category": row.storm_category,
                    },
                    "color": year_colors.get(row.year, "#cccccc"),
                }
            )

        nodes_df = pd.DataFrame(nodes)
        st.session_state["sunburst_nodes_df"] = nodes_df
        sunburst_fig = build_sunburst(nodes_df)
        reset_key = st.session_state.get("sunburst_reset_key", 0)
        sunburst_events = plotly_events(
            sunburst_fig,
            click_event=True,
            select_event=False,
            hover_event=False,
            key=f"sunburst_{reset_key}",
        )
        # #region agent log
        _log_debug(
            "H7",
            "app.py:sunburst_tab",
            "Sunburst click events",
            {"event_count": len(sunburst_events), "event_keys": [list(ev.keys()) for ev in sunburst_events[:3]]},
        )
        # #endregion
        if sunburst_events:
            event = sunburst_events[0]
            cd = event.get("customdata") if isinstance(event, dict) else None
            if cd is None and isinstance(event, dict) and "pointNumber" in event:
                nodes_df = st.session_state.get("sunburst_nodes_df")
                if isinstance(nodes_df, pd.DataFrame):
                    idx = int(event["pointNumber"])
                    if 0 <= idx < len(nodes_df):
                        cd = nodes_df.iloc[idx]["customdata"]
            # #region agent log
            _log_debug(
                "H8",
                "app.py:sunburst_tab",
                "Sunburst click payload",
                {"event": event, "customdata": cd},
            )
            # #endregion
            if isinstance(cd, dict) and cd.get("node_type") == "state":
                hurricane = cd.get("hurricane")
                state = cd.get("state")
                cache = st.session_state.setdefault("sunburst_news_cache", {})
                cache_key = f"news:{hurricane}:{state}"
                st.session_state["sunburst_news_modal"] = {
                    "hurricane": hurricane,
                    "state": state,
                    "cache_key": cache_key,
                }

        modal_payload = st.session_state.get("sunburst_news_modal")
        # #region agent log
        _log_debug(
            "H9",
            "app.py:sunburst_tab",
            "Sunburst modal payload",
            {"modal_payload": modal_payload},
        )
        # #endregion
        if modal_payload:
            @st.dialog(f"Local headlines: {modal_payload['state']}")
            def _show_news_modal():
                cache = st.session_state.setdefault("sunburst_news_cache", {})
                if modal_payload["cache_key"] in cache:
                    st.write(cache[modal_payload["cache_key"]])
                    return

                state_lookup = {
                    "AL": "Alabama",
                    "AK": "Alaska",
                    "AZ": "Arizona",
                    "AR": "Arkansas",
                    "CA": "California",
                    "CO": "Colorado",
                    "CT": "Connecticut",
                    "DE": "Delaware",
                    "FL": "Florida",
                    "GA": "Georgia",
                    "HI": "Hawaii",
                    "ID": "Idaho",
                    "IL": "Illinois",
                    "IN": "Indiana",
                    "IA": "Iowa",
                    "KS": "Kansas",
                    "KY": "Kentucky",
                    "LA": "Louisiana",
                    "ME": "Maine",
                    "MD": "Maryland",
                    "MA": "Massachusetts",
                    "MI": "Michigan",
                    "MN": "Minnesota",
                    "MS": "Mississippi",
                    "MO": "Missouri",
                    "MT": "Montana",
                    "NE": "Nebraska",
                    "NV": "Nevada",
                    "NH": "New Hampshire",
                    "NJ": "New Jersey",
                    "NM": "New Mexico",
                    "NY": "New York",
                    "NC": "North Carolina",
                    "ND": "North Dakota",
                    "OH": "Ohio",
                    "OK": "Oklahoma",
                    "OR": "Oregon",
                    "PA": "Pennsylvania",
                    "RI": "Rhode Island",
                    "SC": "South Carolina",
                    "SD": "South Dakota",
                    "TN": "Tennessee",
                    "TX": "Texas",
                    "UT": "Utah",
                    "VT": "Vermont",
                    "VA": "Virginia",
                    "WA": "Washington",
                    "WV": "West Virginia",
                    "WI": "Wisconsin",
                    "WY": "Wyoming",
                }
                state_full = state_lookup.get(modal_payload["state"], modal_payload["state"])
                query = f'\"{modal_payload["hurricane"]}\" AND (\"{state_full}\" OR {modal_payload["state"]})'
                gnews_key = os.getenv("GNEWS_API_KEY")
                if not gnews_key:
                    st.error("GNEWS_API_KEY is not set.")
                    return
                url = (
                    "https://gnews.io/api/v4/search"
                    f"?q={requests.utils.quote(query)}&lang=en&max=3&token={requests.utils.quote(gnews_key)}"
                )
                with st.spinner("Fetching local headlines..."):
                    try:
                        resp = requests.get(url, timeout=20)
                        resp.raise_for_status()
                        data = resp.json()
                        articles = [
                            {"title": item.get("title"), "url": item.get("url")}
                            for item in data.get("articles", [])
                        ]
                        # #region agent log
                        _log_debug(
                            "H10",
                            "app.py:sunburst_tab",
                            "GDELT articles fetched",
                            {"article_count": len(articles), "query": query},
                        )
                        # #endregion
                        summary = summarize_gdelt_headlines(
                            modal_payload["hurricane"],
                            modal_payload["state"],
                            articles,
                        )
                        cache[modal_payload["cache_key"]] = summary
                        st.write(summary)
                    except Exception as exc:
                        st.error(f"News lookup failed: {exc}")

            _show_news_modal()
            st.session_state["sunburst_news_modal"] = None

