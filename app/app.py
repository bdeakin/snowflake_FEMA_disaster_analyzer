import datetime as dt
import hashlib
import json
import os
import importlib.util
import time
from pathlib import Path
import sys
from typing import List, Optional

import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_plotly_events import plotly_events
import streamlit.components.v1 as components
from dotenv import load_dotenv

app_dir = Path(__file__).resolve().parent
repo_root = app_dir.parent
for path in (str(app_dir), str(repo_root)):
    if path not in sys.path:
        sys.path.insert(0, path)

def _load_env() -> None:
    env_path = app_dir.parent / "config" / "secrets.env"
    # Normalize OpenAI key if it exists (strip whitespace, handle quoted values).
    current_key = os.getenv("OPENAI_API_KEY")
    if current_key:
        normalized = current_key.strip().strip("\"'").strip()
        os.environ["OPENAI_API_KEY"] = normalized
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        load_dotenv(override=True)
    current_key = os.getenv("OPENAI_API_KEY")
    if current_key:
        normalized = current_key.strip().strip("\"'").strip()
        os.environ["OPENAI_API_KEY"] = normalized


_load_env()

def _load_module(module_name: str, file_name: str):
    file_path = app_dir / file_name
    unique_name = f"{module_name}_{int(file_path.stat().st_mtime)}"
    spec = importlib.util.spec_from_file_location(unique_name, file_path)
    module = importlib.util.module_from_spec(spec)
    if spec and spec.loader:
        sys.modules[unique_name] = module
        spec.loader.exec_module(module)
    return module


def _load_module_at(module_name: str, file_path: Path):
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
        get_consistency_runs,
        get_cube_summary,
        get_disaster_date_bounds,
        get_distinct_disaster_types,
        get_drilldown,
        get_dynamic_table_metadata,
        get_name_grouping_cache,
        get_sankey_rows,
        get_state_choropleth,
        get_sunburst_rows,
        get_task_history,
        get_task_status,
        get_trends_bump_ranks,
        upsert_name_grouping_cache,
    )
    from llm import (
        summarize_bump_entry,
        group_declaration_names,
        group_sankey_names,
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
    from sankey import render_sankey
    from views.about import render_about
except ImportError:
    queries = _load_module("app_queries", "queries.py")
    llm = _load_module("app_llm", "llm.py")
    viz = _load_module("app_viz", "viz.py")
    sankey = _load_module("app_sankey", "sankey.py")
    about = _load_module_at("app_about", repo_root / "views" / "about.py")
    get_bump_drilldown_state_summary = queries.get_bump_drilldown_state_summary
    get_consistency_runs = queries.get_consistency_runs
    get_cube_summary = queries.get_cube_summary
    get_disaster_date_bounds = queries.get_disaster_date_bounds
    get_distinct_disaster_types = queries.get_distinct_disaster_types
    get_drilldown = queries.get_drilldown
    get_dynamic_table_metadata = queries.get_dynamic_table_metadata
    get_name_grouping_cache = queries.get_name_grouping_cache
    get_sankey_rows = queries.get_sankey_rows
    get_state_choropleth = queries.get_state_choropleth
    get_sunburst_rows = queries.get_sunburst_rows
    get_task_history = queries.get_task_history
    get_task_status = queries.get_task_status
    get_trends_bump_ranks = queries.get_trends_bump_ranks
    upsert_name_grouping_cache = queries.upsert_name_grouping_cache
    summarize_bump_entry = llm.summarize_bump_entry
    group_declaration_names = llm.group_declaration_names
    group_sankey_names = llm.group_sankey_names
    summarize_year_events = llm.summarize_year_events
    summarize_named_event = llm.summarize_named_event
    summarize_unnamed_events = llm.summarize_unnamed_events
    summarize_event_state = llm.summarize_event_state
    build_bump_chart = viz.build_bump_chart
    build_choropleth = viz.build_choropleth
    build_cube_grid = viz.build_cube_grid
    build_drilldown = viz.build_drilldown
    build_sunburst = viz.build_sunburst
    render_sankey = sankey.render_sankey
    render_about = about.render_about


st.set_page_config(page_title="FEMA Disasters Explorer", layout="wide")

st.title("FEMA Disasters Explorer")

type_result = get_distinct_disaster_types()
type_options = type_result.df["disaster_type"].dropna().tolist()
date_bounds_result = get_disaster_date_bounds()
date_bounds_df = date_bounds_result.df
min_available_date = None
max_available_date = None
if not date_bounds_df.empty:
    min_available_date = date_bounds_df.at[0, "min_date"]
    max_available_date = date_bounds_df.at[0, "max_date"]
max_data_year = pd.to_datetime(max_available_date).year if max_available_date is not None else 2025


def _render_data_range_note() -> None:
    if min_available_date is not None and max_available_date is not None:
        st.caption(
            "Data availability: "
            f"{pd.to_datetime(min_available_date).date()} → "
            f"{pd.to_datetime(max_available_date).date()}"
        )


def _year_range_to_dates(year_range: tuple[int, int]) -> tuple[dt.date, dt.date]:
    start_year, end_year = year_range
    start_date = dt.date(start_year, 1, 1)
    end_date = dt.date(end_year + 1, 1, 1)
    return start_date, end_date




def _render_sankey_content(
    sankey_year: int,
    sankey_types: Optional[List[str]],
) -> None:
    if not sankey_types:
        st.info("No disaster types available to render the Sankey view.")
        return

    sankey_start = dt.date(sankey_year, 1, 1)
    sankey_end = dt.date(sankey_year + 1, 1, 1)
    sankey_result = get_sankey_rows(
        sankey_start.isoformat(),
        sankey_end.isoformat(),
        sankey_types,
    )
    if sankey_result.df.empty:
        st.info("No records found for the selected filters.")
        return

    df = sankey_result.df.copy()
    df["record_id"] = df["record_id"].astype(str)
    if "county_fips" not in df.columns:
        df["county_fips"] = ""
    df["state"] = df["state"].fillna("Unknown").astype(str).str.strip()
    df["declaration_name"] = df["declaration_name"].fillna("").astype(str).str.strip()
    df["disaster_declaration_date"] = pd.to_datetime(df["disaster_declaration_date"])
    df["disaster_begin_date"] = pd.to_datetime(df.get("disaster_begin_date"), errors="coerce")
    df["disaster_end_date"] = pd.to_datetime(df.get("disaster_end_date"), errors="coerce")
    df["year"] = df["disaster_declaration_date"].dt.year.astype(int).astype(str)
    df["source_text_hash"] = df.apply(
        lambda row: hashlib.sha256(
            f"{row['disaster_type']}|{row['declaration_name']}".encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    record_ids = sorted(df["record_id"].unique().tolist())
    cache_result = get_name_grouping_cache(record_ids)
    cache_df = cache_result.df.copy()
    if cache_df.empty:
        cache_df = pd.DataFrame(
            columns=[
                "record_id",
                "cache_source_text_hash",
                "cache_name_group",
                "cache_theme_group",
                "cache_theme_confidence",
                "cache_is_named_event",
                "cache_canonical_event_name",
                "cache_confidence",
                "cache_llm_model",
            ]
        )
    else:
        cache_df = cache_df.rename(
            columns={
                "source_text_hash": "cache_source_text_hash",
                "name_group": "cache_name_group",
                "theme_group": "cache_theme_group",
                "theme_confidence": "cache_theme_confidence",
                "is_named_event": "cache_is_named_event",
                "canonical_event_name": "cache_canonical_event_name",
                "confidence": "cache_confidence",
                "llm_model": "cache_llm_model",
            }
        )
    df = df.merge(cache_df, on="record_id", how="left")
    needs_enrich = df["cache_source_text_hash"].isna() | (
        df["cache_source_text_hash"] != df["source_text_hash"]
    )
    missing_records = (
        df.loc[
            needs_enrich,
            ["record_id", "year", "disaster_type", "declaration_name", "source_text_hash"],
        ]
        .drop_duplicates(subset=["record_id"])
        .reset_index(drop=True)
    )

    llm_rows = []
    if not missing_records.empty:
        missing_count = int(missing_records.shape[0])
        if missing_count > 5000:
            st.warning(
                "Too many uncached records to enrich at once. "
                "Narrow the filters or re-run after the cache warms. "
                "Enriching the first 500 records for now."
            )
            missing_records = missing_records.head(500)
        llm_disabled = st.session_state.get("sankey_llm_disabled", False)
        if not os.getenv("OPENAI_API_KEY"):
            st.warning(
                "OPENAI_API_KEY is not set. Using fallback 'Unnamed' labels "
                "for uncached records."
            )
        elif llm_disabled:
            st.info(
                "OpenAI name grouping is disabled due to a prior authentication error. "
                "Using fallback 'Unnamed' labels for uncached records."
            )
        else:
            total_records = int(missing_records.shape[0])
            chunk_size = 25
            total_batches = max((total_records + chunk_size - 1) // chunk_size, 1)
            progress_state = {"completed_batches": 0, "processed_records": 0}
            progress_bar = st.progress(0)
            status_text = st.caption(
                f"LLM grouping: 0/{total_batches} batches completed (0/{total_records} records)."
            )
            started_at = time.monotonic()

            def _format_eta(seconds: float) -> str:
                seconds = max(seconds, 0.0)
                minutes = int(seconds // 60)
                secs = int(seconds % 60)
                return f"{minutes}m {secs:02d}s"

            def _update_progress(processed: int) -> None:
                progress_state["completed_batches"] += 1
                progress_state["processed_records"] += processed
                completed_batches = progress_state["completed_batches"]
                processed_records = progress_state["processed_records"]
                progress = min(completed_batches / total_batches, 1.0)
                progress_bar.progress(progress)
                elapsed = time.monotonic() - started_at
                avg_per_batch = elapsed / completed_batches if completed_batches else 0
                remaining_batches = max(total_batches - completed_batches, 0)
                eta_text = _format_eta(avg_per_batch * remaining_batches)
                status_text.caption(
                    "LLM grouping: "
                    f"{completed_batches}/{total_batches} batches completed "
                    f"({processed_records}/{total_records} records). "
                    f"ETA {eta_text}."
                )

            with st.spinner("Grouping event names with OpenAI..."):
                try:
                    llm_rows = group_sankey_names(
                        missing_records[
                            ["record_id", "year", "disaster_type", "declaration_name"]
                        ].to_dict("records"),
                        timeout_s=60,
                        chunk_size=25,
                        progress_callback=_update_progress,
                    )
                    if len(llm_rows) < len(missing_records):
                        st.warning(
                            "OpenAI name grouping returned partial results (timeout or connection issue). "
                            "Using fallback 'Unnamed' labels for remaining records."
                        )
                except Exception as exc:
                    if "401" in str(exc):
                        st.session_state["sankey_llm_disabled"] = True
                    st.warning(
                        "OpenAI name grouping failed. Using fallback 'Unnamed' labels "
                        f"for uncached records. Error: {exc}"
                    )
        if llm_rows:
            hash_map = dict(
                zip(missing_records["record_id"], missing_records["source_text_hash"])
            )
            llm_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            for row in llm_rows:
                record_id = str(row.get("record_id"))
                row["record_id"] = record_id
                row["source_text_hash"] = hash_map.get(record_id, "")
                row["llm_model"] = llm_model
            upsert_name_grouping_cache(llm_rows)

            llm_df = pd.DataFrame(llm_rows).rename(
                columns={
                    "name_group": "llm_name_group",
                    "theme_group": "llm_theme_group",
                    "theme_confidence": "llm_theme_confidence",
                    "canonical_event_name": "llm_canonical_event_name",
                }
            )
            df = df.merge(
                llm_df[
                    [
                        "record_id",
                        "llm_name_group",
                        "llm_theme_group",
                        "llm_theme_confidence",
                        "llm_canonical_event_name",
                    ]
                ],
                on="record_id",
                how="left",
            )
    record_status = df.drop_duplicates(subset=["record_id"])[
        ["record_id", "cache_source_text_hash", "source_text_hash"]
    ]
    cached_count = int(
        (record_status["cache_source_text_hash"].notna()
         & (record_status["cache_source_text_hash"] == record_status["source_text_hash"])
        ).sum()
    )
    total_count = int(record_status.shape[0])
    enriched_count = len(llm_rows)
    remaining_count = max(total_count - cached_count - enriched_count, 0)
    st.caption(
        "Name grouping cache status: "
        f"cached {cached_count}/{total_count}, "
        f"enriched {enriched_count}, "
        f"remaining {remaining_count}."
    )
    df["canonical_event_name"] = df["cache_canonical_event_name"]
    if "llm_canonical_event_name" in df.columns:
        df["canonical_event_name"] = df["canonical_event_name"].combine_first(
            df["llm_canonical_event_name"]
        )

    df["name_group"] = df["cache_name_group"]
    if "llm_name_group" in df.columns:
        df["name_group"] = df["name_group"].combine_first(df["llm_name_group"])
    missing_name = df["name_group"].isna() | (df["name_group"].astype(str).str.strip() == "")
    if missing_name.any():
        df.loc[missing_name, "name_group"] = "Unnamed"

    named_tokens = {"named", "named event", "named events"}

    def _normalize_name_group(row: pd.Series) -> str:
        name_group = str(row.get("name_group", "")).strip()
        if name_group == "Unnamed":
            dtype = str(row.get("disaster_type", "")).strip()
            return f"Unnamed ({dtype})" if dtype else "Unnamed"
        if name_group.lower() in named_tokens:
            candidate = (
                row.get("canonical_event_name")
                or row.get("declaration_name")
                or "Unnamed"
            )
            candidate = str(candidate).strip()
            return candidate if candidate else "Unnamed"
        return name_group

    df["name_group"] = df.apply(_normalize_name_group, axis=1)

    df["event_name"] = (
        df["canonical_event_name"]
        .fillna(df["declaration_name"])
        .fillna("Unnamed")
        .astype(str)
        .str.strip()
    )
    df.loc[df["event_name"] == "", "event_name"] = "Unnamed"

    def _format_event_range(row: pd.Series) -> str:
        start = row.get("disaster_begin_date")
        end = row.get("disaster_end_date")
        if pd.isna(start) and pd.isna(end):
            start = row.get("disaster_declaration_date")
            end = row.get("disaster_declaration_date")
        if pd.isna(start) and pd.isna(end):
            return "Unknown date"
        if pd.isna(end) or start == end:
            return pd.to_datetime(start).strftime("%Y-%m-%d")
        return (
            f"{pd.to_datetime(start).strftime('%Y-%m-%d')} "
            f"to {pd.to_datetime(end).strftime('%Y-%m-%d')}"
        )

    df["event_tooltip"] = None
    fire_mask = df["disaster_type"].astype(str).str.lower().eq("fire")
    if fire_mask.any():
        fire_df = df.loc[fire_mask].copy()
        fire_df["event_date"] = fire_df["disaster_begin_date"]
        fire_df.loc[fire_df["event_date"].isna(), "event_date"] = fire_df[
            "disaster_declaration_date"
        ]
        fire_df.loc[fire_df["event_date"].isna(), "event_date"] = fire_df[
            "disaster_end_date"
        ]
        fire_df = fire_df.sort_values(["state", "event_date"]).reset_index()
        cluster_ids = []
        current_state = None
        cluster_start = None
        cluster_id = 0
        for _, row in fire_df.iterrows():
            state = row["state"]
            event_date = row["event_date"]
            if current_state != state:
                current_state = state
                cluster_id = 0
                cluster_start = event_date
            if (
                cluster_start is not None
                and event_date is not None
                and not pd.isna(cluster_start)
                and not pd.isna(event_date)
                and (event_date - cluster_start).days > 92
            ):
                cluster_id += 1
                cluster_start = event_date
            cluster_ids.append(cluster_id)
        fire_df["fire_cluster_id"] = cluster_ids
        fire_df["cluster_key"] = (
            fire_df["state"].astype(str) + "|" + fire_df["fire_cluster_id"].astype(str)
        )
        cluster_labels = {}
        cluster_tooltips = {}
        for cluster_key, group in fire_df.groupby("cluster_key"):
            event_names = (
                group["event_name"].dropna().astype(str).str.strip().unique().tolist()
            )
            label = "Fire Cluster (" + "; ".join(event_names) + ")"
            cluster_labels[cluster_key] = label
            tooltip_lines = []
            for _, row in group.iterrows():
                tooltip_lines.append(
                    f"{row['event_name']}: {_format_event_range(row)}"
                )
            cluster_tooltips[cluster_key] = "\n".join(tooltip_lines)
        fire_df["name_group"] = fire_df["cluster_key"].map(cluster_labels)
        df.loc[fire_df["index"], "name_group"] = fire_df["name_group"].values

    def _event_range_for_group(group: pd.DataFrame) -> str:
        start_min = group["disaster_begin_date"].min()
        end_max = group["disaster_end_date"].max()
        decl_min = group["disaster_declaration_date"].min()
        decl_max = group["disaster_declaration_date"].max()
        if pd.isna(start_min) and pd.isna(end_max):
            start_min = decl_min
            end_max = decl_max
        if pd.isna(start_min) and pd.isna(end_max):
            return "Unknown date"
        if pd.isna(end_max) or start_min == end_max:
            return pd.to_datetime(start_min).strftime("%Y-%m-%d")
        return (
            f"{pd.to_datetime(start_min).strftime('%Y-%m-%d')} "
            f"to {pd.to_datetime(end_max).strftime('%Y-%m-%d')}"
        )

    df["theme_group"] = df["cache_theme_group"]
    if "llm_theme_group" in df.columns:
        df["theme_group"] = df["theme_group"].combine_first(df["llm_theme_group"])
    missing_theme = df["theme_group"].isna() | (df["theme_group"].astype(str).str.strip() == "")
    if missing_theme.any():
        df.loc[missing_theme, "theme_group"] = "No Theme"

    county_df = df[df["county_fips"].astype(str).str.strip() != ""]
    tooltip_parts = []
    for (name_group, event_name), group in df.groupby(["name_group", "event_name"]):
        tooltip_parts.append(
            {
                "name_group": name_group,
                "tooltip_line": f"{event_name}: {_event_range_for_group(group)}",
            }
        )
    tooltip_df = pd.DataFrame(tooltip_parts)
    tooltip_counts = (
        county_df.groupby("name_group")["county_fips"]
        .count()
        .to_dict()
    )
    event_tooltip_map = {}
    for name_group, lines in tooltip_df.groupby("name_group")["tooltip_line"]:
        total_count = int(tooltip_counts.get(name_group, 0))
        tooltip_text = "\n".join(lines)
        tooltip_text += (
            f"\n(Total declared county-level disasters: {total_count})"
        )
        event_tooltip_map[name_group] = tooltip_text

    st.caption("Flow: Theme → Event → State")
    theme_counts = (
        county_df.groupby(["theme_group", "name_group"])["county_fips"]
        .count()
        .reset_index(name="value")
    )
    state_counts = (
        county_df.groupby(["name_group", "state"])["county_fips"]
        .count()
        .reset_index(name="value")
    )

    nodes = []
    node_ids = set()
    node_lookup = {}

    def _add_node(prefix: str, label: str, tooltip: Optional[str] = None) -> str:
        node_id = f"{prefix}:{label}"
        if node_id not in node_ids:
            node_ids.add(node_id)
            node = {"id": node_id, "name": label}
            if tooltip:
                node["tooltip"] = tooltip
            nodes.append(node)
            node_lookup[node_id] = node
        elif tooltip and node_id in node_lookup and "tooltip" not in node_lookup[node_id]:
            node_lookup[node_id]["tooltip"] = tooltip
        return node_id

    for row in theme_counts.itertuples(index=False):
        _add_node("TH", str(row.theme_group))
        _add_node(
            "N",
            str(row.name_group),
            tooltip=event_tooltip_map.get(row.name_group),
        )
    for row in state_counts.itertuples(index=False):
        _add_node(
            "N",
            str(row.name_group),
            tooltip=event_tooltip_map.get(row.name_group),
        )
        _add_node("S", str(row.state))

    links = []
    for row in theme_counts.itertuples(index=False):
        links.append(
            {
                "source": f"TH:{row.theme_group}",
                "target": f"N:{row.name_group}",
                "value": int(row.value),
            }
        )
    for row in state_counts.itertuples(index=False):
        links.append(
            {
                "source": f"N:{row.name_group}",
                "target": f"S:{row.state}",
                "value": int(row.value),
            }
        )

    if len(nodes) > 400:
        st.warning(
            f"Too many nodes to render clearly ({len(nodes)}). Narrow the year range "
            "or disaster type to reduce complexity."
        )
    else:
        components.html(render_sankey(nodes, links, height=650), height=670, scrolling=True)


def _format_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if "year" not in col.lower():
            continue
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            df[col] = series.dt.year.astype(int).astype(str)
        elif pd.api.types.is_numeric_dtype(series):
            df[col] = series.astype("Int64").astype(str)
        else:
            df[col] = series.astype(str).str.replace(",", "", regex=False)
    return df


tabs = st.tabs(
    [
        "About",
        "Map View",
        "Change in Disaster Types Over Time",
        "Annual Disaster Themes",
        "Disaster Impact Assessment",
        "⚙️ Consistency Checker",
    ]
)

with tabs[0]:
    render_about()

with tabs[1]:
    st.subheader("Map View")
    filter_col, content_col = st.columns([1, 4])
    with filter_col:
        st.subheader("Filters")
        _render_data_range_note()
        explore_end_default = max_data_year
        explore_start_default = max(1953, explore_end_default - 2)
        explore_years = st.slider(
            "Year range",
            min_value=1953,
            max_value=max_data_year,
            value=(explore_start_default, explore_end_default),
            key="filters_explore_year_range",
        )
        explore_types = []
        for dtype in type_options:
            checked = st.checkbox(
                dtype,
                value=True,
                key=f"filters_explore_type_{dtype}",
            )
            if checked:
                explore_types.append(dtype)
        if not explore_types:
            explore_types = None
    with content_col:
        current_filter_sig = (
            tuple(explore_years),
            tuple(sorted(explore_types)) if explore_types else None,
        )
        if st.session_state.get("explore_filter_sig") != current_filter_sig:
            st.session_state["explore_filter_sig"] = current_filter_sig
            st.session_state.pop("selected_state", None)
            st.session_state.pop("selected_cube", None)
            st.session_state.pop("declaration_color_map", None)
            st.session_state.pop("declaration_color_index", None)

        def _select_grain(start: dt.date, end: dt.date) -> str:
            if end.year - start.year >= 1:
                return "year"
            if (end.year * 12 + end.month) - (start.year * 12 + start.month) >= 1:
                return "month"
            return "week"

        start_date, end_date = _year_range_to_dates(explore_years)
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
            st.subheader(
                f"Disaster Summary by Period: {selected_state} (log-scaled size)"
            )
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
                drilldown_df = drilldown_result.df.copy()
                drilldown_df["declaration_name"] = (
                    drilldown_df["declaration_name"].fillna("").astype(str).str.strip()
                )
                drilldown_df.loc[
                    drilldown_df["declaration_name"] == "", "declaration_name"
                ] = "Unnamed"
                drilldown_df["disaster_begin_date"] = pd.to_datetime(
                    drilldown_df.get("disaster_begin_date"), errors="coerce"
                )
                drilldown_df["disaster_end_date"] = pd.to_datetime(
                    drilldown_df.get("disaster_end_date"), errors="coerce"
                )
                drilldown_df["disaster_declaration_date"] = pd.to_datetime(
                    drilldown_df.get("disaster_declaration_date"), errors="coerce"
                )
                drilldown_df["hover_start_date"] = drilldown_df["disaster_begin_date"].dt.strftime(
                    "%Y-%m-%d"
                )
                drilldown_df["hover_end_date"] = drilldown_df["disaster_end_date"].dt.strftime(
                    "%Y-%m-%d"
                )

                generic_names = {
                    "severe storm",
                    "severe storms",
                    "severe weather",
                    "storm",
                    "storms",
                    "flood",
                    "flooding",
                    "wildfire",
                    "wildfires",
                    "snowstorm",
                    "snowstorms",
                    "winter storm",
                    "winter storms",
                    "tornado",
                    "tornadoes",
                    "hurricane",
                    "tropical storm",
                    "earthquake",
                    "volcanic eruption",
                    "volcano",
                    "drought",
                    "fire",
                }

                def _format_date_range(row: pd.Series) -> str:
                    start = row.get("disaster_begin_date")
                    end = row.get("disaster_end_date")
                    if pd.isna(start) and pd.isna(end):
                        start = row.get("disaster_declaration_date")
                        end = row.get("disaster_declaration_date")
                    if pd.isna(start) and pd.isna(end):
                        return ""
                    if pd.isna(end) or start == end:
                        return pd.to_datetime(start).strftime("%Y-%m-%d")
                    return (
                        f"{pd.to_datetime(start).strftime('%Y-%m-%d')} "
                        f"to {pd.to_datetime(end).strftime('%Y-%m-%d')}"
                    )

                def _is_generic_name(name: str, dtype: str) -> bool:
                    cleaned = name.strip().lower()
                    dtype_clean = (dtype or "").strip().lower()
                    if cleaned in generic_names:
                        return True
                    if dtype_clean and cleaned == dtype_clean:
                        return True
                    if dtype_clean and cleaned.startswith(dtype_clean) and len(cleaned.split()) <= 2:
                        return True
                    return False

                drilldown_df["display_name"] = drilldown_df["declaration_name"]
                date_ranges = drilldown_df.apply(_format_date_range, axis=1)
                generic_mask = drilldown_df.apply(
                    lambda row: _is_generic_name(
                        str(row.get("declaration_name", "")),
                        str(row.get("disaster_type", "")),
                    ),
                    axis=1,
                )
                drilldown_df.loc[generic_mask, "display_name"] = (
                    drilldown_df.loc[generic_mask, "declaration_name"]
                    + " ("
                    + date_ranges[generic_mask].fillna("")
                    + ")"
                )
                drilldown_df["display_name"] = drilldown_df["display_name"].str.replace(
                    " ()", "", regex=False
                )

                available_names = sorted(drilldown_df["display_name"].unique().tolist())

                color_map = st.session_state.get("declaration_color_map", {})
                color_index = st.session_state.get("declaration_color_index", 0)
                palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
                for name in available_names:
                    if name not in color_map:
                        color_map[name] = palette[color_index % len(palette)]
                        color_index += 1
                st.session_state["declaration_color_map"] = color_map
                st.session_state["declaration_color_index"] = color_index

                filter_sig = (
                    f"{selected_state}|{selected_cube['disaster_type']}|"
                    f"{selected_cube['period_bucket']}|{grain}"
                )
                filter_key = (
                    "filters_drilldown_names_"
                    + hashlib.md5(filter_sig.encode("utf-8")).hexdigest()
                )
                drilldown_chart_col, drilldown_legend_col = st.columns([4, 1])
                with drilldown_legend_col:
                    st.caption("Declaration names")
                    name_ranges = (
                        drilldown_df.groupby("display_name", dropna=False)
                        .agg(
                            start_min=("disaster_begin_date", "min"),
                            end_max=("disaster_end_date", "max"),
                            decl_min=("disaster_declaration_date", "min"),
                        )
                        .reset_index()
                    )
                    name_range_map = {}
                    for _, row in name_ranges.iterrows():
                        start = row.get("start_min")
                        end = row.get("end_max")
                        if pd.isna(start) and pd.isna(end):
                            start = row.get("decl_min")
                            end = row.get("decl_min")
                        if pd.isna(start) and pd.isna(end):
                            range_text = ""
                        elif pd.isna(end) or start == end:
                            range_text = pd.to_datetime(start).strftime("%Y-%m-%d")
                        else:
                            range_text = (
                                f"{pd.to_datetime(start).strftime('%Y-%m-%d')} "
                                f"to {pd.to_datetime(end).strftime('%Y-%m-%d')}"
                            )
                        name_range_map[row["display_name"]] = range_text

                    st.caption("Color mapping is shown in the chart legend.")
                    selected_names = []
                    for name in available_names:
                        range_text = name_range_map.get(name, "")
                        label_text = f"{name} ({range_text})" if range_text else name
                        key = f"{filter_key}_{hashlib.md5(name.encode('utf-8')).hexdigest()}"
                        checked = st.checkbox(label_text, value=True, key=key)
                        if checked:
                            selected_names.append(name)

                filtered_df = drilldown_df[drilldown_df["display_name"].isin(selected_names)]
                with drilldown_chart_col:
                    if filtered_df.empty:
                        st.info("No drilldown data for selected declaration names.")
                    else:
                        drilldown_fig = build_drilldown(filtered_df, color_map=color_map)
                        drilldown_fig.update_layout(
                            margin={"r": 0, "t": 0, "l": 0, "b": 0},
                            showlegend=True,
                        )
                        st.plotly_chart(drilldown_fig, use_container_width=True)

with tabs[2]:
    st.subheader("Change in Disaster Types Over Time")
    filter_col, content_col = st.columns([1, 4])
    with filter_col:
        st.subheader("Filters")
        _render_data_range_note()
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
                min_value=1953,
                max_value=max_data_year,
                value=(1953, max_data_year),
                key="filters_trends_year_range",
            )
            trends_start, trends_end = _year_range_to_dates(trends_years)
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

    with content_col:
        chart_title = f"Top Disaster Types by {binning.title()} (Top {top_n})"
        st.caption(chart_title)
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
                    period_start = dt.date(period_dt.year, period_dt.month, 1)
                    if period_dt.month == 12:
                        period_end = dt.date(period_dt.year + 1, 1, 1)
                    else:
                        period_end = dt.date(period_dt.year, period_dt.month + 1, 1)
                elif selected_bump.get("binning") == "years" and pd.notna(period_dt):
                    period_label = period_dt.strftime("%Y")
                    period_start = dt.date(period_dt.year, 1, 1)
                    period_end = dt.date(period_dt.year + 1, 1, 1)
                elif pd.notna(period_dt):
                    period_label = f"{period_dt.strftime('%Y')}s"
                    period_start = dt.date(period_dt.year, 1, 1)
                    period_end = dt.date(period_dt.year + 10, 1, 1)
                else:
                    period_label = str(selected_bump["period_bucket"])
                    period_start = dt.date(2000, 1, 1)
                    period_end = dt.date(2001, 1, 1)
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
                    st.dataframe(_format_year_columns(drilldown_summary.df), use_container_width=True)
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
                                            binning=selected_bump.get("binning", "decades"),
                                        )
                                        cache[cache_key] = summary
                                        st.write(summary)
                                    except Exception as exc:
                                        st.error(f"LLM summary failed: {exc}")
                        _show_llm_summary()
                        st.session_state["show_bump_llm_modal"] = False
            else:
                st.caption("Select a point in the bump chart to view drilldown details.")

with tabs[3]:
    st.subheader("Annual Disaster Themes")
    filter_col, content_col = st.columns([1, 4])
    with filter_col:
        st.subheader("Filters")
        _render_data_range_note()
        min_year = 2000
        max_year = dt.date.today().year
        if min_available_date is not None:
            min_year = pd.to_datetime(min_available_date).year
        if max_available_date is not None:
            max_year = pd.to_datetime(max_available_date).year
        year_options = list(range(min_year, max_year + 1))
        default_year = min(max_year, 2025) if 2025 in year_options else max_year
        sankey_year = st.selectbox(
            "Year",
            options=year_options,
            index=year_options.index(default_year),
            key="filters_sankey_year",
        )
        default_sankey = {"Hurricane", "Tropical Storm", "Volcanic Eruption"}
        for dtype in type_options:
            key = f"filters_sankey_type_{dtype}"
            if key not in st.session_state:
                st.session_state[key] = dtype in default_sankey

        select_col, deselect_col = st.columns(2)
        with select_col:
            if st.button("Select all", key="filters_sankey_select_all"):
                for dtype in type_options:
                    st.session_state[f"filters_sankey_type_{dtype}"] = True
        with deselect_col:
            if st.button("Deselect all", key="filters_sankey_deselect_all"):
                for dtype in type_options:
                    st.session_state[f"filters_sankey_type_{dtype}"] = False
        sankey_types = []
        for dtype in type_options:
            checked = st.checkbox(
                dtype,
                key=f"filters_sankey_type_{dtype}",
            )
            if checked:
                sankey_types.append(dtype)
        if not sankey_types:
            sankey_types = None

    with content_col:
        _render_sankey_content(sankey_year, sankey_types)

with tabs[4]:
    st.subheader("Disaster Impact Assessment")
    filter_col, content_col = st.columns([1, 4])
    with filter_col:
        st.subheader("Filters")
        _render_data_range_note()
        if st.session_state.get("sunburst_focus_category") or st.session_state.get("sunburst_focus_year"):
            st.caption("Filters locked while drilldown is active.")
            if st.button("Unlock filters", key="sunburst_unlock_filters"):
                st.session_state.pop("sunburst_focus_category", None)
                st.session_state.pop("sunburst_focus_year", None)
                st.session_state.pop("sunburst_selected_node", None)
                st.session_state.pop("sunburst_last_click_key", None)
        sunburst_years = st.slider(
            "Year range",
            min_value=2000,
            max_value=2025,
            value=(2023, 2025),
            key="filters_sunburst_year_range",
            disabled=bool(
                st.session_state.get("sunburst_focus_category")
                or st.session_state.get("sunburst_focus_year")
            ),
        )
        default_sunburst = {"Hurricane", "Tropical Storm"}
        pending_category = st.session_state.pop("sunburst_pending_category", None)
        focus_category = st.session_state.get("sunburst_focus_category")
        focus_year = st.session_state.get("sunburst_focus_year")
        for dtype in type_options:
            key = f"filters_sunburst_type_{dtype}"
            if key not in st.session_state:
                st.session_state[key] = dtype in default_sunburst
            elif pending_category is not None:
                st.session_state[key] = dtype == pending_category
        select_col, deselect_col = st.columns(2)
        with select_col:
            if st.button("Select all", key="filters_sunburst_select_all", disabled=bool(focus_category)):
                for dtype in type_options:
                    st.session_state[f"filters_sunburst_type_{dtype}"] = True
        with deselect_col:
            if st.button("Deselect all", key="filters_sunburst_deselect_all", disabled=bool(focus_category)):
                for dtype in type_options:
                    st.session_state[f"filters_sunburst_type_{dtype}"] = False
        sunburst_types = []
        for dtype in type_options:
            checked = st.checkbox(
                dtype,
                key=f"filters_sunburst_type_{dtype}",
                disabled=bool(focus_category),
            )
            if checked:
                sunburst_types.append(dtype)
        if not sunburst_types:
            sunburst_types = None
        if focus_category:
            sunburst_types = [focus_category]
        if focus_year:
            sunburst_years = (int(focus_year), int(focus_year))

    with content_col:
        st.caption("Named Events → Year → Event → State")
        if st.button("Reset Sunburst view"):
            st.session_state["sunburst_reset_key"] = st.session_state.get("sunburst_reset_key", 0) + 1
            st.session_state.pop("sunburst_selected_node", None)
        sunburst_start, sunburst_end = _year_range_to_dates(sunburst_years)
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
            name_map = st.session_state.get("sunburst_name_map_global", {})
            missing_names = [name for name in unique_names if name not in name_map]
            if missing_names:
                with st.spinner("Grouping named events..."):
                    try:
                        name_map.update(group_declaration_names(missing_names))
                    except Exception as exc:
                        st.error(f"LLM grouping failed: {exc}")
                st.session_state["sunburst_name_map_global"] = name_map
            df["event"] = names.apply(
                lambda value: name_map.get(value, value) if value else "Other/Unnamed"
            )
            df.loc[df["event"].eq(""), "event"] = "Other/Unnamed"
            df["state"] = df["state"].fillna("Unknown")
            @st.cache_data(show_spinner=False)
            def _build_sunburst_nodes(
                input_df: pd.DataFrame, year_color_items: tuple[tuple[str, str], ...]
            ) -> pd.DataFrame:
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
                year_colors = dict(year_color_items)
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

            year_list = (
                df["year"].astype(str).dropna().unique().tolist()
                if "year" in df.columns
                else []
            )
            unique_years = sorted(set(year_list))
            year_color_map = st.session_state.get("sunburst_year_color_map", {})
            year_color_index = int(st.session_state.get("sunburst_year_color_index", 0))
            palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
            for year in unique_years:
                if year not in year_color_map:
                    year_color_map[year] = palette[year_color_index % len(palette)]
                    year_color_index += 1
            st.session_state["sunburst_year_color_map"] = year_color_map
            st.session_state["sunburst_year_color_index"] = year_color_index
            year_color_items = tuple((year, year_color_map[year]) for year in unique_years)
        nodes_df = _build_sunburst_nodes(df, year_color_items)
        st.session_state["sunburst_nodes_df"] = nodes_df
        selected_node = st.session_state.get("sunburst_selected_node")
        breadcrumb_parts = ["Named Events"]
        if isinstance(selected_node, dict):
            if selected_node.get("category"):
                breadcrumb_parts.append(str(selected_node["category"]))
            if selected_node.get("year"):
                breadcrumb_parts.append(str(selected_node["year"]))
            if selected_node.get("event"):
                breadcrumb_parts.append(str(selected_node["event"]))
            if selected_node.get("state"):
                breadcrumb_parts.append(str(selected_node["state"]))
        if len(breadcrumb_parts) > 1:
            st.caption(" > ".join(breadcrumb_parts))
        # #region agent log
        _debug_log(
            "H5",
            "app.py:sunburst_nodes:selected",
            "Selected node before filtering",
            {"selected_node": selected_node},
        )
        # #endregion
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

        focus_on_selection = bool(selected_id)

        if selected_id and focus_on_selection:
            filtered_nodes = nodes_df[
                (nodes_df["id"] == selected_id)
                | (nodes_df["id"].str.startswith(f"{selected_id}|"))
            ]
            filtered_nodes = filtered_nodes.copy()
            subtree_ids = set(filtered_nodes["id"].tolist())
            filtered_nodes["parent"] = filtered_nodes["parent"].apply(
                lambda parent: parent if parent in subtree_ids else ""
            )
        if focus_on_selection and len(filtered_nodes) < len(nodes_df):
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
        sunburst_col, narrative_col = st.columns([3, 2], gap="large")
        with sunburst_col:
            chart_container = st.empty()
            sunburst_fig = build_sunburst(filtered_nodes)
            sunburst_fig.update_layout(
                height=780,
                margin={"r": 0, "t": 0, "l": 0, "b": 40},
                paper_bgcolor="#ffffff",
                plot_bgcolor="#ffffff",
            )
            reset_key = st.session_state.get("sunburst_reset_key", 0)
            event_nonce = st.session_state.get("sunburst_event_nonce", 0)
            with chart_container:
                sunburst_events = plotly_events(
                    sunburst_fig,
                    click_event=True,
                    select_event=False,
                    hover_event=False,
                    override_height=780,
                    key=f"sunburst_{reset_key}_{event_nonce}",
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
                if isinstance(cd, dict):
                    selected_category = cd.get("category")
                    selected_year = cd.get("year")
                    click_key = (
                        f"{cd.get('node_type')}|{cd.get('category')}|"
                        f"{cd.get('year')}|{cd.get('event')}|{cd.get('state')}"
                    )
                    last_click = st.session_state.get("sunburst_last_click_key")
                    if click_key != last_click:
                        if selected_category:
                            st.session_state["sunburst_focus_category"] = selected_category
                        if selected_year:
                            st.session_state["sunburst_focus_year"] = selected_year
                    if click_key != last_click:
                        st.session_state["sunburst_selected_node"] = cd
                        st.session_state["sunburst_last_click_key"] = click_key
                        st.session_state["sunburst_event_nonce"] = (
                            st.session_state.get("sunburst_event_nonce", 0) + 1
                        )
                        chart_container.empty()
                        st.rerun()
                        # No forced rerun; let Streamlit update naturally.

        with narrative_col:
            st.subheader("Impact Assessment")
            selected_node = st.session_state.get("sunburst_selected_node")
            if not selected_node:
                st.caption("Click a year, named event, or state to see a narrative summary.")
            else:
                st.caption("Click the button below to open the narrative.")
                if st.button("Open Impact Assessment", key="sunburst_open_modal"):
                    st.session_state["sunburst_show_modal"] = True

        selected_node = st.session_state.get("sunburst_selected_node")
        if selected_node and st.session_state.get("sunburst_show_modal"):
            @st.dialog("Impact Assessment")
            def _show_impact_assessment():
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
                                    headlines = []
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
                                    headlines = []
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

            _show_impact_assessment()
            st.session_state["sunburst_show_modal"] = False

with tabs[5]:
    filter_col, content_col = st.columns([1, 4])
    with filter_col:
        st.subheader("Filters")
        default_end = dt.date.today()
        default_start = default_end - dt.timedelta(days=30)
        window_start = st.date_input(
            "Results window start",
            value=default_start,
            key="filters_consistency_window_start",
        )
        window_end = st.date_input(
            "Results window end",
            value=default_end,
            key="filters_consistency_window_end",
        )
        if window_start > window_end:
            st.error("Window start must be before window end.")
            st.stop()
        status_filter = st.multiselect(
            "Run status",
            options=["IN_SYNC", "STALE_OK", "OUT_OF_SYNC", "ERROR"],
            default=["IN_SYNC", "STALE_OK"],
            key="filters_consistency_status",
        )
        max_rows = st.slider(
            "Max rows",
            min_value=10,
            max_value=200,
            value=50,
            step=10,
            key="filters_consistency_limit",
        )
    with content_col:
        st.subheader("Snowflake Tasks Enabled")
        task_names = [
            "ANALYTICS.MONITORING.TASK_RUN_CONSISTENCY_CHECK_12H",
        ]
        task_status = get_task_status(task_names).df
        if task_status.empty:
            st.info("Task metadata not available.")
        else:
            st.dataframe(_format_year_columns(task_status), use_container_width=True)
            if "next_scheduled_time" in task_status.columns:
                next_run = task_status["next_scheduled_time"].dropna()
                if not next_run.empty:
                    st.caption(f"Next scheduled run: {next_run.iloc[0]}")
            with st.expander("Task history (last 5 runs)"):
                history_df = get_task_history(task_names, limit_rows=5).df
                st.dataframe(_format_year_columns(history_df), use_container_width=True)

        st.subheader("Dynamic Tables")
        dt_names = [
            "ANALYTICS.SILVER.FCT_DISASTERS",
            "ANALYTICS.GOLD.DISASTERS_BY_STATE",
        ]
        dt_meta = get_dynamic_table_metadata(dt_names).df
        dt_help = {}
        for row in dt_meta.itertuples(index=False):
            schema_name = getattr(row, "schema_name", None) or getattr(row, "table_schema", None)
            table_name = getattr(row, "name", None) or getattr(row, "table_name", None)
            if not schema_name or not table_name:
                continue
            key = f"{schema_name}.{table_name}"
            target_lag = getattr(row, "target_lag", None)
            warehouse = getattr(row, "warehouse", None)
            refresh_mode = getattr(row, "refresh_mode", None)
            last_refresh = getattr(row, "last_refresh", None) or getattr(row, "last_refresh_time", None)
            dt_help[key] = (
                f"Target lag: {target_lag}\n"
                f"Warehouse: {warehouse}\n"
                f"Refresh mode: {refresh_mode}\n"
                f"Last refresh: {last_refresh}"
            )
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                label="SILVER Dynamic Table",
                value="ANALYTICS.SILVER.FCT_DISASTERS",
                help=dt_help.get("SILVER.FCT_DISASTERS", "Metadata unavailable"),
            )
        with col2:
            st.metric(
                label="GOLD Dynamic Table",
                value="ANALYTICS.GOLD.DISASTERS_BY_STATE",
                help=dt_help.get("GOLD.DISASTERS_BY_STATE", "Metadata unavailable"),
            )

        st.subheader("Consistency Results")
        if st.button("Refresh Results"):
            st.rerun()
        results = get_consistency_runs(
            window_start.isoformat(),
            window_end.isoformat(),
            status_filter or None,
            limit_rows=max_rows,
        ).df
        if results.empty:
            st.info("No consistency runs found for the selected filters.")
        else:
            display_cols = [
                "run_ts",
                "window_start_date",
                "window_end_date",
                "public_row_count",
                "public_distinct_id_count",
                "public_min_start_date",
                "public_max_start_date",
                "public_id_signature",
                "silver_row_count",
                "silver_distinct_id_count",
                "silver_min_start_date",
                "silver_max_start_date",
                "silver_id_signature",
                "silver_last_refresh_ts",
                "silver_target_lag",
                "gold_total_count",
                "gold_dim_row_count",
                "gold_last_refresh_ts",
                "gold_target_lag",
                "silver_vs_public_status",
                "silver_vs_public_reason",
                "gold_vs_silver_status",
                "gold_vs_silver_reason",
                "gold_vs_public_status",
                "gold_vs_public_reason",
                "notes",
            ]
            existing_cols = [col for col in display_cols if col in results.columns]
            st.dataframe(_format_year_columns(results[existing_cols]), use_container_width=True)


