import json
import os
import time
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_plotly_events import plotly_events
from dotenv import load_dotenv

from src.snowflake_client import fetch_dataframe, fetch_dataframe_plain


load_dotenv(dotenv_path=os.path.join("config", "secrets.env"))

st.set_page_config(page_title="FEMA Disaster Analyzer", layout="wide")

DEBUG_LOG_PATH = os.path.join(
    "/Users/bdeakin/Documents/Vibe Coding/snowflake_FEMA_disaster_analyzer",
    ".cursor",
    "debug.log",
)
DEBUG_SESSION_ID = "debug-session"
DEBUG_RUN_ID = "post-fix-11"


def _debug_log(message: str, data: Dict[str, object], location: str, hypothesis_id: str) -> None:
    payload = {
        "sessionId": DEBUG_SESSION_ID,
        "runId": DEBUG_RUN_ID,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(pd.Timestamp.utcnow().timestamp() * 1000),
    }
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(payload) + "\n")
    except Exception:
        pass


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _table_fqn() -> str:
    unified = _get_env("FEMA_JOINED_VIEW_FQN")
    if unified:
        return unified
    override = _get_env("FEMA_TABLE_FQN")
    if override:
        return override
    db = _get_env("SNOWFLAKE_DATABASE")
    schema = _get_env("SNOWFLAKE_SCHEMA")
    table = _get_env("FEMA_TABLE")
    if not (db and schema and table):
        raise ValueError(
            "Set FEMA_JOINED_VIEW_FQN or FEMA_TABLE_FQN or SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, FEMA_TABLE."
        )
    return f"{db}.{schema}.{table}"


def _get_database_schema() -> Tuple[str, str]:
    db = _get_env("SNOWFLAKE_DATABASE")
    schema = _get_env("SNOWFLAKE_SCHEMA")
    if not (db and schema):
        raise ValueError("Set SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA.")
    return db, schema


TABLE_FQN = _table_fqn()


def _build_in_clause(values: List[str], prefix: str) -> Tuple[str, Dict[str, str]]:
    params: Dict[str, str] = {}
    placeholders = []
    for idx, value in enumerate(values):
        key = f"{prefix}{idx}"
        params[key] = value
        placeholders.append(f"%({key})s")
    return f"({', '.join(placeholders)})", params


@st.cache_data(ttl=300)
def load_state_options(table_fqn: str) -> Tuple[List[str], Dict[str, str]]:
    state_id_column = "STATE_GEO_ID"
    state_name_column = "STATE_NAME"
    if not state_id_column or not state_name_column:
        return [], {}
    sql = (
        f"SELECT DISTINCT {state_name_column} AS state_name, "
        f"{state_id_column} AS state_id "
        f"FROM {table_fqn} WHERE {state_name_column} IS NOT NULL"
    )
    df = fetch_dataframe(sql)
    if df.empty:
        return [], {}
    df["STATE_NAME"] = df["STATE_NAME"].astype(str)
    df["STATE_ID"] = df["STATE_ID"].astype(str)
    mapping = dict(zip(df["STATE_NAME"], df["STATE_ID"]))
    options = sorted(mapping.keys())
    return options, mapping


@st.cache_data(ttl=300)
def load_incident_options(table_fqn: str) -> List[str]:
    incident_column = "INCIDENT_TYPE"
    if not incident_column:
        return []
    sql = (
        f"SELECT DISTINCT {incident_column} AS incident FROM {table_fqn} "
        f"WHERE {incident_column} IS NOT NULL"
    )
    df = fetch_dataframe(sql)
    return sorted(df["INCIDENT"].astype(str).tolist())


@st.cache_data(ttl=300)
def load_year_bounds(table_fqn: str) -> Tuple[int, int]:
    date_column = "DISASTER_DECLARATION_DATE"
    try:
        stats_sql = (
            "SELECT "
            f"COUNT(*) AS total_rows, "
            f"COUNT(TRY_TO_DATE(TO_VARCHAR({date_column}))) AS valid_dates, "
            f"COUNT_IF(TRY_TO_DATE(TO_VARCHAR({date_column})) IS NULL "
            f"AND TO_VARCHAR({date_column}) IS NOT NULL) AS invalid_dates "
            f"FROM {table_fqn}"
        )
        stats_df = fetch_dataframe(stats_sql)
        sample_sql = (
            "SELECT DISTINCT "
            f"TO_VARCHAR({date_column}) AS raw_value "
            f"FROM {table_fqn} "
            f"WHERE TRY_TO_DATE(TO_VARCHAR({date_column})) IS NULL "
            f"AND TO_VARCHAR({date_column}) IS NOT NULL "
            "LIMIT 5"
        )
        sample_df = fetch_dataframe(sample_sql)
    except Exception as exc:
        pass
    sql = (
        f"SELECT MIN(YEAR(TRY_TO_DATE(TO_VARCHAR({date_column})))) AS min_year, "
        f"MAX(YEAR(TRY_TO_DATE(TO_VARCHAR({date_column})))) AS max_year "
        f"FROM {table_fqn} "
        f"WHERE TRY_TO_DATE(TO_VARCHAR({date_column})) IS NOT NULL"
    )
    try:
        df = fetch_dataframe(sql)
    except Exception as exc:
        raise
    min_year = int(df.iloc[0]["MIN_YEAR"])
    max_year = int(df.iloc[0]["MAX_YEAR"])
    return min_year, max_year


def build_filtered_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
    bounds: Tuple[float, float, float, float] = None,
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    state_column = "STATE_GEO_ID"
    incident_column = "INCIDENT_TYPE"
    date_column = "DISASTER_DECLARATION_DATE"
    disaster_id_column = "DISASTER_ID"
    lat_column = "COUNTY_LATITUDE"
    lon_column = "COUNTY_LONGITUDE"

    if states and state_column:
        clause, clause_params = _build_in_clause(states, "state")
        where_clauses.append(f"{state_column} IN {clause}")
        params.update(clause_params)
    if incidents and incident_column:
        clause, clause_params = _build_in_clause(incidents, "incident")
        where_clauses.append(f"{incident_column} IN {clause}")
        params.update(clause_params)

    if date_column and year_range != (0, 0):
        where_clauses.append(
            f"YEAR(TO_DATE({date_column})) BETWEEN %(min_year)s AND %(max_year)s"
        )
        params["min_year"] = str(year_range[0])
        params["max_year"] = str(year_range[1])
    if bounds:
        min_lat, max_lat, min_lon, max_lon = bounds
        where_clauses.append(
            "COALESCE(COUNTY_LATITUDE, REGION_LATITUDE) BETWEEN %(min_lat)s AND %(max_lat)s"
        )
        where_clauses.append(
            "COALESCE(COUNTY_LONGITUDE, REGION_LONGITUDE) BETWEEN %(min_lon)s AND %(max_lon)s"
        )
        params["min_lat"] = float(min_lat)
        params["max_lat"] = float(max_lat)
        params["min_lon"] = float(min_lon)
        params["max_lon"] = float(max_lon)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = (
        "SELECT "
        f"{state_column if state_column else 'NULL'} AS state, "
        f"{incident_column if incident_column else 'NULL'} AS incident_type, "
        f"{date_column if date_column else 'NULL'} AS declaration_date, "
        f"{disaster_id_column if disaster_id_column else 'NULL'} AS disaster_id, "
        f"DISASTER_DECLARATION_NAME AS disaster_declaration_name, "
        f"DISASTER_DECLARATION_TYPE AS disaster_declaration_type, "
        f"FEMA_REGION_NAME AS fema_region_name, "
        f"DESIGNATED_AREAS AS designated_areas, "
        f"DECLARED_PROGRAMS AS declared_programs, "
        f"FEMA_DISASTER_DECLARATION_ID AS fema_disaster_declaration_id, "
        f"DISASTER_BEGIN_DATE AS disaster_begin_date, "
        f"DISASTER_END_DATE AS disaster_end_date, "
        f"STATE_GEO_ID AS state_geo_id, "
        f"COUNTY_GEO_ID AS county_geo_id, "
        f"COALESCE({lat_column}, REGION_LATITUDE) AS latitude, "
        f"COALESCE({lon_column}, REGION_LONGITUDE) AS longitude "
        f"FROM {table_fqn} WHERE {where_sql} "
        f"LIMIT {int(limit_rows)}"
    )
    return sql, params


def build_agg_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
    grid_large_degrees: float,
    grid_small_degrees: float,
    metro_threshold: int,
    bounds: Tuple[float, float, float, float] = None,
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    state_column = "STATE_GEO_ID"
    incident_column = "INCIDENT_TYPE"
    year_column = "DISASTER_YEAR"
    if states and state_column:
        clause, clause_params = _build_in_clause(states, "state")
        where_clauses.append(f"{state_column} IN {clause}")
        params.update(clause_params)
    if incidents and incident_column:
        clause, clause_params = _build_in_clause(incidents, "incident")
        where_clauses.append(f"{incident_column} IN {clause}")
        params.update(clause_params)
    if year_range != (0, 0):
        where_clauses.append(f"{year_column} BETWEEN %(min_year)s AND %(max_year)s")
        params["min_year"] = str(year_range[0])
        params["max_year"] = str(year_range[1])
    if bounds:
        min_lat, max_lat, min_lon, max_lon = bounds
        where_clauses.append("AVG_LATITUDE BETWEEN %(min_lat)s AND %(max_lat)s")
        where_clauses.append("AVG_LONGITUDE BETWEEN %(min_lon)s AND %(max_lon)s")
        params["min_lat"] = float(min_lat)
        params["max_lat"] = float(max_lat)
        params["min_lon"] = float(min_lon)
        params["max_lon"] = float(max_lon)
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = f"""
WITH base AS (
    SELECT
        AVG_LATITUDE AS lat,
        AVG_LONGITUDE AS lon,
        DISASTER_COUNT AS count
    FROM {table_fqn}
    WHERE {where_sql}
),
large AS (
    SELECT
        ROUND(lat / {grid_large_degrees}) * {grid_large_degrees} AS lat_l,
        ROUND(lon / {grid_large_degrees}) * {grid_large_degrees} AS lon_l,
        SUM(count) AS count_l
    FROM base
    GROUP BY lat_l, lon_l
),
small AS (
    SELECT
        ROUND(lat / {grid_small_degrees}) * {grid_small_degrees} AS lat_s,
        ROUND(lon / {grid_small_degrees}) * {grid_small_degrees} AS lon_s,
        ROUND(lat / {grid_large_degrees}) * {grid_large_degrees} AS lat_l,
        ROUND(lon / {grid_large_degrees}) * {grid_large_degrees} AS lon_l,
        SUM(count) AS count_s
    FROM base
    GROUP BY lat_s, lon_s, lat_l, lon_l
)
SELECT lat_l AS latitude, lon_l AS longitude, count_l AS count,
       'large' AS grid_level, {grid_large_degrees} AS grid_size
FROM large
WHERE count_l >= {metro_threshold}
UNION ALL
SELECT s.lat_s AS latitude, s.lon_s AS longitude, s.count_s AS count,
       'small' AS grid_level, {grid_small_degrees} AS grid_size
FROM small s
JOIN large l ON s.lat_l = l.lat_l AND s.lon_l = l.lon_l
WHERE l.count_l < {metro_threshold}
LIMIT {int(limit_rows)}
"""
    return sql, params


def build_hover_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    bounds: Tuple[float, float, float, float],
    grid_large_degrees: float,
    grid_small_degrees: float,
    metro_threshold: int,
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    if states:
        clause, clause_params = _build_in_clause(states, "state")
        where_clauses.append(f"STATE_GEO_ID IN {clause}")
        params.update(clause_params)
    if incidents:
        clause, clause_params = _build_in_clause(incidents, "incident")
        where_clauses.append(f"INCIDENT_TYPE IN {clause}")
        params.update(clause_params)
    if year_range != (0, 0):
        where_clauses.append(
            "YEAR(TO_DATE(DISASTER_DECLARATION_DATE)) BETWEEN %(min_year)s AND %(max_year)s"
        )
        params["min_year"] = str(year_range[0])
        params["max_year"] = str(year_range[1])
    if bounds:
        min_lat, max_lat, min_lon, max_lon = bounds
        where_clauses.append(
            "COALESCE(COUNTY_LATITUDE, REGION_LATITUDE) BETWEEN %(min_lat)s AND %(max_lat)s"
        )
        where_clauses.append(
            "COALESCE(COUNTY_LONGITUDE, REGION_LONGITUDE) BETWEEN %(min_lon)s AND %(max_lon)s"
        )
        params["min_lat"] = float(min_lat)
        params["max_lat"] = float(max_lat)
        params["min_lon"] = float(min_lon)
        params["max_lon"] = float(max_lon)
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = f"""
WITH detail AS (
    SELECT
        COALESCE(COUNTY_LATITUDE, REGION_LATITUDE) AS lat,
        COALESCE(COUNTY_LONGITUDE, REGION_LONGITUDE) AS lon,
        INCIDENT_TYPE,
        DISASTER_DECLARATION_NAME
    FROM {table_fqn}
    WHERE {where_sql}
),
large AS (
    SELECT
        ROUND(lat / {grid_large_degrees}) * {grid_large_degrees} AS lat_l,
        ROUND(lon / {grid_large_degrees}) * {grid_large_degrees} AS lon_l,
        COUNT(*) AS cnt
    FROM detail
    GROUP BY lat_l, lon_l
),
assigned AS (
    SELECT
        CASE WHEN l.cnt >= {metro_threshold} THEN l.lat_l ELSE ROUND(d.lat / {grid_small_degrees}) * {grid_small_degrees} END AS clat,
        CASE WHEN l.cnt >= {metro_threshold} THEN l.lon_l ELSE ROUND(d.lon / {grid_small_degrees}) * {grid_small_degrees} END AS clon,
        d.INCIDENT_TYPE,
        d.DISASTER_DECLARATION_NAME
    FROM detail d
    JOIN large l
      ON ROUND(d.lat / {grid_large_degrees}) * {grid_large_degrees} = l.lat_l
     AND ROUND(d.lon / {grid_large_degrees}) * {grid_large_degrees} = l.lon_l
),
incident_counts AS (
    SELECT clat, clon, INCIDENT_TYPE, COUNT(*) AS cnt
    FROM assigned
    GROUP BY clat, clon, INCIDENT_TYPE
),
incident_summary AS (
    SELECT
        clat,
        clon,
        LISTAGG(INCIDENT_TYPE || ': ' || cnt, ', ') WITHIN GROUP (ORDER BY cnt DESC) AS incident_summary,
        SUM(cnt) AS total_count
    FROM incident_counts
    GROUP BY clat, clon
),
names AS (
    SELECT
        clat,
        clon,
        LISTAGG(DISTINCT DISASTER_DECLARATION_NAME, ', ') WITHIN GROUP (ORDER BY DISASTER_DECLARATION_NAME) AS name_summary,
        COUNT(DISTINCT DISASTER_DECLARATION_NAME) AS name_count
    FROM assigned
    GROUP BY clat, clon
)
SELECT
    i.clat AS latitude,
    i.clon AS longitude,
    i.incident_summary,
    CASE WHEN i.total_count < 5 THEN n.name_summary ELSE NULL END AS name_summary,
    i.total_count AS count
FROM incident_summary i
LEFT JOIN names n ON i.clat = n.clat AND i.clon = n.clon
"""
    return sql, params


def build_map_figure(df: pd.DataFrame, is_agg: bool) -> px.scatter_mapbox:
    df = df.copy()
    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    if "count" in df.columns:
        df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    if "latitude" in df.columns and "longitude" in df.columns:
        df = df.dropna(subset=["latitude", "longitude"])
    if df.empty:
        st.info("No records returned for the selected filters.")
        return

    mapbox_token = _get_env("MAPBOX_ACCESS_TOKEN", "").strip()
    mapbox_style = "open-street-map"
    if mapbox_token:
        px.set_mapbox_access_token(mapbox_token)

    center_lat = float(df["latitude"].mean()) if "latitude" in df.columns else 37.8
    center_lon = float(df["longitude"].mean()) if "longitude" in df.columns else -96.0
    if pd.isna(center_lat) or pd.isna(center_lon):
        center_lat, center_lon = 37.8, -96.0

    if is_agg:
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            size="count",
            size_max=40,
            zoom=3,
            center={"lat": center_lat, "lon": center_lon},
            height=600,
            color="count",
            color_continuous_scale=["#9a9a9a", "#ff0000"],
            hover_data={
                "count": True,
                "incident_summary": True,
                "name_summary": True,
            },
        )
    else:
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            zoom=3,
            center={"lat": center_lat, "lon": center_lon},
            height=600,
            hover_data={
                "incident_type": True,
                "disaster_id": True,
                "declaration_date": True,
                "state": True,
            },
        )
        fig.update_traces(
            marker=dict(size=8, color="rgba(55,126,184,0.6)"),
        )

    fig.update_layout(mapbox_style=mapbox_style, margin=dict(l=0, r=0, t=0, b=0))
    return fig


def validate_generated_sql(sql: str, table_fqn: str, limit_rows: int) -> str:
    cleaned = " ".join(sql.strip().split())
    lower = cleaned.lower()

    if not lower.startswith("select"):
        raise ValueError("Only SELECT statements are allowed.")
    forbidden = [" insert ", " update ", " delete ", " merge ", " drop ", " alter ", " create "]
    if any(word in lower for word in forbidden):
        raise ValueError("Only read-only queries are allowed.")
    if ";" in cleaned:
        raise ValueError("Semicolons are not allowed.")
    if table_fqn.lower() not in lower:
        raise ValueError("Query must reference the configured FEMA table.")
    if " limit " not in lower:
        cleaned = f"{cleaned} LIMIT {int(limit_rows)}"
    return cleaned


def _extract_bounds(relayout: Dict[str, object]) -> Tuple[float, float, float, float]:
    coords = None
    if "mapbox._derived.coordinates" in relayout:
        coords = relayout.get("mapbox._derived.coordinates")
    elif isinstance(relayout.get("mapbox._derived"), dict):
        coords = relayout.get("mapbox._derived", {}).get("coordinates")
    if not coords:
        return None
    lats = [pt[1] for pt in coords]
    lons = [pt[0] for pt in coords]
    return (min(lats), max(lats), min(lons), max(lons))


def _cluster_bounds(latitude: float, longitude: float, grid_size: float) -> Tuple[float, float, float, float]:
    half = grid_size / 2.0
    return (latitude - half, latitude + half, longitude - half, longitude + half)


def _extract_click(events: object, df: pd.DataFrame) -> Dict[str, float]:
    if not events or df.empty:
        return None
    event_list = events if isinstance(events, list) else [events]
    for event in event_list:
        if not isinstance(event, dict):
            continue
        if "pointIndex" in event:
            idx = event.get("pointIndex")
        elif "pointNumber" in event:
            idx = event.get("pointNumber")
        else:
            continue
        try:
            row = df.iloc[int(idx)]
        except Exception:
            continue
        grid_size = row.get("grid_size") if "grid_size" in row else None
        return {
            "latitude": float(row.get("latitude", event.get("lat", 0.0))),
            "longitude": float(row.get("longitude", event.get("lon", 0.0))),
            "grid_size": float(grid_size) if grid_size else 0.0,
            "count": int(row.get("count", 0)),
        }
    return None


st.title("FEMA Disaster Analyzer")

with st.sidebar:
    st.header("Filters")
    limit_rows = 1000
    try:
        min_year, max_year = load_year_bounds(TABLE_FQN)
    except Exception as exc:
        st.error(f"Failed to read year range: {exc}")
        st.stop()
    if min_year == 0 and max_year == 0:
        st.warning("No date column found for this view. Year filter disabled.")
        year_range = (0, 0)
    else:
        default_low = max(min_year, 2000)
        default_high = min(max_year, 2025)
        if default_low > default_high:
            default_low, default_high = min_year, max_year
        year_range = st.slider(
            "Year range",
            min_value=min_year,
            max_value=max_year,
            value=(default_low, default_high),
        )

    try:
        state_options, state_name_to_id = load_state_options(TABLE_FQN)
        incident_options = load_incident_options(TABLE_FQN)
    except Exception as exc:
        st.error(f"Failed to read filter options: {exc}")
        st.stop()

    states = st.multiselect("State", options=state_options)
    incidents = st.multiselect("Incident type", options=incident_options)

st.subheader("Map")
use_agg = True
render_status = st.progress(0, text="Preparing query...")
query_start = time.time()
try:
    state_ids = [state_name_to_id[name] for name in states if name in state_name_to_id]
    bounds = st.session_state.get("map_bounds", (24.0, 50.0, -125.0, -66.0))
    grid_large_degrees = 3.6
    grid_small_degrees = 1.45
    metro_threshold = 50
    filters_key = (tuple(state_ids), tuple(incidents), year_range)
    if st.session_state.get("filters_key") != filters_key:
        st.session_state["filters_key"] = filters_key
        st.session_state.pop("selected_cluster", None)
        st.session_state.pop("cluster_detail_df", None)
    # #region agent log
    _debug_log(
        "map query prep",
        {
            "table_fqn": TABLE_FQN,
            "bounds": bounds,
            "state_ids_count": len(state_ids),
            "incident_count": len(incidents),
            "year_range": year_range,
            "switching_disabled": True,
        },
        "app.py:map",
        "OCSP1",
    )
    # #endregion agent log
    view_label = "Aggregated"
    st.caption("View mode: Aggregated (click a cluster for details)")
    render_status.progress(40, text="Loading aggregated data...")
    agg_sql, agg_params = build_agg_query(
        "FEMA_ANALYTICS.PUBLIC.FEMA_DISASTER_AGG_VIEW",
        state_ids,
        incidents,
        year_range,
        20000,
        grid_large_degrees,
        grid_small_degrees,
        metro_threshold,
        bounds=bounds,
    )
    df = fetch_dataframe_plain(agg_sql, params=agg_params)
    hover_sql, hover_params = build_hover_query(
        TABLE_FQN,
        state_ids,
        incidents,
        year_range,
        bounds,
        grid_large_degrees,
        grid_small_degrees,
        metro_threshold,
    )
    hover_df = fetch_dataframe_plain(hover_sql, params=hover_params)
    df.columns = [str(col).lower() for col in df.columns]
    if not hover_df.empty:
        hover_df.columns = [str(col).lower() for col in hover_df.columns]
        df = df.merge(hover_df, on=["latitude", "longitude", "count"], how="left")
    if df.empty:
        st.info("No records returned for the selected filters.")
    else:
        render_status.progress(75, text="Rendering map...")
        df = df.reset_index(drop=True)
        fig = build_map_figure(df, use_agg)
        events = None
        try:
            events = plotly_events(
                fig,
                click_event=True,
                hover_event=False,
                select_event=False,
                override_height=600,
                override_width="100%",
                key="mapbox-plot",
                relayout_event=True,
            )
        except TypeError:
            events = plotly_events(
                fig,
                click_event=True,
                hover_event=False,
                select_event=False,
                override_height=600,
                override_width="100%",
                key="mapbox-plot",
            )
        if events:
            relayout = None
            if isinstance(events, dict) and "relayoutData" in events:
                relayout = events["relayoutData"]
            elif isinstance(events, list):
                for event in events:
                    if isinstance(event, dict) and "relayoutData" in event:
                        relayout = event["relayoutData"]
                        break
            if relayout:
                bounds_from_event = _extract_bounds(relayout)
                if bounds_from_event:
                    st.session_state["map_bounds"] = bounds_from_event
            clicked = _extract_click(events, df)
            if clicked:
                if not clicked.get("grid_size"):
                    clicked["grid_size"] = grid_small_degrees
                if clicked != st.session_state.get("selected_cluster"):
                    st.session_state["selected_cluster"] = clicked
                    st.session_state["cluster_detail_df"] = None
        if st.session_state.get("selected_cluster") and st.session_state.get("cluster_detail_df") is None:
            render_status.progress(90, text="Loading cluster details...")
            selected = st.session_state["selected_cluster"]
            cluster_bounds = _cluster_bounds(
                selected["latitude"], selected["longitude"], selected["grid_size"]
            )
            detail_sql, detail_params = build_filtered_query(
                TABLE_FQN,
                state_ids,
                incidents,
                year_range,
                500,
                bounds=cluster_bounds,
            )
            detail_df = fetch_dataframe_plain(detail_sql, params=detail_params)
            detail_df.columns = [str(col).lower() for col in detail_df.columns]
            st.session_state["cluster_detail_df"] = detail_df
    render_status.progress(100, text="Render complete.")
except Exception as exc:
    if "certificate is revoked" in str(exc).lower():
        st.error(
            "Query failed due to OCSP certificate validation. "
            "Set SNOWFLAKE_OCSP_FAIL_OPEN=true in config/secrets.env. "
            "If it still fails, set SNOWFLAKE_DISABLE_OCSP_CHECKS=true "
            "and restart the app."
        )
    else:
        st.error(f"Query failed: {exc}")
    st.stop()
finally:
    elapsed = time.time() - query_start
    st.caption(f"Query time: {elapsed:.2f}s")

with st.expander("Data Preview", expanded=False):
    if use_agg:
        detail_df = st.session_state.get("cluster_detail_df")
        if detail_df is None:
            st.info("Click a cluster to load detailed records for that area.")
        elif detail_df.empty:
            st.info("No detailed records found for the selected cluster.")
        else:
            preview_columns = [
                "state_name",
                "incident_type",
                "disaster_declaration_name",
                "disaster_declaration_type",
                "fema_region_name",
                "designated_areas",
                "declared_programs",
                "fema_disaster_declaration_id",
                "disaster_begin_date",
                "disaster_end_date",
                "state_geo_id",
                "county_geo_id",
                "disaster_id",
                "disaster_declaration_date",
            ]
            available = [col for col in preview_columns if col in detail_df.columns]
            st.dataframe(detail_df[available].head(100))
    else:
        preview_columns = [
            "state_name",
            "incident_type",
            "disaster_declaration_name",
            "disaster_declaration_type",
            "fema_region_name",
            "designated_areas",
            "declared_programs",
            "fema_disaster_declaration_id",
            "disaster_begin_date",
            "disaster_end_date",
            "state_geo_id",
            "county_geo_id",
            "disaster_id",
            "disaster_declaration_date",
        ]
        available = [col for col in preview_columns if col in df.columns]
        st.dataframe(df[available].head(100))


