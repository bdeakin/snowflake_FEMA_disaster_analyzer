import os
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

from src.snowflake_client import (
    call_cortex_complete,
    describe_relation,
    fetch_dataframe,
    show_views_in_schema,
)


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


@st.cache_data(ttl=300)
def load_view_options() -> List[str]:
    db, schema = _get_database_schema()
    views = show_views_in_schema(db, schema)
    if "NAME" in views.columns:
        names = views["NAME"].astype(str).tolist()
    elif "name" in views.columns:
        names = views["name"].astype(str).tolist()
    else:
        return []
    fema_names = sorted([name for name in names if name.startswith("FEMA_")])
    return fema_names if fema_names else sorted(names)
    return []


def _relation_columns(table_fqn: str) -> List[str]:
    db, schema = _get_database_schema()
    table = table_fqn.split(".")[-1]
    sql = (
        "SELECT COLUMN_NAME AS name "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_CATALOG = %(db)s AND TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s "
        "ORDER BY ORDINAL_POSITION"
    )
    df = fetch_dataframe(sql, params={"db": db, "schema": schema, "table": table})
    return df["NAME"].astype(str).tolist()


def _col(name: str, default: str) -> str:
    return _get_env(name, default)

def _relation_type() -> str:
    return _get_env("FEMA_RELATION_TYPE", "VIEW").upper()


TABLE_FQN = _table_fqn()
COL_STATE = _col("FEMA_COL_STATE", "STATE")
COL_INCIDENT_TYPE = _col("FEMA_COL_INCIDENT_TYPE", "INCIDENT_TYPE")
COL_DECLARATION_DATE = _col("FEMA_COL_DECLARATION_DATE", "DECLARATION_DATE")
COL_LAT = _col("FEMA_COL_LATITUDE", "LATITUDE")
COL_LON = _col("FEMA_COL_LONGITUDE", "LONGITUDE")
COL_DISASTER_ID = _col("FEMA_COL_DISASTER_ID", "FEMA_DECLARATION_ID")


try:
    relation_columns = _relation_columns(TABLE_FQN)
except Exception as exc:
    relation_columns = []


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
def resolve_date_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    candidates = [
        COL_DECLARATION_DATE,
        "DISASTER_DECLARATION_DATE",
        "DECLARATION_DATE",
        "DISASTER_BEGIN_DATE",
        "DISASTER_END_DATE",
        "DESIGNATED_DATE",
        "REQUESTED_DATE",
        "OBLIGATION_DATE",
        "DATE_OF_LOSS",
        "ENTRY_DATE",
        "UPDATE_DATE",
    ]
    for candidate in candidates:
        if candidate in relation_columns:
            return candidate
    return ""


@st.cache_data(ttl=300)
def resolve_state_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    candidates = [
        COL_STATE,
        "STATE",
        "STATE_GEO_ID",
        "STATE_CODE",
        "STATE_ABBR",
        "STATE_ABBREVIATION",
    ]
    for candidate in candidates:
        if candidate in relation_columns:
            return candidate
    return ""


@st.cache_data(ttl=300)
def resolve_incident_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    candidates = [
        COL_INCIDENT_TYPE,
        "INCIDENT_TYPE",
        "DISASTER_TYPE",
        "MISSION_ASSIGNMENT_TYPE",
        "FLOOD_TYPE",
    ]
    for candidate in candidates:
        if candidate in relation_columns:
            return candidate
    return ""


@st.cache_data(ttl=300)
def resolve_disaster_id_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    candidates = [
        COL_DISASTER_ID,
        "FEMA_DISASTER_DECLARATION_ID",
        "DISASTER_ID",
        "DISASTER_DECLARATION_RECORD_ID",
        "FEMA_RECORD_ID",
        "MISSION_ASSIGNMENT_ID",
        "NATIONAL_FLOOD_INSURANCE_PROGRAM_CLAIM_ID",
    ]
    for candidate in candidates:
        if candidate in relation_columns:
            return candidate
    return ""


@st.cache_data(ttl=300)
def resolve_lat_lon_columns(table_fqn: str) -> Tuple[str, str]:
    relation_columns = _relation_columns(table_fqn)
    lat_candidates = [COL_LAT, "LATITUDE", "LAT", "LAT_DECIMAL", "LATITUDE_DECIMAL"]
    lon_candidates = [COL_LON, "LONGITUDE", "LON", "LNG", "LONG", "LONG_DECIMAL", "LONGITUDE_DECIMAL"]
    lat = next((c for c in lat_candidates if c in relation_columns), "")
    lon = next((c for c in lon_candidates if c in relation_columns), "")
    return lat, lon

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

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = (
        "SELECT "
        f"{state_column if state_column else 'NULL'} AS state, "
        f"{incident_column if incident_column else 'NULL'} AS incident_type, "
        f"{date_column if date_column else 'NULL'} AS declaration_date, "
        f"{disaster_id_column if disaster_id_column else 'NULL'} AS disaster_id, "
        f"{lat_column if lat_column else 'NULL'} AS latitude, "
        f"{lon_column if lon_column else 'NULL'} AS longitude "
        f"FROM {table_fqn} WHERE {where_sql} "
        f"LIMIT {int(limit_rows)}"
    )
    return sql, params


def build_aggregate_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
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

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = (
        "SELECT "
        f"{lat_column} AS latitude, "
        f"{lon_column} AS longitude, "
        f"COUNT(DISTINCT {disaster_id_column}) AS count "
        f"FROM {table_fqn} WHERE {where_sql} "
        f"GROUP BY {lat_column}, {lon_column} "
        f"LIMIT {int(limit_rows)}"
    )
    return sql, params


def render_map(df: pd.DataFrame, zoom_level: float) -> None:
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

    if "count" in df.columns:
        fig = px.scatter_geo(
            df,
            lat="latitude",
            lon="longitude",
            size="count",
            size_max=20,
            projection="albers usa",
            hover_data={"count": True, "latitude": True, "longitude": True},
        )
    else:
        fig = px.scatter_geo(
            df,
            lat="latitude",
            lon="longitude",
            projection="albers usa",
            hover_data={
                "incident_type": True,
                "disaster_id": True,
                "declaration_date": True,
                "state": True,
            },
        )
        fig.update_traces(marker=dict(size=6, color="rgb(55,126,184)", opacity=0.7))

    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)


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


st.title("FEMA Disaster Analyzer")

with st.sidebar:
    st.header("Filters")
    limit_default = int(_get_env("FEMA_QUERY_LIMIT", "1000"))
    limit_rows = st.slider("Row limit", min_value=100, max_value=20000, value=limit_default)
    try:
        min_year, max_year = load_year_bounds(TABLE_FQN)
    except Exception as exc:
        st.error(f"Failed to read year range: {exc}")
        st.stop()
    if min_year == 0 and max_year == 0:
        st.warning("No date column found for this view. Year filter disabled.")
        year_range = (0, 0)
    else:
        default_low = max(min_year, 2023)
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
try:
    state_ids = [state_name_to_id[name] for name in states if name in state_name_to_id]
    zoom_level = 6.0
    sql, params = build_filtered_query(TABLE_FQN, state_ids, incidents, year_range, limit_rows)
    df = fetch_dataframe(sql, params=params)
    df.columns = [str(col).lower() for col in df.columns]
    if df.empty:
        st.info("No records returned for the selected filters.")
    else:
        render_map(df, zoom_level)
except Exception as exc:
    st.error(f"Query failed: {exc}")
    st.stop()

with st.expander("Data Preview", expanded=False):
    st.dataframe(df.head(100))


st.subheader("Ask in Natural Language (Cortex)")
nl_question = st.text_input("Ask a question about the FEMA data", value="")
show_sql = st.checkbox("Show generated SQL", value=True)

if nl_question:
    cortex_model = _get_env("SNOWFLAKE_CORTEX_MODEL", "snowflake-arctic")
    try:
        relation_type = _relation_type()
        schema_info = describe_relation(TABLE_FQN, relation_type)
        columns = ", ".join(schema_info["name"].astype(str).tolist())
        prompt = (
            "You are a helpful data assistant. Write a single SELECT SQL query for Snowflake.\n"
            f"Use only the table {TABLE_FQN}.\n"
            f"Available columns: {columns}.\n"
            f"Question: {nl_question}\n"
            "Return only SQL, no commentary."
        )
        raw_sql = call_cortex_complete(prompt=prompt, model=cortex_model)
        generated_sql = validate_generated_sql(raw_sql, TABLE_FQN, limit_rows)
        if show_sql:
            st.code(generated_sql)
        nl_df = fetch_dataframe(generated_sql)
        st.dataframe(nl_df)
    except Exception as exc:
        st.error(f"Cortex query failed: {exc}")
