import os
from typing import Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
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
    # #region agent log
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(f"{pd.Series(payload).to_json()}\n")
    except Exception:
        pass
    # #endregion agent log


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _table_fqn() -> str:
    override = _get_env("FEMA_TABLE_FQN")
    if override:
        return override
    db = _get_env("SNOWFLAKE_DATABASE")
    schema = _get_env("SNOWFLAKE_SCHEMA")
    table = _get_env("FEMA_TABLE")
    if not (db and schema and table):
        raise ValueError(
            "Set FEMA_TABLE_FQN or SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, FEMA_TABLE."
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
    # #region agent log
    _debug_log(
        "load_view_options",
        {
            "database": db,
            "schema": schema,
            "columns": views.columns.tolist(),
            "count": int(len(views)),
            "sample": (
                views["NAME"].astype(str).tolist()[:10]
                if "NAME" in views.columns
                else (views["name"].astype(str).tolist()[:10] if "name" in views.columns else [])
            ),
        },
        "app.py:load_view_options",
        "C",
    )
    # #endregion agent log
    if "NAME" in views.columns:
        names = views["NAME"].astype(str).tolist()
    elif "name" in views.columns:
        names = views["name"].astype(str).tolist()
    else:
        # #region agent log
        _debug_log(
            "load_view_options missing column",
            {"columns": views.columns.tolist()},
            "app.py:load_view_options",
            "C",
        )
        # #endregion agent log
        return []
    fema_names = sorted([name for name in names if name.startswith("FEMA_")])
    # #region agent log
    _debug_log(
        "load_view_options filtered",
        {"count_total": len(names), "count_fema": len(fema_names)},
        "app.py:load_view_options",
        "C",
    )
    # #endregion agent log
    return fema_names if fema_names else sorted(names)
    # #region agent log
    _debug_log(
        "load_view_options missing column",
        {"columns": views.columns.tolist()},
        "app.py:load_view_options",
        "C",
    )
    # #endregion agent log
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

# #region agent log
_debug_log(
    "column config",
    {
        "col_state": COL_STATE,
        "col_incident_type": COL_INCIDENT_TYPE,
        "col_declaration_date": COL_DECLARATION_DATE,
        "col_lat": COL_LAT,
        "col_lon": COL_LON,
        "col_disaster_id": COL_DISASTER_ID,
    },
    "app.py:column_config",
    "B",
)
# #endregion agent log

try:
    relation_columns = _relation_columns(TABLE_FQN)
    # #region agent log
    _debug_log(
        "default relation columns info_schema",
        {"table_fqn": TABLE_FQN, "columns": relation_columns[:120]},
        "app.py:column_config",
        "A",
    )
    # #endregion agent log
except Exception as exc:
    # #region agent log
    _debug_log(
        "default relation columns error",
        {"table_fqn": TABLE_FQN, "error": str(exc)},
        "app.py:column_config",
        "A",
    )
    # #endregion agent log


def _build_in_clause(values: List[str], prefix: str) -> Tuple[str, Dict[str, str]]:
    params: Dict[str, str] = {}
    placeholders = []
    for idx, value in enumerate(values):
        key = f"{prefix}{idx}"
        params[key] = value
        placeholders.append(f"%({key})s")
    return f"({', '.join(placeholders)})", params


@st.cache_data(ttl=300)
def load_state_options(table_fqn: str) -> List[str]:
    state_column = resolve_state_column(table_fqn)
    if not state_column:
        # #region agent log
        _debug_log(
            "load_state_options no column",
            {"table_fqn": table_fqn},
            "app.py:load_state_options",
            "E",
        )
        # #endregion agent log
        return []
    sql = f"SELECT DISTINCT {state_column} AS state FROM {table_fqn} WHERE {state_column} IS NOT NULL"
    df = fetch_dataframe(sql)
    return sorted(df["STATE"].astype(str).tolist())


@st.cache_data(ttl=300)
def load_incident_options(table_fqn: str) -> List[str]:
    incident_column = resolve_incident_column(table_fqn)
    if not incident_column:
        # #region agent log
        _debug_log(
            "load_incident_options no column",
            {"table_fqn": table_fqn},
            "app.py:load_incident_options",
            "F",
        )
        # #endregion agent log
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
    # #region agent log
    _debug_log(
        "resolve_date_column columns",
        {
            "table_fqn": table_fqn,
            "count": len(relation_columns),
            "columns": relation_columns[:120],
        },
        "app.py:resolve_date_column",
        "A",
    )
    # #endregion agent log
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
            # #region agent log
            _debug_log(
                "resolve_date_column match",
                {"resolved": candidate},
                "app.py:resolve_date_column",
                "A",
            )
            # #endregion agent log
            return candidate
    # #region agent log
    _debug_log(
        "resolve_date_column none",
        {"table_fqn": table_fqn},
        "app.py:resolve_date_column",
        "A",
    )
    # #endregion agent log
    return ""


@st.cache_data(ttl=300)
def resolve_state_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    # #region agent log
    _debug_log(
        "resolve_state_column columns",
        {
            "table_fqn": table_fqn,
            "count": len(relation_columns),
            "columns": relation_columns[:120],
        },
        "app.py:resolve_state_column",
        "E",
    )
    # #endregion agent log
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
            # #region agent log
            _debug_log(
                "resolve_state_column match",
                {"resolved": candidate},
                "app.py:resolve_state_column",
                "E",
            )
            # #endregion agent log
            return candidate
    # #region agent log
    _debug_log(
        "resolve_state_column none",
        {"table_fqn": table_fqn},
        "app.py:resolve_state_column",
        "E",
    )
    # #endregion agent log
    return ""


@st.cache_data(ttl=300)
def resolve_incident_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    # #region agent log
    _debug_log(
        "resolve_incident_column columns",
        {
            "table_fqn": table_fqn,
            "count": len(relation_columns),
            "columns": relation_columns[:120],
        },
        "app.py:resolve_incident_column",
        "F",
    )
    # #endregion agent log
    candidates = [
        COL_INCIDENT_TYPE,
        "INCIDENT_TYPE",
        "DISASTER_TYPE",
        "MISSION_ASSIGNMENT_TYPE",
        "FLOOD_TYPE",
    ]
    for candidate in candidates:
        if candidate in relation_columns:
            # #region agent log
            _debug_log(
                "resolve_incident_column match",
                {"resolved": candidate},
                "app.py:resolve_incident_column",
                "F",
            )
            # #endregion agent log
            return candidate
    # #region agent log
    _debug_log(
        "resolve_incident_column none",
        {"table_fqn": table_fqn},
        "app.py:resolve_incident_column",
        "F",
    )
    # #endregion agent log
    return ""


@st.cache_data(ttl=300)
def resolve_disaster_id_column(table_fqn: str) -> str:
    relation_columns = _relation_columns(table_fqn)
    # #region agent log
    _debug_log(
        "resolve_disaster_id_column columns",
        {
            "table_fqn": table_fqn,
            "count": len(relation_columns),
            "columns": relation_columns[:120],
        },
        "app.py:resolve_disaster_id_column",
        "G",
    )
    # #endregion agent log
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
            # #region agent log
            _debug_log(
                "resolve_disaster_id_column match",
                {"resolved": candidate},
                "app.py:resolve_disaster_id_column",
                "G",
            )
            # #endregion agent log
            return candidate
    # #region agent log
    _debug_log(
        "resolve_disaster_id_column none",
        {"table_fqn": table_fqn},
        "app.py:resolve_disaster_id_column",
        "G",
    )
    # #endregion agent log
    return ""


@st.cache_data(ttl=300)
def resolve_lat_lon_columns(table_fqn: str) -> Tuple[str, str]:
    relation_columns = _relation_columns(table_fqn)
    # #region agent log
    _debug_log(
        "resolve_lat_lon columns",
        {
            "table_fqn": table_fqn,
            "count": len(relation_columns),
            "columns": relation_columns[:120],
        },
        "app.py:resolve_lat_lon_columns",
        "H",
    )
    # #endregion agent log
    lat_candidates = [COL_LAT, "LATITUDE", "LAT", "LAT_DECIMAL", "LATITUDE_DECIMAL"]
    lon_candidates = [COL_LON, "LONGITUDE", "LON", "LNG", "LONG", "LONG_DECIMAL", "LONGITUDE_DECIMAL"]
    lat = next((c for c in lat_candidates if c in relation_columns), "")
    lon = next((c for c in lon_candidates if c in relation_columns), "")
    # #region agent log
    _debug_log(
        "resolve_lat_lon match",
        {"lat": lat, "lon": lon},
        "app.py:resolve_lat_lon_columns",
        "H",
    )
    # #endregion agent log
    return lat, lon

@st.cache_data(ttl=300)
def load_year_bounds(table_fqn: str) -> Tuple[int, int]:
    date_column = resolve_date_column(table_fqn)
    if not date_column:
        return (0, 0)
    # #region agent log
    _debug_log(
        "resolved date column",
        {
            "table_fqn": table_fqn,
            "configured": COL_DECLARATION_DATE,
            "resolved": date_column,
            "has_configured": date_column == COL_DECLARATION_DATE,
        },
        "app.py:load_year_bounds",
        "A",
    )
    # #endregion agent log
    sql = (
        f"SELECT MIN(YEAR(TO_DATE({date_column}))) AS min_year, "
        f"MAX(YEAR(TO_DATE({date_column}))) AS max_year "
        f"FROM {table_fqn} WHERE {date_column} IS NOT NULL"
    )
    # #region agent log
    _debug_log(
        "load_year_bounds query",
        {"table_fqn": table_fqn, "declaration_col": date_column, "sql": sql},
        "app.py:load_year_bounds",
        "A",
    )
    # #endregion agent log
    try:
        df = fetch_dataframe(sql)
    except Exception as exc:
        # #region agent log
        _debug_log(
            "load_year_bounds error",
            {"error": str(exc)},
            "app.py:load_year_bounds",
            "A",
        )
        # #endregion agent log
        raise
    # #region agent log
    _debug_log(
        "load_year_bounds result",
        {"min_year": df.iloc[0]["MIN_YEAR"], "max_year": df.iloc[0]["MAX_YEAR"]},
        "app.py:load_year_bounds",
        "A",
    )
    # #endregion agent log
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
    state_column = resolve_state_column(table_fqn)
    incident_column = resolve_incident_column(table_fqn)
    date_column = resolve_date_column(table_fqn)
    disaster_id_column = resolve_disaster_id_column(table_fqn)
    lat_column, lon_column = resolve_lat_lon_columns(table_fqn)
    # #region agent log
    _debug_log(
        "build_filtered_query columns",
        {
            "table_fqn": table_fqn,
            "state_column": state_column,
            "incident_column": incident_column,
            "date_column": date_column,
            "disaster_id_column": disaster_id_column,
            "lat_column": lat_column,
            "lon_column": lon_column,
        },
        "app.py:build_filtered_query",
        "E",
    )
    # #endregion agent log

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


def render_map(df: pd.DataFrame) -> None:
    # #region agent log
    _debug_log(
        "render_map columns",
        {"columns": df.columns.tolist()},
        "app.py:render_map",
        "I",
    )
    # #endregion agent log
    has_geo = "latitude" in df.columns and "longitude" in df.columns
    has_points = has_geo and df["latitude"].notna().any() and df["longitude"].notna().any()

    if has_points:
        fig = px.scatter_geo(
            df,
            lat="latitude",
            lon="longitude",
            color="incident_type",
            hover_name="disaster_id",
            hover_data=["state", "declaration_date"],
            scope="usa",
            title="FEMA Disaster Declarations (Points)",
        )
    else:
        agg = df.groupby("state", dropna=True).size().reset_index(name="count")
        fig = px.choropleth(
            agg,
            locations="state",
            locationmode="USA-states",
            color="count",
            scope="usa",
            title="FEMA Disaster Declarations (Counts by State)",
            color_continuous_scale="Reds",
        )
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
    try:
        view_options = load_view_options()
    except Exception:
        view_options = []
    if view_options:
        default_index = 0
        if "FEMA_DISASTER_DECLARATION_INDEX" in view_options:
            default_index = view_options.index("FEMA_DISASTER_DECLARATION_INDEX")
        selected_view = st.selectbox("FEMA view", options=view_options, index=default_index)
        TABLE_FQN = f"{_get_database_schema()[0]}.{_get_database_schema()[1]}.{selected_view}"
        # #region agent log
        _debug_log(
            "selected view",
            {"selected_view": selected_view, "table_fqn": TABLE_FQN},
            "app.py:sidebar",
            "C",
        )
        # #endregion agent log
        try:
            relation_columns = _relation_columns(TABLE_FQN)
            # #region agent log
            _debug_log(
                "selected view columns info_schema",
                {"columns": relation_columns[:120]},
                "app.py:sidebar",
                "A",
            )
            # #endregion agent log
        except Exception as exc:
            # #region agent log
            _debug_log(
                "relation_columns error",
                {"error": str(exc), "table_fqn": TABLE_FQN},
                "app.py:sidebar",
                "A",
            )
            # #endregion agent log
    else:
        # #region agent log
        _debug_log(
            "no view options",
            {"table_fqn": TABLE_FQN},
            "app.py:sidebar",
            "C",
        )
        # #endregion agent log
        try:
            relation_type = _relation_type()
            schema_info = describe_relation(TABLE_FQN, relation_type)
            # #region agent log
            _debug_log(
                "selected view columns",
                {"relation_type": relation_type, "columns": schema_info["name"].astype(str).tolist()[:80]},
                "app.py:sidebar",
                "C",
            )
            # #endregion agent log
        except Exception as exc:
            # #region agent log
            _debug_log(
                "selected view describe_relation error",
                {"error": str(exc), "relation_type": _relation_type()},
                "app.py:sidebar",
                "C",
            )
            # #endregion agent log
    limit_default = int(_get_env("FEMA_QUERY_LIMIT", "5000"))
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
        year_range = st.slider(
            "Year range",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
        )

    try:
        state_options = load_state_options(TABLE_FQN)
        incident_options = load_incident_options(TABLE_FQN)
    except Exception as exc:
        st.error(f"Failed to read filter options: {exc}")
        st.stop()

    states = st.multiselect("State", options=state_options)
    incidents = st.multiselect("Incident type", options=incident_options)

st.subheader("Map")
try:
    sql, params = build_filtered_query(TABLE_FQN, states, incidents, year_range, limit_rows)
    # #region agent log
    _debug_log(
        "map query",
        {"sql": sql, "params": params},
        "app.py:map",
        "I",
    )
    # #endregion agent log
    df = fetch_dataframe(sql, params=params)
    # #region agent log
    _debug_log(
        "map dataframe",
        {"columns": df.columns.tolist(), "rows": int(len(df))},
        "app.py:map",
        "I",
    )
    # #endregion agent log
    df.columns = [str(col).lower() for col in df.columns]
    # #region agent log
    _debug_log(
        "map dataframe normalized",
        {"columns": df.columns.tolist()},
        "app.py:map",
        "I",
    )
    # #endregion agent log
    if df.empty:
        st.info("No records returned for the selected filters.")
    else:
        render_map(df)
except Exception as exc:
    # #region agent log
    _debug_log(
        "map error",
        {"error": str(exc)},
        "app.py:map",
        "I",
    )
    # #endregion agent log
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
        # #region agent log
        _debug_log(
            "nlq request",
            {
                "model": cortex_model,
                "question": nl_question,
                "table_fqn": TABLE_FQN,
            },
            "app.py:nlq",
            "D",
        )
        # #endregion agent log
        relation_type = _relation_type()
        # #region agent log
        _debug_log(
            "describe_relation start",
            {"table_fqn": TABLE_FQN, "relation_type": relation_type},
            "app.py:nlq",
            "D",
        )
        # #endregion agent log
        schema_info = describe_relation(TABLE_FQN, relation_type)
        # #region agent log
        _debug_log(
            "describe_relation columns",
            {"columns": schema_info["name"].astype(str).tolist()[:50]},
            "app.py:nlq",
            "D",
        )
        # #endregion agent log
        columns = ", ".join(schema_info["name"].astype(str).tolist())
        prompt = (
            "You are a helpful data assistant. Write a single SELECT SQL query for Snowflake.\n"
            f"Use only the table {TABLE_FQN}.\n"
            f"Available columns: {columns}.\n"
            f"Question: {nl_question}\n"
            "Return only SQL, no commentary."
        )
        # #region agent log
        _debug_log(
            "cortex prompt",
            {"length": len(prompt)},
            "app.py:nlq",
            "D",
        )
        # #endregion agent log
        raw_sql = call_cortex_complete(prompt=prompt, model=cortex_model)
        generated_sql = validate_generated_sql(raw_sql, TABLE_FQN, limit_rows)
        if show_sql:
            st.code(generated_sql)
        nl_df = fetch_dataframe(generated_sql)
        st.dataframe(nl_df)
    except Exception as exc:
        # #region agent log
        _debug_log(
            "nlq error",
            {"error": str(exc)},
            "app.py:nlq",
            "D",
        )
        # #endregion agent log
        st.error(f"Cortex query failed: {exc}")
