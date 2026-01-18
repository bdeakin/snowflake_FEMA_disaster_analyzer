import json
import os
import time
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
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
DETAIL_VIEW_FQN = _get_env("FEMA_DETAIL_VIEW_FQN", "FEMA_ANALYTICS.PUBLIC.FEMA_DISASTER_DETAIL_VIEW")

METRO_POINTS = [
    ("New York-Newark-Jersey City, NY-NJ-PA", 40.7128, -74.0060),
    ("Boston-Cambridge-Newton, MA-NH", 42.3601, -71.0589),
    ("Philadelphia-Camden-Wilmington, PA-NJ-DE-MD", 39.9526, -75.1652),
    ("Washington-Arlington-Alexandria, DC-VA-MD-WV", 38.9072, -77.0369),
    ("Baltimore-Columbia-Towson, MD", 39.2904, -76.6122),
    ("Pittsburgh, PA", 40.4406, -79.9959),
    ("Providence-Warwick, RI-MA", 41.8240, -71.4128),
    ("Buffalo-Cheektowaga-Niagara Falls, NY", 42.8864, -78.8784),
    ("Rochester, NY", 43.1566, -77.6088),
    ("Hartford-East Hartford-Middletown, CT", 41.7658, -72.6734),
    ("New Haven-Milford, CT", 41.3083, -72.9279),
    ("Albany-Schenectady-Troy, NY", 42.6526, -73.7562),
    ("Syracuse, NY", 43.0481, -76.1474),
    ("Atlanta-Sandy Springs-Roswell, GA", 33.7490, -84.3880),
    ("Miami-Fort Lauderdale-Pompano Beach, FL", 25.7617, -80.1918),
    ("Orlando-Kissimmee-Sanford, FL", 28.5383, -81.3792),
    ("Tampa-St. Petersburg-Clearwater, FL", 27.9506, -82.4572),
    ("Jacksonville, FL", 30.3322, -81.6557),
    ("Charlotte-Concord-Gastonia, NC-SC", 35.2271, -80.8431),
    ("Raleigh-Cary, NC", 35.7796, -78.6382),
    ("Nashville-Davidson-Murfreesboro-Franklin, TN", 36.1627, -86.7816),
    ("Richmond, VA", 37.5407, -77.4360),
    ("Virginia Beach-Norfolk-Newport News, VA-NC", 36.8508, -76.2859),
    ("Birmingham-Hoover, AL", 33.5186, -86.8104),
    ("Louisville/Jefferson County, KY-IN", 38.2527, -85.7585),
    ("Memphis, TN-MS-AR", 35.1495, -90.0490),
    ("New Orleans-Metairie, LA", 29.9511, -90.0715),
    ("Charleston-North Charleston, SC", 32.7765, -79.9311),
    ("Greenville-Anderson, SC", 34.8526, -82.3940),
    ("Columbia, SC", 34.0007, -81.0348),
    ("Chicago-Naperville-Elgin, IL-IN-WI", 41.8781, -87.6298),
    ("Detroit-Warren-Dearborn, MI", 42.3314, -83.0458),
    ("Minneapolis-St. Paul-Bloomington, MN-WI", 44.9778, -93.2650),
    ("St. Louis, MO-IL", 38.6270, -90.1994),
    ("Cleveland-Elyria, OH", 41.4993, -81.6944),
    ("Cincinnati, OH-KY-IN", 39.1031, -84.5120),
    ("Columbus, OH", 39.9612, -82.9988),
    ("Indianapolis-Carmel-Anderson, IN", 39.7684, -86.1581),
    ("Kansas City, MO-KS", 39.0997, -94.5786),
    ("Milwaukee-Waukesha-West Allis, WI", 43.0389, -87.9065),
    ("Madison, WI", 43.0731, -89.4012),
    ("Grand Rapids-Wyoming, MI", 42.9634, -85.6681),
    ("Omaha-Council Bluffs, NE-IA", 41.2565, -95.9345),
    ("Dayton, OH", 39.7589, -84.1916),
    ("Toledo, OH", 41.6528, -83.5379),
    ("Des Moines-West Des Moines, IA", 41.5868, -93.6250),
    ("Phoenix-Mesa-Chandler, AZ", 33.4484, -112.0740),
    ("Las Vegas-Henderson-Paradise, NV", 36.1699, -115.1398),
    ("Denver-Aurora-Lakewood, CO", 39.7392, -104.9903),
    ("Salt Lake City, UT", 40.7608, -111.8910),
    ("Tucson, AZ", 32.2226, -110.9747),
    ("Albuquerque, NM", 35.0844, -106.6504),
    ("Colorado Springs, CO", 38.8339, -104.8214),
    ("Reno, NV", 39.5296, -119.8138),
    ("Boise City, ID", 43.6150, -116.2023),
    ("Dallas-Fort Worth-Arlington, TX", 32.7767, -96.7970),
    ("Houston-The Woodlands-Sugar Land, TX", 29.7604, -95.3698),
    ("San Antonio-New Braunfels, TX", 29.4241, -98.4936),
    ("Austin-Round Rock-Georgetown, TX", 30.2672, -97.7431),
    ("Fort Worth-Arlington, TX", 32.7555, -97.3308),
    ("El Paso, TX", 31.7619, -106.4850),
    ("Los Angeles-Long Beach-Anaheim, CA", 34.0522, -118.2437),
    ("San Francisco-Oakland-Berkeley, CA", 37.7749, -122.4194),
    ("San Jose-Sunnyvale-Santa Clara, CA", 37.3382, -121.8863),
    ("San Diego-Chula Vista-Carlsbad, CA", 32.7157, -117.1611),
    ("Seattle-Tacoma-Bellevue, WA", 47.6062, -122.3321),
    ("Portland-Vancouver-Hillsboro, OR-WA", 45.5152, -122.6784),
    ("Sacramento-Roseville-Folsom, CA", 38.5816, -121.4944),
    ("Riverside-San Bernardino-Ontario, CA", 33.9533, -117.3962),
    ("Fresno, CA", 36.7378, -119.7871),
    ("Bakersfield, CA", 35.3733, -119.0187),
    ("Stockton-Lodi, CA", 37.9577, -121.2908),
    ("Oxnard-Thousand Oaks-Ventura, CA", 34.1975, -119.1771),
    ("Santa Rosa-Petaluma, CA", 38.4404, -122.7141),
    ("Anchorage, AK", 61.2181, -149.9003),
    ("Urban Honolulu, HI", 21.3069, -157.8583),
]

MID_METRO_POINTS = [
    ("Allentown-Bethlehem-Easton, PA-NJ", 40.6023, -75.4714),
    ("Harrisburg-Carlisle, PA", 40.2732, -76.8867),
    ("Lancaster, PA", 40.0379, -76.3055),
    ("Scranton-Wilkes-Barre, PA", 41.4089, -75.6624),
    ("York-Hanover, PA", 39.9626, -76.7277),
    ("Springfield, MA", 42.1015, -72.5898),
    ("Worcester, MA-CT", 42.2626, -71.8023),
    ("Portland-South Portland, ME", 43.6591, -70.2568),
    ("Burlington-South Burlington, VT", 44.4759, -73.2121),
    ("Manchester-Nashua, NH", 42.9956, -71.4548),
    ("Bridgeport-Stamford-Norwalk, CT", 41.1792, -73.1894),
    ("Trenton-Princeton, NJ", 40.2204, -74.7643),
    ("Poughkeepsie-Newburgh-Middletown, NY", 41.7004, -73.9210),
    ("Utica-Rome, NY", 43.1009, -75.2327),
    ("Binghamton, NY", 42.0987, -75.9180),
    ("Palm Bay-Melbourne-Titusville, FL", 28.0836, -80.6081),
    ("Cape Coral-Fort Myers, FL", 26.6406, -81.8723),
    ("Sarasota-Bradenton-Venice, FL", 27.3364, -82.5307),
    ("Port St. Lucie, FL", 27.2730, -80.3582),
    ("Pensacola-Ferry Pass-Brent, FL", 30.4213, -87.2169),
    ("Tallahassee, FL", 30.4383, -84.2807),
    ("Gainesville, FL", 29.6516, -82.3248),
    ("Lakeland-Winter Haven, FL", 28.0395, -81.9498),
    ("Deltona-Daytona Beach-Ormond Beach, FL", 29.2108, -81.0228),
    ("Augusta-Richmond County, GA-SC", 33.4735, -82.0105),
    ("Savannah, GA", 32.0809, -81.0912),
    ("Columbus, GA-AL", 32.4609, -84.9877),
    ("Macon-Bibb County, GA", 32.8407, -83.6324),
    ("Chattanooga, TN-GA", 35.0456, -85.3097),
    ("Knoxville, TN", 35.9606, -83.9207),
    ("Johnson City, TN", 36.3134, -82.3535),
    ("Asheville, NC", 35.5951, -82.5515),
    ("Wilmington, NC", 34.2257, -77.9447),
    ("Fayetteville, NC", 35.0527, -78.8784),
    ("Winston-Salem, NC", 36.0999, -80.2442),
    ("Greensboro-High Point, NC", 36.0726, -79.7920),
    ("Durham-Chapel Hill, NC", 35.9940, -78.8986),
    ("Lexington-Fayette, KY", 38.0406, -84.5037),
    ("Huntsville, AL", 34.7304, -86.5861),
    ("Mobile, AL", 30.6954, -88.0399),
    ("Baton Rouge, LA", 30.4515, -91.1871),
    ("Lafayette, LA", 30.2241, -92.0198),
    ("Shreveport-Bossier City, LA", 32.5252, -93.7502),
    ("Little Rock-North Little Rock-Conway, AR", 34.7465, -92.2896),
    ("Jackson, MS", 32.2988, -90.1848),
    ("Gulfport-Biloxi-Pascagoula, MS", 30.3674, -89.0928),
    ("Akron, OH", 41.0814, -81.5190),
    ("Youngstown-Warren-Boardman, OH-PA", 41.0998, -80.6495),
    ("Canton-Massillon, OH", 40.7989, -81.3784),
    ("Fort Wayne, IN", 41.0793, -85.1394),
    ("South Bend-Mishawaka, IN-MI", 41.6764, -86.2520),
    ("Evansville, IN-KY", 37.9716, -87.5711),
    ("Lansing-East Lansing, MI", 42.7325, -84.5555),
    ("Ann Arbor, MI", 42.2808, -83.7430),
    ("Flint, MI", 43.0125, -83.6875),
    ("Kalamazoo-Portage, MI", 42.2917, -85.5872),
    ("Rockford, IL", 42.2711, -89.0940),
    ("Peoria, IL", 40.6936, -89.5890),
    ("Champaign-Urbana, IL", 40.1164, -88.2434),
    ("Springfield, IL", 39.7817, -89.6501),
    ("Quad Cities, IA-IL", 41.5236, -90.5776),
    ("Cedar Rapids, IA", 41.9779, -91.6656),
    ("Iowa City, IA", 41.6611, -91.5302),
    ("Sioux Falls, SD", 43.5446, -96.7311),
    ("Fargo, ND-MN", 46.8772, -96.7898),
    ("Lincoln, NE", 40.8136, -96.7026),
    ("Wichita, KS", 37.6872, -97.3301),
    ("Topeka, KS", 39.0558, -95.6890),
    ("Springfield, MO", 37.2089, -93.2923),
    ("Columbia, MO", 38.9517, -92.3341),
    ("Duluth, MN-WI", 46.7867, -92.1005),
    ("Green Bay, WI", 44.5133, -88.0133),
    ("Appleton, WI", 44.2619, -88.4154),
    ("Corpus Christi, TX", 27.8006, -97.3964),
    ("McAllen-Edinburg-Mission, TX", 26.2034, -98.2300),
    ("Brownsville-Harlingen, TX", 25.9017, -97.4975),
    ("Lubbock, TX", 33.5779, -101.8552),
    ("Amarillo, TX", 35.221997, -101.8313),
    ("Waco, TX", 31.5493, -97.1467),
    ("Killeen-Temple, TX", 31.1171, -97.7278),
    ("College Station-Bryan, TX", 30.6280, -96.3344),
    ("Beaumont-Port Arthur, TX", 30.0802, -94.1266),
    ("Midland, TX", 31.9973, -102.0779),
    ("Odessa, TX", 31.8457, -102.3676),
    ("Tyler, TX", 32.3513, -95.3011),
    ("Abilene, TX", 32.4487, -99.7331),
    ("Wichita Falls, TX", 33.9137, -98.4934),
    ("Santa Fe, NM", 35.6870, -105.9378),
    ("Las Cruces, NM", 32.3199, -106.7637),
    ("Flagstaff, AZ", 35.1983, -111.6513),
    ("Prescott, AZ", 34.5400, -112.4685),
    ("Yuma, AZ", 32.6927, -114.6277),
    ("St. George, UT", 37.0965, -113.5684),
    ("Provo-Orem, UT", 40.2969, -111.6946),
    ("Ogden-Clearfield, UT", 41.2230, -111.9738),
    ("Grand Junction, CO", 39.0639, -108.5506),
    ("Fort Collins, CO", 40.5853, -105.0844),
    ("Boulder, CO", 40.01499, -105.2705),
    ("Pueblo, CO", 38.2544, -104.6091),
    ("Billings, MT", 45.7833, -108.5007),
    ("Missoula, MT", 46.8721, -113.9940),
    ("Bozeman, MT", 45.6770, -111.0429),
    ("Cheyenne, WY", 41.1400, -104.8202),
    ("Spokane-Spokane Valley, WA", 47.6588, -117.4260),
    ("Tri-Cities, WA", 46.2306, -119.0911),
    ("Yakima, WA", 46.6021, -120.5059),
    ("Eugene-Springfield, OR", 44.0521, -123.0868),
    ("Salem, OR", 44.9429, -123.0351),
    ("Bend, OR", 44.0582, -121.3153),
    ("Medford, OR", 42.3265, -122.8756),
    ("Santa Barbara, CA", 34.4208, -119.6982),
    ("Monterey, CA", 36.6002, -121.8947),
    ("San Luis Obispo-Paso Robles, CA", 35.2828, -120.6596),
    ("Chico, CA", 39.7285, -121.8375),
    ("Redding, CA", 40.5865, -122.3917),
    ("Visalia, CA", 36.3302, -119.2921),
    ("Modesto, CA", 37.6391, -120.9969),
    ("Carson City, NV", 39.1638, -119.7674),
    ("Idaho Falls, ID", 43.4917, -112.0333),
    ("Coeur d'Alene, ID", 47.6777, -116.7805),
    ("Pocatello, ID", 42.8713, -112.4455),
    ("Fairbanks, AK", 64.8378, -147.7164),
    ("Juneau, AK", 58.3019, -134.4197),
    ("Kenai-Soldotna, AK", 60.5544, -151.2583),
    ("Ketchikan, AK", 55.3422, -131.6461),
    ("Kahului-Wailuku-Lahaina, HI", 20.8893, -156.4740),
    ("Hilo, HI", 19.7074, -155.0897),
    ("Kailua-Kona, HI", 19.6399, -155.9969),
]


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


def build_cluster_detail_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
    bounds: Tuple[float, float, float, float] = None,
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    state_column = "STATE_NAME"
    incident_column = "INCIDENT_TYPE"
    date_column = "DISASTER_BEGIN_DATE"

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
        where_clauses.append("LATITUDE BETWEEN %(min_lat)s AND %(max_lat)s")
        where_clauses.append("LONGITUDE BETWEEN %(min_lon)s AND %(max_lon)s")
        params["min_lat"] = float(min_lat)
        params["max_lat"] = float(max_lat)
        params["min_lon"] = float(min_lon)
        params["max_lon"] = float(max_lon)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = (
        "SELECT "
        "STATE_NAME AS state_name, "
        "INCIDENT_TYPE AS incident_type, "
        "DISASTER_DECLARATION_NAME AS disaster_declaration_name, "
        "DISASTER_BEGIN_DATE AS disaster_begin_date, "
        "DISASTER_END_DATE AS disaster_end_date, "
        "LATITUDE AS latitude, "
        "LONGITUDE AS longitude "
        f"FROM {table_fqn} WHERE {where_sql} "
        "ORDER BY DISASTER_BEGIN_DATE DESC "
        f"LIMIT {int(limit_rows)}"
    )
    return sql, params


def build_agg_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
    grid_size: float,
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
)
SELECT
    ROUND(lat / {grid_size}) * {grid_size} AS latitude,
    ROUND(lon / {grid_size}) * {grid_size} AS longitude,
    SUM(count) AS count,
    {grid_size} AS grid_size
FROM base
GROUP BY latitude, longitude
LIMIT {int(limit_rows)}
"""
    return sql, params


def build_metro_query(
    detail_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    limit_rows: int,
    radius_miles: float,
    metro_points: List[Tuple[str, float, float]],
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    state_column = "STATE_NAME"
    incident_column = "INCIDENT_TYPE"
    date_column = "DISASTER_BEGIN_DATE"
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
            f"YEAR(TRY_TO_DATE(TO_VARCHAR({date_column}))) BETWEEN %(min_year)s AND %(max_year)s"
        )
        params["min_year"] = str(year_range[0])
        params["max_year"] = str(year_range[1])
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    def _escape_sql(value: str) -> str:
        return str(value).replace("'", "''")

    values_sql = ",\n".join(
        [f"('{_escape_sql(name)}', {lat}, {lon})" for name, lat, lon in metro_points]
    )
    radius_deg = radius_miles / 69.0
    sql = f"""
WITH metros AS (
    SELECT column1 AS metro_name, column2::FLOAT AS metro_lat, column3::FLOAT AS metro_lon
    FROM VALUES
    {values_sql}
),
detail AS (
    SELECT
        STATE_NAME,
        INCIDENT_TYPE,
        DISASTER_DECLARATION_NAME,
        TRY_TO_DATE(TO_VARCHAR(DISASTER_BEGIN_DATE)) AS disaster_begin_date,
        TRY_TO_DATE(TO_VARCHAR(DISASTER_END_DATE)) AS disaster_end_date,
        LATITUDE,
        LONGITUDE,
        MD5(
            COALESCE(STATE_NAME, '') || '|' ||
            COALESCE(INCIDENT_TYPE, '') || '|' ||
            COALESCE(TO_VARCHAR(DISASTER_BEGIN_DATE), '') || '|' ||
            COALESCE(TO_VARCHAR(DISASTER_END_DATE), '') || '|' ||
            COALESCE(TO_VARCHAR(LATITUDE), '') || '|' ||
            COALESCE(TO_VARCHAR(LONGITUDE), '')
        ) AS uid
    FROM {detail_fqn}
    WHERE {where_sql}
      AND LATITUDE IS NOT NULL
      AND LONGITUDE IS NOT NULL
),
joined AS (
    SELECT
        d.*,
        m.metro_name,
        m.metro_lat,
        m.metro_lon,
        69 * SQRT(POWER(d.LATITUDE - m.metro_lat, 2) +
            POWER((d.LONGITUDE - m.metro_lon) * COS(RADIANS(m.metro_lat)), 2)) AS dist_miles
    FROM detail d
    JOIN metros m
      ON 69 * SQRT(POWER(d.LATITUDE - m.metro_lat, 2) +
        POWER((d.LONGITUDE - m.metro_lon) * COS(RADIANS(m.metro_lat)), 2)) <= {radius_miles}
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY dist_miles) AS rn
    FROM joined
),
assigned AS (
    SELECT * FROM ranked WHERE rn = 1
),
counts AS (
    SELECT
        metro_name,
        metro_lat,
        metro_lon,
        YEAR(disaster_begin_date) AS disaster_year,
        INCIDENT_TYPE,
        COUNT(*) AS cnt
    FROM assigned
    GROUP BY metro_name, metro_lat, metro_lon, disaster_year, INCIDENT_TYPE
),
summary AS (
    SELECT
        metro_name,
        metro_lat,
        metro_lon,
        LISTAGG(
            TO_VARCHAR(disaster_year) || ' ' || INCIDENT_TYPE || ': ' || cnt,
            ', '
        ) WITHIN GROUP (ORDER BY disaster_year DESC, cnt DESC) AS type_year_summary,
        SUM(cnt) AS count
    FROM counts
    GROUP BY metro_name, metro_lat, metro_lon
)
SELECT
    metro_name,
    metro_lat AS latitude,
    metro_lon AS longitude,
    count,
    type_year_summary,
    {radius_deg * 2} AS grid_size
FROM summary
ORDER BY count DESC
LIMIT {int(limit_rows)}
"""
    return sql, params


def build_region_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
) -> Tuple[str, Dict[str, str]]:
    where_clauses = []
    params: Dict[str, str] = {}
    state_column = "STATE_GEO_ID"
    incident_column = "INCIDENT_TYPE"
    date_column = "DISASTER_DECLARATION_DATE"
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
            f"YEAR(TRY_TO_DATE(TO_VARCHAR({date_column}))) BETWEEN %(min_year)s AND %(max_year)s"
        )
        params["min_year"] = str(year_range[0])
        params["max_year"] = str(year_range[1])
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
WITH detail AS (
    SELECT
        FEMA_REGION_NAME AS region_name,
        COALESCE(REGION_LATITUDE, COUNTY_LATITUDE) AS lat,
        COALESCE(REGION_LONGITUDE, COUNTY_LONGITUDE) AS lon,
        INCIDENT_TYPE,
        YEAR(TRY_TO_DATE(TO_VARCHAR(DISASTER_DECLARATION_DATE))) AS disaster_year
    FROM {table_fqn}
    WHERE {where_sql}
      AND COALESCE(REGION_LATITUDE, COUNTY_LATITUDE) IS NOT NULL
      AND COALESCE(REGION_LONGITUDE, COUNTY_LONGITUDE) IS NOT NULL
),
counts AS (
    SELECT
        region_name,
        AVG(lat) AS latitude,
        AVG(lon) AS longitude,
        disaster_year,
        INCIDENT_TYPE,
        COUNT(*) AS cnt
    FROM detail
    GROUP BY region_name, disaster_year, INCIDENT_TYPE
),
summary AS (
    SELECT
        region_name,
        latitude,
        longitude,
        LISTAGG(
            TO_VARCHAR(disaster_year) || ' ' || INCIDENT_TYPE || ': ' || cnt,
            ', '
        ) WITHIN GROUP (ORDER BY disaster_year DESC, cnt DESC) AS type_year_summary,
        SUM(cnt) AS count
    FROM counts
    GROUP BY region_name, latitude, longitude
)
SELECT
    region_name AS metro_name,
    latitude,
    longitude,
    count,
    type_year_summary,
    8.0 AS grid_size
FROM summary
ORDER BY count DESC
"""
    return sql, params


def build_hover_query(
    table_fqn: str,
    states: List[str],
    incidents: List[str],
    year_range: Tuple[int, int],
    bounds: Tuple[float, float, float, float],
    grid_size: float,
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
        YEAR(TRY_TO_DATE(TO_VARCHAR(DISASTER_DECLARATION_DATE))) AS disaster_year
    FROM {table_fqn}
    WHERE {where_sql}
),
clustered AS (
    SELECT
        ROUND(lat / {grid_size}) * {grid_size} AS clat,
        ROUND(lon / {grid_size}) * {grid_size} AS clon,
        INCIDENT_TYPE,
        disaster_year
    FROM detail
),
counts AS (
    SELECT clat, clon, disaster_year, INCIDENT_TYPE, COUNT(*) AS cnt
    FROM clustered
    GROUP BY clat, clon, disaster_year, INCIDENT_TYPE
),
summary AS (
    SELECT
        clat,
        clon,
        LISTAGG(
            TO_VARCHAR(disaster_year) || ' ' || INCIDENT_TYPE || ': ' || cnt,
            ', '
        ) WITHIN GROUP (ORDER BY disaster_year DESC, cnt DESC) AS type_year_summary,
        SUM(cnt) AS total_count
    FROM counts
    GROUP BY clat, clon
)
SELECT
    clat AS latitude,
    clon AS longitude,
    type_year_summary,
    total_count AS count
FROM summary
"""
    return sql, params


def build_map_figure(df: pd.DataFrame, view_mode: str, map_zoom: float) -> px.scatter_mapbox:
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

    if view_mode == "aggregated":
        hover_fields = {
            "count": True,
            "type_year_summary": True,
        }
        if "metro_name" in df.columns:
            hover_fields["metro_name"] = True
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            size="count",
            size_max=40,
            zoom=map_zoom,
            center={"lat": center_lat, "lon": center_lon},
            height=600,
            color="count",
            color_continuous_scale=["#9a9a9a", "#ff0000"],
            hover_data=hover_fields,
        )
    else:
        if "incident_type" in df.columns:
            incident_series = df["incident_type"]
        else:
            incident_series = pd.Series([""] * len(df))
        df["icon"] = incident_series.apply(_incident_icon)
        customdata = df[
            [
                "disaster_declaration_name",
                "incident_type",
                "disaster_begin_date",
                "disaster_end_date",
            ]
        ].fillna("")
        fig = go.Figure(
            go.Scattermapbox(
                lat=df["latitude"],
                lon=df["longitude"],
                mode="markers+text",
                text=df["icon"],
                textposition="middle center",
                marker=dict(size=22, color="rgba(0,0,0,0)"),
                customdata=customdata,
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Type: %{customdata[1]}<br>"
                    "Begin: %{customdata[2]}<br>"
                    "End: %{customdata[3]}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            mapbox_style=mapbox_style,
            mapbox_zoom=map_zoom,
            mapbox_center={"lat": center_lat, "lon": center_lon},
            height=600,
            margin=dict(l=0, r=0, t=0, b=0),
        )
        return fig

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


def _grid_size_for_bounds(
    bounds: Tuple[float, float, float, float],
    target_clusters: int,
    min_size: float,
    max_size: float,
) -> float:
    if not bounds:
        return max_size
    min_lat, max_lat, min_lon, max_lon = bounds
    lat_span = max(max_lat - min_lat, 0.1)
    lon_span = max(max_lon - min_lon, 0.1)
    raw = (lat_span * lon_span / max(target_clusters, 1)) ** 0.5
    return float(min(max(raw, min_size), max_size))


def _incident_icon(incident_type: str) -> str:
    if not incident_type:
        return "❓"
    value = str(incident_type).lower()
    if "fire" in value:
        return "🔥"
    if "hurricane" in value or "cyclone" in value:
        return "🌀"
    if "flood" in value or "inund" in value:
        return "🌊"
    if "tornado" in value:
        return "🌪️"
    if "storm" in value:
        return "⛈️"
    if "drought" in value:
        return "🌵"
    if "earthquake" in value:
        return "🌎"
    if "volcano" in value:
        return "🌋"
    if "blizzard" in value or "snow" in value or "winter" in value:
        return "❄️"
    if "landslide" in value or "mud" in value:
        return "🪨"
    if "pandemic" in value or "virus" in value or "disease" in value:
        return "🦠"
    if "west nile" in value or "mosquito" in value:
        return "🦟"
    if "tsunami" in value:
        return "🌊"
    return "❓"


def _cluster_bounds(latitude: float, longitude: float, grid_size: float) -> Tuple[float, float, float, float]:
    half = grid_size / 2.0
    return (latitude - half, latitude + half, longitude - half, longitude + half)


def _extract_click(events: object, df: pd.DataFrame) -> Dict[str, float]:
    if not events or df.empty or not isinstance(events, dict):
        return None
    points = None
    if "clickData" in events and isinstance(events["clickData"], dict):
        points = events["clickData"].get("points")
    elif "points" in events:
        points = events.get("points")
    if not points:
        return None
    idx = points[0].get("pointIndex", points[0].get("pointNumber"))
    if idx is None:
        return None
    try:
        row = df.iloc[int(idx)]
    except Exception:
        return None
    grid_size = row.get("grid_size") if "grid_size" in row else None
    return {
        "latitude": float(row.get("latitude", 0.0)),
        "longitude": float(row.get("longitude", 0.0)),
        "grid_size": float(grid_size) if grid_size else 0.0,
        "count": int(row.get("count", 0)),
    }


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
    st.subheader("Zoom")
    zoom_options = [
        "FEMA Region ID",
        "Major Metropolitan Area",
        "Metropolitan Statistical Area",
    ]
    zoom_map = {
        "FEMA Region ID": 3.0,
        "Major Metropolitan Area": 5.0,
        "Metropolitan Statistical Area": 8.0,
    }
    current_zoom = float(st.session_state.get("map_zoom", 3.0))
    current_label = next(
        (label for label, value in zoom_map.items() if value == current_zoom),
        "FEMA Region ID",
    )
    selected_label = st.selectbox("Zoom level", zoom_options, index=zoom_options.index(current_label))
    selected_zoom = zoom_map[selected_label]
    if selected_zoom != current_zoom:
        st.session_state["map_zoom"] = selected_zoom
        st.rerun()

st.subheader("Map")
use_agg = True
view_mode = "aggregated"
render_status = st.progress(0, text="Preparing query...")
query_start = time.time()
try:
    state_ids = [state_name_to_id[name] for name in states if name in state_name_to_id]
    state_names = list(states)
    bounds = (24.0, 50.0, -125.0, -66.0)
    map_zoom = float(st.session_state.get("map_zoom", 3.0))
    if map_zoom <= 4:
        view_mode = "aggregated"
        grid_size = 8.0
        view_label = "FEMA regions"
    elif map_zoom < 7:
        view_mode = "aggregated"
        grid_size = 3.0
        view_label = "Major metros"
    else:
        view_mode = "aggregated"
        grid_size = 1.0
        view_label = "MSA clusters"
    st.session_state["last_view_mode"] = view_mode
    st.session_state["last_grid_size"] = grid_size
    st.session_state["last_bounds"] = bounds
    # #region agent log
    _debug_log(
        "map zoom tier",
        {
            "map_zoom": map_zoom,
            "view_mode": view_mode,
            "grid_size": grid_size,
            "bounds": bounds,
        },
        "app.py:map",
        "ZOOM1",
    )
    # #endregion agent log
    use_agg = view_mode == "aggregated"
    filters_key = (tuple(state_ids), tuple(incidents), year_range, view_mode, round(grid_size, 2))
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
            "switching_disabled": False,
            "view_mode": view_mode,
            "map_zoom": map_zoom,
        },
        "app.py:map",
        "OCSP1",
    )
    # #endregion agent log
    if view_mode == "aggregated":
        st.caption(f"View mode: {view_label} (click a cluster for details)")
        st.caption(f"Zoom: {map_zoom:.2f}")
        render_status.progress(40, text="Loading aggregated data...")
        if map_zoom <= 4:
            region_sql, region_params = build_region_query(
                TABLE_FQN,
                state_ids,
                incidents,
                year_range,
            )
            df = fetch_dataframe_plain(region_sql, params=region_params)
            df.columns = [str(col).lower() for col in df.columns]
        elif map_zoom < 7:
            metro_sql, metro_params = build_metro_query(
                DETAIL_VIEW_FQN,
                state_names,
                incidents,
                year_range,
                40,
                200.0,
                METRO_POINTS,
            )
            df = fetch_dataframe_plain(metro_sql, params=metro_params)
            df.columns = [str(col).lower() for col in df.columns]
        else:
            msa_sql, msa_params = build_metro_query(
                DETAIL_VIEW_FQN,
                state_names,
                incidents,
                year_range,
                300,
                80.0,
                METRO_POINTS + MID_METRO_POINTS,
            )
            df = fetch_dataframe_plain(msa_sql, params=msa_params)
            df.columns = [str(col).lower() for col in df.columns]
    else:
        st.caption(f"View mode: {view_label}")
        st.caption(f"Zoom: {map_zoom:.2f}")
        render_status.progress(40, text="Loading incident data...")
        detail_sql, detail_params = build_cluster_detail_query(
            DETAIL_VIEW_FQN, state_names, incidents, year_range, 500, bounds=bounds
        )
        df = fetch_dataframe_plain(detail_sql, params=detail_params)
        df.columns = [str(col).lower() for col in df.columns]
    if df.empty:
        st.info("No records returned for the selected filters.")
    else:
        render_status.progress(75, text="Rendering map...")
        df = df.reset_index(drop=True)
        fig = build_map_figure(df, view_mode, map_zoom)
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
        # #region agent log
        _debug_log(
            "plotly events received",
            {
                "events_type": type(events).__name__,
                "events_keys": list(events.keys()) if isinstance(events, dict) else None,
                "events_len": len(events) if isinstance(events, list) else None,
            },
            "app.py:map",
            "ZOOM3",
        )
        # #endregion agent log
        if view_mode == "aggregated" and events:
            clicked = _extract_click(events, df)
            if clicked:
                if not clicked.get("grid_size"):
                    clicked["grid_size"] = grid_size
                if clicked != st.session_state.get("selected_cluster"):
                    st.session_state["selected_cluster"] = clicked
                    st.session_state["cluster_detail_df"] = None
        if view_mode == "aggregated" and st.session_state.get("selected_cluster") and st.session_state.get("cluster_detail_df") is None:
            render_status.progress(90, text="Loading cluster details...")
            selected = st.session_state["selected_cluster"]
            cluster_bounds = _cluster_bounds(
                selected["latitude"], selected["longitude"], selected["grid_size"]
            )
            detail_sql, detail_params = build_cluster_detail_query(
                DETAIL_VIEW_FQN,
                state_names,
                incidents,
                year_range,
                200,
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

with st.expander("Map Status", expanded=False):
    status_payload = {
        "zoom": float(st.session_state.get("map_zoom", 0.0)),
        "view_mode": st.session_state.get("last_view_mode"),
        "grid_size": st.session_state.get("last_grid_size"),
        "bounds": st.session_state.get("last_bounds"),
    }
    st.json(status_payload)

with st.expander("Data Preview", expanded=False):
    if use_agg:
        detail_df = st.session_state.get("cluster_detail_df")
        if detail_df is None:
            st.info("Click a cluster to load detailed records for that area.")
        elif detail_df.empty:
            st.info("No detailed records found for the selected cluster.")
        else:
            preview_columns = [
                "disaster_begin_date",
                "disaster_end_date",
                "incident_type",
                "disaster_declaration_name",
                "state_name",
                "latitude",
                "longitude",
            ]
            available = [col for col in preview_columns if col in detail_df.columns]
            st.dataframe(detail_df[available].head(100))
    else:
        preview_columns = [
            "disaster_begin_date",
            "disaster_end_date",
            "incident_type",
            "disaster_declaration_name",
            "state_name",
            "latitude",
            "longitude",
        ]
        available = [col for col in preview_columns if col in df.columns]
        st.dataframe(df[available].head(100))


