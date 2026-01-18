import datetime as dt
from pathlib import Path
import sys

import streamlit as st

app_dir = Path(__file__).resolve().parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

from queries import get_cube_summary, get_drilldown, get_state_choropleth
from viz import build_choropleth, build_cube_grid, build_drilldown


st.set_page_config(page_title="FEMA Disasters Explorer", layout="wide")

st.title("FEMA Disasters Explorer")

st.sidebar.header("Filters")
current_year = dt.date.today().year
year_start, year_end = st.sidebar.slider(
    "Year range",
    min_value=1990,
    max_value=current_year,
    value=(2023, 2025),
)

span_years = year_end - year_start
if span_years >= 2:
    grain = "year"
elif span_years == 1:
    grain = "month"
else:
    grain = "week"

st.caption(f"Using period grain: {grain}")

selected_state = st.session_state.get("selected_state")
selected_cube = st.session_state.get("selected_cube")

with st.spinner("Loading choropleth..."):
    choropleth_result = get_state_choropleth(year_start, year_end)
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
    st.subheader(f"Cube Summary: {selected_state}")
    cube_result = get_cube_summary(selected_state, year_start, year_end, grain)
    if not cube_result.df.empty:
        cube_fig = build_cube_grid(cube_result.df)
        cube_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        cube_event = st.plotly_chart(
            cube_fig,
            use_container_width=True,
            on_select="rerun",
            selection_mode="points",
        )
        if cube_event and cube_event.selection and cube_event.selection.points:
            point = cube_event.selection.points[0]
            selected_cube = {
                "disaster_type": point.get("y"),
                "period_bucket": point.get("x"),
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
        drilldown_fig = build_drilldown(drilldown_result.df)
        drilldown_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(drilldown_fig, use_container_width=True)
