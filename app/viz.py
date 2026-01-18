from __future__ import annotations

import hashlib

import pandas as pd
import plotly.express as px


def _jitter(value: float, seed: str, scale: float = 0.05) -> float:
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()[:8]
    offset = (int(digest, 16) / 16**8) - 0.5
    return value + (offset * scale)


def build_choropleth(df: pd.DataFrame):
    return px.choropleth(
        df,
        locations="state",
        locationmode="USA-states",
        color="disaster_count",
        color_continuous_scale="Reds",
        scope="usa",
    )


def build_cube_grid(df: pd.DataFrame):
    return px.scatter(
        df,
        x="period_bucket",
        y="disaster_type",
        size="disaster_count",
        color="disaster_count",
        color_continuous_scale="Reds",
        hover_data={"disaster_count": True, "period_bucket": True, "disaster_type": True},
    )


def build_drilldown(df: pd.DataFrame):
    df = df.copy()
    df["lat_jitter"] = df.apply(
        lambda row: _jitter(float(row["centroid_lat"]), str(row["disaster_id"])), axis=1
    )
    df["lon_jitter"] = df.apply(
        lambda row: _jitter(float(row["centroid_lon"]), str(row["disaster_id"])), axis=1
    )
    return px.scatter_geo(
        df,
        lat="lat_jitter",
        lon="lon_jitter",
        hover_name="declaration_name",
        hover_data={
            "disaster_id": True,
            "disaster_declaration_date": True,
            "disaster_type": True,
            "county_name": True,
            "state": True,
            "centroid_lat": False,
            "centroid_lon": False,
            "lat_jitter": False,
            "lon_jitter": False,
        },
        scope="usa",
    )
