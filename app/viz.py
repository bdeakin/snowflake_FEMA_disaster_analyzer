from __future__ import annotations

import hashlib
import math

import pandas as pd
import plotly.express as px


def _jitter_pair(lat: float, lon: float, seed: str, scale: float = 0.06) -> tuple[float, float]:
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()[:8]
    angle = (int(digest[:4], 16) / 16**4) * (2 * math.pi)
    radius = (int(digest[4:], 16) / 16**4) * scale
    return lat + math.cos(angle) * radius, lon + math.sin(angle) * radius


def build_choropleth(df: pd.DataFrame):
    return px.choropleth(
        df,
        locations="state",
        locationmode="USA-states",
        color="disaster_count",
        color_continuous_scale="Reds",
        scope="usa",
    )


def build_cube_grid(df: pd.DataFrame, grain: str):
    df = df.copy()
    df["period_bucket"] = pd.to_datetime(df["period_bucket"])
    if grain == "month":
        df["period_label"] = df["period_bucket"].dt.strftime("%b %Y")
    elif grain == "year":
        df["period_label"] = df["period_bucket"].dt.strftime("%Y")
    else:
        df["period_label"] = df["period_bucket"].dt.strftime("%Y-%m-%d")

    return px.scatter(
        df,
        x="period_label",
        y="disaster_type",
        size="disaster_count",
        color="disaster_count",
        color_continuous_scale="Reds",
        hover_data={"disaster_count": True, "period_label": True, "disaster_type": True},
        custom_data=["period_bucket"],
    )


def build_drilldown(df: pd.DataFrame, color_map: dict[str, str] | None = None):
    df = df.copy()
    df[["lat_jitter", "lon_jitter"]] = df.apply(
        lambda row: _jitter_pair(
            float(row["centroid_lat"]),
            float(row["centroid_lon"]),
            f"{row['disaster_id']}-{row['declaration_name']}",
        ),
        axis=1,
        result_type="expand",
    )
    fig = px.scatter_geo(
        df,
        lat="lat_jitter",
        lon="lon_jitter",
        hover_name="declaration_name",
        color="declaration_name",
        color_discrete_map=color_map,
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
    fig.update_geos(fitbounds="locations")
    return fig
