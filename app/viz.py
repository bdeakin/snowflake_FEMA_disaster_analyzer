from __future__ import annotations

import hashlib
import math

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


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


def build_bump_chart(df: pd.DataFrame):
    if df.empty:
        return go.Figure()
    df = df.copy()
    df["period_decade"] = pd.to_datetime(df["period_decade"])
    df["decade_label"] = df["period_decade"].dt.strftime("%Y") + "s"
    types = sorted(df["disaster_type"].dropna().unique().tolist())
    palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
    color_map = {t: palette[i % len(palette)] for i, t in enumerate(types)}

    fig = go.Figure()
    for disaster_type, group in df.groupby("disaster_type"):
        group = group.sort_values("period_decade")
        labels = [f"#{int(r)}" for r in group["rank"].tolist()]
        customdata = [
            [d.strftime("%Y-%m-%d"), disaster_type]
            for d in group["period_decade"].tolist()
        ]
        fig.add_trace(
            go.Scatter(
                x=group["period_decade"],
                y=group["rank"],
                mode="lines",
                line={"color": color_map.get(disaster_type), "width": 2},
                name=disaster_type,
                customdata=customdata,
                hoverinfo="skip",
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=group["period_decade"],
                y=group["rank"],
                mode="markers+text",
                text=labels,
                textposition="middle center",
                textfont={"color": "#ffffff"},
                marker={
                    "size": 26,
                    "color": color_map.get(disaster_type),
                    "line": {"width": 1, "color": "#333"},
                },
                name=disaster_type,
                customdata=customdata,
                hovertemplate=(
                    "Disaster type: %{customdata[1]}<br>"
                    "Decade: %{customdata[0]|%Y}s<br>"
                    "Rank: %{y}<br>"
                    "Count: %{customdata[2]}<extra></extra>"
                ),
                showlegend=True,
            )
        )

    # Attach counts to hover customdata
    for trace in fig.data:
        if not getattr(trace, 'customdata', None):
            continue
        disaster_type = trace.name
        if not disaster_type:
            continue
        group = df[df["disaster_type"] == disaster_type].sort_values("period_decade")
        customdata = [
            [d.strftime("%Y-%m-%d"), disaster_type, int(c)]
            for d, c in zip(group["period_decade"].tolist(), group["disaster_count"].tolist())
        ]
        trace.customdata = customdata

    fig.update_yaxes(
        autorange="reversed",
        title="Rank (1 = most frequent)",
        tickmode="linear",
        tick0=1,
        dtick=1,
    )
    fig.update_xaxes(title="Decade", tickformat="%Y", ticklabelmode="period")
    fig.update_layout(legend_title_text="Disaster type")
    return fig


def build_sankey(
    links: pd.DataFrame,
    left_nodes: list[str],
    middle_nodes: list[str],
    right_nodes: list[str],
) -> go.Figure:
    if links.empty:
        return go.Figure()
    labels = list(dict.fromkeys(left_nodes + middle_nodes + right_nodes))
    index = {label: idx for idx, label in enumerate(labels)}
    palette = px.colors.qualitative.Safe + px.colors.qualitative.Plotly
    source_labels = links["source"].dropna().unique().tolist()
    color_map = {label: palette[i % len(palette)] for i, label in enumerate(source_labels)}
    link_colors = links["source"].map(color_map).fillna("#999999")
    link_values = links["value"].astype(float) * 0.5

    def _positions(nodes: list[str], x: float) -> tuple[list[float], list[float]]:
        if not nodes:
            return [], []
        step = 1.0 / (len(nodes) + 1)
        ys = [1.0 - step * (i + 1) for i in range(len(nodes))]
        xs = [x] * len(nodes)
        return xs, ys

    left_x, left_y = _positions(left_nodes, 0.01)
    mid_x, mid_y = _positions(middle_nodes, 0.5)
    right_x, right_y = _positions(right_nodes, 0.99)
    node_x = left_x + mid_x + right_x
    node_y = left_y + mid_y + right_y

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="fixed",
                node={
                    "label": [""] * len(labels),
                    "customdata": labels,
                    "hovertemplate": "%{customdata}<extra></extra>",
                    "pad": 18,
                    "thickness": 18,
                    "x": node_x,
                    "y": node_y,
                    "color": ["#ffffff"] * len(labels),
                    "line": {"color": "#333333", "width": 1},
                },
                link={
                    "source": links["source"].map(index),
                    "target": links["target"].map(index),
                    "value": link_values,
                    "color": link_colors,
                    "customdata": links[["source", "target"]].values.tolist(),
                    "hovertemplate": "%{customdata[0]} → %{customdata[1]}<br>Count: %{value}<extra></extra>",
                },
            )
        ]
    )
    annotations = []
    for label, x, y in zip(labels, node_x, node_y):
        annotations.append(
            {
                "x": x,
                "y": y,
                "text": label,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"color": "#000000"},
                "bgcolor": "#ffffff",
                "bordercolor": "#333333",
                "borderwidth": 1,
                "xanchor": "center",
                "yanchor": "middle",
            }
        )
    fig.update_layout(
        margin={"r": 0, "t": 20, "l": 0, "b": 0},
        font={"color": "#000000"},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        annotations=annotations,
    )
    return fig


def build_sunburst(nodes: pd.DataFrame) -> go.Figure:
    if nodes.empty:
        return go.Figure()
    fig = go.Figure(
        data=[
            go.Sunburst(
                ids=nodes["id"],
                labels=nodes["label"],
                parents=nodes["parent"],
                values=nodes["value"],
                customdata=nodes["customdata"],
                marker={"colors": nodes["color"], "line": {"color": "#ffffff", "width": 1}},
                hovertemplate="%{label}<br>Count: %{value}<extra></extra>",
                branchvalues="total",
            )
        ]
    )
    fig.update_layout(margin={"r": 0, "t": 20, "l": 0, "b": 0})
    return fig
