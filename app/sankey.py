from __future__ import annotations

import json
from typing import Any, Iterable


def render_sankey(
    nodes: Iterable[dict[str, Any]],
    links: Iterable[dict[str, Any]],
    height: int = 600,
) -> str:
    payload = {
        "nodes": list(nodes),
        "links": list(links),
    }
    data_json = json.dumps(payload, ensure_ascii=True)

    return f"""
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-sankey@0.12.3/dist/d3-sankey.min.js"></script>
    <style>
      body {{
        margin: 0;
        font-family: sans-serif;
      }}
      #sankey {{
        width: 100%;
        height: {height}px;
      }}
      .node rect {{
        fill-opacity: 0.85;
        stroke: #1f1f1f;
        stroke-width: 0.5px;
      }}
      .node text {{
        font-size: 11px;
        fill: #1f1f1f;
      }}
      .link {{
        fill: none;
        stroke-opacity: 0.35;
      }}
    </style>
  </head>
  <body>
    <div id="sankey"></div>
    <script>
      const payload = {data_json};
      const width = document.getElementById("sankey").clientWidth || 900;
      const height = {height};
      const svg = d3.select("#sankey")
        .append("svg")
        .attr("viewBox", [0, 0, width, height])
        .attr("width", width)
        .attr("height", height);

      const color = d3.scaleOrdinal(d3.schemeTableau10);
      const sankey = d3.sankey()
        .nodeId(d => d.id)
        .nodeWidth(20)
        .nodePadding(14)
        .extent([[1, 5], [width - 1, height - 5]]);

      const graph = sankey({{
        nodes: payload.nodes.map(d => Object.assign({{}}, d)),
        links: payload.links.map(d => Object.assign({{}}, d))
      }});

      svg.append("g")
        .attr("stroke", "#666")
        .selectAll("path")
        .data(graph.links)
        .join("path")
        .attr("class", "link")
        .attr("d", d3.sankeyLinkHorizontal())
        .attr("stroke-width", d => Math.max(1, d.width))
        .attr("stroke", d => color(d.source.name || d.source.id))
        .append("title")
        .text(d => `${{d.source.name || d.source.id}} → ${{d.target.name || d.target.id}}\\n${{d.value}}`);

      const node = svg.append("g")
        .selectAll("g")
        .data(graph.nodes)
        .join("g")
        .attr("class", "node");

      node.append("rect")
        .attr("x", d => d.x0)
        .attr("y", d => d.y0)
        .attr("height", d => d.y1 - d.y0)
        .attr("width", d => d.x1 - d.x0)
        .attr("fill", d => color(d.name || d.id))
        .append("title")
        .text(d => `${{d.name || d.id}}\\n${{d.value}}`);

      node.append("text")
        .attr("x", d => d.x0 < width / 2 ? d.x1 + 6 : d.x0 - 6)
        .attr("y", d => (d.y0 + d.y1) / 2)
        .attr("dy", "0.35em")
        .attr("text-anchor", d => d.x0 < width / 2 ? "start" : "end")
        .text(d => d.name || d.id);
    </script>
  </body>
</html>
"""
