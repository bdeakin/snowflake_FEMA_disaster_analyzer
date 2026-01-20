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
      .link.highlighted {{
        stroke-opacity: 0.8;
      }}
      .node.highlighted text {{
        fill: #ffffff;
      }}
      .node.highlighted .label-bg {{
        fill: #0b3d91;
        stroke: #0b3d91;
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
      const truncateLabel = (text, maxChars = 100) => {{
        if (!text) return "";
        if (text.length <= maxChars) return text;
        return text.slice(0, Math.max(0, maxChars - 3)) + "...";
      }};
      const rightPadding = 160;
      const sankey = d3.sankey()
        .nodeId(d => d.id)
        .nodeWidth(20)
        .nodePadding(14)
        .extent([[1, 5], [width - rightPadding, height - 5]]);

      const graph = sankey({{
        nodes: payload.nodes.map(d => Object.assign({{}}, d)),
        links: payload.links.map(d => Object.assign({{}}, d))
      }});

      const linkGroup = svg.append("g")
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
        .text(d => `${{d.tooltip || d.name || d.id}}\\n${{d.value}}`);

      const labels = node.append("text")
        .attr("x", d => d.x0 < width / 2 ? d.x1 + 6 : d.x0 - 6)
        .attr("y", d => (d.y0 + d.y1) / 2)
        .attr("dy", "0.35em")
        .attr("text-anchor", d => d.x0 < width / 2 ? "start" : "end")
        .text(d => truncateLabel(d.name || d.id));

      labels.append("title")
        .text(d => d.tooltip || d.name || d.id);

      labels.each(function() {{
        const text = d3.select(this);
        const bbox = this.getBBox();
        const padding = 2;
        d3.select(this.parentNode)
          .insert("rect", "text")
          .attr("class", "label-bg")
          .attr("x", bbox.x - padding)
          .attr("y", bbox.y - padding)
          .attr("width", bbox.width + padding * 2)
          .attr("height", bbox.height + padding * 2)
          .attr("fill", "#ffffff")
          .attr("stroke", "#000000")
          .attr("stroke-width", 0.5)
          .attr("rx", 2)
          .attr("ry", 2);
      }});

      const collectUpstream = (node, acc = new Set()) => {{
        if (!node || acc.has(node.id)) return acc;
        acc.add(node.id);
        (node.sourceLinks || []).forEach(link => {{
          acc.add(link);
          collectUpstream(link.source, acc);
        }});
        return acc;
      }};

      const collectDownstream = (node, acc = new Set()) => {{
        if (!node || acc.has(node.id)) return acc;
        acc.add(node.id);
        (node.targetLinks || []).forEach(link => {{
          acc.add(link);
          collectDownstream(link.target, acc);
        }});
        return acc;
      }};

      const highlightFlow = (link) => {{
        const upstream = collectUpstream(link.source);
        const downstream = collectDownstream(link.target);
        const highlightLinks = new Set([link]);
        graph.links.forEach(l => {{
          if (upstream.has(l) || downstream.has(l)) {{
            highlightLinks.add(l);
          }}
        }});
        const highlightNodes = new Set();
        highlightLinks.forEach(l => {{
          highlightNodes.add(l.source);
          highlightNodes.add(l.target);
        }});
        svg.selectAll(".link").classed("highlighted", d => highlightLinks.has(d));
        svg.selectAll(".node").classed("highlighted", d => highlightNodes.has(d));
      }};

      const clearHighlight = () => {{
        svg.selectAll(".link").classed("highlighted", false);
        svg.selectAll(".node").classed("highlighted", false);
      }};

      svg.selectAll(".link")
        .on("mouseover", (event, d) => highlightFlow(d))
        .on("mouseout", () => clearHighlight());
    </script>
  </body>
</html>
"""
