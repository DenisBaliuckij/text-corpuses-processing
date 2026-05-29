from __future__ import annotations

import math
import sys
from pathlib import Path

from pyvis.network import Network

from ..schemas.clustered_graph import ClusteredGraph
from .loader import load_graph

_BG = "#FAFAFA"
_NODE_COLOR = "#2B2D42"
_NODE_BORDER = "#8D99AE"
_EDGE_COLOR = "#C4C9D4"
_FONT = "Inter, system-ui, sans-serif"


def _node_title(node) -> str:
    """Build an HTML tooltip for a node."""
    members = node.members or []
    mentions_texts = list({m.text for m in node.mentions})

    lines = [
        f"<b>{node.label}</b>",
        f"<hr style='margin:4px 0;border-color:#E5E7EB'>",
        f"<b>ID:</b> {node.id}",
        f"<b>Cluster size:</b> {node.size}",
    ]
    if members:
        lines.append(f"<b>Members ({len(members)}):</b>")
        for m in members:
            lines.append(f"&nbsp;&nbsp;- {m}")
    if mentions_texts:
        lines.append(f"<b>Mentions ({len(mentions_texts)}):</b>")
        for t in mentions_texts[:15]:
            lines.append(f"&nbsp;&nbsp;- {t}")
        if len(mentions_texts) > 15:
            lines.append(f"&nbsp;&nbsp;... +{len(mentions_texts) - 15} more")

    return "<br>".join(lines)


def _edge_title(edge, src_label: str, tgt_label: str) -> str:
    """Build an HTML tooltip for an edge."""
    lines = [
        f"<b>{src_label}</b> → <b>{tgt_label}</b>",
        f"<hr style='margin:4px 0;border-color:#E5E7EB'>",
        f"<b>ID:</b> {edge.id}",
        f"<b>Relation:</b> {edge.label}",
        f"<b>Edge size:</b> {edge.size}",
    ]
    if edge.members:
        lines.append(f"<b>Merged edges ({len(edge.members)}):</b>")
        for m in edge.members:
            lines.append(f"&nbsp;&nbsp;- {m}")
    return "<br>".join(lines)


def build_pyvis(graph: ClusteredGraph, height: str = "100%") -> Network:
    net = Network(
        height=height,
        width="100%",
        directed=True,
        bgcolor=_BG,
        font_color="#2B2D42",
        select_menu=False,
        filter_menu=False,
    )

    net.set_options("""
    {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -80,
          "centralGravity": 0.01,
          "springLength": 180,
          "springConstant": 0.04,
          "damping": 0.6
        },
        "solver": "forceAtlas2Based",
        "stabilization": {"iterations": 200}
      },
      "nodes": {
        "font": {"face": "Inter, system-ui, sans-serif", "size": 14}
      },
      "edges": {
        "font": {"face": "Inter, system-ui, sans-serif", "size": 11, "align": "top"},
        "smooth": {"type": "curvedCW", "roundness": 0.15}
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100
      }
    }
    """)

    sizes = [n.size for n in graph.nodes] or [1]
    min_s, max_s = min(sizes), max(sizes)

    def _radius(size: int) -> int:
        if max_s == min_s:
            return 25
        t = (size - min_s) / (max_s - min_s)
        return int(18 + t * 40)

    node_label_map: dict[str, str] = {}
    for node in graph.nodes:
        node_label_map[node.id] = node.label
        r = _radius(node.size)
        net.add_node(
            node.id,
            label=node.label,
            title=_node_title(node),
            size=r,
            color={
                "background": _NODE_COLOR,
                "border": _NODE_BORDER,
                "highlight": {"background": "#EF233C", "border": "#EF233C"},
                "hover": {"background": "#3A3D56", "border": "#EF233C"},
            },
            font={"color": "#2B2D42", "size": max(11, 14 - int(math.log2(max(len(node.label) - 10, 1) + 1)))},
            borderWidth=2,
            shape="dot",
        )

    for edge in graph.edges:
        src_label = node_label_map.get(edge.source, edge.source)
        tgt_label = node_label_map.get(edge.target, edge.target)
        width = 1.0 + min(edge.size * 0.5, 4.0)
        net.add_edge(
            edge.source,
            edge.target,
            label=edge.label,
            title=_edge_title(edge, src_label, tgt_label),
            width=width,
            color={"color": _EDGE_COLOR, "highlight": "#EF233C", "hover": "#8D99AE"},
            arrows="to",
            arrowStrikethrough=False,
        )

    return net


def render(graph: ClusteredGraph, output_path: str | Path = "graph_pyvis.html") -> Path:
    net = build_pyvis(graph)
    out = Path(output_path)
    net.save_graph(str(out))
    return out


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m llm_v2.visualization.viewer_pyvis <clustered_graph.json> [output.html]")
        sys.exit(1)

    graph = load_graph(sys.argv[1])
    out = sys.argv[2] if len(sys.argv) > 2 else "graph_pyvis.html"
    path = render(graph, out)
    print(f"Saved to {path}")


if __name__ == "__main__":
    main()
