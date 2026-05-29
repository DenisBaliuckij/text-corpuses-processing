from __future__ import annotations

import argparse
import sys
from pathlib import Path

import re

import dash
import dash_cytoscape as cyto
from dash import Input, Output, State, dcc, html

from ..schemas.clustered_graph import ClusteredGraph, MultiClusteredGraph
from .loader import load_auto

cyto.load_extra_layouts()

_C_PRIMARY = "#2B2D42"
_C_SECONDARY = "#8D99AE"
_C_BORDER = "#E5E7EB"
_C_ACCENT = "#EF233C"
_C_EDGE = "#C4C9D4"
_C_BG = "#FAFAFA"
_C_CARD = "#FFFFFF"
_FONT = "Inter, system-ui, sans-serif"


def _build_elements(graph: ClusteredGraph) -> list[dict]:
    elements: list[dict] = []

    sizes = [n.size for n in graph.nodes] or [1]
    min_s, max_s = min(sizes), max(sizes)

    for node in graph.nodes:
        mentions_texts = list({m.text for m in node.mentions})
        elements.append({
            "data": {
                "id": node.id,
                "label": _strip_dollars(node.label),
                "raw_label": node.label,
                "cluster_size": node.size,
                "members": ", ".join(node.members) if node.members else "",
                "members_count": len(node.members),
                "mentions": "; ".join(mentions_texts[:20]),
                "mentions_count": len(mentions_texts),
                "norm_size": (
                    50 if max_s == min_s
                    else int(30 + 60 * (node.size - min_s) / (max_s - min_s))
                ),
            }
        })

    node_raw_label = {n.id: n.label for n in graph.nodes}

    for edge in graph.edges:
        elements.append({
            "data": {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "label": edge.label,
                "edge_size": edge.size,
                "merged_edges": ", ".join(edge.members) if edge.members else "",
                "merged_count": len(edge.members),
                "source_label": node_raw_label.get(edge.source, edge.source),
                "target_label": node_raw_label.get(edge.target, edge.target),
            }
        })

    return elements


_STYLESHEET = [
    {
        "selector": "node",
        "style": {
            "label": "data(label)",
            "width": "data(norm_size)",
            "height": "data(norm_size)",
            "font-size": "13px",
            "font-family": _FONT,
            "font-weight": 500,
            "color": _C_PRIMARY,
            "text-valign": "bottom",
            "text-margin-y": 8,
            "text-wrap": "wrap",
            "text-max-width": "120px",
            "background-color": _C_PRIMARY,
            "border-width": 2,
            "border-color": _C_SECONDARY,
        },
    },
    {
        "selector": "edge",
        "style": {
            "label": "data(label)",
            "font-size": "11px",
            "font-family": _FONT,
            "color": _C_SECONDARY,
            "text-rotation": "autorotate",
            "text-margin-y": -10,
            "width": 1.5,
            "line-color": _C_EDGE,
            "target-arrow-color": _C_EDGE,
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
            "curve-style": "bezier",
        },
    },
    {
        "selector": "node:selected",
        "style": {"border-color": _C_ACCENT, "border-width": 3},
    },
    {
        "selector": "edge:selected",
        "style": {
            "line-color": _C_ACCENT,
            "target-arrow-color": _C_ACCENT,
            "width": 2.5,
        },
    },
]


_METHOD_LABELS = {
    "agglomerative": "Agglomerative",
    "kmeans": "KMeans",
    "hdbscan": "HDBSCAN",
}


def create_app(graph: ClusteredGraph | MultiClusteredGraph) -> dash.Dash:
    is_multi = isinstance(graph, MultiClusteredGraph)

    if is_multi:
        all_elements: dict[str, dict[str, list[dict]]] = {}
        for method_name, method_result in graph.methods.items():
            all_elements[method_name] = {}
            for param_label, g in method_result.graphs.items():
                all_elements[method_name][param_label] = _build_elements(g)

        method_names = list(graph.methods.keys())
        default_method = method_names[0]
        default_result = graph.methods[default_method]
        default_param_idx = len(default_result.param_labels) // 2
        default_param = default_result.param_labels[default_param_idx]

        elements = all_elements[default_method][default_param]
        default_graph = default_result.graphs[default_param]
        meta = graph.meta
    else:
        elements = _build_elements(graph)
        meta = graph.meta
        default_graph = graph

    app = dash.Dash(__name__)

    controls_row = []
    if is_multi:
        method_buttons = dcc.RadioItems(
            id="method-selector",
            options=[
                {"label": _METHOD_LABELS.get(m, m), "value": m}
                for m in method_names
            ],
            value=default_method,
            inline=True,
            style={"display": "flex", "gap": "12px", "fontSize": "14px"},
            inputStyle={"marginRight": "4px"},
            labelStyle={
                "padding": "6px 16px",
                "borderRadius": "6px",
                "cursor": "pointer",
                "fontWeight": 500,
            },
        )

        default_marks = {
            i: lbl for i, lbl in enumerate(default_result.param_labels)
        }

        controls_row = [
            html.Div(
                style={
                    "padding": "12px 32px",
                    "borderBottom": f"1px solid {_C_BORDER}",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "24px",
                },
                children=[
                    html.Span(
                        "Method:",
                        style={"color": _C_PRIMARY, "fontWeight": 600, "fontSize": "14px", "whiteSpace": "nowrap"},
                    ),
                    method_buttons,
                ],
            ),
            html.Div(
                style={
                    "padding": "12px 32px",
                    "borderBottom": f"1px solid {_C_BORDER}",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "16px",
                },
                children=[
                    html.Span(
                        "Parameter:",
                        style={"color": _C_PRIMARY, "fontWeight": 600, "fontSize": "14px", "whiteSpace": "nowrap"},
                    ),
                    html.Div(
                        dcc.Slider(
                            id="param-slider",
                            min=0,
                            max=len(default_result.param_labels) - 1,
                            step=1,
                            value=default_param_idx,
                            marks=default_marks,
                            tooltip={"placement": "bottom"},
                        ),
                        style={"flex": 1},
                    ),
                ],
            ),
        ]

    app.layout = html.Div(
        style={
            "fontFamily": _FONT,
            "backgroundColor": _C_BG,
            "height": "100vh",
            "display": "flex",
            "flexDirection": "column",
        },
        children=[
            html.Div(
                style={"padding": "20px 32px 12px", "borderBottom": f"1px solid {_C_BORDER}"},
                children=[
                    html.H2(
                        "Semantic Graph Viewer",
                        style={"margin": 0, "color": _C_PRIMARY, "fontWeight": 600, "fontSize": "22px"},
                    ),
                    html.P(
                        id="stats-line",
                        children=(
                            f"Nodes: {meta.stats.get('nodes', len(default_graph.nodes))}  |  "
                            f"Edges: {meta.stats.get('edges', len(default_graph.edges))}  |  "
                            f"Pipeline: {meta.pipeline_version}"
                        ),
                        style={"margin": "4px 0 0", "color": _C_SECONDARY, "fontSize": "13px"},
                    ),
                ],
            ),
            *controls_row,
            html.Div(
                style={"display": "flex", "flex": 1, "overflow": "hidden"},
                children=[
                    cyto.Cytoscape(
                        id="cyto",
                        elements=elements,
                        stylesheet=_STYLESHEET,
                        layout={
                            "name": "cola",
                            "nodeSpacing": 60,
                            "edgeLengthVal": 200,
                            "animate": True,
                            "maxSimulationTime": 2000,
                        },
                        style={"flex": 1, "height": "100%"},
                        responsive=True,
                        userZoomingEnabled=True,
                        userPanningEnabled=True,
                        boxSelectionEnabled=False,
                    ),
                    html.Div(
                        id="info-panel",
                        style={
                            "width": "320px",
                            "padding": "24px",
                            "borderLeft": f"1px solid {_C_BORDER}",
                            "backgroundColor": _C_CARD,
                            "overflowY": "auto",
                        },
                        children=[_placeholder("Click a node or edge to see details")],
                    ),
                ],
            ),
        ],
    )

    @app.callback(
        Output("info-panel", "children"),
        Input("cyto", "tapNodeData"),
        Input("cyto", "tapEdgeData"),
    )
    def _show_info(node_data, edge_data):
        ctx = dash.callback_context
        if not ctx.triggered:
            return _placeholder("Click a node or edge to see details")

        prop_id = ctx.triggered[0]["prop_id"]

        if "tapNodeData" in prop_id and node_data:
            return _render_node(node_data)
        if "tapEdgeData" in prop_id and edge_data:
            return _render_edge(edge_data)

        return _placeholder("Click a node or edge to see details")

    if is_multi:
        @app.callback(
            Output("param-slider", "marks"),
            Output("param-slider", "max"),
            Output("param-slider", "value"),
            Input("method-selector", "value"),
        )
        def _update_slider(method_name):
            mr = graph.methods[method_name]
            marks = {i: lbl for i, lbl in enumerate(mr.param_labels)}
            max_val = len(mr.param_labels) - 1
            value = max_val // 2
            return marks, max_val, value

        @app.callback(
            Output("cyto", "elements"),
            Output("stats-line", "children"),
            Input("method-selector", "value"),
            Input("param-slider", "value"),
        )
        def _update_graph(method_name, slider_idx):
            mr = graph.methods[method_name]
            if slider_idx is None or slider_idx >= len(mr.param_labels):
                slider_idx = len(mr.param_labels) // 2
            param_label = mr.param_labels[slider_idx]
            g = mr.graphs[param_label]
            new_elements = all_elements[method_name][param_label]
            method_display = _METHOD_LABELS.get(method_name, method_name)
            stats_text = (
                f"Nodes: {len(g.nodes)}  |  "
                f"Edges: {len(g.edges)}  |  "
                f"Method: {method_display}  |  "
                f"Param: {param_label}  |  "
                f"Pipeline: {meta.pipeline_version}"
            )
            return new_elements, stats_text

    return app


_HAS_LATEX = re.compile(r"\$")


def _latex(text: str):
    """Return a dcc.Markdown with MathJax if text contains $…$, else plain span."""
    if _HAS_LATEX.search(text):
        return dcc.Markdown(
            text,
            mathjax=True,
            style={"fontSize": "inherit", "margin": 0, "padding": 0},
        )
    return html.Span(text)


def _strip_dollars(text: str) -> str:
    """Strip $ wrappers for canvas labels (Cytoscape can't render LaTeX)."""
    return text.replace("$", "")


def _placeholder(text: str):
    return html.Div(text, style={"color": _C_SECONDARY, "fontSize": "14px"})


def _badge(text: str) -> html.Span:
    return html.Span(
        text,
        style={
            "display": "inline-block",
            "padding": "2px 10px",
            "backgroundColor": "#F3F4F6",
            "borderRadius": "12px",
            "fontSize": "12px",
            "color": _C_PRIMARY,
            "marginRight": "4px",
            "marginBottom": "4px",
        },
    )


def _section(title: str, children) -> html.Div:
    return html.Div(
        style={"marginBottom": "16px"},
        children=[
            html.Div(title, style={"color": _C_SECONDARY, "fontSize": "12px", "marginBottom": "6px"}),
            html.Div(children),
        ],
    )


def _stat_card(label: str, value) -> html.Div:
    return html.Div(
        style={
            "padding": "12px 16px",
            "backgroundColor": "#F3F4F6",
            "borderRadius": "8px",
            "marginBottom": "12px",
        },
        children=[
            html.Span(f"{label}: ", style={"color": _C_SECONDARY, "fontSize": "13px"}),
            html.Span(str(value), style={"color": _C_PRIMARY, "fontWeight": 600, "fontSize": "18px"}),
        ],
    )


def _render_node(data: dict):
    members_str = data.get("members", "")
    members = [m.strip() for m in members_str.split(",") if m.strip()] if members_str else []
    mentions_str = data.get("mentions", "")
    mention_items = [m.strip() for m in mentions_str.split(";") if m.strip()] if mentions_str else []

    raw_label = data.get("raw_label", data["label"])

    children = [
        html.Div(
            _latex(raw_label),
            style={"margin": "0 0 16px", "color": _C_PRIMARY, "fontSize": "18px", "fontWeight": 600},
        ),
        _stat_card("Cluster size", data.get("cluster_size", 1)),
        _section("ID", html.Code(data["id"], style={"fontSize": "13px"})),
    ]

    if members:
        children.append(
            _section(
                f"Members ({data.get('members_count', len(members))})",
                html.Div([_badge(m) for m in members]),
            )
        )

    if mention_items:
        children.append(
            _section(
                f"Mentions ({data.get('mentions_count', len(mention_items))})",
                html.Ul(
                    [html.Li(_latex(t), style={"padding": "3px 0", "fontSize": "13px", "color": _C_PRIMARY}) for t in mention_items],
                    style={"listStyle": "none", "padding": 0, "margin": 0},
                ),
            )
        )

    return html.Div(children)


def _render_edge(data: dict):
    merged_str = data.get("merged_edges", "")
    merged = [m.strip() for m in merged_str.split(",") if m.strip()] if merged_str else []

    src_label = data.get("source_label", data["source"])
    tgt_label = data.get("target_label", data["target"])

    children = [
        html.Div(
            [
                html.Span(_latex(src_label), style={"display": "inline"}),
                html.Span(" → ", style={"color": _C_SECONDARY}),
                html.Span(_latex(tgt_label), style={"display": "inline"}),
            ],
            style={"margin": "0 0 16px", "color": _C_PRIMARY, "fontSize": "16px", "fontWeight": 600},
        ),
        _section("Relation", html.Span(data["label"], style={"fontWeight": 600, "fontSize": "15px"})),
        _stat_card("Edge size", data.get("edge_size", 1)),
        _section("ID", html.Code(data["id"], style={"fontSize": "13px"})),
    ]

    if merged:
        children.append(
            _section(
                f"Merged raw edges ({data.get('merged_count', len(merged))})",
                html.Div([_badge(m) for m in merged]),
            )
        )

    return html.Div(children)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dash Cytoscape graph viewer")
    parser.add_argument("graph_json", help="Path to clustered_graph.json or multi_clustered_graph.json")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    graph = load_auto(args.graph_json)
    app = create_app(graph)
    print(f"Open http://127.0.0.1:{args.port}")
    app.run(debug=args.debug, port=args.port)


if __name__ == "__main__":
    main()
