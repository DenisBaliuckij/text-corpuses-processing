# -*- coding: utf-8 -*-
import math
from pyvis.network import Network


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _importance_to_color(importance: float) -> str:
    # Map 0.0–1.0 importance to blue-to-red via green midpoint
    r = int(255 * importance)
    b = int(255 * (1.0 - importance))
    return f"#{r:02x}40{b:02x}"


def generate_visualization(graph_dict: dict, backend: str) -> str:
    net = Network(height="750px", width="100%", bgcolor="#1a1a2e", font_color="#e0e0ff")
    net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=120)

    if backend == "RuleBased":
        # Compute degree for sizing
        from collections import defaultdict
        deg = defaultdict(int)
        for edge in graph_dict.get("edges", []):
            deg[edge["agent_1"]] += 1
            deg[edge["agent_2"]] += 1

        for node in graph_dict.get("nodes", []):
            size = _clamp(10 + deg[node] * 5, 10, 40)
            net.add_node(node, label=node, title=f"{node} · degree {deg[node]}", size=size, color="#6c63ff")

        for edge in graph_dict.get("edges", []):
            width = _clamp(1 + math.log1p(edge.get("weight", 1)), 1, 8)
            net.add_edge(
                edge["agent_1"], edge["agent_2"],
                title=f"{edge.get('meaning','')} (w={edge.get('weight',1)})",
                width=width, color="#7b8cde"
            )

    else:
        # LLMv2 and Hierarchical
        id_to_label = {n["id"]: n["label"] for n in graph_dict.get("nodes", [])}

        from collections import defaultdict
        deg = defaultdict(int)
        for edge in graph_dict.get("edges", []):
            deg[edge["source"]] += 1
            deg[edge["target"]] += 1

        for node in graph_dict.get("nodes", []):
            nid = node["id"]
            label = node["label"]
            size = _clamp(10 + deg[nid] * 5, 10, 40)
            importance = node.get("importance", None)
            color = _importance_to_color(importance) if importance is not None else "#6c63ff"
            tip = f"{label} · degree {deg[nid]}"
            if importance is not None:
                tip += f" · importance {importance:.2f}"
            net.add_node(nid, label=label, title=tip, size=size, color=color)

        for edge in graph_dict.get("edges", []):
            width = _clamp(1 + math.log1p(edge.get("weight", 1)), 1, 8)
            net.add_edge(
                edge["source"], edge["target"],
                title=f"{edge.get('label','')} (w={edge.get('weight',1)})",
                width=width, color="#7b8cde"
            )

    return net.generate_html(notebook=False)
