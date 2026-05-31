# -*- coding: utf-8 -*-
import networkx as nx


def _to_networkx(graph_dict: dict, backend: str) -> nx.Graph:
    G = nx.Graph()
    if backend == "RuleBased":
        for node in graph_dict.get("nodes", []):
            G.add_node(node)
        for edge in graph_dict.get("edges", []):
            G.add_edge(edge["agent_1"], edge["agent_2"],
                       weight=edge.get("weight", 1),
                       label=edge.get("meaning", ""))
    else:
        # LLMv2 and Hierarchical: nodes have id/label, edges have source/target
        id_to_label = {}
        for node in graph_dict.get("nodes", []):
            id_to_label[node["id"]] = node["label"]
            G.add_node(node["id"], label=node["label"])
        for edge in graph_dict.get("edges", []):
            G.add_edge(edge["source"], edge["target"],
                       weight=edge.get("weight", 1),
                       label=edge.get("label", ""))
    return G


def compute_metrics(graph_dict: dict, backend: str) -> dict:
    G = _to_networkx(graph_dict, backend)
    N = G.number_of_nodes()
    E = G.number_of_edges()

    degrees = [d for _, d in G.degree()]
    avg_degree = sum(degrees) / N if N > 0 else 0.0

    components = list(nx.connected_components(G))
    largest_cc = max(components, key=len) if components else set()

    # Diameter and avg shortest path: only for connected graphs with N <= 500
    diameter = None
    avg_shortest_path = None
    if nx.is_connected(G) and N <= 500:
        diameter = nx.diameter(G)
        avg_shortest_path = nx.average_shortest_path_length(G)

    # Degree distribution: {degree_value: count}
    from collections import Counter
    deg_dist = dict(Counter(degrees))

    # Top 10 hubs by degree
    sorted_nodes = sorted(G.degree(), key=lambda x: x[1], reverse=True)[:10]
    if backend == "RuleBased":
        top_hubs = [{"label": n, "degree": d} for n, d in sorted_nodes]
    else:
        id_to_label = {node["id"]: node["label"] for node in graph_dict.get("nodes", [])}
        top_hubs = [{"label": id_to_label.get(n, n), "degree": d} for n, d in sorted_nodes]

    return {
        "node_count": N,
        "edge_count": E,
        "density": nx.density(G),
        "avg_degree": avg_degree,
        "max_degree": max(degrees) if degrees else 0,
        "min_degree": min(degrees) if degrees else 0,
        "avg_clustering_coefficient": nx.average_clustering(G),
        "connected_components": len(components),
        "largest_component_fraction": len(largest_cc) / N if N > 0 else 0.0,
        "diameter": diameter,
        "avg_shortest_path": avg_shortest_path,
        "top_10_hubs": top_hubs,
        "degree_distribution": {str(k): v for k, v in sorted(deg_dist.items())},
    }
