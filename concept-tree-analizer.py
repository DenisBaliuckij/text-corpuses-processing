# -*- coding: utf-8 -*-
"""
Created on Thu May 14 20:32:18 2026

@author: denis
"""

"""
Analyze a concept tree / concept graph.

Features:
- Load concept graph from JSON
- Compute:
    - central concepts
    - leaf concepts
    - communities/topics
    - graph density
    - shortest paths
    - concept importance ranking
- Visualize graph
- Export analytics report

Input:
    concept_tree.json

Install:
    pip install networkx matplotlib python-louvain
"""

import json
from collections import Counter

import networkx as nx
import matplotlib.pyplot as plt
import community.community_louvain as community_louvain


# -----------------------------------
# LOAD TREE
# -----------------------------------

def load_tree(filename="concept_tree.json"):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------------
# TREE -> GRAPH
# -----------------------------------

def tree_to_graph(tree_data):
    G = nx.Graph()

    tree = tree_data["tree"]

    for parent, children in tree.items():
        for child in children:
            G.add_edge(parent, child)

    return G


# -----------------------------------
# BASIC GRAPH STATS
# -----------------------------------

def graph_stats(G):
    print("\n=== GRAPH STATS ===")

    print(f"Nodes: {G.number_of_nodes()}")
    print(f"Edges: {G.number_of_edges()}")

    density = nx.density(G)
    print(f"Density: {density:.4f}")

    connected = nx.is_connected(G)
    print(f"Connected: {connected}")

    if connected:
        diameter = nx.diameter(G)
        print(f"Diameter: {diameter}")


# -----------------------------------
# CENTRAL CONCEPTS
# -----------------------------------

def central_concepts(G, top_n=10):
    print("\n=== CENTRAL CONCEPTS ===")

    centrality = nx.betweenness_centrality(G)

    ranked = sorted(
        centrality.items(),
        key=lambda x: x[1],
        reverse=True
    )

    for concept, score in ranked[:top_n]:
        print(f"{concept:30} {score:.4f}")

    return ranked


# -----------------------------------
# DEGREE ANALYSIS
# -----------------------------------

def degree_analysis(G, top_n=10):
    print("\n=== HIGH DEGREE CONCEPTS ===")

    degree = dict(G.degree())

    ranked = sorted(
        degree.items(),
        key=lambda x: x[1],
        reverse=True
    )

    for concept, deg in ranked[:top_n]:
        print(f"{concept:30} degree={deg}")

    return ranked


# -----------------------------------
# LEAF CONCEPTS
# -----------------------------------

def leaf_concepts(G):
    print("\n=== LEAF CONCEPTS ===")

    leaves = [
        node for node, degree in G.degree()
        if degree == 1
    ]

    for leaf in leaves[:20]:
        print(leaf)

    print(f"\nTotal leaves: {len(leaves)}")

    return leaves


# -----------------------------------
# COMMUNITY DETECTION
# -----------------------------------

def detect_communities(G):
    print("\n=== COMMUNITIES ===")

    partition = community_louvain.best_partition(G)

    grouped = {}

    for node, community_id in partition.items():
        grouped.setdefault(community_id, []).append(node)

    for community_id, nodes in grouped.items():
        print(f"\nCommunity {community_id}:")
        print(", ".join(nodes[:15]))

    return partition


# -----------------------------------
# SHORTEST PATH ANALYSIS
# -----------------------------------

def shortest_path_analysis(G, source, target):
    print("\n=== SHORTEST PATH ===")

    try:
        path = nx.shortest_path(G, source, target)

        print(" -> ".join(path))

        return path

    except nx.NetworkXNoPath:
        print("No path found")
        return None


# -----------------------------------
# CONCEPT CLUSTERING SCORE
# -----------------------------------

def clustering_analysis(G):
    print("\n=== CLUSTERING ===")

    clustering = nx.clustering(G)

    ranked = sorted(
        clustering.items(),
        key=lambda x: x[1],
        reverse=True
    )

    for node, score in ranked[:10]:
        print(f"{node:30} {score:.4f}")

    return ranked


# -----------------------------------
# VISUALIZATION
# -----------------------------------

def visualize_graph(G, partition=None):
    plt.figure(figsize=(16, 12))

    pos = nx.spring_layout(G, seed=42)

    if partition:
        colors = [
            partition[node]
            for node in G.nodes()
        ]
    else:
        colors = "lightblue"

    nx.draw_networkx_nodes(
        G,
        pos,
        node_color=colors,
        cmap=plt.cm.tab20,
        node_size=1000,
        alpha=0.9
    )

    nx.draw_networkx_edges(
        G,
        pos,
        alpha=0.4
    )

    nx.draw_networkx_labels(
        G,
        pos,
        font_size=8
    )

    plt.title("Concept Graph Analysis")
    plt.axis("off")
    plt.show()


# -----------------------------------
# SAVE ANALYTICS REPORT
# -----------------------------------

def save_report(G, filename="concept_report.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=== CONCEPT GRAPH REPORT ===\n\n")

        f.write(f"Nodes: {G.number_of_nodes()}\n")
        f.write(f"Edges: {G.number_of_edges()}\n")
        f.write(f"Density: {nx.density(G):.4f}\n")

        degree = sorted(
            G.degree(),
            key=lambda x: x[1],
            reverse=True
        )

        f.write("\nTop Concepts:\n")

        for node, deg in degree[:20]:
            f.write(f"{node}: {deg}\n")


# -----------------------------------
# MAIN
# -----------------------------------

def main():
    tree_data = load_tree()

    G = tree_to_graph(tree_data)

    graph_stats(G)

    central_concepts(G)

    degree_analysis(G)

    leaf_concepts(G)

    partition = detect_communities(G)

    clustering_analysis(G)

    # Example path analysis
    nodes = list(G.nodes())

    if len(nodes) >= 2:
        shortest_path_analysis(
            G,
            nodes[0],
            nodes[-1]
        )

    save_report(G)

    visualize_graph(G, partition)


if __name__ == "__main__":
    main()