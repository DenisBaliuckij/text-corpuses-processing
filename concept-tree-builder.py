# -*- coding: utf-8 -*-
"""
Created on Thu May 14 20:30:44 2026

@author: denis
"""

"""
Build a concept tree from multiple text documents.

Features:
- Reads .txt files from a folder
- Extracts important noun phrases using spaCy
- Builds a hierarchical concept tree based on co-occurrence
- Exports:
    - JSON tree
    - NetworkX graph
    - Optional visualization

Install:
    pip install spacy networkx matplotlib scikit-learn
    python -m spacy download en_core_web_sm
"""

from pathlib import Path
from collections import defaultdict, Counter
import json
import itertools

import spacy
import networkx as nx
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer


# -----------------------------
# CONFIG
# -----------------------------

TEXT_FOLDER = "texts"
TOP_K_TERMS = 15
MIN_EDGE_WEIGHT = 2

# -----------------------------
# LOAD NLP MODEL
# -----------------------------

nlp = spacy.load("en_core_web_sm")


# -----------------------------
# READ DOCUMENTS
# -----------------------------

def load_documents(folder):
    docs = []
    paths = list(Path(folder).glob("*.txt"))

    for path in paths:
        text = path.read_text(encoding="utf-8")
        docs.append({
            "name": path.stem,
            "text": text
        })

    return docs


# -----------------------------
# EXTRACT CONCEPTS
# -----------------------------

def extract_concepts(text):
    doc = nlp(text)

    concepts = []

    for chunk in doc.noun_chunks:
        phrase = chunk.text.lower().strip()

        # Clean short/noisy phrases
        if len(phrase) < 3:
            continue

        if phrase in nlp.Defaults.stop_words:
            continue

        concepts.append(phrase)

    return concepts


# -----------------------------
# TF-IDF FILTERING
# -----------------------------

def get_top_terms(documents, top_k=TOP_K_TERMS):
    texts = [" ".join(doc["concepts"]) for doc in documents]

    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(texts)

    feature_names = vectorizer.get_feature_names_out()

    selected_terms = set()

    for row in X:
        scores = zip(feature_names, row.toarray()[0])
        ranked = sorted(scores, key=lambda x: x[1], reverse=True)

        for term, score in ranked[:top_k]:
            selected_terms.add(term)

    return selected_terms


# -----------------------------
# BUILD CONCEPT GRAPH
# -----------------------------

def build_graph(documents, selected_terms):
    edge_counter = Counter()

    for doc in documents:
        concepts = [
            c for c in doc["concepts"]
            if c in selected_terms
        ]

        unique_concepts = list(set(concepts))

        for a, b in itertools.combinations(sorted(unique_concepts), 2):
            edge_counter[(a, b)] += 1

    G = nx.Graph()

    for (a, b), weight in edge_counter.items():
        if weight >= MIN_EDGE_WEIGHT:
            G.add_edge(a, b, weight=weight)

    return G


# -----------------------------
# BUILD TREE STRUCTURE
# -----------------------------

def build_tree(graph):
    """
    Convert graph into a simple rooted tree
    using highest-degree node as root.
    """

    if len(graph.nodes) == 0:
        return {}

    root = max(graph.degree, key=lambda x: x[1])[0]

    visited = set()
    tree = defaultdict(list)

    def dfs(node):
        visited.add(node)

        for neighbor in graph.neighbors(node):
            if neighbor not in visited:
                tree[node].append(neighbor)
                dfs(neighbor)

    dfs(root)

    return {
        "root": root,
        "tree": dict(tree)
    }


# -----------------------------
# SAVE TREE
# -----------------------------

def save_tree(tree, filename="concept_tree.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(tree, f, indent=2)


# -----------------------------
# VISUALIZE GRAPH
# -----------------------------

def visualize_graph(graph):
    plt.figure(figsize=(14, 10))

    pos = nx.spring_layout(graph, seed=42)

    weights = [
        graph[u][v]["weight"]
        for u, v in graph.edges()
    ]

    nx.draw_networkx_nodes(
        graph,
        pos,
        node_size=1200,
        node_color="lightblue"
    )

    nx.draw_networkx_edges(
        graph,
        pos,
        width=weights
    )

    nx.draw_networkx_labels(
        graph,
        pos,
        font_size=9
    )

    plt.title("Concept Graph")
    plt.axis("off")
    plt.show()


# -----------------------------
# MAIN
# -----------------------------

def main():
    documents = load_documents(TEXT_FOLDER)

    print(f"Loaded {len(documents)} documents")

    for doc in documents:
        doc["concepts"] = extract_concepts(doc["text"])

    selected_terms = get_top_terms(documents)

    print(f"Selected {len(selected_terms)} concepts")

    graph = build_graph(documents, selected_terms)

    print(
        f"Graph has {graph.number_of_nodes()} nodes "
        f"and {graph.number_of_edges()} edges"
    )

    tree = build_tree(graph)

    save_tree(tree)

    print("Saved concept tree to concept_tree.json")

    visualize_graph(graph)


if __name__ == "__main__":
    main()