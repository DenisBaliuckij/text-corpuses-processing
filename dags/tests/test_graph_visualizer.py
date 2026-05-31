# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from graphVisualizer import generate_visualization

RULEBASED_GRAPH = {
    "nodes": ["cat", "dog", "fish"],
    "edges": [
        {"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 2},
        {"agent_1": "dog", "agent_2": "fish", "meaning": "eats",   "weight": 1},
    ]
}

CLUSTERED_GRAPH = {
    "meta": {},
    "nodes": [
        {"id": "n1", "label": "cat",  "members": [], "size": 1, "embedding": []},
        {"id": "n2", "label": "dog",  "members": [], "size": 1, "embedding": []},
    ],
    "edges": [
        {"id": "e1", "source": "n1", "target": "n2", "label": "chases", "members": [], "size": 1, "weight": 2},
    ]
}

HIERARCHICAL_GRAPH = {
    "meta": {},
    "nodes": [
        {"id": "n1", "label": "cat",  "members": [], "size": 1, "embedding": [], "importance": 0.9},
        {"id": "n2", "label": "dog",  "members": [], "size": 1, "embedding": [], "importance": 0.4},
    ],
    "edges": [
        {"id": "e1", "source": "n1", "target": "n2", "label": "chases", "members": [], "size": 1, "weight": 1, "importance": 0.5},
    ]
}

def test_rulebased_returns_html_string():
    html = generate_visualization(RULEBASED_GRAPH, "RuleBased")
    assert isinstance(html, str)
    assert "<html" in html.lower()

def test_html_contains_vis_js():
    html = generate_visualization(RULEBASED_GRAPH, "RuleBased")
    assert "vis" in html.lower()

def test_llmv2_returns_html_string():
    html = generate_visualization(CLUSTERED_GRAPH, "LLMv2")
    assert isinstance(html, str)
    assert "<html" in html.lower()

def test_hierarchical_returns_html_string():
    html = generate_visualization(HIERARCHICAL_GRAPH, "Hierarchical")
    assert isinstance(html, str)
    assert "<html" in html.lower()

def test_node_labels_appear_in_html():
    html = generate_visualization(RULEBASED_GRAPH, "RuleBased")
    assert "cat" in html
    assert "dog" in html
