import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from graphMetrics import compute_metrics

# ── RuleBased fixture ─────────────────────────────────────────────────────────
RULEBASED_GRAPH = {
    "nodes": ["cat", "dog", "fish"],
    "edges": [
        {"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 2},
        {"agent_1": "dog", "agent_2": "fish", "meaning": "eats",   "weight": 1},
        {"agent_1": "cat", "agent_2": "fish", "meaning": "ignores","weight": 1},
    ]
}

# ── LLMv2 / Hierarchical clustered fixture ────────────────────────────────────
CLUSTERED_GRAPH = {
    "meta": {},
    "nodes": [
        {"id": "n1", "label": "cat",  "members": [], "size": 1, "embedding": []},
        {"id": "n2", "label": "dog",  "members": [], "size": 1, "embedding": []},
        {"id": "n3", "label": "fish", "members": [], "size": 1, "embedding": []},
    ],
    "edges": [
        {"id": "e1", "source": "n1", "target": "n2", "label": "chases", "members": [], "size": 1, "weight": 2},
        {"id": "e2", "source": "n2", "target": "n3", "label": "eats",   "members": [], "size": 1, "weight": 1},
    ]
}

def test_rulebased_node_and_edge_count():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert m["node_count"] == 3
    assert m["edge_count"] == 3

def test_rulebased_density():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    # undirected density = 2*E / (N*(N-1)) = 6/6 = 1.0
    assert abs(m["density"] - 1.0) < 1e-6

def test_rulebased_degree_stats():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert m["max_degree"] == 2
    assert m["min_degree"] == 2

def test_rulebased_top_hubs_are_list_of_dicts():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert isinstance(m["top_10_hubs"], list)
    assert all("label" in h and "degree" in h for h in m["top_10_hubs"])

def test_rulebased_degree_distribution_is_dict():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert isinstance(m["degree_distribution"], dict)

def test_clustered_graph_node_and_edge_count():
    m = compute_metrics(CLUSTERED_GRAPH, "LLMv2")
    assert m["node_count"] == 3
    assert m["edge_count"] == 2

def test_diameter_skipped_for_disconnected():
    disconnected = {
        "nodes": ["a", "b", "c"],
        "edges": [{"agent_1": "a", "agent_2": "b", "meaning": "r", "weight": 1}]
    }
    m = compute_metrics(disconnected, "RuleBased")
    assert m["diameter"] is None
    assert m["avg_shortest_path"] is None

def test_diameter_skipped_for_large_graph():
    large = {
        "nodes": [str(i) for i in range(501)],
        "edges": [{"agent_1": str(i), "agent_2": str(i+1), "meaning": "r", "weight": 1}
                  for i in range(500)]
    }
    m = compute_metrics(large, "RuleBased")
    assert m["diameter"] is None

def test_connected_components_count():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert m["connected_components"] == 1

def test_largest_component_fraction():
    m = compute_metrics(RULEBASED_GRAPH, "RuleBased")
    assert abs(m["largest_component_fraction"] - 1.0) < 1e-6
