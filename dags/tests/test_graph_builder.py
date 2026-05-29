import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from graphBuilder import merge_graph, extract_graph_edges


def test_merge_graph_into_empty_adds_node_and_edge():
    graph = {"nodes": [], "edges": []}
    result = merge_graph(graph, [("cat", "dog", "chases")])
    assert "cat" in result["nodes"]
    assert "dog" in result["nodes"]
    assert result["edges"] == [
        {"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}
    ]


def test_merge_graph_increments_weight_on_duplicate_edge():
    graph = {
        "nodes": ["cat", "dog"],
        "edges": [{"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}]
    }
    result = merge_graph(graph, [("cat", "dog", "chases")])
    assert len(result["edges"]) == 1
    assert result["edges"][0]["weight"] == 2


def test_merge_graph_appends_new_edge():
    graph = {
        "nodes": ["cat", "dog"],
        "edges": [{"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}]
    }
    result = merge_graph(graph, [("cat", "fish", "eats")])
    assert len(result["edges"]) == 2
    assert "fish" in result["nodes"]
    assert result["edges"][1] == {
        "agent_1": "cat", "agent_2": "fish", "meaning": "eats", "weight": 1
    }


def test_merge_graph_multiple_edges_in_one_call():
    graph = {"nodes": [], "edges": []}
    edges = [("a", "b", "rel"), ("a", "b", "rel"), ("b", "c", "rel2")]
    result = merge_graph(graph, edges)
    assert len(result["edges"]) == 2
    assert result["edges"][0]["weight"] == 2
    assert result["edges"][1]["weight"] == 1


def test_extract_graph_edges_returns_list_of_triples():
    edges = extract_graph_edges("The scientist studies the phenomenon.")
    assert isinstance(edges, list)
    for edge in edges:
        assert len(edge) == 3
        a1, a2, meaning = edge
        assert isinstance(a1, str) and isinstance(a2, str) and isinstance(meaning, str)


def test_extract_graph_edges_no_self_loops():
    edges = extract_graph_edges("The cat and the cat eat fish.")
    for a1, a2, _ in edges:
        assert a1 != a2
