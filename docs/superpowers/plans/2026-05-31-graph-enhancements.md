# Graph Enhancements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add switchable anaphora resolution (Lapin-Liass or spaCy neural), per-graph metrics JSON, and offline pyvis visualization HTML to the existing Airflow graph-construction pipeline.

**Architecture:** Three independent modules (`anaphoraResolver.py` dispatcher, `graphMetrics.py`, `graphVisualizer.py`) are each unit-tested in isolation. Two DAGs (`resolve-anaphora-dag.py`, `finalize-job-dag.py`) and one DAG trigger (`start-graph-formation-job-dag.py`) are minimally modified to call these modules. Two new DB connector methods provide the data the finalize DAG needs.

**Tech Stack:** Python 3.10+, spaCy `en_coreference_web_trf`, networkx, pyvis, Apache Airflow 2, pyodbc (SQL Server), ftplib

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `dags/anaphoraResolverSpacyNeural.py` | Create | Neural coref via spaCy `en_coreference_web_trf` |
| `dags/anaphoraResolver.py` | Create | Dispatcher: routes to Lapin-Liass or SpacyNeural |
| `dags/graphMetrics.py` | Create | Converts any backend graph → networkx → metrics dict |
| `dags/graphVisualizer.py` | Create | Converts any backend graph → pyvis → HTML string |
| `dags/tools/generate_metrics_report.py` | Create | CLI: fetches metrics.json files, produces HTML report |
| `dags/dbConnector.py` | Modify | Add `getProcessorConfig`, `getFilesForJob` |
| `dags/start-graph-formation-job-dag.py` | Modify | Add `anaphoraResolverName` param |
| `dags/resolve-anaphora-dag.py` | Modify | Use dispatcher, read config from DB |
| `dags/finalize-job-dag.py` | Modify | Generate metrics + visualization after finalizing |
| `dags/tests/test_anaphora_resolver.py` | Create | Dispatcher routing + SpacyNeural interface tests |
| `dags/tests/test_graph_metrics.py` | Create | Metrics for all 3 backend formats |
| `dags/tests/test_graph_visualizer.py` | Create | HTML output validation for all 3 backend formats |

---

## Task 1: Anaphora resolver dispatcher

**Files:**
- Create: `dags/anaphoraResolver.py`
- Create: `dags/tests/test_anaphora_resolver.py`

- [ ] **Step 1: Write the failing test**

```python
# dags/tests/test_anaphora_resolver.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from anaphoraResolver import resolve_and_substitute

def test_dispatcher_defaults_to_lapin_liass():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text)
    assert isinstance(out, str)
    assert isinstance(subs, list)
    assert isinstance(ress, list)

def test_dispatcher_lapin_liass_explicit():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="LapinLiass")
    assert "Alice" in out

def test_dispatcher_unknown_name_falls_back_to_lapin_liass():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="UnknownResolver")
    assert isinstance(out, str)
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest dags/tests/test_anaphora_resolver.py -v
```
Expected: `ImportError` or `ModuleNotFoundError` for `anaphoraResolver`.

- [ ] **Step 3: Create `dags/anaphoraResolver.py`**

```python
# -*- coding: utf-8 -*-
def resolve_and_substitute(text: str, resolver_name: str = "LapinLiass", mark: bool = False):
    if resolver_name == "SpacyNeural":
        from anaphoraResolverSpacyNeural import resolve_and_substitute as _resolve
    else:
        from anaphoraResolverLapinLiass import resolve_and_substitute as _resolve
    return _resolve(text, mark=mark)
```

- [ ] **Step 4: Run tests to confirm they pass**

```
pytest dags/tests/test_anaphora_resolver.py -v
```
Expected: 3 PASSED.

- [ ] **Step 5: Commit**

```bash
git add dags/anaphoraResolver.py dags/tests/test_anaphora_resolver.py
git commit -m "feat: add anaphora resolver dispatcher"
```

---

## Task 2: SpacyNeural resolver module

**Files:**
- Create: `dags/anaphoraResolverSpacyNeural.py`

Prerequisites:
```bash
pip install "spacy[transformers]"
python -m spacy download en_coreference_web_trf
```

- [ ] **Step 1: Add SpacyNeural integration test to `test_anaphora_resolver.py`**

Append to `dags/tests/test_anaphora_resolver.py`:

```python
def test_dispatcher_spacy_neural_returns_correct_shape():
    # Requires: pip install "spacy[transformers]" && python -m spacy download en_coreference_web_trf
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="SpacyNeural")
    assert isinstance(out, str)
    assert isinstance(subs, list)
    assert isinstance(ress, list)
    # Each substitution must have the same fields as Lapin-Liass Substitution
    for s in subs:
        assert hasattr(s, 'start')
        assert hasattr(s, 'end')
        assert hasattr(s, 'original')
        assert hasattr(s, 'replacement')
```

- [ ] **Step 2: Run to confirm it fails**

```
pytest dags/tests/test_anaphora_resolver.py::test_dispatcher_spacy_neural_returns_correct_shape -v
```
Expected: `ModuleNotFoundError` for `anaphoraResolverSpacyNeural`.

- [ ] **Step 3: Create `dags/anaphoraResolverSpacyNeural.py`**

```python
# -*- coding: utf-8 -*-
import spacy
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Imported here for type compatibility with anaphoraResolverLapinLiass
from anaphoraResolverLapinLiass import Substitution, Resolution

_nlp = None

def _get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_coreference_web_trf")
    return _nlp


def resolve_and_substitute(text: str, mark: bool = False) -> Tuple[str, List[Substitution], List[Resolution]]:
    nlp = _get_nlp()
    doc = nlp(text)

    # Collect clusters: doc.spans keys like "coref_clusters_1", "coref_clusters_2", ...
    clusters = [
        spans for key, spans in doc.spans.items()
        if key.startswith("coref_clusters")
    ]

    # Build replacement map: character span -> replacement text
    # For each cluster, first span is the head antecedent
    replacements: List[Substitution] = []
    resolutions: List[Resolution] = []

    for cluster in clusters:
        if len(cluster) < 2:
            continue
        head_span = cluster[0]
        antecedent_text = head_span.text

        for mention_span in cluster[1:]:
            start = mention_span.start_char
            end = mention_span.end_char
            original = mention_span.text
            replacement = antecedent_text if not mark else f"{antecedent_text}{{{original}}}"
            replacements.append(Substitution(
                start=start,
                end=end,
                original=original,
                replacement=replacement,
                pronoun_index=mention_span.start,
                antecedent_index=head_span.start,
                score=1.0,
            ))
            resolutions.append(Resolution(
                pronoun=original,
                pronoun_index=mention_span.start,
                antecedent=antecedent_text,
                antecedent_index=head_span.start,
                score=1.0,
            ))

    # Apply in reverse order to preserve offsets
    replacements.sort(key=lambda s: s.start, reverse=True)
    out = text
    for s in replacements:
        out = out[:s.start] + s.replacement + out[s.end:]

    return out, replacements, resolutions
```

- [ ] **Step 4: Run to confirm test passes**

```
pytest dags/tests/test_anaphora_resolver.py -v
```
Expected: 4 PASSED.

- [ ] **Step 5: Commit**

```bash
git add dags/anaphoraResolverSpacyNeural.py dags/tests/test_anaphora_resolver.py
git commit -m "feat: add spaCy neural anaphora resolver"
```

---

## Task 3: Update `start-graph-formation-job-dag.py`

**Files:**
- Modify: `dags/start-graph-formation-job-dag.py`

- [ ] **Step 1: Add `anaphoraResolverName` param and include in config JSON**

Replace the `params` block and `config` dict (lines 21–43):

```python
    params={
       "paths": Param("", type="string", title="Paths to use for text gathering"),
       "textProcessorName": Param("RuleBased",
            enum=["RuleBased", "AIBased"],
            description="Name of text processor.",
            title="Text processor name",
        ),
       "anaphoraResolverName": Param("LapinLiass",
            enum=["LapinLiass", "SpacyNeural"],
            description="Anaphora resolver to use.",
            title="Anaphora resolver",
        ),
   },
```

And update the `config` dict inside `insertGraphProcessingJob`:

```python
        config = {
            "processorName": params["textProcessorName"],
            "anaphoraResolverName": params["anaphoraResolverName"],
        }
```

- [ ] **Step 2: Verify the DAG loads without error**

```bash
python -c "import ast; ast.parse(open('dags/start-graph-formation-job-dag.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dags/start-graph-formation-job-dag.py
git commit -m "feat: add anaphoraResolverName param to start_tree_formation_job DAG"
```

---

## Task 4: Update `dbConnector.py` and `resolve-anaphora-dag.py`

**Files:**
- Modify: `dags/dbConnector.py`
- Modify: `dags/resolve-anaphora-dag.py`

- [ ] **Step 1: Add two methods to `dags/dbConnector.py`**

Append before the final closing of the `databaseConnector` class (after `finalizeCompletedJobs`, line 205):

```python
    def getProcessorConfig(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("SELECT ProcessorConfig FROM [dbo].[GraphConstructionJob] WHERE ID = ?", (jobId,))
        result = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return result[0] if result else None

    def getFilesForJob(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT ID FROM [dbo].[GraphConstructionFiles] WHERE GraphConstructionJobId = ? AND Status = 20",
            (jobId,)
        )
        results = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return results
```

- [ ] **Step 2: Update `dags/resolve-anaphora-dag.py`** to use the dispatcher

Replace the entire file content:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="resolve_anaphora",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def resolve_anaphora():
        import io
        import json
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector
        from anaphoraResolver import resolve_and_substitute

        file_row = databaseConnector.getFileForAnaphoraResolution()
        if file_row is None:
            return

        file_id = file_row[0]
        file_path = file_row[1]
        job_id = file_row[2]

        try:
            config_json = databaseConnector.getProcessorConfig(job_id)
            config = json.loads(config_json) if config_json else {}
            resolver_name = config.get("anaphoraResolverName", "LapinLiass")

            raw_file = ftpConnector.getFile(file_path, 'Tex')
            raw_file.seek(0)
            text = raw_file.read().decode('utf-8', errors='replace')

            output, _, _ = resolve_and_substitute(text, resolver_name=resolver_name)

            resolved_path = f"graphJobs/{job_id}/anaphora/{file_id}.txt"
            ftpConnector.storeFile(
                resolved_path,
                io.BytesIO(output.encode('utf-8')),
                'Graph'
            )

            databaseConnector.markFileAnaphoraDone(file_id, resolved_path)
        except Exception as e:
            databaseConnector.setFileError(file_id, str(e))

    resolve_anaphora()
```

- [ ] **Step 3: Verify both files parse cleanly**

```bash
python -c "import ast; ast.parse(open('dags/dbConnector.py').read()); print('dbConnector OK')"
python -c "import ast; ast.parse(open('dags/resolve-anaphora-dag.py').read()); print('DAG OK')"
```
Expected: both print `OK`.

- [ ] **Step 4: Commit**

```bash
git add dags/dbConnector.py dags/resolve-anaphora-dag.py
git commit -m "feat: wire anaphora resolver selection into resolve_anaphora DAG"
```

---

## Task 5: Graph metrics module

**Files:**
- Create: `dags/graphMetrics.py`
- Create: `dags/tests/test_graph_metrics.py`

- [ ] **Step 1: Write the failing tests**

```python
# dags/tests/test_graph_metrics.py
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
```

- [ ] **Step 2: Run to confirm they fail**

```
pytest dags/tests/test_graph_metrics.py -v
```
Expected: `ModuleNotFoundError` for `graphMetrics`.

- [ ] **Step 3: Create `dags/graphMetrics.py`**

```python
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
```

- [ ] **Step 4: Install networkx if needed, run tests**

```bash
pip install networkx
pytest dags/tests/test_graph_metrics.py -v
```
Expected: 10 PASSED.

- [ ] **Step 5: Commit**

```bash
git add dags/graphMetrics.py dags/tests/test_graph_metrics.py
git commit -m "feat: add graph metrics module with networkx"
```

---

## Task 6: Graph visualizer module

**Files:**
- Create: `dags/graphVisualizer.py`
- Create: `dags/tests/test_graph_visualizer.py`

- [ ] **Step 1: Write the failing tests**

```python
# dags/tests/test_graph_visualizer.py
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
```

- [ ] **Step 2: Run to confirm they fail**

```
pytest dags/tests/test_graph_visualizer.py -v
```
Expected: `ModuleNotFoundError` for `graphVisualizer`.

- [ ] **Step 3: Create `dags/graphVisualizer.py`**

```python
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
```

- [ ] **Step 4: Install pyvis if needed, run tests**

```bash
pip install pyvis
pytest dags/tests/test_graph_visualizer.py -v
```
Expected: 5 PASSED.

- [ ] **Step 5: Commit**

```bash
git add dags/graphVisualizer.py dags/tests/test_graph_visualizer.py
git commit -m "feat: add pyvis graph visualizer module"
```

---

## Task 7: Update `finalize-job-dag.py`

**Files:**
- Modify: `dags/finalize-job-dag.py`

- [ ] **Step 1: Replace file content**

```python
# -*- coding: utf-8 -*-
import io
import json
import logging
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task
from dbConnector import databaseConnector
from ftpConnector import ftpConnector
from graphMetrics import compute_metrics
from graphVisualizer import generate_visualization


def _process_rulebased(job_id):
    graph_path = f"graphJobs/{job_id}/graph.json"
    raw = ftpConnector.getFile(graph_path, 'Graph')
    raw.seek(0)
    graph_dict = json.loads(raw.read().decode('utf-8'))

    metrics = compute_metrics(graph_dict, "RuleBased")
    ftpConnector.storeFile(
        f"graphJobs/{job_id}/metrics.json",
        io.BytesIO(json.dumps(metrics, indent=2).encode('utf-8')),
        'Graph'
    )
    html = generate_visualization(graph_dict, "RuleBased")
    ftpConnector.storeFile(
        f"graphJobs/{job_id}/visualization.html",
        io.BytesIO(html.encode('utf-8')),
        'Graph'
    )


def _process_per_file(job_id, file_id, backend):
    prefix = "llm_v2" if backend == "LLMv2" else "hierarchical"
    base = f"graphJobs/{job_id}/{prefix}/{file_id}"

    raw = ftpConnector.getFile(f"{base}/clustered_graph.json", 'Graph')
    raw.seek(0)
    graph_dict = json.loads(raw.read().decode('utf-8'))

    metrics = compute_metrics(graph_dict, backend)
    ftpConnector.storeFile(
        f"{base}/metrics.json",
        io.BytesIO(json.dumps(metrics, indent=2).encode('utf-8')),
        'Graph'
    )
    html = generate_visualization(graph_dict, backend)
    ftpConnector.storeFile(
        f"{base}/visualization.html",
        io.BytesIO(html.encode('utf-8')),
        'Graph'
    )


with DAG(
    dag_id="finalize_job",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def finalize_job():
        result = databaseConnector.finalizeCompletedJobs()
        if result is None:
            return

        job_id = result[0]
        print(f"Finalized job ID: {job_id}")

        config_json = databaseConnector.getProcessorConfig(job_id)
        config = json.loads(config_json) if config_json else {}
        processor = config.get("processorName", "RuleBased")

        try:
            if processor == "RuleBased":
                _process_rulebased(job_id)
            else:
                backend = "Hierarchical" if processor == "Hierarchical" else "LLMv2"
                file_rows = databaseConnector.getFilesForJob(job_id)
                for row in file_rows:
                    _process_per_file(job_id, row[0], backend)
        except Exception as e:
            logging.error(f"Metrics/visualization generation failed for job {job_id}: {e}")

    finalize_job()
```

- [ ] **Step 2: Verify it parses cleanly**

```bash
python -c "import ast; ast.parse(open('dags/finalize-job-dag.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dags/finalize-job-dag.py
git commit -m "feat: generate metrics and visualization in finalize_job DAG"
```

---

## Task 8: On-demand metrics report script

**Files:**
- Create: `dags/tools/generate_metrics_report.py`

- [ ] **Step 1: Create the directory and file**

```bash
mkdir -p dags/tools
touch dags/tools/__init__.py
```

```python
# dags/tools/generate_metrics_report.py
"""
Usage: python dags/tools/generate_metrics_report.py <job_id>

Fetches all metrics.json files for a completed job from FTP,
produces a self-contained metrics_report.html, and saves it to FTP.
Prints the FTP path on success.
"""
import sys
import os
import json
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dbConnector import databaseConnector
from ftpConnector import ftpConnector

# Chart.js CDN (pinned version, small, works offline if cached; swap for inline bundle if needed)
_CHARTJS = "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"


def _fetch_metrics(job_id: int) -> list[dict]:
    config_json = databaseConnector.getProcessorConfig(job_id)
    config = json.loads(config_json) if config_json else {}
    processor = config.get("processorName", "RuleBased")

    entries = []

    if processor == "RuleBased":
        raw = ftpConnector.getFile(f"graphJobs/{job_id}/metrics.json", 'Graph')
        raw.seek(0)
        m = json.loads(raw.read().decode('utf-8'))
        entries.append({"file_id": "job", "metrics": m})
    else:
        prefix = "llm_v2" if processor != "Hierarchical" else "hierarchical"
        file_rows = databaseConnector.getFilesForJob(job_id)
        for row in file_rows:
            file_id = row[0]
            path = f"graphJobs/{job_id}/{prefix}/{file_id}/metrics.json"
            try:
                raw = ftpConnector.getFile(path, 'Graph')
                raw.seek(0)
                m = json.loads(raw.read().decode('utf-8'))
                entries.append({"file_id": file_id, "metrics": m})
            except Exception as e:
                print(f"Warning: could not fetch metrics for file {file_id}: {e}", file=sys.stderr)

    return entries


def _render_html(job_id: int, entries: list[dict]) -> str:
    rows = ""
    chart_datasets = []
    chart_labels = []

    for entry in entries:
        fid = entry["file_id"]
        m = entry["metrics"]
        rows += f"""
        <tr>
          <td>{fid}</td>
          <td>{m.get('node_count', '-')}</td>
          <td>{m.get('edge_count', '-')}</td>
          <td>{m.get('density', 0):.4f}</td>
          <td>{m.get('avg_degree', 0):.2f}</td>
          <td>{m.get('avg_clustering_coefficient', 0):.4f}</td>
          <td>{m.get('connected_components', '-')}</td>
          <td>{m.get('diameter') or 'N/A'}</td>
        </tr>"""

        deg_dist = m.get("degree_distribution", {})
        if chart_labels == []:
            chart_labels = sorted(deg_dist.keys(), key=lambda x: int(x))
        chart_datasets.append({
            "label": f"File {fid}",
            "data": [deg_dist.get(k, 0) for k in chart_labels],
            "borderWidth": 1,
        })

    hub_sections = ""
    for entry in entries:
        fid = entry["file_id"]
        hubs = entry["metrics"].get("top_10_hubs", [])
        hub_rows = "".join(
            f"<tr><td>{h['label']}</td><td>{h['degree']}</td></tr>"
            for h in hubs
        )
        hub_sections += f"""
        <h3>Top hubs — file {fid}</h3>
        <table><thead><tr><th>Node</th><th>Degree</th></tr></thead>
        <tbody>{hub_rows}</tbody></table>"""

    total_nodes = sum(e["metrics"].get("node_count", 0) for e in entries)
    total_edges = sum(e["metrics"].get("edge_count", 0) for e in entries)
    densities = [e["metrics"].get("density", 0) for e in entries]
    mean_density = sum(densities) / len(densities) if densities else 0

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Metrics Report — Job {job_id}</title>
<script src="{_CHARTJS}"></script>
<style>
  body {{ font-family: monospace; background: #1a1a2e; color: #e0e0ff; padding: 2rem; }}
  h1, h2, h3 {{ color: #6c63ff; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
  th, td {{ border: 1px solid #333; padding: 6px 12px; text-align: left; }}
  th {{ background: #2d2d4e; }}
  .summary {{ display: flex; gap: 2rem; margin-bottom: 2rem; }}
  .stat {{ background: #2d2d4e; padding: 1rem 2rem; border-radius: 8px; }}
  .stat-value {{ font-size: 2rem; color: #6c63ff; }}
  canvas {{ max-height: 300px; margin-bottom: 2rem; }}
</style>
</head>
<body>
<h1>Graph Metrics Report</h1>
<p>Job ID: <strong>{job_id}</strong> &nbsp;·&nbsp; {len(entries)} file(s)</p>
<div class="summary">
  <div class="stat"><div class="stat-value">{total_nodes}</div>Total nodes</div>
  <div class="stat"><div class="stat-value">{total_edges}</div>Total edges</div>
  <div class="stat"><div class="stat-value">{mean_density:.4f}</div>Mean density</div>
</div>
<h2>Per-file metrics</h2>
<table>
  <thead><tr>
    <th>File</th><th>Nodes</th><th>Edges</th><th>Density</th>
    <th>Avg degree</th><th>Clustering</th><th>Components</th><th>Diameter</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>
<h2>Degree distribution</h2>
<canvas id="degChart"></canvas>
<script>
new Chart(document.getElementById('degChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(chart_labels)},
    datasets: {json.dumps(chart_datasets)}
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ labels: {{ color: '#e0e0ff' }} }} }},
    scales: {{ x: {{ ticks: {{ color: '#e0e0ff' }} }}, y: {{ ticks: {{ color: '#e0e0ff' }} }} }} }}
}});
</script>
{hub_sections}
</body></html>"""


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_metrics_report.py <job_id>", file=sys.stderr)
        sys.exit(1)

    job_id = int(sys.argv[1])
    entries = _fetch_metrics(job_id)
    if not entries:
        print(f"No metrics found for job {job_id}.", file=sys.stderr)
        sys.exit(1)

    html = _render_html(job_id, entries)
    report_path = f"graphJobs/{job_id}/metrics_report.html"
    ftpConnector.storeFile(report_path, io.BytesIO(html.encode('utf-8')), 'Graph')
    print(f"Report saved to FTP: {report_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it parses cleanly**

```bash
python -c "import ast; ast.parse(open('dags/tools/generate_metrics_report.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dags/tools/__init__.py dags/tools/generate_metrics_report.py
git commit -m "feat: add on-demand metrics report generator script"
```

---

## Task 9: Run full test suite and final commit

- [ ] **Step 1: Run all tests**

```bash
pytest dags/tests/ -v
```
Expected output (all tests passing):
```
dags/tests/test_anaphora_resolver.py::test_dispatcher_defaults_to_lapin_liass PASSED
dags/tests/test_anaphora_resolver.py::test_dispatcher_lapin_liass_explicit PASSED
dags/tests/test_anaphora_resolver.py::test_dispatcher_unknown_name_falls_back_to_lapin_liass PASSED
dags/tests/test_graph_builder.py::... PASSED (existing tests)
dags/tests/test_graph_metrics.py::... PASSED (10 tests)
dags/tests/test_graph_visualizer.py::... PASSED (5 tests)
```
The SpacyNeural test (`test_dispatcher_spacy_neural_returns_correct_shape`) requires the `en_coreference_web_trf` model; skip with `-k "not spacy_neural"` if the model is not installed in the test environment.

- [ ] **Step 2: Verify all new DAG files parse cleanly**

```bash
python -c "
import ast
for f in [
    'dags/start-graph-formation-job-dag.py',
    'dags/resolve-anaphora-dag.py',
    'dags/finalize-job-dag.py',
]:
    ast.parse(open(f).read())
    print(f, 'OK')
"
```
Expected: 3 lines each ending in `OK`.

- [ ] **Step 3: Final commit**

```bash
git add -A
git status  # confirm only expected files
git commit -m "feat: complete graph enhancements — metrics, visualization, anaphora selector"
```
