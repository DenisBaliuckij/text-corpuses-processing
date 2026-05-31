# Graph Enhancements Design

**Date:** 2026-05-31  
**Features:** Graph visualization, graph metrics, switchable anaphora resolver

---

## Overview

Three additions to the existing Airflow-based text-corpus-to-graph pipeline:

1. **Switchable anaphora resolver** — choose between the existing rule-based Lapin-Liass algorithm or a spaCy transformer-based neural resolver at job start time.
2. **Graph metrics** — automatic per-graph `metrics.json` saved alongside graph files on FTP after job completion, plus an on-demand rich HTML report.
3. **Graph visualization** — automatic per-graph `visualization.html` (pyvis force-directed, self-contained, offline) saved to FTP after job completion.

All three are integrated into existing DAGs with no database schema changes.

---

## 1. Switchable Anaphora Resolver

### Parameter

`start_tree_formation_job` DAG gets a new Airflow parameter:

```python
"anaphoraResolverName": Param(
    default="LapinLiass",
    enum=["LapinLiass", "SpacyNeural"],
    title="Anaphora resolver"
)
```

Stored in `GraphConstructionJob.ProcessorConfig` JSON:

```json
{"processorName": "RuleBased", "anaphoraResolverName": "SpacyNeural"}
```

Existing jobs with no `anaphoraResolverName` key default to `"LapinLiass"` — no migration needed.

### New files

**`dags/anaphoraResolverSpacyNeural.py`**

Same public interface as `anaphoraResolverLapinLiass.py`:

```python
def resolve_and_substitute(text: str, mark: bool = False) -> tuple[str, list, list]
```

Implementation:
- Loads `spacy.load("en_coreference_web_trf")` (singleton, loaded once per process).
- Processes text → `doc.spans` contains coreference clusters keyed `"coref_clusters_N"`.
- For each cluster: first span is the head antecedent; all other mentions are replaced with the head's surface form.
- Returns `(resolved_text, substitutions, resolutions)` in the same dataclass shapes as the Lapin-Liass module.

**`dags/anaphoraResolver.py`**

Thin dispatcher:

```python
def resolve_and_substitute(text: str, resolver_name: str = "LapinLiass", mark: bool = False):
    if resolver_name == "SpacyNeural":
        from anaphoraResolverSpacyNeural import resolve_and_substitute as _resolve
    else:
        from anaphoraResolverLapinLiass import resolve_and_substitute as _resolve
    return _resolve(text, mark=mark)
```

### Modified DAG: `resolve-anaphora-dag.py`

Two changes to the `resolve_anaphora` task:

1. After retrieving `(file_id, file_path, job_id)`, call `databaseConnector.getProcessorConfig(job_id)` and parse `anaphoraResolverName` (default `"LapinLiass"`).
2. Replace the direct import of `anaphoraResolverLapinLiass` with a call to `anaphoraResolver.resolve_and_substitute(text, resolver_name=resolver_name)`.

### Installation (SpacyNeural only)

```bash
pip install "spacy[transformers]"
python -m spacy download en_coreference_web_trf
```

---

## 2. Graph Metrics

### New module: `dags/graphMetrics.py`

```python
def compute_metrics(graph_dict: dict, backend: str) -> dict
```

Converts the graph to a `networkx.Graph` (normalizing all three backend formats), then computes:

| Key | Description |
|-----|-------------|
| `node_count` | Total nodes |
| `edge_count` | Total edges |
| `density` | `2E / N(N-1)` |
| `avg_degree` | Mean node degree |
| `max_degree` | Maximum node degree |
| `min_degree` | Minimum node degree |
| `avg_clustering_coefficient` | Average local clustering coefficient |
| `connected_components` | Number of weakly connected components |
| `largest_component_fraction` | LCC size / total nodes |
| `diameter` | Longest shortest path (skipped with `null` if disconnected or N > 500) |
| `avg_shortest_path` | Average shortest path length (same guard as diameter) |
| `top_10_hubs` | `[{"label": str, "degree": int}, ...]` |
| `degree_distribution` | `{"1": count, "2": count, ...}` histogram |

**Format normalization:**
- RuleBased: nodes from `graph["nodes"]` (strings), edges from `graph["edges"]` (`agent_1`/`agent_2`/`weight`)
- LLMv2 / Hierarchical: nodes from `graph["nodes"]` (`id`/`label`), edges from `graph["edges"]` (`source`/`target`/`weight`). Uses `clustered_graph.json` when available (preferred over `raw_graph.json`).

### Automatic output (via `finalize_job` DAG)

| Backend | Path |
|---------|------|
| RuleBased | `graphJobs/{jobId}/metrics.json` |
| LLMv2 | `graphJobs/{jobId}/llm_v2/{fileId}/metrics.json` |
| Hierarchical | `graphJobs/{jobId}/hierarchical/{fileId}/metrics.json` |

### On-demand report: `dags/tools/generate_metrics_report.py`

```bash
python dags/tools/generate_metrics_report.py <job_id>
```

- Reads `ProcessorConfig` via `databaseConnector.getProcessorConfig(job_id)` to determine backend and FTP paths.
- Fetches all `metrics.json` files for the job from FTP.
- Produces a self-contained `metrics_report.html` with:
  - Per-file metrics table
  - Degree distribution bar chart (Chart.js inline, no CDN)
  - Top-10 hub nodes table per file
  - Summary stats across all files (aggregate node/edge counts, mean density)
- Saves to `graphJobs/{jobId}/metrics_report.html` on FTP and prints the local path.

---

## 3. Graph Visualization

### New module: `dags/graphVisualizer.py`

```python
def generate_visualization(graph_dict: dict, backend: str) -> str
```

Returns a self-contained HTML string (vis.js embedded inline — no internet required).

Uses `pyvis.network.Network` with:
- **Node size** proportional to degree (min 10, max 40).
- **Edge width** proportional to weight.
- **Hover tooltip** shows node label and degree; edge tooltip shows relation label and weight.
- **Hierarchical backend**: node color intensity reflects `importance` score.
- Force-directed physics (Barnes-Hut), zoom/pan/drag enabled.

Graph format normalization follows the same logic as `graphMetrics.py`.

### Automatic output (via `finalize_job` DAG)

| Backend | Path |
|---------|------|
| RuleBased | `graphJobs/{jobId}/visualization.html` |
| LLMv2 | `graphJobs/{jobId}/llm_v2/{fileId}/visualization.html` |
| Hierarchical | `graphJobs/{jobId}/hierarchical/{fileId}/visualization.html` |

---

## 4. Integration — Modified DAGs & DB Connector

### `finalize-job-dag.py`

After `finalizeCompletedJobs()` returns a `job_id`:

1. Call `databaseConnector.getProcessorConfig(job_id)` to read `processorName`.
2. Call `databaseConnector.getFilesForJob(job_id)` to enumerate completed file IDs.
3. For each file (or job-level for RuleBased):
   - **RuleBased:** fetch `graphJobs/{jobId}/graph.json` — one graph for the whole job.
   - **LLMv2 / Hierarchical:** fetch `clustered_graph.json` per file (preferred over `raw_graph.json`).
   - Call `graphMetrics.compute_metrics(graph_dict, backend)` → save `metrics.json` to FTP.
   - Call `graphVisualizer.generate_visualization(graph_dict, backend)` → save `visualization.html` to FTP.
4. Errors in metrics/visualization are logged but do not change the job's Status=30 — finalization is not rolled back.

### New DB connector methods (no schema changes)

```python
def getProcessorConfig(job_id: int) -> str:
    # SELECT ProcessorConfig FROM GraphConstructionJob WHERE ID = ?
    # Returns JSON string

def getFilesForJob(job_id: int) -> list[tuple[int]]:
    # SELECT ID FROM GraphConstructionFiles
    # WHERE GraphConstructionJobId = ? AND Status = 20
```

### Summary of all file changes

| File | Action |
|------|--------|
| `dags/anaphoraResolverSpacyNeural.py` | **New** |
| `dags/anaphoraResolver.py` | **New** |
| `dags/graphMetrics.py` | **New** |
| `dags/graphVisualizer.py` | **New** |
| `dags/tools/generate_metrics_report.py` | **New** |
| `dags/start-graph-formation-job-dag.py` | Add `anaphoraResolverName` param |
| `dags/resolve-anaphora-dag.py` | Use dispatcher, read config |
| `dags/finalize-job-dag.py` | Add metrics + visualization generation |
| `dags/dbConnector.py` | Add `getProcessorConfig`, `getFilesForJob` |

### Dependencies

```bash
pip install pyvis networkx
pip install "spacy[transformers]"          # SpacyNeural resolver only
python -m spacy download en_coreference_web_trf  # SpacyNeural resolver only
```

---

## Testing

- `test_graph_metrics.py` — unit tests for `compute_metrics` across all three backend formats; verify diameter guard (N > 500 returns `null`); verify degree distribution shape.
- `test_graph_visualizer.py` — verify `generate_visualization` returns valid HTML string containing `vis.js` for each backend format.
- `test_anaphora_resolver.py` — verify dispatcher routes correctly; verify SpacyNeural module returns same tuple shape as Lapin-Liass.
