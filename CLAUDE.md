# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Text Corpuses Processing Pipeline** — An Apache Airflow-based system for crawling scientific papers, downloading PDFs, extracting text, and building semantic knowledge graphs. The pipeline integrates three official API-based paper downloaders (arXiv, PubMed, Semantic Scholar) with web scrapers, and offers three graph-building backends (rule-based NLP, local HuggingFace LLM, and Yandex Cloud hierarchical LLM).

**Technology Stack:**
- Apache Airflow 2 for orchestration
- Python 3.10+
- SQL Server (TextCorpuses database)
- FTP server for artifact storage
- spaCy + NLTK for rule-based NLP
- HuggingFace transformers (for llm_v2 backend)
- OpenAI-compatible API client (for hierarchical backend)

**Platform:** Windows (SQL Server Express, PowerShell environment)

---

## Build & Run Commands

### Prerequisites

```
pip install apache-airflow pyodbc requests beautifulsoup4 pypdf spacy nltk
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
```

Install optional LLM backends:
```
pip install -r dags/llm_v2/requirements.txt                    # HuggingFace
pip install -r dags/hierarchical_llm_version/requirements.txt  # Yandex Cloud
```

### Configuration

1. **dags/configs/configs.json** — SQL Server connection string, FTP credentials, API keys
2. **dags/configs/search_configs.json** — Search criteria for arXiv, PubMed, Semantic Scholar

### Database Setup

Apply migrations in order (SSMS):
```
Database/database-v0.1.sql
Database/database-v0.2-pdf-to-latex.sql
Database/database-v0.3.sql
Database/database-v0.4.sql
```

### Running Tests

```
pytest dags/tests/
pytest dags/tests/test_paper_downloader.py::test_advance_increments_page_when_has_more -v
```

---

## Architecture

### DAG Pipeline (18 DAGs organized in 4 stages)

**1. Proxy Management (3 DAGs)** — Fetches from geonode.com, proxydb.net, BrightData

**2. URL Discovery (6 DAGs)**
- Web scrapers: arxiv, springer, cyberleninka
- API downloaders: arxiv, pubmed, semantic_scholar (service_id: 4, 5, 6)
- Stateful pagination via ServiceState DB, shared logic in `paperDownloader.py`

**3. PDF & Text (2 DAGs)** — pdf_downloading, pdf_conversion

**4. Graph Construction (7 DAGs)**
```
start_tree_formation_job (manual)
  → prepare_graph_construction_job (register files)
  → resolve_anaphora (coreference resolution)
  → [exactly ONE of]:
     ├─ build_graph (rule-based spaCy)
     ├─ build_graph_llm_v2 (local HuggingFace)
     └─ build_graph_hierarchical (Yandex Cloud)
  → finalize_job
```

Status codes: 0→10→20→30, or 99 (error)

### Graph Building Backends

**Rule-Based:** spaCy dependency parsing → subject–verb–object triplets. Single `graph.json`.

**LLM v2:** Local HuggingFace (Qwen2-1.5B default). Per-file: `llm_v2/{fileId}/{raw_graph.json, clustered_graph.json}`

**Hierarchical:** Yandex Cloud async API. Multi-level hierarchy + context-aware extraction. Per-file: `hierarchical/{fileId}/{raw_graph, clustered_graph, hierarchy_tree}.json`

### Database

TextCorpuses SQL Server tables: `IPProxy`, `PdfDocuments`, `LatexDocuments`, `GraphConstructionJob`, `GraphConstructionFiles`, `ServiceState`

`databaseConnector.py`: Static methods wrapping stored procedures.

### FTP Layout

```
arxiv/, springer/, cyberleninka/ — downloaded PDFs
Tex/                             — converted text files
graphJobs/{jobId}/anaphora/{fileId}.txt     — resolved text
graphJobs/{jobId}/graph.json               — rule-based output
graphJobs/{jobId}/llm_v2/{fileId}/         — LLM v2 output
graphJobs/{jobId}/hierarchical/{fileId}/   — hierarchical output
```

---

## Key Modules

**`paperDownloader.py`**
- `load_search_config`, `load_state`/`save_state`/`clear_state`
- `advance_state(state, criteria, has_more)` — state machine for pagination
- `get_proxy`, `mark_proxy_broken`, `save_urls`
- `run_search(service_id, source, adapter_fn)` — main entry point
- Adapter signature: `(criterion, page, proxy) → (urls, has_more)`

**`graphBuilder.py`**
- `extract_graph_edges(text) → [(agent_1, agent_2, meaning), ...]`
- `merge_graph(graph, new_edges)` — incremental with weights

**`anaphoraResolverLapinLiass.py`**
- `resolve_and_substitute(text) → (resolved_text, stats, debug)`

**`dags/hierarchical_llm_version/`**
- `pipeline.py` (orchestrator), `models/` (async LLMClient, Embedder), `schemas/` (Pydantic models), `stages/` (pipeline components)
- `config.yaml`, `prompts/` (Russian-language prompts)

---

## search_configs.json

```json
{
  "arxiv": [
    {"query": "neural networks", "categories": ["cs.LG"],
     "date_from": "2022-01-01", "max_results": 500, "repeat": true}
  ],
  "pubmed": [
    {"query": "CRISPR", "date_from": "2020-01-01",
     "open_access_only": true, "repeat": false}
  ],
  "semantic_scholar": [
    {"query": "knowledge graph", "fields_of_study": ["CS"],
     "min_citations": 10, "repeat": true}
  ]
}
```

Behavior:
- `repeat: true` → restart from page 1 after exhausting results
- `repeat: false` → one-shot, added to `done_criteria` after completion
- State recovery: exhausted criteria are skipped on resume
- Proxy failure: state is NOT changed; Airflow retries the task

---

## Backend Selection

Only one active graph-building DAG at a time:
- Enable exactly one of: `build_graph`, `build_graph_llm_v2`, `build_graph_hierarchical`
- Selected via `start_tree_formation_job`: RuleBased / LLMv2 / Hierarchical
- Each backend writes to a separate FTP path

---

## Testing

**`test_paper_downloader.py`** — State transitions, pagination, repeat/once behavior, edge cases

**`test_graph_builder.py`** — Merging, deduplication, weights, triple extraction

Run all: `pytest dags/tests/`

---

## Common Tasks

**Add a scientific paper source:**
1. New DAG (copy `download-arxiv-scientific-dag.py`)
2. Implement `fetch_page(criterion, page, proxy) → (urls, has_more)`
3. Call `paperDownloader.run_search(service_id, source, fetch_page)`
4. Add criteria to `search_configs.json`, assign a unique `service_id`

**Modify graph extraction:**
- Rule-based: edit `_get_syntactic_relations()` in `graphBuilder.py`
- LLM v2: update `dags/llm_v2/` stages or `ProcessorConfig`
- Hierarchical: update `dags/hierarchical_llm_version/stages/` or `config.yaml`

**Monitor pipeline progress:**
```sql
SELECT j.ID, j.Status, j.LastStatusChangeAt,
       COUNT(f.ID) AS TotalFiles,
       SUM(CASE WHEN f.Status = 0  THEN 1 END) AS Pending,
       SUM(CASE WHEN f.Status = 10 THEN 1 END) AS AnaphDone,
       SUM(CASE WHEN f.Status = 20 THEN 1 END) AS GraphDone,
       SUM(CASE WHEN f.Status = 99 THEN 1 END) AS Errors
FROM dbo.GraphConstructionJob j
LEFT JOIN dbo.GraphConstructionFiles f ON f.GraphConstructionJobId = j.ID
GROUP BY j.ID, j.Status, j.LastStatusChangeAt
ORDER BY j.ID DESC;
```
