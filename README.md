# Text Corpuses Processing Pipeline

An Apache Airflow-based pipeline for crawling scientific text corpuses, downloading PDFs, converting them to text, and building semantic knowledge graphs from the extracted content. Three targeted scientific paper downloaders (arXiv API, PubMed, Semantic Scholar) complement the existing web scrapers, alongside a dedicated Gujarati-language corpus pipeline (literature, news/periodicals, and natural/social science theses). Three graph-building backends are available: a fast rule-based NLP engine, a local HuggingFace LLM pipeline, and a hierarchical Yandex Cloud LLM pipeline.

---

## Overview

The pipeline automates the full journey from raw web sources to a structured semantic graph:

1. **Proxy management** — maintains a validated pool of HTTP proxies for scraping and downloading, plus an optional shared paid proxy
2. **URL crawling** — discovers PDF links via web scrapers (arXiv, Springer, CyberLeninka), official scientific APIs (arXiv API, PubMed, Semantic Scholar), and Gujarati-language sources (Internet Archive, Shodhganga)
3. **PDF downloading** — downloads discovered PDFs, optionally through proxies
4. **PDF-to-text conversion** — extracts plain text from PDFs
5. **Graph construction** — resolves anaphora, builds a semantic graph using one of three backends, then automatically computes graph metrics and generates an interactive visualization

Each stage is an independent Airflow DAG that does exactly one unit of work per run. All job and file state is tracked in SQL Server. File content (PDFs, text, graphs) is stored on an FTP server.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Apache Airflow Scheduler              │
└──────────┬──────────────────────────────────────────┘
           │
    ┌──────▼───────┐     ┌─────────────────────────┐     ┌──────────────────┐
    │  get_proxies │     │ URL discovery (6 DAGs)  │     │  pdf_downloading │
    │  (3 DAGs)    │     │ 3 scrapers + 3 API DAGs │     │                  │
    └──────────────┘     └────────────┬────────────┘     └────────┬─────────┘
                                │                      │
                         PdfDocuments             PdfDocuments
                         (DB table)               (FTP storage)
                                                       │
                                              ┌────────▼─────────┐
                                              │  pdf_conversion  │
                                              └────────┬─────────┘
                                                       │
                                              LatexDocuments (FTP)
                                                       │
              ┌────────────────────────────────────────▼──────────────────────────────────────┐
              │                          Graph Construction Pipeline                            │
              │                                                                                │
              │  start_tree_formation_job  →  prepare_graph_construction_job                   │
              │              ↓                                                                 │
              │        resolve_anaphora                                                        │
              │              ↓  (file Status=10: resolved text ready on FTP)                  │
              │   ┌──────────┴───────────────────────────────────┐                            │
              │   │                   │                           │                            │
              │   ▼                   ▼                           ▼                            │
              │ build_graph     build_graph_llm_v2   build_graph_hierarchical                  │
              │ (rule-based     (local HuggingFace   (Yandex Cloud API,                        │
              │  spaCy NLP)      LLM pipeline)        hierarchical pass)                       │
              │   └──────────┬───────────────────────────────────┘                            │
              │              ↓ (file Status=20: graph saved to FTP)                           │
              │          finalize_job  →  metrics.json + visualization.html  →  job Status=30  │
              └────────────────────────────────────────────────────────────────────────────────┘
```

> **Note:** Enable exactly one graph-building DAG at a time (`build_graph`, `build_graph_llm_v2`, or `build_graph_hierarchical`). All three compete for the same file queue (Status=10 files) and produce output in different FTP sub-paths under `graphJobs/{jobId}/`.

### Module structure

| Module | Purpose |
|--------|---------|
| `configs.py` | Self-contained config loader (reads `dags/configs/configs.json` relative to `__file__`) |
| `repositories/` | 5 domain DB repositories: `ProxyRepository`, `PdfRepository`, `LatexRepository`, `GraphJobRepository`, `ServiceStateRepository` |
| `ftpConnector.py` | FTP upload / download / file listing (30s socket timeout on every connection) |
| `paperDownloader.py` | Crawl state machine, proxy and URL helpers |
| `proxyValidator.py` | Shared free-proxy validator: tests each candidate against a real HTTPS target (arxiv.org) with a latency cutoff and content check before importing it |
| `archiveOrgDownloader.py` | Internet Archive `advancedsearch` API adapter (Gujarati literature + news) |
| `shodhgangaDownloader.py` | Shodhganga (INFLIBNET) thesis repository adapter (Gujarati science) |
| `pdfConverter.py` | PDF → plain text extraction (parallelized, `ThreadPoolExecutor`) |
| `graphBuilder.py` | Rule-based SVO triplet extraction |
| `graphMetrics.py` | networkx graph statistics |
| `graphVisualizer.py` | pyvis interactive HTML visualization |
| `anaphoraResolver*.py` | Anaphora resolution dispatcher + LapinLiass + SpacyNeural backends |

---

## DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `get_proxies_for_calls` | `@continuous` | Fetches and validates proxies from geonode.com |
| `get_proxies_for_calls_2` | `@continuous` | Fetches and validates proxies from proxydb.net |
| `get_proxies_for_calls_3` | `@continuous` | Fetches and validates proxies from the ProxyScrape API |
| `get_proxies_for_calls_4` | `@continuous` | Fetches and validates proxies from free-proxy-list.net |
| `update-brightdata-proxy` | every 5 min | Keeps the shared paid (BrightData) proxy entry current — pause this DAG to exclude it entirely |
| `get_arxiv_urls` | `@continuous` | Scrapes arXiv search pages for PDF URLs |
| `get_springer_urls` | `@continuous` | Scrapes Springer for open-access PDF URLs |
| `get_lenin_urls` | `@continuous` | Scrapes CyberLeninka for PDF URLs |
| `download_arxiv_scientific` | `@continuous` | arXiv API — keyword + category + date search |
| `download_pubmed` | `@continuous` | PubMed E-utilities — keyword + date + open-access search |
| `download_semantic_scholar` | `@continuous` | Semantic Scholar API — keyword + field + citation filter |
| `download_gujarati_literature` | `@continuous` | Internet Archive — Gujarati-language books, excluding periodicals |
| `download_gujarati_news` | `@continuous` | Internet Archive — Gujarati-language newspapers/magazines |
| `download_gujarati_science_natural` | `@continuous` | Shodhganga — Gujarati-language theses in Science/Physics/Chemistry |
| `download_gujarati_science_social` | `@continuous` | Shodhganga — Gujarati-language theses in Social Science/Economics/Sociology |
| `pdf_downloading` | `@continuous` | Downloads PDFs from discovered URLs |
| `pdf_conversion` | `@continuous` | Converts PDFs to plain text |
| `start_tree_formation_job` | manual trigger | Creates a new graph construction job |
| `prepare_graph_construction_job` | `@continuous` | Registers text files for a job |
| `resolve_anaphora` | `@continuous` | Resolves anaphora in one text file |
| `build_graph` | `@continuous` | Rule-based NLP (spaCy), incremental graph |
| `build_graph_llm_v2` | `@continuous` | Local HuggingFace LLM pipeline |
| `build_graph_hierarchical` | `@continuous` | Hierarchical Yandex Cloud LLM pipeline |
| `finalize_job` | `@continuous` | Marks a job complete; generates `metrics.json` and `visualization.html` per graph |

---

## Scientific Paper Downloaders

Three API-based DAGs download targeted scientific paper URLs using official APIs. Unlike the web scrapers (which crawl indiscriminately), these DAGs search by keyword, field, date range, and access type.

### Configuration — `dags/configs/search_configs.json`

Search criteria are defined in a JSON file with one key per source. Each criterion object may include:

| Field | Sources | Description |
|-------|---------|-------------|
| `query` | all | Free-text keyword search |
| `categories` | arXiv only | arXiv category codes, e.g. `["cs.AI", "cs.LG"]` |
| `fields_of_study` | Semantic Scholar only | Field names, e.g. `["Computer Science", "Biology"]` |
| `date_from` / `date_to` | all | `YYYY-MM-DD` (Semantic Scholar uses year only) |
| `open_access_only` | all | Skip papers without a freely available PDF |
| `min_citations` | Semantic Scholar only | Minimum citation count filter |
| `max_results` | all | Cap on total URLs collected for this criterion |
| `repeat` | all | `true` = restart from page 1 when done; `false` = run once then skip |

Example:
```json
{
  "arxiv": [
    {"query": "graph neural networks", "categories": ["cs.LG", "cs.AI"],
     "date_from": "2022-01-01", "date_to": "2025-12-31", "max_results": 500, "repeat": true}
  ],
  "pubmed": [
    {"query": "CRISPR gene editing", "date_from": "2020-01-01",
     "open_access_only": true, "max_results": 300, "repeat": false}
  ],
  "semantic_scholar": [
    {"query": "knowledge graph reasoning", "fields_of_study": ["Computer Science"],
     "min_citations": 10, "max_results": 400, "repeat": true}
  ]
}
```

Edit this file at any time — the DAG picks up changes on its next restart.

### State management

Crawl state (current criterion index, current page, exhausted criteria) is stored in the `ServiceState` DB table. `serviceID` assignments:

| serviceID | DAG |
|-----------|-----|
| 4 | `download_arxiv_scientific` |
| 5 | `download_pubmed` |
| 6 | `download_semantic_scholar` |

### API details

| DAG | API endpoint | Page size | Key required |
|-----|-------------|-----------|-------------|
| `download_arxiv_scientific` | `export.arxiv.org/api/query` (Atom XML) | 50 | No |
| `download_pubmed` | NCBI E-utilities `esearch` + `efetch` | 50 | No (≤3 req/s) |
| `download_semantic_scholar` | `api.semanticscholar.org/graph/v1/paper/search` | 100 | Optional — set `SemanticScholarApiKey` in `configs.json` |

PubMed returns PMC open-access PDF URLs: `https://www.ncbi.nlm.nih.gov/pmc/articles/{PMCID}/pdf/`. Papers without a PMC ID are skipped.

Semantic Scholar returns `openAccessPdf.url` from the response. Papers filtered by `min_citations` or `open_access_only` are skipped silently.

---

## Proxy Pool & Validation

`IPProxy` (+ `ProxyProtocols` / `relIpProxyProxyProtocols`) holds the shared proxy pool used by every scraper and by `pdf_downloading`.

### Sources

Four DAGs continuously import free proxies, each running candidates through `proxyValidator.validate_and_import()` before trusting them:

1. Fetch a candidate list from the source (geonode.com, proxydb.net, the ProxyScrape API, or free-proxy-list.net)
2. Test each candidate concurrently with a real HTTPS request to **arxiv.org itself** (the actual target the pipeline needs) — reject if the response is slow (>5s), non-200, or doesn't contain the expected page content
3. Only proxies that pass are written to `IPProxy` via `AddOrUpdateProxy`

Testing against a real target with certificate verification (the `requests` default) also naturally rejects proxies that intercept/MITM traffic with a self-signed certificate, rather than a generic reachability check.

A fifth DAG, `update-brightdata-proxy`, keeps a single shared **paid** proxy current by re-upserting it with an artificial far-future `lastChecked` timestamp every 5 minutes — this is optional infrastructure; **pause the DAG to exclude it from selection entirely** (its row will age out via the normal broken-proxy path and won't be reinserted while paused).

### Selection

- `ProxyRepository.get_latest()` → `GetLatestProxy`: the single non-broken proxy with the highest `SuccessCount`, falling back to `lastChecked` for ties. Used by every URL-discovery DAG and by `pdf_downloading` for all sources.
- `ProxyRepository.get_latest_free()` → `GetLatestFreeProxy`: identical, but excludes any proxy whose `IP` contains `@` (the paid proxy is stored as a `user:pass@host` string, unlike free proxies' plain dotted IPs). Available for callers that need to bypass the paid proxy specifically; not currently wired into any DAG.

### Success tracking & broken-proxy handling

- `ProxyRepository.mark_success(ip)` → `MarkProxySuccess`: called after every real successful download. Increments `IPProxy.SuccessCount`, so proxies with a proven track record are preferred over untested ones — a working proxy naturally becomes the "champion" and gets reused.
- `ProxyRepository.mark_broken(ip)` → `MarkProxyAsBroken`: **deletes** the proxy row outright (not a soft flag). Only called on an actual `requests.exceptions.ProxyError` (a real proxy-connection failure) — not on generic timeouts, SSL errors, or connection resets, which are usually the *target site's* fault, not the proxy's.

### Known-excluded sources

`pdf_downloading` currently skips Springer URLs entirely (returned to the queue, not marked failed) due to a known Springer-specific issue — `GetPdfToDownload` also filters them out at the query level, since otherwise a stuck Springer URL sitting at the head of its unordered `SELECT TOP 1` scan silently blocks every other download behind it. Re-enable both together once the issue is resolved.

---

## Gujarati Corpus Pipeline

Four DAGs build a categorized Gujarati-language text corpus, feeding the same `PdfDocuments` queue and `pdf_downloading`/`pdf_conversion` DAGs as every other source — no separate tables. Since `PdfDocuments` has no category column, each discovered URL carries its category as a URL fragment (e.g. `#gujarati_literature`, never sent over the wire) so `pdf_downloading` can route it to a dedicated FTP subfolder.

| Category | Source | Query strategy |
|----------|--------|-----------------|
| Literature | Internet Archive | `language:(guj)`, excluding newspaper/magazine subjects |
| News / periodicals | Internet Archive | `language:(guj)` with newspaper or magazine subjects (e.g. the long-running *Kumar* periodical) |
| Natural sciences | Shodhganga | `language=Gujarati`, subject contains Science / Physics / Chemistry |
| Social sciences | Shodhganga | `language=Gujarati`, subject contains Social Science / Economics / Sociology |

Shodhganga splits each thesis into one PDF per chapter (title page, declaration, chapter01, ... bibliography) rather than a single combined file — `shodhgangaDownloader.py` collects every bitstream PDF for a thesis, not just the first.

Both science DAGs bypass the proxy pool entirely (`use_proxy=False`) — Shodhganga and Internet Archive are public, non-paywalled repositories that don't need IP rotation, and a direct connection avoids adding proxy-pool load for no benefit.

---

## Graph Construction Job Lifecycle

```
start_tree_formation_job (manual)
        │  params: textProcessorName, anaphoraResolverName
        ▼ job status=0
prepare_graph_construction_job
        │ lists .tex files from FTP, registers in GraphConstructionFiles
        ▼ job status=10, files status=0
resolve_anaphora  (per file: 0→5→10)
        │  resolver selected from ProcessorConfig: LapinLiass (default) or SpacyNeural
        ▼ files status=10  ← one of the three graph-building DAGs picks up here
build_graph / build_graph_llm_v2 / build_graph_hierarchical  (per file: 10→15→20)
        │
        ▼ all files status=20
finalize_job
        │  generates metrics.json + visualization.html per graph
        ▼ job status=30 (completed)
```

### Job status codes (`GraphConstructionJob.Status`)

| Code | Meaning |
|------|---------|
| 0 | Created, awaiting preparation |
| 5 | Preparation in progress |
| 10 | Files registered, awaiting processing |
| 20 | Graph building in progress |
| 30 | Completed |
| 99 | Error |

### File status codes (`GraphConstructionFiles.Status`)

| Code | Meaning |
|------|---------|
| 0 | Pending anaphora resolution |
| 5 | Anaphora resolution in progress |
| 10 | Anaphora done, ready for graph building |
| 15 | Graph building in progress |
| 20 | Done |
| 99 | Error |

---

## Anaphora Resolution

Before graph building, each text file goes through coreference resolution via the `resolve_anaphora` DAG. The resolver is selected per-job via the `anaphoraResolverName` parameter in `start_tree_formation_job`:

| Value | Module | Description |
|-------|--------|-------------|
| `LapinLiass` (default) | `anaphoraResolverLapinLiass.py` | Rule-based salience scoring (recency + grammatical role + proper-noun bonus) using spaCy `en_core_web_sm` |
| `SpacyNeural` | `anaphoraResolverSpacyNeural.py` | Transformer-based coreference via `en_coreference_web_trf`; requires Python 3.8–3.10 + spaCy 3.4–3.5 |

Both resolvers share the same interface and return identical data structures (`Substitution`, `Resolution` dataclasses), so the rest of the pipeline is unaffected by the choice.

---

## Graph Building Backends

Three backends are available. Enable exactly one at a time in the Airflow UI.

### 1. Rule-based (`build_graph`)

Uses spaCy (`en_core_web_lg`) and NLTK to extract syntactic relations (subject–verb–object) and build an incremental graph per job. Lightweight, no external APIs required.

Output: `graphJobs/{jobId}/graph.json` — a single graph grown across all files in the job.

```json
{"nodes": ["concept a", "concept b"],
 "edges": [{"agent_1": "concept a", "agent_2": "concept b", "meaning": "verb", "weight": 3}]}
```

### 2. LLM v2 (`build_graph_llm_v2`)

Located in `dags/llm_v2/`. Uses a local HuggingFace LLM (default: `Qwen2-1.5B-Instruct`) for coreference resolution and triplet extraction, plus `sentence-transformers` for deduplication and clustering.

**Stages:** preprocessing → coreference resolution → chunking → triplet extraction → normalization → deduplication → graph assembly → clustering

**Requirements:** `transformers`, `torch`, `sentence-transformers`, `razdel`/`nltk`

Output per file: `graphJobs/{jobId}/llm_v2/{fileId}/raw_graph.json` and `clustered_graph.json`

Override the default model via `GraphConstructionJob.ProcessorConfig`:
```json
{"processorName": "LLMv2", "llm": {"model_name": "Qwen/Qwen2.5-7B-Instruct", "device": "cuda"}}
```

### 3. Hierarchical (`build_graph_hierarchical`)

Located in `dags/hierarchical_llm_version/`. Uses the Yandex Cloud OpenAI-compatible API (async). Builds a multi-level concept hierarchy from chunk summaries first, then extracts entities and relations with full hierarchical context. Produces richer, more structured graphs.

**Stages:** preprocessing → chunking → pass 1 (chunk summaries + concept hierarchy) → pass 2 (context-aware extraction) → entity resolution → graph assembly → importance filtering / clustering

**Requirements:** `openai`, `sentence-transformers`, `razdel`/`nltk`  
**Environment variable:** `YANDEX_CLOUD_API_KEY`

Output per file: `graphJobs/{jobId}/hierarchical/{fileId}/raw_graph.json`, `clustered_graph.json`, `hierarchy_tree.json`

---

## Graph Analysis

After every job is finalized, the `finalize_job` DAG automatically generates two artifacts per graph:

- **`metrics.json`** — graph statistics computed by `graphMetrics.py` using networkx: node/edge counts, density, degree stats, average clustering coefficient, connected components, largest-component fraction, diameter, average shortest path, top-10 hub nodes, degree distribution
- **`visualization.html`** — self-contained interactive force-directed graph generated by `graphVisualizer.py` using pyvis (vis.js embedded inline, works fully offline). Node size reflects degree; edge width reflects weight. Hierarchical backend additionally maps node importance to color intensity.

For a richer cross-file report, run the on-demand script against any completed job:

```bash
python dags/tools/generate_metrics_report.py <job_id>
```

This fetches all `metrics.json` files for the job, produces `metrics_report.html` with a Chart.js degree-distribution bar chart and per-file hub tables, and saves it back to FTP at `graphJobs/{jobId}/metrics_report.html`.

---

## FTP Layout

```
/
├── arxiv/              ← downloaded arXiv PDFs
├── springer/           ← downloaded Springer PDFs (currently excluded from downloading)
├── cyberleninka/       ← downloaded CyberLeninka PDFs
├── gujarati/
│   ├── literature/     ← Gujarati books (Internet Archive)
│   ├── news/           ← Gujarati newspapers/magazines (Internet Archive)
│   ├── science_natural/  ← Gujarati natural-science theses (Shodhganga)
│   └── science_social/   ← Gujarati social-science theses (Shodhganga)
├── Tex/                ← converted plain text files
└── graphJobs/
    └── {jobId}/
        ├── anaphora/
        │   └── {fileId}.txt        ← anaphora-resolved text (written by resolve_anaphora)
        ├── graph.json              ← rule-based incremental graph (build_graph)
        ├── metrics.json            ← rule-based graph metrics
        ├── visualization.html      ← rule-based interactive visualization
        ├── metrics_report.html     ← on-demand rich report (any backend)
        ├── llm_v2/
        │   └── {fileId}/
        │       ├── raw_graph.json
        │       ├── clustered_graph.json
        │       ├── metrics.json
        │       └── visualization.html
        └── hierarchical/
            └── {fileId}/
                ├── raw_graph.json
                ├── clustered_graph.json
                ├── hierarchy_tree.json
                ├── metrics.json
                └── visualization.html
```

---

## Database

SQL Server database `TextCorpuses` on `LAPTOP-I91584GB\SQLEXPRESS`.

| Table | Purpose |
|-------|---------|
| `IPProxy` + `ProxyProtocols` | Proxy pool |
| `PdfDocuments` | Discovered PDF URLs and download locations |
| `LatexDocuments` | Converted text file locations |
| `GraphConstructionJob` | Graph build jobs with status and config |
| `GraphConstructionFiles` | Per-file tracking within a job |
| `ServiceState` | Persistent state for crawling DAGs |

Apply migrations in order using SSMS:

| Migration | Change |
|-----------|--------|
| `database-v0.1.sql` | Initial schema |
| `database-v0.2-pdf-to-latex.sql` | Adds `LatexDocuments` (PDF → text tracking) |
| `database-v0.3.sql` | Schema updates |
| `database-v0.4.sql` | Schema updates |
| `database-v0.5.sql` | `GetPDFLocationForLatexConvertation`: guard against inserting NULL when the conversion queue is empty |
| `database-v0.6.sql` | Adds a cross-process arXiv rate limiter (superseded, see v0.8) |
| `database-v0.7.sql` | `MarkProxyAsBroken`: fixes a race between concurrent deletes/inserts via explicit transaction + `UPDLOCK`/`HOLDLOCK` |
| `database-v0.8.sql` | Removes the v0.6 rate limiter after reverting to proxy-based arXiv access |
| `database-v0.9.sql` | Adds `GetLatestFreeProxy` — same as `GetLatestProxy` but excludes the shared paid proxy |
| `database-v0.10.sql` | Dedupes `relIpProxyProxyProtocols` (351K → ~6.5K rows), adds a unique index, fixes `AddOrUpdateProxy`'s missing existence check |
| `database-v0.11.sql` | Adds `IPProxy.SuccessCount` + `MarkProxySuccess`; `GetLatestProxy`/`GetLatestFreeProxy` now rank by proven track record first |
| `database-v0.12.sql` | Fixes a v0.11 regression: `AddOrUpdateProxy`'s bare `INSERT ... VALUES` broke for every new proxy after `SuccessCount` was added |
| `database-v0.13.sql` | `GetPdfToDownload`: excludes Springer URLs at the query level (temporary, paired with the Springer exclusion in `pdf-downloading-dag.py`) so a stuck Springer row can't head-of-line-block the whole queue |
| `database-v0.14.sql` | `AddOrUpdateProxy`: rewritten with a single explicit transaction and `UPDLOCK`/`HOLDLOCK` (matching `MarkProxyAsBroken`'s pattern) to fix deadlocks under concurrent proxy-DAG load |

---

## Setup

### Prerequisites

- Python 3.10+
- Apache Airflow 2 with `airflow.sdk`
- SQL Server Express with `TextCorpuses` database
- FTP server accessible at the address in `dags/configs/configs.json`

### Install dependencies

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
sh scripts/install-hooks.sh   # Windows: scripts\install-hooks.bat
```

### Install LLM pipeline dependencies (optional)

For `build_graph_llm_v2` (local HuggingFace models):
```bash
pip install -r dags/llm_v2/requirements.txt
```

For `build_graph_hierarchical` (Yandex Cloud API):
```bash
pip install -r dags/hierarchical_llm_version/requirements.txt
export YANDEX_CLOUD_API_KEY=your_key_here
```

For the `SpacyNeural` anaphora resolver (requires Python 3.8–3.10 + spaCy 3.4–3.5):
```bash
pip install "spacy[transformers]"
python -m spacy download en_coreference_web_trf
```

### Configuration

Edit `dags/configs/configs.json`:

```json
{
  "ConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=...;Database=TextCorpuses;...",
  "FtpHost": "127.0.0.1",
  "FtpPort": 21,
  "FtpUser": "airflow",
  "FtpPassword": "airflow",
  "FtpHostGraph": "127.0.0.1",
  "FtpPortGraph": 21,
  "FtpUserGraph": "...",
  "FtpPasswordGraph": "..."
}
```

### Apply database migrations

Run each `.sql` file in SSMS in order (see Database section above).

### Deploy DAGs

Copy the `dags/` folder to your Airflow DAGs directory (or configure `dags_folder` in `airflow.cfg` to point here). `dags/` is fully self-contained — copy it to any Airflow installation without any files from the repo root.

### Start a graph construction job

In the Airflow UI, trigger `start_tree_formation_job` with:
- **paths**: semicolon-separated FTP paths containing `.tex` files, e.g. `arxiv/;springer/`
- **textProcessorName**: `RuleBased` (use `build_graph`), `AIBased` (use `build_graph_llm_v2` or `build_graph_hierarchical`)
- **anaphoraResolverName**: `LapinLiass` (default, rule-based) or `SpacyNeural` (transformer-based)

Enable only the graph-building DAG that matches your chosen processor. After the job completes, `finalize_job` automatically saves `metrics.json` and `visualization.html` alongside each graph on FTP.

---

## Testing

```bash
pytest dags/tests/ -k "not spacy_neural" -v
```

105 tests, 1 deselected (SpacyNeural — requires Python 3.8–3.10 + spaCy 3.4–3.5).

---

## Monitoring

Track pipeline progress in SSMS:

```sql
SELECT j.ID, j.Status AS JobStatus, j.LastStatusChangeAt,
       COUNT(f.ID) AS TotalFiles,
       SUM(CASE WHEN f.Status = 0 THEN 1 ELSE 0 END) AS Pending,
       SUM(CASE WHEN f.Status = 10 THEN 1 ELSE 0 END) AS AnaphDone,
       SUM(CASE WHEN f.Status = 20 THEN 1 ELSE 0 END) AS GraphDone,
       SUM(CASE WHEN f.Status = 99 THEN 1 ELSE 0 END) AS Errors
FROM dbo.GraphConstructionJob j
LEFT JOIN dbo.GraphConstructionFiles f ON f.GraphConstructionJobId = j.ID
GROUP BY j.ID, j.Status, j.LastStatusChangeAt
ORDER BY j.ID DESC;
```

---

---

# Конвейер обработки текстовых корпусов

Конвейер на базе Apache Airflow для краулинга научных текстовых корпусов, загрузки PDF-файлов, их конвертации в текст и построения семантических графов знаний. Три специализированных загрузчика научных статей (API arXiv, PubMed, Semantic Scholar) дополняют существующие веб-скраперы, наряду с отдельным конвейером для гуджаратиязычного корпуса (художественная литература, новости/периодика, диссертации по естественным и общественным наукам). Доступны три бэкенда построения графа: быстрый движок на основе правил NLP, конвейер с локальной LLM (HuggingFace) и иерархический конвейер с облачной LLM (Яндекс Облако).

---

## Описание

Конвейер автоматизирует полный цикл — от сырых веб-источников до структурированного семантического графа:

1. **Управление прокси** — поддерживает провалидированный пул HTTP-прокси для скрапинга и загрузки, плюс опциональный общий платный прокси
2. **Краулинг URL** — обнаруживает ссылки на PDF через веб-скраперы (arXiv, Springer, КиберЛенинка), официальные API научных публикаций (API arXiv, PubMed, Semantic Scholar) и гуджаратиязычные источники (Internet Archive, Shodhganga)
3. **Загрузка PDF** — скачивает найденные PDF, опционально через прокси
4. **Конвертация PDF в текст** — извлекает текст из PDF-файлов
5. **Построение графа** — разрешает анафору, строит семантический граф одним из трёх бэкендов, затем автоматически вычисляет метрики графа и генерирует интерактивную визуализацию

Каждый этап — отдельный Airflow DAG, выполняющий ровно одну единицу работы за запуск. Состояние всех заданий и файлов хранится в SQL Server. Содержимое файлов (PDF, тексты, графы) хранится на FTP-сервере.

---

## Архитектура

Конвейер состоит из **24 DAG**, разбитых на группы:

- **Прокси:** `get_proxies_for_calls`, `get_proxies_for_calls_2`, `get_proxies_for_calls_3`, `get_proxies_for_calls_4`, `update-brightdata-proxy`
- **Веб-скраперы:** `get_arxiv_urls`, `get_springer_urls`, `get_lenin_urls`
- **API-загрузчики научных статей:** `download_arxiv_scientific`, `download_pubmed`, `download_semantic_scholar`
- **Гуджаратиязычный корпус:** `download_gujarati_literature`, `download_gujarati_news`, `download_gujarati_science_natural`, `download_gujarati_science_social`
- **Загрузка и конвертация:** `pdf_downloading`, `pdf_conversion`
- **Построение графа:** `start_tree_formation_job` (ручной запуск), `prepare_graph_construction_job`, `resolve_anaphora`, **`build_graph`**, **`build_graph_llm_v2`**, **`build_graph_hierarchical`**, `finalize_job`

> **Важно:** Единовременно должен быть включён только один DAG построения графа: `build_graph`, `build_graph_llm_v2` или `build_graph_hierarchical`. Все три конкурируют за одну очередь файлов (Status=10) и сохраняют результаты в разные подпапки FTP.

### Структура модулей

| Модуль | Назначение |
|--------|------------|
| `configs.py` | Самодостаточный загрузчик конфигурации (читает `dags/configs/configs.json` относительно `__file__`) |
| `repositories/` | 5 доменных DB-репозиториев: `ProxyRepository`, `PdfRepository`, `LatexRepository`, `GraphJobRepository`, `ServiceStateRepository` |
| `ftpConnector.py` | Загрузка, скачивание и листинг файлов на FTP (таймаут сокета 30с на каждое соединение) |
| `paperDownloader.py` | Машина состояний краулинга, вспомогательные функции прокси и URL |
| `proxyValidator.py` | Общий валидатор бесплатных прокси: проверяет каждого кандидата реальным HTTPS-запросом (arxiv.org) с ограничением по задержке и проверкой содержимого перед импортом |
| `archiveOrgDownloader.py` | Адаптер API `advancedsearch` Internet Archive (гуджаратская литература и новости) |
| `shodhgangaDownloader.py` | Адаптер репозитория диссертаций Shodhganga (INFLIBNET) (гуджаратская наука) |
| `pdfConverter.py` | Извлечение текста из PDF (распараллелено через `ThreadPoolExecutor`) |
| `graphBuilder.py` | Извлечение триплетов SVO на основе правил |
| `graphMetrics.py` | Статистика графа через networkx |
| `graphVisualizer.py` | Интерактивная HTML-визуализация через pyvis |
| `anaphoraResolver*.py` | Диспетчер разрешения анафоры + бэкенды LapinLiass и SpacyNeural |

---

## Загрузчики научных статей

Три DAG на базе официальных API скачивают ссылки на научные статьи по заданным критериям. В отличие от веб-скраперов, они выполняют целевой поиск по ключевым словам, областям, датам и типу доступа.

### Конфигурация — `dags/configs/search_configs.json`

Критерии поиска задаются в JSON-файле с одним ключом на источник. Каждый критерий может содержать:

| Поле | Источники | Описание |
|------|-----------|----------|
| `query` | все | Поиск по ключевым словам |
| `categories` | только arXiv | Коды категорий arXiv, например `["cs.AI", "cs.LG"]` |
| `fields_of_study` | только Semantic Scholar | Названия областей, например `["Computer Science", "Biology"]` |
| `date_from` / `date_to` | все | `YYYY-MM-DD` (Semantic Scholar использует только год) |
| `open_access_only` | все | Пропустить статьи без свободного PDF |
| `min_citations` | только Semantic Scholar | Минимальное число цитирований |
| `max_results` | все | Ограничение на количество собираемых URL |
| `repeat` | все | `true` — перезапускать с первой страницы; `false` — выполнить один раз |

Пример:
```json
{
  "arxiv": [
    {"query": "graph neural networks", "categories": ["cs.LG", "cs.AI"],
     "date_from": "2022-01-01", "date_to": "2025-12-31", "max_results": 500, "repeat": true}
  ],
  "pubmed": [
    {"query": "CRISPR gene editing", "date_from": "2020-01-01",
     "open_access_only": true, "max_results": 300, "repeat": false}
  ],
  "semantic_scholar": [
    {"query": "knowledge graph reasoning", "fields_of_study": ["Computer Science"],
     "min_citations": 10, "max_results": 400, "repeat": true}
  ]
}
```

Файл можно редактировать в любое время — DAG учитывает изменения при следующем запуске.

### Управление состоянием

Состояние обхода (индекс текущего критерия, номер страницы, исчерпанные критерии) хранится в таблице `ServiceState`. Назначения `serviceID`:

| serviceID | DAG |
|-----------|-----|
| 4 | `download_arxiv_scientific` |
| 5 | `download_pubmed` |
| 6 | `download_semantic_scholar` |

### Детали API

| DAG | Конечная точка API | Размер страницы | Ключ |
|-----|--------------------|-----------------|------|
| `download_arxiv_scientific` | `export.arxiv.org/api/query` (Atom XML) | 50 | Нет |
| `download_pubmed` | NCBI E-utilities `esearch` + `efetch` | 50 | Нет (≤3 зап/с) |
| `download_semantic_scholar` | `api.semanticscholar.org/graph/v1/paper/search` | 100 | Опционально — задайте `SemanticScholarApiKey` в `configs.json` |

PubMed возвращает URL PDF из PMC: `https://www.ncbi.nlm.nih.gov/pmc/articles/{PMCID}/pdf/`. Статьи без PMC ID пропускаются. Semantic Scholar возвращает `openAccessPdf.url`; статьи, не прошедшие фильтры `min_citations` или `open_access_only`, молча пропускаются.

---

## Пул прокси и валидация

Таблицы `IPProxy` (+ `ProxyProtocols` / `relIpProxyProxyProtocols`) хранят общий пул прокси, используемый всеми скраперами и DAG `pdf_downloading`.

### Источники

Четыре DAG непрерывно импортируют бесплатные прокси, пропуская каждого кандидата через `proxyValidator.validate_and_import()` перед тем, как ему довериться:

1. Получить список кандидатов из источника (geonode.com, proxydb.net, API ProxyScrape или free-proxy-list.net)
2. Параллельно проверить каждого кандидата реальным HTTPS-запросом к **самому arxiv.org** (реальной цели, нужной конвейеру) — отклонить, если ответ медленный (>5с), не 200 или не содержит ожидаемого содержимого страницы
3. Только прошедшие проверку прокси записываются в `IPProxy` через `AddOrUpdateProxy`

Проверка на реальной цели с верификацией сертификата (поведение `requests` по умолчанию) также естественным образом отклоняет прокси, перехватывающие трафик через самоподписанный сертификат (MITM), в отличие от простой проверки доступности.

Пятый DAG, `update-brightdata-proxy`, поддерживает актуальность записи единственного общего **платного** прокси, обновляя её каждые 5 минут с искусственно далёкой временной меткой `lastChecked` — это опциональная инфраструктура; **чтобы полностью исключить его из выбора, поставьте DAG на паузу** (его запись устареет через обычный механизм пометки сломанных прокси и не будет пересоздана, пока DAG на паузе).

### Выбор прокси

- `ProxyRepository.get_latest()` → `GetLatestProxy`: единственный неисправный прокси с наивысшим `SuccessCount`, при равенстве — по `lastChecked`. Используется всеми DAG обнаружения URL и `pdf_downloading` для всех источников.
- `ProxyRepository.get_latest_free()` → `GetLatestFreeProxy`: то же самое, но исключает любой прокси, чей `IP` содержит `@` (платный прокси хранится в виде строки `user:pass@host`, в отличие от обычных IP-адресов бесплатных прокси). Доступен для вызывающих, которым нужно обойти платный прокси; пока не подключён ни к одному DAG.

### Учёт успешности и обработка сломанных прокси

- `ProxyRepository.mark_success(ip)` → `MarkProxySuccess`: вызывается после каждой реально успешной загрузки. Увеличивает `IPProxy.SuccessCount`, поэтому прокси с доказанной репутацией предпочитаются непроверенным — рабочий прокси естественным образом становится «чемпионом» и переиспользуется.
- `ProxyRepository.mark_broken(ip)` → `MarkProxyAsBroken`: **удаляет** запись прокси полностью (не мягкий флаг). Вызывается только при настоящей ошибке `requests.exceptions.ProxyError` (реальный сбой соединения с прокси) — не при обычных таймаутах, ошибках SSL или сбросах соединения, которые обычно являются виной *целевого сайта*, а не прокси.

### Временно исключённые источники

`pdf_downloading` сейчас полностью пропускает URL Springer (возвращаются в очередь, не помечаются как ошибочные) из-за известной проблемы, специфичной для Springer — `GetPdfToDownload` также фильтрует их на уровне запроса, поскольку иначе застрявший URL Springer в начале неупорядоченного скана `SELECT TOP 1` молча блокирует все загрузки за ним. Верните оба изменения вместе, когда проблема будет решена.

---

## Гуджаратиязычный корпус

Четыре DAG строят категоризированный гуджаратиязычный текстовый корпус, используя ту же очередь `PdfDocuments` и DAG `pdf_downloading`/`pdf_conversion`, что и все остальные источники — без отдельных таблиц. Поскольку в `PdfDocuments` нет столбца категории, каждый найденный URL несёт свою категорию в виде фрагмента URL (например, `#gujarati_literature`, никогда не передаётся по сети), чтобы `pdf_downloading` мог направить его в соответствующую подпапку FTP.

| Категория | Источник | Стратегия запроса |
|-----------|----------|---------------------|
| Литература | Internet Archive | `language:(guj)`, исключая темы «газета»/«журнал» |
| Новости / периодика | Internet Archive | `language:(guj)` с темами «газета» или «журнал» (например, многолетний журнал *Kumar*) |
| Естественные науки | Shodhganga | `language=Gujarati`, тема содержит Science / Physics / Chemistry |
| Общественные науки | Shodhganga | `language=Gujarati`, тема содержит Social Science / Economics / Sociology |

Shodhganga разбивает каждую диссертацию на отдельный PDF по главам (титульный лист, декларация, глава 1, ... библиография), а не единый файл — `shodhgangaDownloader.py` собирает все PDF-файлы диссертации, а не только первый.

Оба DAG для наук полностью обходят пул прокси (`use_proxy=False`) — Shodhganga и Internet Archive являются публичными репозиториями без платного доступа, не требующими ротации IP, а прямое соединение избегает лишней нагрузки на пул прокси без какой-либо пользы.

---

## Жизненный цикл задания построения графа

```
start_tree_formation_job (ручной запуск)
        │  параметры: textProcessorName, anaphoraResolverName
        ▼ статус задания = 0
prepare_graph_construction_job
        │ перечисляет .tex-файлы с FTP, регистрирует в GraphConstructionFiles
        ▼ статус задания = 10, статус файлов = 0
resolve_anaphora  (для каждого файла: 0→5→10)
        │  резолвер из ProcessorConfig: LapinLiass (по умолчанию) или SpacyNeural
        ▼ статус файлов = 10  ← один из трёх бэкендов подхватывает здесь
build_graph / build_graph_llm_v2 / build_graph_hierarchical  (файл: 10→15→20)
        │
        ▼ все файлы в статусе 20
finalize_job
        │  генерирует metrics.json + visualization.html для каждого графа
        ▼ статус задания = 30 (завершено)
```

### Коды статусов задания (`GraphConstructionJob.Status`)

| Код | Значение |
|-----|----------|
| 0 | Создано, ожидает подготовки |
| 5 | Подготовка выполняется |
| 10 | Файлы зарегистрированы, ожидают обработки |
| 20 | Построение графа выполняется |
| 30 | Завершено |
| 99 | Ошибка |

### Коды статусов файлов (`GraphConstructionFiles.Status`)

| Код | Значение |
|-----|----------|
| 0 | Ожидает разрешения анафоры |
| 5 | Разрешение анафоры выполняется |
| 10 | Анафора разрешена, готов к построению графа |
| 15 | Построение графа выполняется |
| 20 | Завершено |
| 99 | Ошибка |

---

## Разрешение анафоры

Перед построением графа каждый текстовый файл проходит разрешение кореференций в DAG `resolve_anaphora`. Резолвер выбирается для каждого задания через параметр `anaphoraResolverName` при запуске `start_tree_formation_job`:

| Значение | Модуль | Описание |
|----------|--------|----------|
| `LapinLiass` (по умолчанию) | `anaphoraResolverLapinLiass.py` | Правиловое ранжирование по значимости (свежесть + грамматическая роль + бонус за имя собственное), использует spaCy `en_core_web_sm` |
| `SpacyNeural` | `anaphoraResolverSpacyNeural.py` | Трансформерные кореференции через `en_coreference_web_trf`; требует Python 3.8–3.10 + spaCy 3.4–3.5 |

Оба резолвера имеют одинаковый интерфейс и возвращают идентичные структуры данных (`Substitution`, `Resolution`), поэтому выбор резолвера не влияет на остальной конвейер.

---

## Бэкенды построения графа

Доступны три бэкенда. В интерфейсе Airflow одновременно включайте только один из них.

### 1. На основе правил (`build_graph`)

Использует spaCy (`en_core_web_lg`) и NLTK для извлечения синтаксических отношений (подлежащее–глагол–дополнение) и строит инкрементальный граф для всего задания. Быстрый, не требует внешних API.

Результат: `graphJobs/{jobId}/graph.json` — единый граф, накапливаемый по всем файлам задания.

### 2. LLM v2 (`build_graph_llm_v2`)

Код: `dags/llm_v2/`. Использует локальную LLM (по умолчанию `Qwen2-1.5B-Instruct` через HuggingFace), а также `sentence-transformers` для дедупликации и кластеризации.

**Этапы:** предобработка → разрешение кореференций → разбивка на чанки → извлечение триплетов → нормализация → дедупликация → сборка графа → кластеризация

**Зависимости:** `transformers`, `torch`, `sentence-transformers`, `razdel`/`nltk`

Результат на файл: `graphJobs/{jobId}/llm_v2/{fileId}/raw_graph.json` и `clustered_graph.json`

Переопределить модель можно через `GraphConstructionJob.ProcessorConfig`:
```json
{"processorName": "LLMv2", "llm": {"model_name": "Qwen/Qwen2.5-7B-Instruct", "device": "cuda"}}
```

### 3. Иерархический (`build_graph_hierarchical`)

Код: `dags/hierarchical_llm_version/`. Использует асинхронный OpenAI-совместимый API Яндекс Облако. Сначала строит многоуровневую иерархию концептов по резюме чанков, затем извлекает сущности и отношения с учётом полного иерархического контекста. Позволяет получить более структурированные графы.

**Этапы:** предобработка → разбивка на чанки → проход 1 (резюме чанков + иерархия концептов) → проход 2 (контекстно-зависимое извлечение) → разрешение сущностей → сборка графа → фильтрация по важности / кластеризация

**Зависимости:** `openai`, `sentence-transformers`, `razdel`/`nltk`  
**Переменная среды:** `YANDEX_CLOUD_API_KEY`

Результат на файл: `graphJobs/{jobId}/hierarchical/{fileId}/raw_graph.json`, `clustered_graph.json`, `hierarchy_tree.json`

---

## Анализ графа

После завершения каждого задания DAG `finalize_job` автоматически генерирует два артефакта для каждого графа:

- **`metrics.json`** — статистика графа, вычисленная `graphMetrics.py` через networkx: количество узлов/рёбер, плотность, статистика степеней, средний коэффициент кластеризации, связные компоненты, доля наибольшей компоненты, диаметр, средняя длина кратчайшего пути, топ-10 узлов-хабов, распределение степеней
- **`visualization.html`** — самодостаточный интерактивный граф с силовой компоновкой, генерируемый `graphVisualizer.py` через pyvis (vis.js встроен, работает полностью офлайн). Размер узла отражает степень; толщина ребра — вес. В иерархическом бэкенде важность узла дополнительно отображается цветом.

Для подробного отчёта по нескольким файлам запустите скрипт для любого завершённого задания:

```bash
python dags/tools/generate_metrics_report.py <job_id>
```

Скрипт собирает все `metrics.json` задания, создаёт `metrics_report.html` с гистограммой распределения степеней (Chart.js) и таблицами хабов по файлам, и сохраняет результат на FTP по пути `graphJobs/{jobId}/metrics_report.html`.

---

## Структура FTP

```
/
├── arxiv/              ← загруженные PDF с arXiv
├── springer/           ← загруженные PDF со Springer (сейчас исключены из загрузки)
├── cyberleninka/       ← загруженные PDF с КиберЛенинки
├── gujarati/
│   ├── literature/     ← гуджаратские книги (Internet Archive)
│   ├── news/           ← гуджаратские газеты/журналы (Internet Archive)
│   ├── science_natural/  ← гуджаратские диссертации по естественным наукам (Shodhganga)
│   └── science_social/   ← гуджаратские диссертации по общественным наукам (Shodhganga)
├── Tex/                ← конвертированные текстовые файлы
└── graphJobs/
    └── {jobId}/
        ├── anaphora/
        │   └── {fileId}.txt          ← текст после разрешения анафоры
        ├── graph.json                ← инкрементальный граф (build_graph)
        ├── metrics.json              ← метрики графа (rule-based)
        ├── visualization.html        ← интерактивная визуализация (rule-based)
        ├── metrics_report.html       ← сводный отчёт по заданию (любой бэкенд)
        ├── llm_v2/
        │   └── {fileId}/
        │       ├── raw_graph.json
        │       ├── clustered_graph.json
        │       ├── metrics.json
        │       └── visualization.html
        └── hierarchical/
            └── {fileId}/
                ├── raw_graph.json
                ├── clustered_graph.json
                ├── hierarchy_tree.json
                ├── metrics.json
                └── visualization.html
```

---

## База данных

SQL Server база данных `TextCorpuses` на `LAPTOP-I91584GB\SQLEXPRESS`.

Применяйте миграции по порядку в SSMS:

| Миграция | Изменение |
|----------|-----------|
| `database-v0.1.sql` | Исходная схема |
| `database-v0.2-pdf-to-latex.sql` | Добавляет `LatexDocuments` (отслеживание PDF → текст) |
| `database-v0.3.sql` | Обновления схемы |
| `database-v0.4.sql` | Обновления схемы |
| `database-v0.5.sql` | `GetPDFLocationForLatexConvertation`: защита от вставки NULL, когда очередь конвертации пуста |
| `database-v0.6.sql` | Добавляет межпроцессный ограничитель частоты запросов к arXiv (заменён, см. v0.8) |
| `database-v0.7.sql` | `MarkProxyAsBroken`: устраняет гонку между конкурентными удалениями/вставками через явную транзакцию + `UPDLOCK`/`HOLDLOCK` |
| `database-v0.8.sql` | Удаляет ограничитель из v0.6 после возврата к доступу к arXiv через прокси |
| `database-v0.9.sql` | Добавляет `GetLatestFreeProxy` — аналог `GetLatestProxy`, но исключает общий платный прокси |
| `database-v0.10.sql` | Устраняет дубликаты в `relIpProxyProxyProtocols` (351 тыс. → ~6,5 тыс. строк), добавляет уникальный индекс, исправляет отсутствующую проверку существования в `AddOrUpdateProxy` |
| `database-v0.11.sql` | Добавляет `IPProxy.SuccessCount` + `MarkProxySuccess`; `GetLatestProxy`/`GetLatestFreeProxy` теперь в первую очередь ранжируют по доказанной репутации |
| `database-v0.12.sql` | Исправляет регрессию из v0.11: голый `INSERT ... VALUES` в `AddOrUpdateProxy` ломался для каждого нового прокси после добавления `SuccessCount` |
| `database-v0.13.sql` | `GetPdfToDownload`: исключает URL Springer на уровне запроса (временно, в паре с исключением Springer в `pdf-downloading-dag.py`), чтобы застрявшая строка Springer не блокировала всю очередь |
| `database-v0.14.sql` | `AddOrUpdateProxy`: переписан с единой явной транзакцией и `UPDLOCK`/`HOLDLOCK` (по образцу `MarkProxyAsBroken`) для устранения взаимоблокировок при конкурентной работе DAG прокси |

---

## Установка

### Установка зависимостей

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
sh scripts/install-hooks.sh   # Windows: scripts\install-hooks.bat
```

### Зависимости для LLM-конвейеров (по необходимости)

Для `build_graph_llm_v2` (локальная модель HuggingFace):
```bash
pip install -r dags/llm_v2/requirements.txt
```

Для `build_graph_hierarchical` (API Яндекс Облако):
```bash
pip install -r dags/hierarchical_llm_version/requirements.txt
export YANDEX_CLOUD_API_KEY=ваш_ключ
```

Для резолвера `SpacyNeural` (требуется Python 3.8–3.10 + spaCy 3.4–3.5):
```bash
pip install "spacy[transformers]"
python -m spacy download en_coreference_web_trf
```

### Конфигурация

Отредактируйте `dags/configs/configs.json`, указав строку подключения к SQL Server, адрес и учётные данные FTP-сервера.

### Развёртывание DAG

Скопируйте папку `dags/` в директорию DAG вашего Airflow (или настройте `dags_folder` в `airflow.cfg`). `dags/` полностью самодостаточна — её можно скопировать в любую установку Airflow без файлов из корня репозитория.

### Запуск построения графа

В интерфейсе Airflow запустите DAG `start_tree_formation_job` с параметрами:
- **paths**: пути на FTP через точку с запятой, например `arxiv/;springer/`
- **textProcessorName**: `RuleBased` (→ `build_graph`) или `AIBased` (→ `build_graph_llm_v2` / `build_graph_hierarchical`)
- **anaphoraResolverName**: `LapinLiass` (по умолчанию, правиловый) или `SpacyNeural` (трансформерный)

Включите в Airflow только тот DAG построения графа, который соответствует выбранному процессору. После завершения задания `finalize_job` автоматически сохранит `metrics.json` и `visualization.html` рядом с каждым графом на FTP.

---

## Тестирование

```bash
pytest dags/tests/ -k "not spacy_neural" -v
```

105 тестов, 1 исключён (SpacyNeural — требует Python 3.8–3.10 + spaCy 3.4–3.5).

---

## Мониторинг

Отслеживайте прогресс в SSMS:

```sql
SELECT j.ID, j.Status AS JobStatus, j.LastStatusChangeAt,
       COUNT(f.ID) AS TotalFiles,
       SUM(CASE WHEN f.Status = 0 THEN 1 ELSE 0 END) AS Pending,
       SUM(CASE WHEN f.Status = 10 THEN 1 ELSE 0 END) AS AnaphDone,
       SUM(CASE WHEN f.Status = 20 THEN 1 ELSE 0 END) AS GraphDone,
       SUM(CASE WHEN f.Status = 99 THEN 1 ELSE 0 END) AS Errors
FROM dbo.GraphConstructionJob j
LEFT JOIN dbo.GraphConstructionFiles f ON f.GraphConstructionJobId = j.ID
GROUP BY j.ID, j.Status, j.LastStatusChangeAt
ORDER BY j.ID DESC;
```
