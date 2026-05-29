# Scientific Paper Downloader DAGs Design

**Date:** 2026-05-29  
**Project:** text-corpuses-processing  
**Status:** Approved

## Goal

Add three new Airflow DAGs that download scientific paper URLs from arXiv, PubMed, and Semantic Scholar using their official APIs, with search criteria (keywords, categories, date ranges, open-access filter, citation threshold, max results) configurable per source via a JSON file.

---

## Architecture Overview

```
dags/configs/search_configs.json          ← search criteria per source (new)
dags/paperDownloader.py                   ← shared state + proxy + URL logic (new)
dags/download-arxiv-scientific-dag.py     ← arXiv API DAG (new)
dags/download-pubmed-dag.py               ← PubMed API DAG (new)
dags/download-semantic-scholar-dag.py     ← Semantic Scholar API DAG (new)
```

All three DAGs are `@continuous`, `max_active_runs=1`. Each run processes exactly one page of one criterion, then exits. Airflow restarts immediately for the next page/criterion.

### Per-run flow

```
1. Load criteria list from search_configs.json for this source
2. Load crawl state from ServiceState DB table
3. Skip criteria in done_criteria list
4. Call source API via adapter_fn(criterion, page, proxy)
5. Save discovered URLs via databaseConnector.addPdfUrl()
6. Advance state (page or criterion), save to ServiceState
7. Exit
```

---

## Config File: `dags/configs/search_configs.json`

New file alongside the existing `configs.json`. Three top-level keys, one per source. Each criterion is an object; unused fields are omitted.

```json
{
  "arxiv": [
    {
      "query": "graph neural networks",
      "categories": ["cs.LG", "cs.AI"],
      "date_from": "2022-01-01",
      "date_to": "2025-12-31",
      "open_access_only": true,
      "max_results": 500,
      "repeat": true
    }
  ],
  "pubmed": [
    {
      "query": "CRISPR gene editing",
      "date_from": "2020-01-01",
      "date_to": "2025-12-31",
      "open_access_only": true,
      "max_results": 300,
      "repeat": false
    }
  ],
  "semantic_scholar": [
    {
      "query": "knowledge graph reasoning",
      "fields_of_study": ["Computer Science"],
      "date_from": "2021-01-01",
      "date_to": "2025-12-31",
      "open_access_only": true,
      "min_citations": 10,
      "max_results": 400,
      "repeat": true
    }
  ]
}
```

### Parameter availability per source

| Parameter | arXiv | PubMed | Semantic Scholar |
|-----------|-------|--------|-----------------|
| `query` | ✅ | ✅ | ✅ |
| `categories` | ✅ arXiv category codes | — | — |
| `fields_of_study` | — | — | ✅ S2 field names |
| `date_from` / `date_to` | ✅ `YYYY-MM-DD` | ✅ `YYYY-MM-DD` | ✅ year only |
| `open_access_only` | ✅ (all arXiv papers are OA; field accepted but ignored) | ✅ PMC filter | ✅ |
| `min_citations` | — | — | ✅ |
| `max_results` | ✅ | ✅ | ✅ |
| `repeat` | ✅ | ✅ | ✅ |

---

## State Management

Crawl state is stored in the existing `ServiceState` DB table as a JSON string, one row per DAG (keyed by `serviceID`).

### State shape

```json
{
  "criterion_index": 0,
  "page": 1,
  "done_criteria": [1, 3]
}
```

- `criterion_index` — index into the criteria list currently being processed
- `page` — current page within that criterion (1-based)
- `done_criteria` — indices of `repeat=false` criteria that have been fully collected; skipped on every future run

### serviceID assignments

| serviceID | DAG |
|-----------|-----|
| 1 | existing `get_arxiv_urls` (web scraper, unchanged) |
| 2 | existing `get_springer_urls` |
| 3 | existing `get_lenin_urls` (CyberLeninka) |
| 4 | new `download_arxiv_scientific` |
| 5 | new `download_pubmed` |
| 6 | new `download_semantic_scholar` |

### State transitions

```
on each DAG run:
  load state (default: criterion_index=0, page=1, done_criteria=[])
  skip criteria in done_criteria
  call adapter_fn(criterion, page, proxy) → (urls, has_more)
  save urls
  if has_more:
      page += 1
  else:
      if repeat=true:
          page = 1
          criterion_index = (criterion_index + 1) % len(criteria)
      if repeat=false:
          done_criteria.append(criterion_index)
          criterion_index = next non-done index
  if all criteria done (all repeat=false and all in done_criteria):
      clear_state()  ← DAG idles until config changes
  else:
      save state
  exit
```

---

## Shared Module: `dags/paperDownloader.py`

Contains all logic shared across the three DAGs. DAGs import this and pass a source-specific adapter function.

### Public interface

```python
def load_search_config(source: str) -> list[dict]:
    """Reads dags/configs/search_configs.json, returns criteria list for source."""

def load_state(service_id: int) -> dict:
    """Reads crawl state from ServiceState. Returns default if no state exists."""

def save_state(service_id: int, state: dict) -> None:
    """Persists crawl state to ServiceState as JSON string."""

def clear_state(service_id: int) -> None:
    """Deletes state from ServiceState when all criteria are exhausted."""

def get_proxy() -> dict:
    """Returns {'ip': str, 'port': int, 'protocol': str} from proxy pool.
    Raises RuntimeError if no proxy is available."""

def mark_proxy_broken(ip: str) -> None:
    """Marks a proxy as broken in the DB."""

def save_urls(urls: list[str]) -> None:
    """Calls databaseConnector.addPdfUrl() for each URL. Idempotent."""

def run_search(service_id: int, adapter_fn: callable) -> None:
    """
    Main entry point called by each DAG task. Orchestrates the full
    load-state → call-adapter → save-urls → advance-state cycle.
    """
```

### Adapter function contract

Each DAG defines one adapter function and passes it to `run_search`:

```python
def adapter_fn(criterion: dict, page: int, proxy: dict) -> tuple[list[str], bool]:
    """
    criterion  — one object from search_configs.json
    page       — current page number (1-based)
    proxy      — {'ip': str, 'port': int, 'protocol': str}

    Returns:
        urls     — list of PDF/abstract URL strings discovered on this page
        has_more — True if there are additional pages to fetch for this criterion
    """
```

---

## Source Adapters

### arXiv adapter (inside `download-arxiv-scientific-dag.py`)

**API:** `http://export.arxiv.org/api/query` — free, no key required, Atom XML response.  
**Page size:** 50 results per request.

**Parameter mapping:**

| Criterion field | arXiv API |
|----------------|-----------|
| `query` | `search_query=all:{query}` |
| `categories` | `search_query=cat:{c1}+OR+cat:{c2}` ANDed with query |
| `date_from` / `date_to` | `submittedDate:[{YYYYMMDD}0000 TO {YYYYMMDD}2359]` in search_query |
| `open_access_only` | ignored (all arXiv papers are open access) |
| `max_results` | `start=(page-1)*50&max_results=50`, stop when `(page*50) >= max_results` |

**URL extraction:** parse `<link title="pdf" href="..."/>` from Atom XML entries.  
**`has_more`:** `(page * 50) < min(total_results_from_api, criterion["max_results"])`.

---

### PubMed adapter (inside `download-pubmed-dag.py`)

**API:** NCBI E-utilities — `esearch.fcgi` + `efetch.fcgi`. Free, no key for ≤3 req/s.  
**Page size:** 50 PMIDs per request.

**Two-step per page:**
1. `esearch.fcgi?db=pubmed&term={query}&retstart={(page-1)*50}&retmax=50` → list of PMIDs + total count
2. `efetch.fcgi?db=pubmed&id={pmids}&rettype=xml` → article XML containing PMC IDs

**Parameter mapping:**

| Criterion field | E-utilities fragment |
|----------------|---------------------|
| `query` | base term |
| `date_from` / `date_to` | `{YYYY/MM/DD}:{YYYY/MM/DD}[dp]` appended with AND |
| `open_access_only` | `AND "pmc open access"[filter]` |

**URL format:** papers with a PMC ID → `https://www.ncbi.nlm.nih.gov/pmc/articles/{PMCID}/pdf/`. Papers without PMC ID are skipped (no freely accessible PDF).  
**`has_more`:** `(page * 50) < min(total_count, criterion["max_results"])`.

---

### Semantic Scholar adapter (inside `download-semantic-scholar-dag.py`)

**API:** `https://api.semanticscholar.org/graph/v1/paper/search`. Free tier: 100 req/5min. Optional `x-api-key` header for higher limits (key stored in `configs.json` as `SemanticScholarApiKey`, optional).  
**Page size:** 100 results per request.

**Parameter mapping:**

| Criterion field | API param / response filter |
|----------------|---------------------------|
| `query` | `query=` |
| `fields_of_study` | `fieldsOfStudy=Computer+Science,...` |
| `date_from` / `date_to` | `year={year_from}-{year_to}` (year part only) |
| `open_access_only` | filter response: skip entries where `openAccessPdf` is null |
| `min_citations` | filter response: skip entries where `citationCount < min_citations` |
| `max_results` | stop when `(page * 100) >= max_results` |

**Requested fields:** `fields=openAccessPdf,citationCount,year`  
**URL extraction:** `paper["openAccessPdf"]["url"]` from each result entry.  
**`has_more`:** response has a non-null `next` token AND `(page * 100) < max_results`.

---

## DAG Files

### `dags/download-arxiv-scientific-dag.py`

```
DAG id:      download_arxiv_scientific
serviceID:   4
Schedule:    @continuous
max_active:  1
Tags:        pdfUrls, scientific
```

Single `@task` function `download_arxiv_scientific()` that imports `paperDownloader`, defines the arXiv `fetch_page` adapter inline, and calls `run_search(service_id=4, adapter_fn=fetch_page)`.

### `dags/download-pubmed-dag.py`

```
DAG id:      download_pubmed
serviceID:   5
Schedule:    @continuous
max_active:  1
Tags:        pdfUrls, scientific
```

Single `@task` function `download_pubmed()` with inline PubMed adapter, calls `run_search(service_id=5, ...)`.

### `dags/download-semantic-scholar-dag.py`

```
DAG id:      download_semantic_scholar
serviceID:   6
Schedule:    @continuous
max_active:  1
Tags:        pdfUrls, scientific
```

Single `@task` function `download_semantic_scholar()` with inline Semantic Scholar adapter, calls `run_search(service_id=6, ...)`. Reads optional `SemanticScholarApiKey` from `configs.json` if present.

---

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| API call fails (timeout, HTTP error) | Exception caught in `run_search`; state NOT advanced; Airflow restarts DAG for retry |
| Proxy error | `mark_proxy_broken()` called; state not advanced; retry on next run |
| No proxy available | `run_search` exits immediately without changing state |
| Empty results page | `has_more=False` returned; criterion advances normally |
| All criteria exhausted (`repeat=false` only) | `clear_state()` called; DAG exits; idles until `search_configs.json` is updated |
| `search_configs.json` missing or malformed | Exception propagates; Airflow marks task failed; no state change |

---

## Files to Create / Modify

| Action | File |
|--------|------|
| Create | `dags/configs/search_configs.json` |
| Create | `dags/paperDownloader.py` |
| Create | `dags/download-arxiv-scientific-dag.py` |
| Create | `dags/download-pubmed-dag.py` |
| Create | `dags/download-semantic-scholar-dag.py` |
| Modify | `dags/configs/configs.json` — add optional `SemanticScholarApiKey` field |

No database schema changes required. No new stored procedures needed.
