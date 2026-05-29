# Scientific Paper Downloader DAGs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three Airflow DAGs that download scientific paper URLs from arXiv, PubMed, and Semantic Scholar using their official APIs, with per-source search criteria configurable via a JSON file.

**Architecture:** A shared `paperDownloader.py` module handles state management, proxy rotation, and URL persistence. Each DAG defines a source-specific adapter function and calls `run_search(service_id, source, adapter_fn)`. Crawl state (current criterion index, current page, exhausted criteria) is stored in the existing `ServiceState` DB table.

**Tech Stack:** Python 3, Apache Airflow 2 (airflow.sdk), requests, xml.etree.ElementTree, pyodbc (via existing dbConnector), pytest.

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `dags/configs/search_configs.json` | Per-source search criteria config |
| Create | `dags/paperDownloader.py` | Shared state, proxy, URL, and orchestration logic |
| Create | `dags/tests/test_paper_downloader.py` | Unit tests for pure state-transition logic |
| Create | `dags/download-arxiv-scientific-dag.py` | arXiv API adapter + DAG |
| Create | `dags/download-pubmed-dag.py` | PubMed E-utilities adapter + DAG |
| Create | `dags/download-semantic-scholar-dag.py` | Semantic Scholar API adapter + DAG |
| Modify | `dags/configs/configs.json` | Add optional `SemanticScholarApiKey` field |

---

## Task 1: Create `dags/configs/search_configs.json`

**Files:**
- Create: `dags/configs/search_configs.json`

No tests needed — this is a data file.

- [ ] **Step 1: Create the config file**

Create `dags/configs/search_configs.json` with the following content. This file is the single place to add, remove, or change search criteria for all three sources. Each criterion object uses only the fields relevant to its source; unused fields are omitted.

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
    },
    {
      "query": "large language model",
      "categories": ["cs.CL", "cs.AI"],
      "date_from": "2023-01-01",
      "date_to": "2025-12-31",
      "max_results": 300,
      "repeat": false
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
    },
    {
      "query": "machine learning drug discovery",
      "date_from": "2021-01-01",
      "date_to": "2025-12-31",
      "open_access_only": true,
      "max_results": 200,
      "repeat": true
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
    },
    {
      "query": "protein structure prediction",
      "fields_of_study": ["Biology", "Computer Science"],
      "date_from": "2020-01-01",
      "date_to": "2025-12-31",
      "open_access_only": true,
      "min_citations": 5,
      "max_results": 200,
      "repeat": false
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add dags/configs/search_configs.json
git commit -m "feat: add search_configs.json for scientific paper downloaders"
```

---

## Task 2: Create `dags/paperDownloader.py` with Tests (TDD)

**Files:**
- Create: `dags/paperDownloader.py`
- Create: `dags/tests/test_paper_downloader.py`

This is the shared module. The two pure functions (`_next_active_index` and `advance_state`) contain all state-machine logic and are fully unit-testable without a DB or network. `run_search` wires them together with DB/proxy calls.

### State shape (stored in ServiceState as JSON)
```json
{"criterion_index": 0, "page": 1, "done_criteria": []}
```
- `criterion_index` — index into the source's criteria list
- `page` — 1-based page within the current criterion
- `done_criteria` — list of criterion indices with `repeat=false` that are fully collected

### serviceID assignments
| serviceID | DAG |
|-----------|-----|
| 1 | existing `get_arxiv_urls` (unchanged) |
| 2 | existing `get_springer_urls` (unchanged) |
| 3 | existing `get_lincoln_urls` (unchanged) |
| 4 | new `download_arxiv_scientific` |
| 5 | new `download_pubmed` |
| 6 | new `download_semantic_scholar` |

- [ ] **Step 1: Write failing tests**

Create `dags/tests/test_paper_downloader.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from paperDownloader import _next_active_index, advance_state

CRITERIA_SINGLE_REPEAT = [{"query": "a", "repeat": True}]
CRITERIA_SINGLE_ONCE   = [{"query": "a", "repeat": False}]
CRITERIA_TWO_REPEAT    = [{"query": "a", "repeat": True},  {"query": "b", "repeat": True}]
CRITERIA_TWO_ONCE      = [{"query": "a", "repeat": False}, {"query": "b", "repeat": False}]
CRITERIA_MIXED         = [{"query": "a", "repeat": True},  {"query": "b", "repeat": False}]


# ── _next_active_index ────────────────────────────────────────────────────────

def test_next_active_skips_done():
    criteria = [{"query": "a"}, {"query": "b"}, {"query": "c"}]
    assert _next_active_index(0, criteria, done={1}) == 2


def test_next_active_wraps_around():
    assert _next_active_index(1, [{"query": "a"}, {"query": "b"}], done=set()) == 0


def test_next_active_single_no_done_returns_self():
    assert _next_active_index(0, [{"query": "a"}], done=set()) == 0


def test_next_active_all_done_returns_none():
    assert _next_active_index(0, [{"query": "a"}, {"query": "b"}], done={0, 1}) is None


# ── advance_state: has_more=True ──────────────────────────────────────────────

def test_advance_increments_page_when_has_more():
    state = {"criterion_index": 0, "page": 3, "done_criteria": []}
    result = advance_state(state, CRITERIA_SINGLE_REPEAT, has_more=True)
    assert result == {"criterion_index": 0, "page": 4, "done_criteria": []}


def test_advance_has_more_does_not_change_criterion():
    state = {"criterion_index": 1, "page": 2, "done_criteria": [0]}
    result = advance_state(state, CRITERIA_TWO_ONCE, has_more=True)
    assert result["criterion_index"] == 1
    assert result["page"] == 3
    assert result["done_criteria"] == [0]


# ── advance_state: repeat=True, has_more=False ────────────────────────────────

def test_advance_repeat_moves_to_next_criterion():
    state = {"criterion_index": 0, "page": 5, "done_criteria": []}
    result = advance_state(state, CRITERIA_TWO_REPEAT, has_more=False)
    assert result == {"criterion_index": 1, "page": 1, "done_criteria": []}


def test_advance_repeat_single_wraps_to_self():
    state = {"criterion_index": 0, "page": 5, "done_criteria": []}
    result = advance_state(state, CRITERIA_SINGLE_REPEAT, has_more=False)
    assert result == {"criterion_index": 0, "page": 1, "done_criteria": []}


def test_advance_repeat_skips_done_on_next():
    criteria = [
        {"query": "a", "repeat": True},
        {"query": "b", "repeat": False},
        {"query": "c", "repeat": True},
    ]
    state = {"criterion_index": 0, "page": 2, "done_criteria": [1]}
    result = advance_state(state, criteria, has_more=False)
    assert result == {"criterion_index": 2, "page": 1, "done_criteria": [1]}


# ── advance_state: repeat=False, has_more=False ───────────────────────────────

def test_advance_once_adds_to_done_and_moves_to_next():
    state = {"criterion_index": 0, "page": 2, "done_criteria": []}
    result = advance_state(state, CRITERIA_TWO_ONCE, has_more=False)
    assert result["criterion_index"] == 1
    assert result["page"] == 1
    assert 0 in result["done_criteria"]


def test_advance_once_last_criterion_returns_none():
    state = {"criterion_index": 0, "page": 2, "done_criteria": []}
    assert advance_state(state, CRITERIA_SINGLE_ONCE, has_more=False) is None


def test_advance_once_second_returns_none_when_first_already_done():
    state = {"criterion_index": 1, "page": 2, "done_criteria": [0]}
    assert advance_state(state, CRITERIA_TWO_ONCE, has_more=False) is None


def test_advance_mixed_once_exhausted_moves_to_repeat():
    # criteria: [repeat=True, repeat=False]; once exhausted → move to repeat
    state = {"criterion_index": 1, "page": 2, "done_criteria": []}
    result = advance_state(state, CRITERIA_MIXED, has_more=False)
    assert result["criterion_index"] == 0
    assert result["page"] == 1
    assert 1 in result["done_criteria"]


def test_advance_once_preserves_existing_done():
    criteria = [
        {"query": "a", "repeat": False},
        {"query": "b", "repeat": False},
        {"query": "c", "repeat": False},
    ]
    state = {"criterion_index": 1, "page": 1, "done_criteria": [0]}
    result = advance_state(state, criteria, has_more=False)
    assert result["criterion_index"] == 2
    assert 0 in result["done_criteria"]
    assert 1 in result["done_criteria"]
```

- [ ] **Step 2: Run tests — they must fail**

```bash
cd C:\Repositories\text-corpuses-processing
python -m pytest dags/tests/test_paper_downloader.py -v
```

Expected: `ImportError: No module named 'paperDownloader'` (or similar). Confirm failure before writing implementation.

- [ ] **Step 3: Create `dags/paperDownloader.py`**

```python
# -*- coding: utf-8 -*-
import json
import os

_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
_SEARCH_CONFIG_PATH = os.path.join(_DAG_FOLDER, 'configs', 'search_configs.json')


def load_search_config(source: str) -> list:
    """Reads search_configs.json and returns the criteria list for the given source."""
    with open(_SEARCH_CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f).get(source, [])


def load_state(service_id: int) -> dict:
    """Reads crawl state from ServiceState. Returns a fresh default state if none exists."""
    import dbConnector
    from dbConnector import databaseConnector
    result = databaseConnector.getServiceState(service_id)
    if result is None:
        return {'criterion_index': 0, 'page': 1, 'done_criteria': []}
    return json.loads(result[0])


def save_state(service_id: int, state: dict) -> None:
    """Persists crawl state to ServiceState as a JSON string."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.updateServiceState(service_id, json.dumps(state))


def clear_state(service_id: int) -> None:
    """Deletes crawl state from ServiceState (all criteria exhausted)."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.removeServiceState(service_id)


def get_proxy() -> dict:
    """Returns {'ip', 'port', 'protocol'} from the proxy pool.
    Raises RuntimeError if no proxy is available."""
    import dbConnector
    from dbConnector import databaseConnector
    result = databaseConnector.getLatestProxy()
    if result is None:
        raise RuntimeError('No proxy available')
    return {
        'ip': str(result['proxieIp']).strip(),
        'port': result['proxiePort'],
        'protocol': str(result['proxieProtocol']).strip(),
    }


def mark_proxy_broken(ip: str) -> None:
    """Marks a proxy as broken in the DB."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.markProxyAsBroken(ip)


def save_urls(urls: list) -> None:
    """Calls databaseConnector.addPdfUrl() for each URL. Idempotent."""
    import dbConnector
    from dbConnector import databaseConnector
    for url in urls:
        databaseConnector.addPdfUrl(url)


def _next_active_index(current: int, criteria: list, done: set):
    """Return the next criterion index not in done, wrapping around.
    Returns None if every index is in done."""
    n = len(criteria)
    for offset in range(1, n + 1):
        idx = (current + offset) % n
        if idx not in done:
            return idx
    return None


def advance_state(state: dict, criteria: list, has_more: bool):
    """Pure function. Computes the next state after one page of one criterion.

    Returns the new state dict, or None if all criteria are exhausted
    (all repeat=false and all in done_criteria).
    """
    current = state['criterion_index']
    done = set(state['done_criteria'])

    if has_more:
        return {**state, 'page': state['page'] + 1}

    criterion = criteria[current]
    if criterion.get('repeat', False):
        next_idx = _next_active_index(current, criteria, done)
        return {
            'criterion_index': next_idx if next_idx is not None else current,
            'page': 1,
            'done_criteria': list(done),
        }
    else:
        done = done | {current}
        next_idx = _next_active_index(current, criteria, done)
        if next_idx is None:
            return None
        return {'criterion_index': next_idx, 'page': 1, 'done_criteria': list(done)}


def run_search(service_id: int, source: str, adapter_fn) -> None:
    """Main entry point called by each DAG task.

    Loads criteria and state, picks the current criterion, calls adapter_fn
    to fetch one page of URLs, saves them, advances state, and persists.

    adapter_fn(criterion, page, proxy) -> (list[str], bool)
      criterion — one dict from search_configs.json
      page      — current 1-based page number
      proxy     — {'ip': str, 'port': int, 'protocol': str}
      returns   — (list of URL strings, has_more bool)
    """
    criteria = load_search_config(source)
    if not criteria:
        return

    state = load_state(service_id)
    done = set(state['done_criteria'])

    # Recover if state points at an already-done criterion
    current = state['criterion_index']
    if current in done:
        current = _next_active_index(current, criteria, done)
        if current is None:
            clear_state(service_id)
            return
        state = {'criterion_index': current, 'page': 1, 'done_criteria': list(done)}

    try:
        proxy = get_proxy()
    except RuntimeError:
        return  # no proxy; exit without changing state

    try:
        urls, has_more = adapter_fn(criteria[state['criterion_index']], state['page'], proxy)
    except Exception as e:
        print(f'[paperDownloader] adapter error: {e}')
        if any(w in str(e).lower() for w in ('proxy', 'connect', 'timeout', 'ssl')):
            mark_proxy_broken(proxy['ip'])
        return  # state NOT advanced; Airflow retries on next run

    save_urls(urls)

    new_state = advance_state(state, criteria, has_more)
    if new_state is None:
        clear_state(service_id)
    else:
        save_state(service_id, new_state)
```

- [ ] **Step 4: Run tests — all must pass**

```bash
python -m pytest dags/tests/test_paper_downloader.py -v
```

Expected:
```
test_next_active_skips_done PASSED
test_next_active_wraps_around PASSED
test_next_active_single_no_done_returns_self PASSED
test_next_active_all_done_returns_none PASSED
test_advance_increments_page_when_has_more PASSED
test_advance_has_more_does_not_change_criterion PASSED
test_advance_repeat_moves_to_next_criterion PASSED
test_advance_repeat_single_wraps_to_self PASSED
test_advance_repeat_skips_done_on_next PASSED
test_advance_once_adds_to_done_and_moves_to_next PASSED
test_advance_once_last_criterion_returns_none PASSED
test_advance_once_second_returns_none_when_first_already_done PASSED
test_advance_mixed_once_exhausted_moves_to_repeat PASSED
test_advance_once_preserves_existing_done PASSED

14 passed
```

- [ ] **Step 5: Commit**

```bash
git add dags/paperDownloader.py dags/tests/test_paper_downloader.py
git commit -m "feat: add paperDownloader shared module with state-transition tests"
```

---

## Task 3: Create `dags/download-arxiv-scientific-dag.py`

**Files:**
- Create: `dags/download-arxiv-scientific-dag.py`

Uses the arXiv Atom API (`http://export.arxiv.org/api/query`). No API key needed. Page size: 50. serviceID: 4.

**API details:**
- `search_query` combines `all:{query}`, `cat:{c}` category filters, and `submittedDate` range — all joined with ` AND `.
- Date format in query: `YYYYMMDD0000` (no hyphens, append `0000` or `2359`).
- `start=(page-1)*50`, `max_results=50` per call.
- Response: Atom XML. PDF URLs are in `<link title="pdf" href="..."/>` inside each `<entry>`.
- `totalResults` (opensearch namespace) gives total count to compute `has_more`.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_arxiv_scientific",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_arxiv_scientific():
        import requests
        import xml.etree.ElementTree as ET
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 50
            start = (page - 1) * PAGE_SIZE

            parts = []
            if criterion.get('query'):
                parts.append(f"all:{criterion['query']}")
            if criterion.get('categories'):
                cat_expr = ' OR '.join(f"cat:{c}" for c in criterion['categories'])
                parts.append(f"({cat_expr})")
            if criterion.get('date_from') or criterion.get('date_to'):
                d_from = criterion.get('date_from', '2000-01-01').replace('-', '') + '0000'
                d_to = criterion.get('date_to', '2099-12-31').replace('-', '') + '2359'
                parts.append(f"submittedDate:[{d_from} TO {d_to}]")

            search_query = ' AND '.join(parts) if parts else 'all:*'

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }

            resp = requests.get(
                'http://export.arxiv.org/api/query',
                params={
                    'search_query': search_query,
                    'start': start,
                    'max_results': PAGE_SIZE,
                    'sortBy': 'submittedDate',
                    'sortOrder': 'descending',
                },
                proxies=proxies,
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0'},
            )
            resp.raise_for_status()

            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'opensearch': 'http://a9.com/-/spec/opensearch/1.1/',
            }
            root = ET.fromstring(resp.content)
            total = int(root.findtext('opensearch:totalResults', namespaces=ns) or 0)
            max_results = criterion.get('max_results', 1000)

            urls = []
            for entry in root.findall('atom:entry', ns):
                for link in entry.findall('atom:link', ns):
                    if link.get('title') == 'pdf':
                        href = link.get('href', '')
                        if href:
                            if not href.startswith('http'):
                                href = 'https://arxiv.org' + href
                            urls.append(href)

            has_more = (page * PAGE_SIZE) < min(total, max_results)
            return urls, has_more

        run_search(service_id=4, source='arxiv', adapter_fn=fetch_page)

    download_arxiv_scientific()
```

- [ ] **Step 2: Verify the DAG parses without errors**

```bash
python -c "
import ast
ast.parse(open('dags/download-arxiv-scientific-dag.py').read())
print('OK')
"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dags/download-arxiv-scientific-dag.py
git commit -m "feat: add download_arxiv_scientific DAG (arXiv API)"
```

---

## Task 4: Create `dags/download-pubmed-dag.py`

**Files:**
- Create: `dags/download-pubmed-dag.py`

Uses NCBI E-utilities: `esearch.fcgi` to get PMIDs, then `efetch.fcgi` to get PMC IDs. No API key needed for ≤3 req/s. Page size: 50. serviceID: 5.

**API details:**
- Step 1 — `esearch`: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={query}&retstart={N}&retmax=50&retmode=xml`
  - Date filter: append `AND "{YYYY/MM/DD}"[dp]:"{YYYY/MM/DD}"[dp]` to query.
  - Open access filter: append `AND "pmc open access"[filter]`.
  - Response XML: `<Count>` (total), list of `<Id>` (PMIDs).
- Step 2 — `efetch`: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmids}&rettype=xml&retmode=xml`
  - Response XML: search `.//ArticleId[@IdType="pmc"]` for PMC IDs.
  - PMC PDF URL: `https://www.ncbi.nlm.nih.gov/pmc/articles/{PMCID}/pdf/`
- Sleep 0.35s between esearch and efetch to respect the 3 req/s rate limit.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_pubmed",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_pubmed():
        import time
        import requests
        import xml.etree.ElementTree as ET
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 50
            retstart = (page - 1) * PAGE_SIZE

            query_parts = [criterion['query']]
            if criterion.get('date_from') or criterion.get('date_to'):
                d_from = criterion.get('date_from', '1900/01/01').replace('-', '/')
                d_to = criterion.get('date_to', '2099/12/31').replace('-', '/')
                query_parts.append(f'"{d_from}"[dp]:"{d_to}"[dp]')
            if criterion.get('open_access_only', False):
                query_parts.append('"pmc open access"[filter]')

            query = ' AND '.join(query_parts)

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }
            headers = {'User-Agent': 'Mozilla/5.0'}

            esearch = requests.get(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
                params={
                    'db': 'pubmed',
                    'term': query,
                    'retstart': retstart,
                    'retmax': PAGE_SIZE,
                    'retmode': 'xml',
                },
                proxies=proxies,
                timeout=30,
                headers=headers,
            )
            esearch.raise_for_status()

            root = ET.fromstring(esearch.content)
            total_count = int(root.findtext('Count') or 0)
            pmids = [el.text for el in root.findall('.//Id')]

            urls = []
            if pmids:
                time.sleep(0.35)  # NCBI rate limit: max 3 req/s

                efetch = requests.get(
                    'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
                    params={
                        'db': 'pubmed',
                        'id': ','.join(pmids),
                        'rettype': 'xml',
                        'retmode': 'xml',
                    },
                    proxies=proxies,
                    timeout=30,
                    headers=headers,
                )
                efetch.raise_for_status()

                efetch_root = ET.fromstring(efetch.content)
                for article_id in efetch_root.findall('.//ArticleId'):
                    if article_id.get('IdType') == 'pmc':
                        pmc_id = article_id.text
                        if pmc_id:
                            urls.append(
                                f'https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/'
                            )

            max_results = criterion.get('max_results', 1000)
            has_more = (page * PAGE_SIZE) < min(total_count, max_results)
            return urls, has_more

        run_search(service_id=5, source='pubmed', adapter_fn=fetch_page)

    download_pubmed()
```

- [ ] **Step 2: Verify the DAG parses without errors**

```bash
python -c "
import ast
ast.parse(open('dags/download-pubmed-dag.py').read())
print('OK')
"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dags/download-pubmed-dag.py
git commit -m "feat: add download_pubmed DAG (NCBI E-utilities)"
```

---

## Task 5: Create `dags/download-semantic-scholar-dag.py`

**Files:**
- Create: `dags/download-semantic-scholar-dag.py`
- Modify: `dags/configs/configs.json`

Uses Semantic Scholar Graph API. Free tier: 100 req/5min. Optional `x-api-key` header for higher limits. Page size: 100. serviceID: 6.

**API details:**
- Endpoint: `GET https://api.semanticscholar.org/graph/v1/paper/search`
- Key params: `query`, `fields=openAccessPdf,citationCount,year`, `limit=100`, `offset=(page-1)*100`
- `fieldsOfStudy`: comma-joined list e.g. `Computer Science,Biology`
- `year`: `YYYY-YYYY` (year part only from `date_from`/`date_to`)
- Response JSON: `data` array of paper objects, `next` token (non-null means more pages exist)
- `open_access_only`: skip papers where `paper["openAccessPdf"]` is null
- `min_citations`: skip papers where `paper["citationCount"] < min_citations`
- URL: `paper["openAccessPdf"]["url"]`
- API key: read from `getConfig().get("SemanticScholarApiKey")` — if present, set `x-api-key` header

- [ ] **Step 1: Add `SemanticScholarApiKey` to `dags/configs/configs.json`**

Open `dags/configs/configs.json` (current content shown below) and add the new optional field:

Current file:
```json
{
	"ConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=LAPTOP-I91584GB\\SQLEXPRESS;Database=TextCorpuses;Trusted_Connection=yes;TrustServerCertificate=yes;",
	"FtpHost": "127.0.0.1",
	"FtpPort": 21,
	"FtpUser": "airflow",
	"FtpPassword": "airflow",
	"FtpHostTex": "127.0.0.1",
	"FtpPortTex": 21,
	"FtpUserTex": "latex",
	"FtpPasswordTex": "latex"
}
```

Updated file (add `SemanticScholarApiKey` with an empty string — fill in your key or leave empty to use free tier):

```json
{
	"ConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=LAPTOP-I91584GB\\SQLEXPRESS;Database=TextCorpuses;Trusted_Connection=yes;TrustServerCertificate=yes;",
	"FtpHost": "127.0.0.1",
	"FtpPort": 21,
	"FtpUser": "airflow",
	"FtpPassword": "airflow",
	"FtpHostTex": "127.0.0.1",
	"FtpPortTex": 21,
	"FtpUserTex": "latex",
	"FtpPasswordTex": "latex",
	"SemanticScholarApiKey": ""
}
```

- [ ] **Step 2: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_semantic_scholar",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_semantic_scholar():
        import requests
        import configs
        from configs import getConfig
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 100
            offset = (page - 1) * PAGE_SIZE

            params = {
                'query': criterion['query'],
                'fields': 'openAccessPdf,citationCount,year',
                'limit': PAGE_SIZE,
                'offset': offset,
            }

            if criterion.get('fields_of_study'):
                params['fieldsOfStudy'] = ','.join(criterion['fields_of_study'])

            if criterion.get('date_from') or criterion.get('date_to'):
                year_from = criterion.get('date_from', '2000-01-01')[:4]
                year_to = criterion.get('date_to', '2099-12-31')[:4]
                params['year'] = f'{year_from}-{year_to}'

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }

            headers = {'User-Agent': 'Mozilla/5.0'}
            api_key = getConfig().get('SemanticScholarApiKey', '')
            if api_key:
                headers['x-api-key'] = api_key

            resp = requests.get(
                'https://api.semanticscholar.org/graph/v1/paper/search',
                params=params,
                proxies=proxies,
                timeout=30,
                headers=headers,
            )
            resp.raise_for_status()

            data = resp.json()
            papers = data.get('data', [])
            next_token = data.get('next')

            min_citations = criterion.get('min_citations', 0)
            open_access_only = criterion.get('open_access_only', False)
            max_results = criterion.get('max_results', 1000)

            urls = []
            for paper in papers:
                if paper.get('citationCount', 0) < min_citations:
                    continue
                oa_pdf = paper.get('openAccessPdf')
                if open_access_only and not oa_pdf:
                    continue
                if oa_pdf and oa_pdf.get('url'):
                    urls.append(oa_pdf['url'])

            has_more = bool(next_token) and (page * PAGE_SIZE) < max_results
            return urls, has_more

        run_search(service_id=6, source='semantic_scholar', adapter_fn=fetch_page)

    download_semantic_scholar()
```

- [ ] **Step 3: Verify the DAG parses without errors**

```bash
python -c "
import ast
ast.parse(open('dags/download-semantic-scholar-dag.py').read())
print('OK')
"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add dags/download-semantic-scholar-dag.py dags/configs/configs.json
git commit -m "feat: add download_semantic_scholar DAG and SemanticScholarApiKey config"
```

---

## Task 6: Verify All Tests Still Pass and Push

- [ ] **Step 1: Run full test suite**

```bash
cd C:\Repositories\text-corpuses-processing
python -m pytest dags/tests/ -v
```

Expected output (all tests green):
```
dags/tests/test_graph_builder.py::test_merge_graph_into_empty_adds_node_and_edge PASSED
dags/tests/test_graph_builder.py::test_merge_graph_increments_weight_on_duplicate_edge PASSED
dags/tests/test_graph_builder.py::test_merge_graph_appends_new_edge PASSED
dags/tests/test_graph_builder.py::test_merge_graph_multiple_edges_in_one_call PASSED
dags/tests/test_graph_builder.py::test_extract_graph_edges_returns_list_of_triples PASSED
dags/tests/test_graph_builder.py::test_extract_graph_edges_no_self_loops PASSED
dags/tests/test_paper_downloader.py::test_next_active_skips_done PASSED
dags/tests/test_paper_downloader.py::test_next_active_wraps_around PASSED
dags/tests/test_paper_downloader.py::test_next_active_single_no_done_returns_self PASSED
dags/tests/test_paper_downloader.py::test_next_active_all_done_returns_none PASSED
dags/tests/test_paper_downloader.py::test_advance_increments_page_when_has_more PASSED
dags/tests/test_paper_downloader.py::test_advance_has_more_does_not_change_criterion PASSED
dags/tests/test_paper_downloader.py::test_advance_repeat_moves_to_next_criterion PASSED
dags/tests/test_paper_downloader.py::test_advance_repeat_single_wraps_to_self PASSED
dags/tests/test_paper_downloader.py::test_advance_repeat_skips_done_on_next PASSED
dags/tests/test_paper_downloader.py::test_advance_once_adds_to_done_and_moves_to_next PASSED
dags/tests/test_paper_downloader.py::test_advance_once_last_criterion_returns_none PASSED
dags/tests/test_paper_downloader.py::test_advance_once_second_returns_none_when_first_already_done PASSED
dags/tests/test_paper_downloader.py::test_advance_mixed_once_exhausted_moves_to_repeat PASSED
dags/tests/test_paper_downloader.py::test_advance_once_preserves_existing_done PASSED

20 passed
```

- [ ] **Step 2: Syntax-check all new DAG files**

```bash
python -c "
import ast
files = [
    'dags/download-arxiv-scientific-dag.py',
    'dags/download-pubmed-dag.py',
    'dags/download-semantic-scholar-dag.py',
]
for f in files:
    ast.parse(open(f).read())
    print(f'OK: {f}')
"
```

Expected: three `OK:` lines.

- [ ] **Step 3: Push to GitHub**

```bash
git push origin main
```
