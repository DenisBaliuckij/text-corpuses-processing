# Project Gutenberg Book Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Project Gutenberg (via the free Gutendex API) as a new URL-discovery source, split into 8 genre subfolders, and give `pdf_downloading` its first real non-PDF-format fallback path (Gutenberg almost never offers a native PDF).

**Architecture:** One new shared adapter module (`gutenbergDownloader.py`, mirroring `archiveOrgDownloader.py`'s `search_pdfs` shape) provides `search_books(query, page, proxy, tag)` and `parse_download_tag(url)`. Eight new DAG files (one per genre) each call `paperDownloader.run_search` with their own `service_id`/tag, following the exact structure of `download-gujarati-law-dag.py`. `pdf-downloading-dag.py` gains one new routing branch that uses `parse_download_tag` to pick the right file extension instead of assuming `.pdf`.

**Tech Stack:** Python 3.10+, `requests`, Airflow `@task`/`DAG` (`airflow.sdk`), pytest + `unittest.mock`, SQL Server (T-SQL stored procedure).

## Global Constraints

- Follow the exact DAG shape used by `dags/download-gujarati-law-dag.py`: `schedule="@continuous"`, `catchup=False`, `is_paused_upon_creation=False`, `max_active_runs=1`.
- `use_proxy=True` for every new DAG (per explicit instruction — all new outbound requests must route through the existing proxy pool), unlike the archive.org-based sources.
- `service_id` values 28–35 (next free after the existing max of 27 — verified via `grep -r "service_id=" dags/`).
- Tag format: `#gutenberg_{category}.{ext}` appended to each discovered URL — carries both the FTP subfolder and the actual downloaded file extension through to `pdf_downloading`.
- No changes to existing sources' behavior: every non-Gutenberg URL must still get `.pdf` exactly as today.
- Spec: `docs/superpowers/specs/2026-07-21-gutenberg-book-source-design.md`.

---

### Task 1: `gutenbergDownloader.py` — `search_books` core

**Files:**
- Create: `dags/gutenbergDownloader.py`
- Test: `dags/tests/test_gutenberg_downloader.py`

**Interfaces:**
- Produces: `search_books(query: str, page: int, proxy: dict|None = None, tag: str = '') -> tuple[list[str], bool]` — `proxy` is `{'ip', 'port', 'protocol'}` (same shape `paperDownloader.get_proxy()` returns); converted to `requests`' `proxies` dict internally so callers never repeat that conversion.
- Produces: `_pick_format(formats: dict) -> tuple[str, str] | None` (internal helper, still testable/importable).

- [ ] **Step 1: Write the failing tests**

Create `dags/tests/test_gutenberg_downloader.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
from gutenbergDownloader import search_books


def _fake_response(json_data):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def test_search_books_picks_epub_when_no_pdf():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 1, 'formats': {
                'text/html': 'https://www.gutenberg.org/ebooks/1.html.images',
                'application/epub+zip': 'https://www.gutenberg.org/ebooks/1.epub3.images',
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/1.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, has_more = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/1.epub3.images#gutenberg_science.epub']
    assert has_more is False


def test_search_books_prefers_pdf_when_present():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 2, 'formats': {
                'application/pdf': 'https://www.gutenberg.org/files/2/2.pdf',
                'application/epub+zip': 'https://www.gutenberg.org/ebooks/2.epub3.images',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/files/2/2.pdf#gutenberg_science.pdf']


def test_search_books_falls_back_to_html_when_no_epub():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 3, 'formats': {
                'text/html': 'https://www.gutenberg.org/ebooks/3.html.images',
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/3.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/3.html.images#gutenberg_science.html']


def test_search_books_falls_back_to_txt_when_only_txt():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 4, 'formats': {
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/4.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/4.txt.utf-8#gutenberg_science.txt']


def test_search_books_skips_item_with_no_usable_format():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 5, 'formats': {
                'image/jpeg': 'https://www.gutenberg.org/cache/epub/5/pg5.cover.medium.jpg',
                'application/rdf+xml': 'https://www.gutenberg.org/ebooks/5.rdf',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == []


def test_search_books_has_more_true_when_next_present():
    data = {'count': 100, 'next': 'https://gutendex.com/books/?page=2&search=science', 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        _, has_more = search_books('science', 1)
    assert has_more is True


def test_search_books_has_more_false_when_next_null():
    data = {'count': 5, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        _, has_more = search_books('science', 1)
    assert has_more is False


def test_search_books_passes_query_and_page_params():
    data = {'count': 0, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('social science', 3)
    _, kwargs = mock_get.call_args
    assert kwargs['params'] == {'search': 'social science', 'page': 3}


def test_search_books_converts_proxy_dict_to_requests_proxies():
    data = {'count': 0, 'next': None, 'results': []}
    proxy = {'ip': '1.2.3.4', 'port': 8080, 'protocol': 'http'}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('science', 1, proxy=proxy)
    _, kwargs = mock_get.call_args
    assert kwargs['proxies'] == {'http': 'http://1.2.3.4:8080', 'https': 'http://1.2.3.4:8080'}


def test_search_books_no_proxy_when_proxy_none():
    data = {'count': 0, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('science', 1, proxy=None)
    _, kwargs = mock_get.call_args
    assert kwargs['proxies'] is None


def test_search_books_no_tag_appends_nothing():
    data = {
        'count': 1, 'next': None,
        'results': [{'id': 6, 'formats': {'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/6.txt.utf-8'}}]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='')
    assert urls == ['https://www.gutenberg.org/ebooks/6.txt.utf-8']
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest dags/tests/test_gutenberg_downloader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gutenbergDownloader'`

- [ ] **Step 3: Write minimal implementation**

Create `dags/gutenbergDownloader.py`:

```python
# -*- coding: utf-8 -*-
import requests

_SEARCH_URL = 'https://gutendex.com/books/'

_FORMAT_PRIORITY = [
    ('application/pdf', 'pdf'),
    ('application/epub+zip', 'epub'),
    ('text/html', 'html'),
    ('text/plain; charset=utf-8', 'txt'),
]


def _pick_format(formats: dict):
    """Returns (url, ext) for the highest-priority format present in
    `formats` (a Gutendex book's mime-type -> URL dict), or None if none
    of the known formats are present."""
    for mime, ext in _FORMAT_PRIORITY:
        if mime in formats:
            return formats[mime], ext
    return None


def search_books(query, page, proxy=None, tag=''):
    """Searches Gutendex for `query`. Returns (urls, has_more).
    Each URL has '#{tag}.{ext}' appended (if tag is non-empty), ext being
    whichever format was actually selected: pdf if present (essentially
    never on Gutenberg), else epub, else html, else txt. Items with none
    of these formats are skipped.

    proxy, if given, is {'ip', 'port', 'protocol'} (as returned by
    paperDownloader.get_proxy()) - converted internally to requests'
    proxies dict so every caller doesn't have to repeat that conversion."""
    proxies = None
    if proxy:
        proxy_url = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
        proxies = {'http': proxy_url, 'https': proxy_url}

    resp = requests.get(
        _SEARCH_URL, params={'search': query, 'page': page}, proxies=proxies,
        timeout=30, headers={'User-Agent': 'Mozilla/5.0'},
    )
    resp.raise_for_status()
    data = resp.json()

    urls = []
    for book in data.get('results', []):
        picked = _pick_format(book.get('formats', {}))
        if picked is None:
            continue
        url, ext = picked
        if tag:
            url += f'#{tag}.{ext}'
        urls.append(url)

    has_more = bool(data.get('next'))
    return urls, has_more
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest dags/tests/test_gutenberg_downloader.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Commit**

```bash
git add dags/gutenbergDownloader.py dags/tests/test_gutenberg_downloader.py
git commit -m "feat: add Gutendex search adapter for Project Gutenberg source"
```

---

### Task 2: `gutenbergDownloader.py` — `parse_download_tag`

**Files:**
- Modify: `dags/gutenbergDownloader.py`
- Test: `dags/tests/test_gutenberg_downloader.py`

**Interfaces:**
- Consumes: nothing from Task 1 (standalone function in the same module).
- Produces: `parse_download_tag(url: str) -> tuple[str, str, str] | None` — `(folder, ext, url_without_tag)` for a Gutenberg-tagged URL, `None` otherwise. Consumed by Task 4 (`pdf-downloading-dag.py`).

- [ ] **Step 1: Write the failing tests**

Append to `dags/tests/test_gutenberg_downloader.py` (add `parse_download_tag` to the existing import line):

```python
from gutenbergDownloader import search_books, parse_download_tag


def test_parse_download_tag_extracts_folder_ext_and_clean_url():
    url = 'https://www.gutenberg.org/ebooks/1.epub3.images#gutenberg_science.epub'
    result = parse_download_tag(url)
    assert result == ('science', 'epub', 'https://www.gutenberg.org/ebooks/1.epub3.images')


def test_parse_download_tag_multi_word_category():
    url = 'https://www.gutenberg.org/ebooks/2.html.images#gutenberg_philosophy_religion.html'
    result = parse_download_tag(url)
    assert result == ('philosophy_religion', 'html', 'https://www.gutenberg.org/ebooks/2.html.images')


def test_parse_download_tag_returns_none_for_non_gutenberg_url():
    url = 'https://archive.org/download/foo/foo.pdf#gujarati_law'
    assert parse_download_tag(url) is None


def test_parse_download_tag_returns_none_for_plain_url():
    assert parse_download_tag('https://example.com/file.pdf') is None
```

(Replace the old `from gutenbergDownloader import search_books` line at the top of the file with the combined import shown above.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest dags/tests/test_gutenberg_downloader.py -v`
Expected: FAIL with `ImportError: cannot import name 'parse_download_tag'`

- [ ] **Step 3: Write minimal implementation**

Append to `dags/gutenbergDownloader.py`:

```python
def parse_download_tag(url: str):
    """If `url` carries a '#gutenberg_{category}.{ext}' tag appended by
    search_books, returns (folder, ext, url_without_tag) where folder is
    the category with the 'gutenberg_' prefix stripped (e.g. 'science',
    'philosophy_religion'). Returns None for any URL without that tag."""
    if '#gutenberg_' not in url:
        return None
    clean_url, tag_part = url.rsplit('#', 1)
    category, ext = tag_part.rsplit('.', 1)
    folder = category[len('gutenberg_'):]
    return folder, ext, clean_url
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest dags/tests/test_gutenberg_downloader.py -v`
Expected: PASS (15 tests)

- [ ] **Step 5: Commit**

```bash
git add dags/gutenbergDownloader.py dags/tests/test_gutenberg_downloader.py
git commit -m "feat: add parse_download_tag for routing gutenberg URLs to pdf_downloading"
```

---

### Task 3: `search_configs.json` — 8 new source keys

**Files:**
- Modify: `dags/configs/search_configs.json`

**Interfaces:**
- Consumes: nothing (data file).
- Produces: 8 new top-level keys (`gutenberg_science`, `gutenberg_social_science`, `gutenberg_law`, `gutenberg_history`, `gutenberg_philosophy_religion`, `gutenberg_poetry_drama`, `gutenberg_children`, `gutenberg_literature`) — consumed by Tasks 5 and 6's DAGs via `paperDownloader.load_search_config(source)`.

- [ ] **Step 1: Add the new keys**

Edit `dags/configs/search_configs.json`. Find the end of the file:

```json
  "english_social_science": [
    {
      "query": "mediatype:(texts) AND language:(eng) AND (subject:(sociology) OR subject:(economics) OR subject:(\"political science\") OR subject:(\"social science\")) AND year:[1000 TO 1929]",
      "max_results": 5000,
      "repeat": true
    }
  ]
}
```

Replace the last two lines (`  ]` and `}`) with (closing the `english_social_science` array, then adding the 8 new keys, then the file's final closing brace):

```json
  ],
  "gutenberg_science": [
    {
      "query": "science",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_social_science": [
    {
      "query": "economics",
      "max_results": 5000,
      "repeat": true
    },
    {
      "query": "sociology",
      "max_results": 5000,
      "repeat": true
    },
    {
      "query": "political science",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_law": [
    {
      "query": "law",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_history": [
    {
      "query": "history",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_philosophy_religion": [
    {
      "query": "philosophy",
      "max_results": 5000,
      "repeat": true
    },
    {
      "query": "religion",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_poetry_drama": [
    {
      "query": "poetry",
      "max_results": 5000,
      "repeat": true
    },
    {
      "query": "drama",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_children": [
    {
      "query": "children",
      "max_results": 5000,
      "repeat": true
    }
  ],
  "gutenberg_literature": [
    {
      "query": "fiction",
      "max_results": 5000,
      "repeat": true
    }
  ]
}
```

The net result must be one valid JSON object with 31 top-level keys (23 existing + 8 new).

- [ ] **Step 2: Verify valid JSON**

Run: `python -c "import json; d = json.load(open('dags/configs/search_configs.json', encoding='utf-8')); print(len(d), 'sources'); assert 'gutenberg_literature' in d"`
Expected: `31 sources` (23 existing + 8 new), no exception.

- [ ] **Step 3: Commit**

```bash
git add dags/configs/search_configs.json
git commit -m "feat: add search criteria for 8 Project Gutenberg genre sources"
```

---

### Task 4: `pdf-downloading-dag.py` — non-PDF extension + gutenberg routing

**Files:**
- Modify: `dags/pdf-downloading-dag.py`

**Interfaces:**
- Consumes: `parse_download_tag(url: str) -> tuple[str, str, str] | None` from Task 2.
- Produces: nothing new (internal DAG task logic).

- [ ] **Step 1: Add the import**

In `dags/pdf-downloading-dag.py`, inside `downloadPdfFiles()`, find:

```python
        from repositories.pdf_repository import PdfRepository
        from repositories.proxy_repository import ProxyRepository
        import ftpConnector
        from ftpConnector import ftpConnector
```

Replace with:

```python
        from repositories.pdf_repository import PdfRepository
        from repositories.proxy_repository import ProxyRepository
        from gutenbergDownloader import parse_download_tag
        import ftpConnector
        from ftpConnector import ftpConnector
```

- [ ] **Step 2: Default the extension to `.pdf`**

Find:

```python
                filename = ""
                if 'arxiv' in url:
                    filename = 'arxiv/'
```

Replace with:

```python
                filename = ""
                ext = 'pdf'
                if 'arxiv' in url:
                    filename = 'arxiv/'
```

- [ ] **Step 3: Add the gutenberg routing branch and use `ext` for the suffix**

Find:

```python
                if '#customquery_' in url:
                    tag = url.rsplit('#customquery_', 1)[1]
                    filename = f'custom/{tag}/'
                    url = url.rsplit('#', 1)[0]
                filename += str(uuid.uuid4())
                filename += '.pdf'
```

Replace with:

```python
                if '#customquery_' in url:
                    tag = url.rsplit('#customquery_', 1)[1]
                    filename = f'custom/{tag}/'
                    url = url.rsplit('#', 1)[0]
                gutenberg_match = parse_download_tag(url)
                if gutenberg_match:
                    folder, ext, url = gutenberg_match
                    filename = f'gutenberg/{folder}/'
                filename += str(uuid.uuid4())
                filename += '.' + ext
```

- [ ] **Step 4: Verify the file still has valid syntax**

Run: `python -m py_compile dags/pdf-downloading-dag.py`
Expected: no output, exit code 0

- [ ] **Step 5: Manually verify the new branch's logic against Task 2's tested function**

Run:

```bash
python -c "
import sys; sys.path.insert(0, 'dags')
from gutenbergDownloader import parse_download_tag

# Gutenberg URL: routes to gutenberg/<folder>/ with the real extension
m = parse_download_tag('https://www.gutenberg.org/ebooks/1.epub3.images#gutenberg_science.epub')
assert m == ('science', 'epub', 'https://www.gutenberg.org/ebooks/1.epub3.images'), m

# Non-gutenberg URL: parse_download_tag returns None, ext stays the 'pdf' default
assert parse_download_tag('https://archive.org/download/foo/foo.pdf#gujarati_law') is None
print('OK')
"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add dags/pdf-downloading-dag.py
git commit -m "feat: download the actual available format for gutenberg URLs instead of assuming .pdf"
```

---

### Task 5: Five single-criterion Gutenberg DAGs (science, law, history, children, literature)

**Files:**
- Create: `dags/download-gutenberg-science-dag.py`
- Create: `dags/download-gutenberg-law-dag.py`
- Create: `dags/download-gutenberg-history-dag.py`
- Create: `dags/download-gutenberg-children-dag.py`
- Create: `dags/download-gutenberg-literature-dag.py`

**Interfaces:**
- Consumes: `search_books(query, page, proxy, tag)` from Task 1; `paperDownloader.run_search(service_id, source, adapter_fn, use_proxy)` (existing).
- Produces: 5 Airflow DAGs, `service_id` 28, 30, 31, 34, 35.

- [ ] **Step 1: Create the 5 DAG files**

`dags/download-gutenberg-science-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_science",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_science():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_science',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=28, source='gutenberg_science', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_science()
```

`dags/download-gutenberg-law-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_law",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_law():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_law',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=30, source='gutenberg_law', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_law()
```

`dags/download-gutenberg-history-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_history",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_history():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_history',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=31, source='gutenberg_history', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_history()
```

`dags/download-gutenberg-children-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_children",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_children():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_children',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=34, source='gutenberg_children', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_children()
```

`dags/download-gutenberg-literature-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_literature",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_literature():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_literature',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=35, source='gutenberg_literature', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_literature()
```

- [ ] **Step 2: Verify all 5 files have valid syntax**

Run: `python -m py_compile dags/download-gutenberg-science-dag.py dags/download-gutenberg-law-dag.py dags/download-gutenberg-history-dag.py dags/download-gutenberg-children-dag.py dags/download-gutenberg-literature-dag.py`
Expected: no output, exit code 0

- [ ] **Step 3: Commit**

```bash
git add dags/download-gutenberg-science-dag.py dags/download-gutenberg-law-dag.py dags/download-gutenberg-history-dag.py dags/download-gutenberg-children-dag.py dags/download-gutenberg-literature-dag.py
git commit -m "feat: add single-criterion Gutenberg DAGs (science, law, history, children, literature)"
```

---

### Task 6: Three multi-criterion Gutenberg DAGs (social_science, philosophy_religion, poetry_drama)

**Files:**
- Create: `dags/download-gutenberg-social-science-dag.py`
- Create: `dags/download-gutenberg-philosophy-religion-dag.py`
- Create: `dags/download-gutenberg-poetry-drama-dag.py`

**Interfaces:**
- Consumes: same as Task 5 (`search_books`, `run_search`) — the DAG code is identical in shape to Task 5; only `search_configs.json` (Task 3) supplies multiple rotating criteria per source.
- Produces: 3 Airflow DAGs, `service_id` 29, 32, 33.

- [ ] **Step 1: Create the 3 DAG files**

`dags/download-gutenberg-social-science-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_social_science",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_social_science():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_social_science',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=29, source='gutenberg_social_science', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_social_science()
```

`dags/download-gutenberg-philosophy-religion-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_philosophy_religion",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_philosophy_religion():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_philosophy_religion',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=32, source='gutenberg_philosophy_religion', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_philosophy_religion()
```

`dags/download-gutenberg-poetry-drama-dag.py`:

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_poetry_drama",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_poetry_drama():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_poetry_drama',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=33, source='gutenberg_poetry_drama', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_poetry_drama()
```

- [ ] **Step 2: Verify all 3 files have valid syntax**

Run: `python -m py_compile dags/download-gutenberg-social-science-dag.py dags/download-gutenberg-philosophy-religion-dag.py dags/download-gutenberg-poetry-drama-dag.py`
Expected: no output, exit code 0

- [ ] **Step 3: Commit**

```bash
git add dags/download-gutenberg-social-science-dag.py dags/download-gutenberg-philosophy-religion-dag.py dags/download-gutenberg-poetry-drama-dag.py
git commit -m "feat: add multi-criterion Gutenberg DAGs (social_science, philosophy_religion, poetry_drama)"
```

---

### Task 7: Database migration — round-robin rotation patterns

**Files:**
- Create: `Database/database-v0.28.sql`

**Interfaces:**
- Consumes: nothing.
- Produces: updated `dbo.GetPdfToDownload` stored procedure, deployed to the live `mssql` container per the project's existing manual-sync deployment process (git repo is not the live database).

- [ ] **Step 1: Write the migration**

Create `Database/database-v0.28.sql`:

```sql
-- Adds the 8 new Gutenberg genre sources to GetPdfToDownload's round-robin
-- rotation, so pdf_downloading actually claims their discovered URLs.
ALTER PROCEDURE [dbo].[GetPdfToDownload]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @claimThreshold datetime2 = DATEADD(MINUTE, -5, SYSUTCDATETIME());
	DECLARE @result TABLE (PDFUrl nvarchar(max));

	DECLARE @sources TABLE (Idx int IDENTITY(0,1), Pattern nvarchar(50));
	INSERT INTO @sources (Pattern) VALUES
		('%gujarati_literature%'),
		('%gujarati_news%'),
		('%gujarati_science_natural%'),
		('%gujarati_science_social%'),
		('%gujarati_law%'),
		('%gujarati_official%'),
		('%gujarati_dictionary%'),
		('%russian_science%'),
		('%russian_literature_modern%'),
		('%russian_literature_classic%'),
		('%russian_news%'),
		('%russian_law%'),
		('%russian_social_science%'),
		('%english_science%'),
		('%english_literature_modern%'),
		('%english_literature_classic%'),
		('%english_news%'),
		('%english_law%'),
		('%english_social_science%'),
		('%gutenberg_science%'),
		('%gutenberg_social_science%'),
		('%gutenberg_law%'),
		('%gutenberg_history%'),
		('%gutenberg_philosophy_religion%'),
		('%gutenberg_poetry_drama%'),
		('%gutenberg_children%'),
		('%gutenberg_literature%'),
		('%arxiv%'),
		('%arxiv%'),
		('%arxiv%'),
		('%lenin%'),
		('%lenin%'),
		('%lenin%'),
		('%ncbi%'),
		('%ncbi%'),
		('%ncbi%');

	DECLARE @numSources int = (SELECT COUNT(*) FROM @sources);
	DECLARE @startIdx int = (SELECT LastIndex FROM dbo.PdfSourceRotation);
	DECLARE @i int = 0;
	DECLARE @pattern nvarchar(50);
	DECLARE @tryIdx int;
	DECLARE @claimedIdx int = NULL;

	WHILE @i < @numSources AND NOT EXISTS (SELECT 1 FROM @result)
	BEGIN
		SET @tryIdx = (@startIdx + 1 + @i) % @numSources;
		SELECT @pattern = Pattern FROM @sources WHERE Idx = @tryIdx;

		UPDATE TOP(1) dbo.PdfDocuments
		SET ClaimedAt = SYSUTCDATETIME()
		OUTPUT INSERTED.PDFUrl INTO @result
		WHERE LocationInFileSystem = ''
		  AND PDFUrl NOT LIKE '%springer%'
		  AND PDFUrl LIKE @pattern
		  AND (ClaimedAt IS NULL OR ClaimedAt <= @claimThreshold);

		IF EXISTS (SELECT 1 FROM @result)
			SET @claimedIdx = @tryIdx;

		SET @i = @i + 1;
	END

	IF NOT EXISTS (SELECT 1 FROM @result)
	BEGIN
		UPDATE TOP(1) dbo.PdfDocuments
		SET ClaimedAt = SYSUTCDATETIME()
		OUTPUT INSERTED.PDFUrl INTO @result
		WHERE LocationInFileSystem = ''
		  AND PDFUrl NOT LIKE '%springer%'
		  AND (ClaimedAt IS NULL OR ClaimedAt <= @claimThreshold);
	END

	IF EXISTS (SELECT 1 FROM @result)
		UPDATE dbo.PdfSourceRotation SET LastIndex = @claimedIdx;

	SELECT PDFUrl FROM @result;
END
GO
```

- [ ] **Step 2: Verify against the current live procedure before applying**

This must be applied to the **live** `mssql` container on `corpus-host` (`/home/s939/apache-airflow/docker-compose.yaml`), not just committed to git — per this repo's established deployment process, the git clone is not the running database. Before running it:

The SA password is a live production credential — never paste it into a git-tracked file (this plan included). Read it at execution time from the deployment's own environment rather than hardcoding it:
```bash
ssh corpus-host "grep MSSQL_SA_PASSWORD /home/s939/apache-airflow/scripts/report.env"
```
Use the value that command prints (call it `$SA_PW` below) only in local shell variables/interactive commands, never committed anywhere.

Run (read-only, confirms the current live body matches what this migration assumes as its starting point):
```bash
ssh corpus-host "docker exec apache-airflow-mssql-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '$SA_PW' -C -Q \"EXEC sp_helptext 'GetPdfToDownload'\""
```
Expected: output matches `database-v0.23.sql`'s body (the 27-pattern version, no `gutenberg_*` rows yet).

- [ ] **Step 3: Apply to the live database (separate, explicit user go-ahead required)**

This is a production DB write — per this repo's established norm, do not run it as part of a combined command chain; get explicit confirmation naming this exact change first, then:
```bash
scp Database/database-v0.28.sql corpus-host:/tmp/database-v0.28.sql
ssh corpus-host "docker cp /tmp/database-v0.28.sql apache-airflow-mssql-1:/tmp/database-v0.28.sql"
ssh corpus-host "docker exec apache-airflow-mssql-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '\$SA_PW' -C -i /tmp/database-v0.28.sql -d TextCorpuses"
```

- [ ] **Step 4: Commit**

```bash
git add Database/database-v0.28.sql
git commit -m "feat: add Gutenberg genre sources to GetPdfToDownload round-robin rotation"
```

---

### Task 8: Deploy DAG/adapter files + README documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing (documentation + deployment only).

- [ ] **Step 1: Deploy the new/changed Python files to the live Airflow deployment**

Per this repo's established deployment process (the live `dag-processor` reads `dags/` from inside the `apache-airflow-airflow-scheduler-1` container, a separate physical copy from the git clone — `docker cp`, not a raw host `cp`, since the target directory is `50000:root`-owned):

```bash
for f in gutenbergDownloader.py pdf-downloading-dag.py download-gutenberg-science-dag.py download-gutenberg-law-dag.py download-gutenberg-history-dag.py download-gutenberg-children-dag.py download-gutenberg-literature-dag.py download-gutenberg-social-science-dag.py download-gutenberg-philosophy-religion-dag.py download-gutenberg-poetry-drama-dag.py; do
  scp "dags/$f" "corpus-host:/tmp/$f"
  ssh corpus-host "docker cp /tmp/$f apache-airflow-airflow-scheduler-1:/opt/airflow/dags/$f"
done
scp dags/configs/search_configs.json corpus-host:/tmp/search_configs.json
ssh corpus-host "docker cp /tmp/search_configs.json apache-airflow-airflow-scheduler-1:/opt/airflow/dags/configs/search_configs.json"
```

- [ ] **Step 2: Verify the new DAGs appear with no import errors**

Run:
```bash
ssh corpus-host "docker exec apache-airflow-airflow-scheduler-1 airflow dags list 2>&1 | grep gutenberg"
ssh corpus-host "docker exec apache-airflow-airflow-scheduler-1 airflow dags list-import-errors"
```
Expected: 8 `download_gutenberg_*` DAGs listed; no import errors mentioning `gutenberg`.

- [ ] **Step 3: Update `README.md` — English DAG table**

Find (the last row of the English source-DAG table, right before `custom_query_processor`):

```
| `download_english_social_science` | `@continuous` | Internet Archive — English-language sociology/economics/political science, `year:[1000 TO 1929]` |
| `custom_query_processor` | `@continuous` | Fulfills ad hoc queries submitted via the custom-query web UI (`webui/`) |
```

Replace with:

```
| `download_english_social_science` | `@continuous` | Internet Archive — English-language sociology/economics/political science, `year:[1000 TO 1929]` |
| `download_gutenberg_science` | `@continuous` | Project Gutenberg (Gutendex API) — science books |
| `download_gutenberg_social_science` | `@continuous` | Project Gutenberg (Gutendex API) — economics/sociology/political science books |
| `download_gutenberg_law` | `@continuous` | Project Gutenberg (Gutendex API) — law books |
| `download_gutenberg_history` | `@continuous` | Project Gutenberg (Gutendex API) — history books |
| `download_gutenberg_philosophy_religion` | `@continuous` | Project Gutenberg (Gutendex API) — philosophy/religion books |
| `download_gutenberg_poetry_drama` | `@continuous` | Project Gutenberg (Gutendex API) — poetry/drama books |
| `download_gutenberg_children` | `@continuous` | Project Gutenberg (Gutendex API) — children's books |
| `download_gutenberg_literature` | `@continuous` | Project Gutenberg (Gutendex API) — general fiction |
| `custom_query_processor` | `@continuous` | Fulfills ad hoc queries submitted via the custom-query web UI (`webui/`) |
```

- [ ] **Step 4: Update `README.md` — Russian architecture bullet list + DAG count**

Find:

```
Конвейер состоит из **42 DAG**, разбитых на группы:
```

Replace with:

```
Конвейер состоит из **50 DAG**, разбитых на группы:
```

Find:

```
- **Англоязычный корпус (6 DAG):** `download_english_science`, `download_english_literature_modern`, `download_english_literature_classic`, `download_english_news`, `download_english_law`, `download_english_social_science`
- **Пользовательские запросы:** `custom_query_processor` — обрабатывает запросы из веб-интерфейса `webui/`
```

Replace with:

```
- **Англоязычный корпус (6 DAG):** `download_english_science`, `download_english_literature_modern`, `download_english_literature_classic`, `download_english_news`, `download_english_law`, `download_english_social_science`
- **Источники книг Project Gutenberg (8 DAG):** `download_gutenberg_science`, `download_gutenberg_social_science`, `download_gutenberg_law`, `download_gutenberg_history`, `download_gutenberg_philosophy_religion`, `download_gutenberg_poetry_drama`, `download_gutenberg_children`, `download_gutenberg_literature`
- **Пользовательские запросы:** `custom_query_processor` — обрабатывает запросы из веб-интерфейса `webui/`
```

- [ ] **Step 5: Update both migration changelog tables**

In the English table, find:

```
| `database-v0.25.sql` | Adds `GetTopProxiesForValidation` — returns the top-N proxies by the same ranking `GetLatestProxy` uses, for the new `validate_proxies` DAG |
```

Replace with:

```
| `database-v0.25.sql` | Adds `GetTopProxiesForValidation` — returns the top-N proxies by the same ranking `GetLatestProxy` uses, for the new `validate_proxies` DAG |
| `database-v0.28.sql` | Adds the 8 Project Gutenberg genre sources to `GetPdfToDownload`'s round-robin rotation |
```

In the Russian table, find:

```
| `database-v0.25.sql` | Добавляет `GetTopProxiesForValidation` — возвращает топ-N прокси по тому же ранжированию, что и `GetLatestProxy`, для нового DAG `validate_proxies` |
```

Replace with:

```
| `database-v0.25.sql` | Добавляет `GetTopProxiesForValidation` — возвращает топ-N прокси по тому же ранжированию, что и `GetLatestProxy`, для нового DAG `validate_proxies` |
| `database-v0.28.sql` | Добавляет 8 источников книг Project Gutenberg в round-robin ротацию `GetPdfToDownload` |
```

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: document the 8 new Project Gutenberg source DAGs"
```

---

## Post-implementation smoke check (not a task — run once Tasks 1–8 are all deployed)

Confirm at least one Gutenberg DAG discovers real URLs and `pdf_downloading` claims one, following the pattern used to verify every prior source expansion in this project:

```bash
ssh corpus-host "docker exec apache-airflow-airflow-scheduler-1 airflow tasks test download_gutenberg_science download_gutenberg_science $(date -u +%Y-%m-%dT%H:%M:%S)"
ssh corpus-host "docker exec apache-airflow-mssql-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P '$SA_PW' -C -Q \"SELECT TOP 5 PDFUrl FROM TextCorpuses.dbo.PdfDocuments WHERE PDFUrl LIKE '%gutenberg_science%' ORDER BY InsertedAt DESC\" -d TextCorpuses"
```
Expected: rows with URLs ending `#gutenberg_science.{epub|html|txt|pdf}`.
