# Infrastructure Consolidation + SOLID Refactoring — Design Spec

**Date:** 2026-06-02
**Sub-project:** 1 of 3 (Infrastructure consolidation + SOLID)
**Scope:** Eliminate root-level module duplicates, make `dags/` fully self-contained, split `dbConnector` by domain (SRP), update all import sites.

---

## Problem

The repository has four critical structural issues:

1. **Duplicated modules** — `ftpConnector.py`, `dbConnector.py`, `anaphoraResolverLapinLiass.py`, and `configs.py` exist at the repo root AND inside `dags/`. Any change must be made twice.
2. **`dags/` is not self-contained** — `dags/ftpConnector.py` and `dags/dbConnector.py` both do `from configs import getConfig`, but there is no `configs.py` inside `dags/`. They depend on the root-level `configs.py` being on Python's path — fragile and prevents copying `dags/` to Airflow independently.
3. **SRP violation in `dbConnector`** — one class with 20+ static methods spanning six domains (proxies, PDFs, latex, graph jobs, file state, service state). Any caller that needs one method imports the entire God object.
4. **Root standalone scripts depend on root-level modules** — they cannot work if those root-level modules are removed without a migration path.

---

## Approach: In-place consolidation

All canonical shared modules live inside `dags/`. Root standalone scripts add `dags/` to `sys.path` and import from there. Root-level duplicate files are deleted.

---

## Directory Changes

### New files created

```
dags/
├── configs.py                          ← NEW: self-contained config loader
└── repositories/
    ├── __init__.py                     ← NEW: empty package marker
    ├── proxy_repository.py             ← NEW: proxy domain
    ├── pdf_repository.py               ← NEW: PDF domain
    ├── latex_repository.py             ← NEW: latex/text domain
    ├── graph_job_repository.py         ← NEW: graph job + file state domain
    └── service_state_repository.py     ← NEW: service state domain
```

### Modified files

| File | Change |
|------|--------|
| `dags/ftpConnector.py` | No logic change; `from configs import getConfig` now resolves to `dags/configs.py` |
| `dags/pdfConverter.py` | `from dbConnector import databaseConnector` → `from repositories.latex_repository import LatexRepository` |
| `dags/paperDownloader.py` | Update to import from `ProxyRepository`, `PdfRepository`, `ServiceStateRepository` as needed |
| All other `dags/` modules importing `dbConnector` | Update to specific repository import |
| All root standalone scripts | Add `sys.path.insert(0, …/dags)` header; update imports to repositories |
| `dags/tests/conftest.py` | Mock `configs` resolves to `dags/configs`; update if needed |

### Deleted files (root-level duplicates)

- `configs.py`
- `ftpConnector.py`
- `dbConnector.py`
- `anaphoraResolverLapinLiass.py`

---

## `dags/configs.py`

```python
import json
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'configs', 'configs.json')

def getConfig() -> dict:
    with open(_CONFIG_PATH) as f:
        return json.load(f)
```

Resolves `dags/configs/configs.json` relative to its own `__file__`, so it works regardless of CWD or Airflow launch directory. The `dags/configs/configs.json` file is unchanged.

---

## `dags/repositories/`

### `proxy_repository.py`

```python
import pyodbc
from configs import getConfig

class ProxyRepository:
    @staticmethod
    def add_or_update(ip, port, last_checked, protocols): ...

    @staticmethod
    def mark_broken(ip): ...

    @staticmethod
    def get_latest() -> dict: ...
```

### `pdf_repository.py`

```python
class PdfRepository:
    @staticmethod
    def add_url(url): ...

    @staticmethod
    def get_next_to_download() -> str | None: ...

    @staticmethod
    def save_location(url, location): ...
```

### `latex_repository.py`

```python
class LatexRepository:
    @staticmethod
    def get_next_to_convert() -> str | None: ...

    @staticmethod
    def save_location(url, location): ...
```

### `graph_job_repository.py`

```python
class GraphJobRepository:
    @staticmethod
    def insert_job(config, paths): ...

    @staticmethod
    def get_job_for_preparation() -> object | None: ...

    @staticmethod
    def set_job_error(job_id, error): ...

    @staticmethod
    def transition_to_execution(job_id): ...

    @staticmethod
    def process_to_text_copying(job_id): ...

    @staticmethod
    def get_processor_config(job_id) -> str | None: ...

    @staticmethod
    def get_files_for_job(job_id) -> list: ...

    @staticmethod
    def add_file_source(location, job_id): ...

    @staticmethod
    def get_file_for_anaphora() -> object | None: ...

    @staticmethod
    def mark_anaphora_done(file_id, resolved_path): ...

    @staticmethod
    def get_file_for_graph_building() -> object | None: ...

    @staticmethod
    def mark_graph_done(file_id): ...

    @staticmethod
    def set_file_error(file_id, error): ...

    @staticmethod
    def finalize_completed_jobs() -> object | None: ...
```

### `service_state_repository.py`

```python
class ServiceStateRepository:
    @staticmethod
    def get(service_id) -> object | None: ...

    @staticmethod
    def update(service_id, state): ...

    @staticmethod
    def remove(service_id): ...
```

Each repository imports `from configs import getConfig` and `import pyodbc`. Each method opens, uses, and closes its own DB connection (preserving current behaviour).

---

## Root Standalone Scripts

Every root-level script gains this two-line header before any other imports:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))
```

Imports are then updated from the monolithic `databaseConnector` to the specific repository needed. Script logic is not changed.

**Affected scripts:**
- `get-proxies.py`
- `get-proxies-2.py`
- `pdf-downloader.py`
- `concept-tree-builder.py`
- `concept-tree-analizer.py`
- `prepare-graph-calculation-worker.py`
- `text-preparation-for-graph-construction.py`
- `springerWorker.py`
- `arxivWorker.py`
- `cyberlenin-pdflinks-downloading.py`
- `graph-construction.py`
- `hardcoded-proxy.py`

Note: `pdf-to-latex-converter.py` already has the correct `sys.path.insert` header and imports only from `pdfConverter` — no changes needed.

---

## Testing

### New: `dags/tests/test_configs.py`

Verifies `getConfig()` resolves the path relative to `__file__`, not CWD:

```python
def test_getConfig_resolves_relative_to_file(tmp_path, monkeypatch):
    # Write a fake configs.json next to a fake configs.py
    ...
    result = getConfig()
    assert result == {"key": "value"}
```

### New: `dags/tests/test_repositories.py`

One test per public method per repository. All DB calls mocked with `unittest.mock.patch` on `pyodbc.connect`. Example:

```python
def test_proxy_repository_mark_broken():
    with patch('repositories.proxy_repository.pyodbc.connect') as mock_connect, \
         patch('repositories.proxy_repository.getConfig', return_value={"ConnectionString": "..."}):
        ProxyRepository.mark_broken('1.2.3.4')
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.assert_called_once_with(
            "execute [dbo].[MarkProxyAsBroken] @ip = ?", ('1.2.3.4')
        )
```

### Regression: all 42 existing tests still pass

Run `pytest dags/tests/ -k "not spacy_neural" -v` after every task. No existing test may be broken.

---

## Out of Scope

- Extracting standalone script logic into `dags/` shared modules (Sub-project 2)
- Full test coverage for standalone script logic (Sub-project 2)
- `requirements.txt`, git hook, README update (Sub-project 3)
- Changing static methods to instance methods or adding constructor injection
