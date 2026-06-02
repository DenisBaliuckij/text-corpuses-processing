# Infrastructure Consolidation + SOLID Refactoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate root-level module duplicates, make `dags/` fully self-contained, split `dbConnector` into domain repositories (SRP), and update all callers.

**Architecture:** A new `dags/configs.py` provides self-contained config loading. Five `dags/repositories/` classes each own exactly one DB domain. Root standalone scripts add `dags/` to `sys.path` and import from there. Root-level duplicate files are deleted last, after all callers are migrated.

**Tech Stack:** Python 3.10+, pyodbc, pytest, unittest.mock

---

## File Map

| File | Action |
|------|--------|
| `dags/configs.py` | **Create** — self-contained config loader |
| `dags/repositories/__init__.py` | **Create** — empty package marker |
| `dags/repositories/proxy_repository.py` | **Create** — ProxyRepository (3 methods) |
| `dags/repositories/pdf_repository.py` | **Create** — PdfRepository (3 methods) |
| `dags/repositories/latex_repository.py` | **Create** — LatexRepository (2 methods) |
| `dags/repositories/graph_job_repository.py` | **Create** — GraphJobRepository (16 methods) |
| `dags/repositories/service_state_repository.py` | **Create** — ServiceStateRepository (3 methods) |
| `dags/tests/test_configs.py` | **Create** — configs tests |
| `dags/tests/test_repositories.py` | **Create** — repository tests |
| `dags/pdfConverter.py` | **Modify** — use LatexRepository |
| `dags/tests/test_pdf_converter.py` | **Modify** — fix patch targets |
| `dags/paperDownloader.py` | **Modify** — use specific repositories |
| `dags/finalize-job-dag.py` | **Modify** — module-level import → GraphJobRepository |
| `dags/tools/generate_metrics_report.py` | **Modify** — module-level import → GraphJobRepository |
| `dags/build-graph-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/build-graph-llm-v2-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/build-graph-hierarchical-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/resolve-anaphora-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/prepare-graph-construction-job-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/pdf-downloading-dag.py` | **Modify** — inline import → PdfRepository + ProxyRepository |
| `dags/start-graph-formation-job-dag.py` | **Modify** — inline import → GraphJobRepository |
| `dags/get-proxies-dag-2.py` | **Modify** — inline import → ProxyRepository |
| `dags/brightdata-proxy-update.py` | **Modify** — inline import → ProxyRepository |
| `dags/get-arxiv-pdf-urls-dag.py` | **Modify** — inline import → ProxyRepository + PdfRepository + ServiceStateRepository |
| `dags/get-springer-pdf-urls-dag.py` | **Modify** — same as get-arxiv |
| `dags/get-Lenin-pdf-urls-dag.py` | **Modify** — same as get-arxiv (note: file is `get-Lenin-pdf-urls-dag.py` or `get-login`) |
| `get-proxies.py` | **Modify** — add sys.path header, use ProxyRepository |
| `get-proxies-2.py` | **Modify** — add sys.path header, use ProxyRepository |
| `pdf-downloader.py` | **Modify** — add sys.path header, use PdfRepository + ProxyRepository |
| `graph-construction.py` | **Modify** — add sys.path header, use GraphJobRepository |
| `text-preparation-for-graph-construction.py` | **Modify** — add sys.path header, use GraphJobRepository |
| `prepare-graph-calculation-worker.py` | **Modify** — add sys.path header, use GraphJobRepository |
| `hardcoded-proxy.py` | **Modify** — add sys.path header, use ProxyRepository |
| `arxivWorker.py` | **Modify** — add sys.path header, use repositories |
| `springerWorker.py` | **Modify** — add sys.path header, use repositories |
| `cyberlenin-pdflinks-downloading.py` | **Modify** — add sys.path header, use repositories |
| `configs.py` | **Delete** — root duplicate |
| `ftpConnector.py` | **Delete** — root duplicate |
| `dbConnector.py` | **Delete** — root duplicate |
| `anaphoraResolverLapinLiass.py` | **Delete** — root duplicate |
| `dags/dbConnector.py` | **Delete** — replaced by repositories/ |

---

## Task 1: Create `dags/configs.py`

**Files:**
- Create: `dags/configs.py`
- Create: `dags/tests/test_configs.py`

- [ ] **Step 1.1: Write failing test**

Create `dags/tests/test_configs.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
from unittest.mock import patch, mock_open
import configs


def test_config_path_ends_with_configs_json():
    assert configs._CONFIG_PATH.endswith(os.path.join('configs', 'configs.json'))


def test_config_path_contains_dags():
    assert 'dags' in configs._CONFIG_PATH


def test_getConfig_returns_parsed_json():
    fake = {'ConnectionString': 'test', 'FtpHost': 'localhost'}
    with patch('builtins.open', mock_open(read_data=json.dumps(fake))):
        result = configs.getConfig()
    assert result == fake
```

- [ ] **Step 1.2: Run tests to confirm they fail**

```
pytest dags/tests/test_configs.py -v
```

Expected: `ModuleNotFoundError: No module named 'configs'` or attribute error (because root `configs.py` has no `_CONFIG_PATH`).

- [ ] **Step 1.3: Create `dags/configs.py`**

```python
import json
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'configs', 'configs.json')


def getConfig() -> dict:
    with open(_CONFIG_PATH) as f:
        return json.load(f)
```

- [ ] **Step 1.4: Run tests to confirm they pass**

```
pytest dags/tests/test_configs.py -v
```

Expected: 3 passed.

- [ ] **Step 1.5: Confirm existing tests still pass**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: all 45 tests pass (42 existing + 3 new).

- [ ] **Step 1.6: Commit**

```
git add dags/configs.py dags/tests/test_configs.py
git commit -m "feat: add self-contained dags/configs.py resolving path relative to __file__"
```

---

## Task 2: Create `dags/repositories/proxy_repository.py`

**Files:**
- Create: `dags/repositories/__init__.py`
- Create: `dags/repositories/proxy_repository.py`
- Create: `dags/tests/test_repositories.py` (partial — proxy section only)

- [ ] **Step 2.1: Write failing tests**

Create `dags/tests/test_repositories.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
from repositories.proxy_repository import ProxyRepository

_CFG = {'ConnectionString': 'Driver={SQL Server};Server=test;'}


def test_proxy_add_or_update_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.add_or_update('1.2.3.4', 8080, 12345, 'http')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            ('1.2.3.4', 8080, 12345, 'http')
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_mark_broken_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.mark_broken('1.2.3.4')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[MarkProxyAsBroken] @ip = ?", ('1.2.3.4',)
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_get_latest_returns_dict():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        mock_cur.fetchone.return_value = ('1.2.3.4', 8080, 'http')
        result = ProxyRepository.get_latest()
        assert result == {'proxieIp': '1.2.3.4', 'proxiePort': 8080, 'proxieProtocol': 'http'}
```

- [ ] **Step 2.2: Run tests to confirm they fail**

```
pytest dags/tests/test_repositories.py -v
```

Expected: `ModuleNotFoundError: No module named 'repositories'`

- [ ] **Step 2.3: Create `dags/repositories/__init__.py` (empty)**

Create an empty file at `dags/repositories/__init__.py`.

- [ ] **Step 2.4: Create `dags/repositories/proxy_repository.py`**

```python
import pyodbc
from configs import getConfig


class ProxyRepository:
    @staticmethod
    def add_or_update(ip, port, last_checked, protocols):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            (ip, port, last_checked, protocols)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def mark_broken(ip):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkProxyAsBroken] @ip = ?", (ip,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_latest() -> dict:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return {'proxieIp': row[0], 'proxiePort': row[1], 'proxieProtocol': row[2]}
```

- [ ] **Step 2.5: Run tests to confirm they pass**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 3 passed.

- [ ] **Step 2.6: Confirm full suite still passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 48 passed.

- [ ] **Step 2.7: Commit**

```
git add dags/repositories/__init__.py dags/repositories/proxy_repository.py dags/tests/test_repositories.py
git commit -m "feat: add ProxyRepository with SRP-compliant proxy domain methods"
```

---

## Task 3: Create `pdf_repository.py` and `latex_repository.py`

**Files:**
- Create: `dags/repositories/pdf_repository.py`
- Create: `dags/repositories/latex_repository.py`
- Modify: `dags/tests/test_repositories.py` (append)

- [ ] **Step 3.1: Append failing tests to `dags/tests/test_repositories.py`**

Append to end of `dags/tests/test_repositories.py`:

```python
from repositories.pdf_repository import PdfRepository
from repositories.latex_repository import LatexRepository


def test_pdf_add_url_calls_stored_proc():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        PdfRepository.add_url('http://example.com/paper.pdf')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddPdfUrl] @url = ?", ('http://example.com/paper.pdf',)
        )


def test_pdf_get_next_to_download_returns_url():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('http://arxiv.org/pdf/123',)
        result = PdfRepository.get_next_to_download()
        assert result == 'http://arxiv.org/pdf/123'


def test_pdf_get_next_to_download_returns_none_when_empty():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = None
        result = PdfRepository.get_next_to_download()
        assert result is None


def test_pdf_save_location_calls_stored_proc():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        PdfRepository.save_location('http://example.com/paper.pdf', 'arxiv/abc.pdf')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?",
            ('http://example.com/paper.pdf', 'arxiv/abc.pdf')
        )


def test_latex_get_next_to_convert_returns_path():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('arxiv/paper.pdf',)
        result = LatexRepository.get_next_to_convert()
        assert result == 'arxiv/paper.pdf'


def test_latex_get_next_to_convert_returns_none_when_queue_empty():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (None,)
        result = LatexRepository.get_next_to_convert()
        assert result is None


def test_latex_save_location_calls_stored_proc():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        LatexRepository.save_location('arxiv/paper.pdf', 'Tex/paper.tex')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SaveLatexDocumentLocation] @pdfUrl = ?, @latexLocation=?",
            ('arxiv/paper.pdf', 'Tex/paper.tex')
        )
```

- [ ] **Step 3.2: Run tests to confirm new ones fail**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 3 existing proxy tests pass, 7 new tests fail with `ModuleNotFoundError`.

- [ ] **Step 3.3: Create `dags/repositories/pdf_repository.py`**

```python
import pyodbc
from configs import getConfig


class PdfRepository:
    @staticmethod
    def add_url(url):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddPdfUrl] @url = ?", (url,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_next_to_download() -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPdfToDownload]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def save_location(url, location):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?",
            (url, location)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()
```

- [ ] **Step 3.4: Create `dags/repositories/latex_repository.py`**

```python
import pyodbc
from configs import getConfig


class LatexRepository:
    @staticmethod
    def get_next_to_convert() -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPDFLocationForLatexConvertation]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def save_location(url, location):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SaveLatexDocumentLocation] @pdfUrl = ?, @latexLocation=?",
            (url, location)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()
```

- [ ] **Step 3.5: Run all repository tests**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 10 passed.

- [ ] **Step 3.6: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 55 passed.

- [ ] **Step 3.7: Commit**

```
git add dags/repositories/pdf_repository.py dags/repositories/latex_repository.py dags/tests/test_repositories.py
git commit -m "feat: add PdfRepository and LatexRepository"
```

---

## Task 4: Create `graph_job_repository.py`

**Files:**
- Create: `dags/repositories/graph_job_repository.py`
- Modify: `dags/tests/test_repositories.py` (append)

- [ ] **Step 4.1: Append failing tests**

Append to end of `dags/tests/test_repositories.py`:

```python
from repositories.graph_job_repository import GraphJobRepository


def test_graph_job_insert_job_calls_stored_proc():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        GraphJobRepository.insert_job('{"processorName":"RuleBased"}', 'arxiv/')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddGraphCreationJob] @config = ?, @paths=?",
            ('{"processorName":"RuleBased"}', 'arxiv/')
        )


def test_graph_job_get_for_preparation_returns_row():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (1, 0, 'arxiv/')
        result = GraphJobRepository.get_job_for_preparation()
        assert result == (1, 0, 'arxiv/')


def test_graph_job_get_for_preparation_returns_none_when_empty():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = None
        result = GraphJobRepository.get_job_for_preparation()
        assert result is None


def test_graph_job_finalize_returns_job_row():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (42,)
        result = GraphJobRepository.finalize_completed_jobs()
        assert result == (42,)
        mock_conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "execute [dbo].[FinalizeCompletedJobs]"
        )


def test_graph_job_set_file_error_calls_stored_proc():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        GraphJobRepository.set_file_error(5, 'oops')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SetFileError] @fileId = ?, @error = ?", (5, 'oops')
        )


def test_graph_job_get_processor_config_returns_string():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('{"processorName":"RuleBased"}',)
        result = GraphJobRepository.get_processor_config(1)
        assert result == '{"processorName":"RuleBased"}'


def test_graph_job_get_files_for_job_returns_list():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchall.return_value = [(1,), (2,)]
        result = GraphJobRepository.get_files_for_job(10)
        assert result == [(1,), (2,)]
```

- [ ] **Step 4.2: Run to confirm new tests fail**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 10 existing pass, 7 new fail with `ModuleNotFoundError`.

- [ ] **Step 4.3: Create `dags/repositories/graph_job_repository.py`**

```python
import pyodbc
from configs import getConfig


class GraphJobRepository:
    @staticmethod
    def insert_job(config, paths):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddGraphCreationJob] @config = ?, @paths=?", (config, paths)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_job_for_preparation():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForPreparation]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def set_job_error(job_id, error):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SetErrorForGraphCreationJob] @id = ?, @error = ?",
            (job_id, str(error))
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def transition_to_execution(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[TransitionJobToExecution] @jobId = ?", (job_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def process_to_text_copying(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[ProcessGraphCreationJobToTextCopying] @jobId = ?", (job_id,)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_processor_config(job_id) -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT ProcessorConfig FROM [dbo].[GraphConstructionJob] WHERE ID = ?", (job_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def get_files_for_job(job_id) -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT ID FROM [dbo].[GraphConstructionFiles] WHERE GraphConstructionJobId = ? AND Status = 20",
            (job_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows

    @staticmethod
    def add_file_source(location, job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddTextSourceForProcessing] @location = ?, @jobId = ?",
            (location, job_id)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_file_for_anaphora():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForAnaphoraResolution]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def mark_anaphora_done(file_id, resolved_path):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[MarkFileAnaphoraDone] @fileId = ?, @resolvedFilePath = ?",
            (file_id, resolved_path)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_file_for_graph_building():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForGraphBuilding]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def mark_graph_done(file_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileGraphDone] @fileId = ?", (file_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def set_file_error(file_id, error):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SetFileError] @fileId = ?, @error = ?", (file_id, str(error))
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def finalize_completed_jobs():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[FinalizeCompletedJobs]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def get_job_for_execution():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForExecution]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def get_text_source(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetTextSourceForProcessing] @jobId=?", (job_id,))
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row
```

- [ ] **Step 4.4: Run tests to confirm they pass**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 17 passed.

- [ ] **Step 4.5: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 62 passed.

- [ ] **Step 4.6: Commit**

```
git add dags/repositories/graph_job_repository.py dags/tests/test_repositories.py
git commit -m "feat: add GraphJobRepository consolidating all graph job and file state methods"
```

---

## Task 5: Create `service_state_repository.py`

**Files:**
- Create: `dags/repositories/service_state_repository.py`
- Modify: `dags/tests/test_repositories.py` (append)

- [ ] **Step 5.1: Append failing tests**

Append to end of `dags/tests/test_repositories.py`:

```python
from repositories.service_state_repository import ServiceStateRepository


def test_service_state_get_returns_row():
    with patch('repositories.service_state_repository.getConfig', return_value=_CFG), \
         patch('repositories.service_state_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('{"page": 1}',)
        result = ServiceStateRepository.get(4)
        assert result == ('{"page": 1}',)


def test_service_state_update_calls_stored_proc():
    with patch('repositories.service_state_repository.getConfig', return_value=_CFG), \
         patch('repositories.service_state_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ServiceStateRepository.update(4, '{"page": 2}')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[UpdateState] @serviceID = ?, @state = ?", (4, '{"page": 2}')
        )
        mock_conn.return_value.commit.assert_called_once()


def test_service_state_remove_calls_stored_proc():
    with patch('repositories.service_state_repository.getConfig', return_value=_CFG), \
         patch('repositories.service_state_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ServiceStateRepository.remove(4)
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[RemoveServiceState] @serviceID = ?", (4,)
        )
```

- [ ] **Step 5.2: Run to confirm new tests fail**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 17 existing pass, 3 new fail.

- [ ] **Step 5.3: Create `dags/repositories/service_state_repository.py`**

```python
import pyodbc
from configs import getConfig


class ServiceStateRepository:
    @staticmethod
    def get(service_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetServiceState] @serviceId = ?", (service_id,))
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def update(service_id, state):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[UpdateState] @serviceID = ?, @state = ?", (service_id, state)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def remove(service_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[RemoveServiceState] @serviceID = ?", (service_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()
```

- [ ] **Step 5.4: Run all repository tests**

```
pytest dags/tests/test_repositories.py -v
```

Expected: 20 passed.

- [ ] **Step 5.5: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 5.6: Commit**

```
git add dags/repositories/service_state_repository.py dags/tests/test_repositories.py
git commit -m "feat: add ServiceStateRepository"
```

---

## Task 6: Update `dags/pdfConverter.py` and its tests

**Files:**
- Modify: `dags/pdfConverter.py`
- Modify: `dags/tests/test_pdf_converter.py`

- [ ] **Step 6.1: Replace `dags/pdfConverter.py`**

Replace the entire file with:

```python
from pypdf import PdfReader
import io
import os
from ftpConnector import ftpConnector
from repositories.latex_repository import LatexRepository


def run_conversion() -> int:
    count = 0
    while True:
        url = LatexRepository.get_next_to_convert()
        if url is None:
            break
        try:
            file = ftpConnector.getFile(url)
            file.seek(0)
            reader = PdfReader(file)
            text = "".join(page.extract_text() or "" for page in reader.pages)
            tex_filename = os.path.splitext(url)[0] + '.tex'
            ftpConnector.storeFile(tex_filename, io.BytesIO(text.encode('utf-8')), 'Tex')
            LatexRepository.save_location(url, tex_filename)
            count += 1
        except Exception as e:
            print(f"Failed to convert {url}: {e}")
            LatexRepository.save_location(url, 'NA')
    return count
```

- [ ] **Step 6.2: Replace `dags/tests/test_pdf_converter.py`** (update patch targets)

Replace the entire file with:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
import pdfConverter


def test_empty_queue_returns_zero():
    with patch('pdfConverter.LatexRepository.get_next_to_convert', return_value=None):
        result = pdfConverter.run_conversion()
    assert result == 0


def test_converts_pdf_and_returns_count():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "hello world"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]
    call_order = []
    mock_file.seek = MagicMock(side_effect=lambda n: call_order.append('seek'))

    def mock_pdf_reader(f):
        call_order.append('PdfReader')
        return mock_reader

    with patch('pdfConverter.LatexRepository.get_next_to_convert', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', side_effect=mock_pdf_reader), \
         patch('pdfConverter.ftpConnector.storeFile') as mock_store, \
         patch('pdfConverter.LatexRepository.save_location') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 1
    assert call_order == ['seek', 'PdfReader'], f"Expected seek before PdfReader, got: {call_order}"
    assert mock_store.call_args[0][0] == 'arxiv/paper.tex'
    mock_save.assert_called_once_with('arxiv/paper.pdf', 'arxiv/paper.tex')


def test_failed_conversion_saves_na_and_loop_continues():
    mock_bad_file = MagicMock()
    mock_bad_file.seek.side_effect = Exception("FTP read error")

    mock_good_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "good text"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.LatexRepository.get_next_to_convert', side_effect=['bad/file.pdf', 'good/file.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', side_effect=[mock_bad_file, mock_good_file]), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile'), \
         patch('pdfConverter.LatexRepository.save_location') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 1
    assert mock_save.call_args_list[0] == ((('bad/file.pdf', 'NA'),), {})
    assert mock_save.call_args_list[1] == ((('good/file.pdf', 'good/file.tex'),), {})


def test_none_page_text_handled():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = None
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.LatexRepository.get_next_to_convert', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile'), \
         patch('pdfConverter.LatexRepository.save_location'):
        result = pdfConverter.run_conversion()

    assert result == 1
```

- [ ] **Step 6.3: Run pdfConverter tests**

```
pytest dags/tests/test_pdf_converter.py -v
```

Expected: 4 passed.

- [ ] **Step 6.4: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 6.5: Commit**

```
git add dags/pdfConverter.py dags/tests/test_pdf_converter.py
git commit -m "refactor: pdfConverter uses LatexRepository instead of databaseConnector"
```

---

## Task 7: Update `dags/paperDownloader.py`

**Files:**
- Modify: `dags/paperDownloader.py`

- [ ] **Step 7.1: Replace the import section at the top of `dags/paperDownloader.py`**

Replace lines 1–18 (everything up to and including `def load_search_config`) with:

```python
# -*- coding: utf-8 -*-
import json
import os

from repositories.service_state_repository import ServiceStateRepository
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository

_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
_SEARCH_CONFIG_PATH = os.path.join(_DAG_FOLDER, 'configs', 'search_configs.json')


def load_search_config(source: str) -> list:
    with open(_SEARCH_CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f).get(source, [])
```

- [ ] **Step 7.2: Replace `load_state`**

Replace:
```python
def load_state(service_id: int) -> dict:
    """Reads crawl state from ServiceState. Returns a fresh default state if none exists."""
    import dbConnector
    from dbConnector import databaseConnector
    result = databaseConnector.getServiceState(service_id)
    if result is None:
        return {'criterion_index': 0, 'page': 1, 'done_criteria': []}
    return json.loads(result[0])
```
With:
```python
def load_state(service_id: int) -> dict:
    result = ServiceStateRepository.get(service_id)
    if result is None:
        return {'criterion_index': 0, 'page': 1, 'done_criteria': []}
    return json.loads(result[0])
```

- [ ] **Step 7.3: Replace `save_state`**

Replace:
```python
def save_state(service_id: int, state: dict) -> None:
    """Persists crawl state to ServiceState as a JSON string."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.updateServiceState(service_id, json.dumps(state))
```
With:
```python
def save_state(service_id: int, state: dict) -> None:
    ServiceStateRepository.update(service_id, json.dumps(state))
```

- [ ] **Step 7.4: Replace `clear_state`**

Replace:
```python
def clear_state(service_id: int) -> None:
    """Deletes crawl state from ServiceState (all criteria exhausted)."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.removeServiceState(service_id)
```
With:
```python
def clear_state(service_id: int) -> None:
    ServiceStateRepository.remove(service_id)
```

- [ ] **Step 7.5: Replace `get_proxy`**

Replace:
```python
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
```
With:
```python
def get_proxy() -> dict:
    result = ProxyRepository.get_latest()
    if result is None:
        raise RuntimeError('No proxy available')
    return {
        'ip': str(result['proxieIp']).strip(),
        'port': result['proxiePort'],
        'protocol': str(result['proxieProtocol']).strip(),
    }
```

- [ ] **Step 7.6: Replace `mark_proxy_broken`**

Replace:
```python
def mark_proxy_broken(ip: str) -> None:
    """Marks a proxy as broken in the DB."""
    import dbConnector
    from dbConnector import databaseConnector
    databaseConnector.markProxyAsBroken(ip)
```
With:
```python
def mark_proxy_broken(ip: str) -> None:
    ProxyRepository.mark_broken(ip)
```

- [ ] **Step 7.7: Replace `save_urls`**

Replace:
```python
def save_urls(urls: list) -> None:
    """Calls databaseConnector.addPdfUrl() for each URL. Idempotent."""
    import dbConnector
    from dbConnector import databaseConnector
    for url in urls:
        databaseConnector.addPdfUrl(url)
```
With:
```python
def save_urls(urls: list) -> None:
    for url in urls:
        PdfRepository.add_url(url)
```

Leave `_next_active_index`, `advance_state`, and `run_search` unchanged.

- [ ] **Step 7.2: Run existing paperDownloader tests**

```
pytest dags/tests/test_paper_downloader.py -v
```

Expected: all existing tests pass (they test pure functions like `advance_state` which don't call DB).

- [ ] **Step 7.3: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 7.4: Commit**

```
git add dags/paperDownloader.py
git commit -m "refactor: paperDownloader uses repositories instead of databaseConnector"
```

---

## Task 8: Update module-level importers (`finalize-job-dag.py` and `generate_metrics_report.py`)

**Files:**
- Modify: `dags/finalize-job-dag.py`
- Modify: `dags/tools/generate_metrics_report.py`

- [ ] **Step 8.1: Replace imports in `dags/finalize-job-dag.py`**

Replace lines 8–9 (the module-level imports):
```python
from dbConnector import databaseConnector
from ftpConnector import ftpConnector
```
With:
```python
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
```

Then replace every `databaseConnector.` call in the file:
- `databaseConnector.finalizeCompletedJobs()` → `GraphJobRepository.finalize_completed_jobs()`
- `databaseConnector.getProcessorConfig(job_id)` → `GraphJobRepository.get_processor_config(job_id)`
- `databaseConnector.getFilesForJob(job_id)` → `GraphJobRepository.get_files_for_job(job_id)`

The complete updated `dags/finalize-job-dag.py`:

```python
# -*- coding: utf-8 -*-
import io
import json
import logging
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task
from repositories.graph_job_repository import GraphJobRepository
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
        result = GraphJobRepository.finalize_completed_jobs()
        if result is None:
            return

        job_id = result[0]
        print(f"Finalized job ID: {job_id}")

        config_json = GraphJobRepository.get_processor_config(job_id)
        config = json.loads(config_json) if config_json else {}
        processor = config.get("processorName", "RuleBased")

        try:
            if processor == "RuleBased":
                _process_rulebased(job_id)
            else:
                backend = "Hierarchical" if processor == "Hierarchical" else "LLMv2"
                file_rows = GraphJobRepository.get_files_for_job(job_id)
                for row in file_rows:
                    _process_per_file(job_id, row[0], backend)
        except Exception as e:
            logging.error(f"Metrics/visualization generation failed for job {job_id}: {e}")

    finalize_job()
```

- [ ] **Step 8.2: Update `dags/tools/generate_metrics_report.py`**

Replace lines 16–17:
```python
from dbConnector import databaseConnector
from ftpConnector import ftpConnector
```
With:
```python
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
```

Then replace:
- `databaseConnector.getProcessorConfig(job_id)` → `GraphJobRepository.get_processor_config(job_id)`
- `databaseConnector.getFilesForJob(job_id)` → `GraphJobRepository.get_files_for_job(job_id)`

- [ ] **Step 8.3: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 8.4: Commit**

```
git add dags/finalize-job-dag.py dags/tools/generate_metrics_report.py
git commit -m "refactor: finalize-job-dag and generate_metrics_report use GraphJobRepository"
```

---

## Task 9: Update all inline-importing DAGs

**Files:**
- Modify: `dags/build-graph-dag.py`
- Modify: `dags/build-graph-llm-v2-dag.py`
- Modify: `dags/build-graph-hierarchical-dag.py`
- Modify: `dags/resolve-anaphora-dag.py`
- Modify: `dags/prepare-graph-construction-job-dag.py`
- Modify: `dags/pdf-downloading-dag.py`
- Modify: `dags/start-graph-formation-job-dag.py`
- Modify: `dags/get-proxies-dag-2.py`
- Modify: `dags/brightdata-proxy-update.py`
- Modify: `dags/get-arxiv-pdf-urls-dag.py`
- Modify: `dags/get-springer-pdf-urls-dag.py`
- Modify: `dags/get-Lenin-pdf-urls-dag.py` (filename is `get-Lenin-pdf-urls-dag.py` or `get-login` — check `dags/get-l*.py`)

In each DAG file the task function has inline `import dbConnector / from dbConnector import databaseConnector`. Replace these with repository imports and update all method calls.

**Substitution table (apply in every file):**

| Old call | New import + call |
|----------|-------------------|
| `databaseConnector.getFileForGraphBuilding()` | `from repositories.graph_job_repository import GraphJobRepository` → `GraphJobRepository.get_file_for_graph_building()` |
| `databaseConnector.transitionJobToExecution(job_id)` | `GraphJobRepository.transition_to_execution(job_id)` |
| `databaseConnector.markFileGraphDone(file_id)` | `GraphJobRepository.mark_graph_done(file_id)` |
| `databaseConnector.setFileError(file_id, str(e))` | `GraphJobRepository.set_file_error(file_id, str(e))` |
| `databaseConnector.getFileForAnaphoraResolution()` | `GraphJobRepository.get_file_for_anaphora()` |
| `databaseConnector.getProcessorConfig(job_id)` | `GraphJobRepository.get_processor_config(job_id)` |
| `databaseConnector.markFileAnaphoraDone(file_id, path)` | `GraphJobRepository.mark_anaphora_done(file_id, path)` |
| `databaseConnector.getJobForPreparation()` | `GraphJobRepository.get_job_for_preparation()` |
| `databaseConnector.addFileSourceForGraphConstructionJob(path, job_id)` | `GraphJobRepository.add_file_source(path, job_id)` |
| `databaseConnector.processGraphCreationJobToTextCopying(job_id)` | `GraphJobRepository.process_to_text_copying(job_id)` |
| `databaseConnector.setErrorForPreparationJob(job_id, str(e))` | `GraphJobRepository.set_job_error(job_id, str(e))` |
| `databaseConnector.insertGraphCreationJob(config, paths)` | `GraphJobRepository.insert_job(config, paths)` |
| `databaseConnector.getPdfToDownload()` | `from repositories.pdf_repository import PdfRepository` → `PdfRepository.get_next_to_download()` |
| `databaseConnector.savePdfFileLocation(url, loc)` | `PdfRepository.save_location(url, loc)` |
| `databaseConnector.getLatestProxy()` | `from repositories.proxy_repository import ProxyRepository` → `ProxyRepository.get_latest()` |
| `databaseConnector.markProxyAsBroken(ip)` | `ProxyRepository.mark_broken(ip)` |
| `databaseConnector.addOrUpdateProxy(ip, port, ts, proto)` | `ProxyRepository.add_or_update(ip, port, ts, proto)` |
| `databaseConnector.addPdfUrl(url)` | `from repositories.pdf_repository import PdfRepository` → `PdfRepository.add_url(url)` |
| `databaseConnector.getServiceState(sid)` | `from repositories.service_state_repository import ServiceStateRepository` → `ServiceStateRepository.get(sid)` |
| `databaseConnector.updateServiceState(sid, state)` | `ServiceStateRepository.update(sid, state)` |
| `databaseConnector.removeServiceState(sid)` | `ServiceStateRepository.remove(sid)` |

- [ ] **Step 9.1: Update `dags/build-graph-dag.py`**

Replace the entire task function body (inside `def build_graph():`). Remove:
```python
import dbConnector
from dbConnector import databaseConnector
```
Add at top of task function:
```python
from repositories.graph_job_repository import GraphJobRepository
```
Replace all `databaseConnector.X()` calls using the table above.

- [ ] **Step 9.2: Update `dags/build-graph-llm-v2-dag.py`**

Same pattern as build-graph-dag. Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.graph_job_repository import GraphJobRepository`, apply substitution table.

- [ ] **Step 9.3: Update `dags/build-graph-hierarchical-dag.py`**

Same as above.

- [ ] **Step 9.4: Update `dags/resolve-anaphora-dag.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.graph_job_repository import GraphJobRepository`, apply substitution table.

- [ ] **Step 9.5: Update `dags/prepare-graph-construction-job-dag.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.graph_job_repository import GraphJobRepository`, apply substitution table.

- [ ] **Step 9.6: Update `dags/pdf-downloading-dag.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add:
```python
from repositories.pdf_repository import PdfRepository
from repositories.proxy_repository import ProxyRepository
```
Apply substitution table.

- [ ] **Step 9.7: Update `dags/start-graph-formation-job-dag.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.graph_job_repository import GraphJobRepository`, apply substitution table (`databaseConnector.insertGraphCreationJob` → `GraphJobRepository.insert_job`). Also remove unused imports (`requests`, `bs4`, `pyodbc`) from the task function.

- [ ] **Step 9.8: Update `dags/get-proxies-dag-2.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.proxy_repository import ProxyRepository`, replace `databaseConnector.addOrUpdateProxy(...)` → `ProxyRepository.add_or_update(...)`.

- [ ] **Step 9.9: Update `dags/brightdata-proxy-update.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add `from repositories.proxy_repository import ProxyRepository`, replace `databaseConnector.addOrUpdateProxy(...)` → `ProxyRepository.add_or_update(...)`. Also remove unused `import json,urllib.request`, `import time`, `import pyodbc`.

- [ ] **Step 9.10: Update `dags/get-arxiv-pdf-urls-dag.py`**

Remove `import dbConnector / from dbConnector import databaseConnector`, add:
```python
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.service_state_repository import ServiceStateRepository
```
Apply substitution table for all `databaseConnector.getServiceState`, `getLatestProxy`, `markProxyAsBroken`, `addPdfUrl`, `updateServiceState`, `removeServiceState` calls.

- [ ] **Step 9.11: Update `dags/get-springer-pdf-urls-dag.py`**

Inside `def getSpringerPdfUrls():`, replace:
```python
import dbConnector
from dbConnector import databaseConnector
```
With:
```python
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.service_state_repository import ServiceStateRepository
```
Then apply these substitutions throughout the function body:
- `databaseConnector.getServiceState(serviceID)` → `ServiceStateRepository.get(serviceID)`
- `databaseConnector.getLatestProxy()` → `ProxyRepository.get_latest()`
- `databaseConnector.markProxyAsBroken(str(proxieIp).strip())` → `ProxyRepository.mark_broken(str(proxieIp).strip())`
- `databaseConnector.addPdfUrl("https://link.springer.com/"+str(url["href"]).strip() + ".pdf")` → `PdfRepository.add_url("https://link.springer.com/"+str(url["href"]).strip() + ".pdf")`
- `databaseConnector.updateServiceState(serviceID, json.dumps(state))` → `ServiceStateRepository.update(serviceID, json.dumps(state))`
- `databaseConnector.removeServiceState(serviceID)` → `ServiceStateRepository.remove(serviceID)`

- [ ] **Step 9.12: Update `dags/get-Lenin-pdf-urls-dag.py`**

Inside `def getLeninPdfUrls():`, replace:
```python
import dbConnector
from dbConnector import databaseConnector
```
With:
```python
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.service_state_repository import ServiceStateRepository
```
Then apply these substitutions throughout the function body:
- `databaseConnector.getServiceState(serviceID)` → `ServiceStateRepository.get(serviceID)`
- `databaseConnector.getLatestProxy()` → `ProxyRepository.get_latest()`
- `databaseConnector.markProxyAsBroken(str(proxieIp).strip())` → `ProxyRepository.mark_broken(str(proxieIp).strip())`
- `databaseConnector.addPdfUrl("https://cyberleninka.ru/"+str(url["href"]).strip() + "/pdf")` → `PdfRepository.add_url("https://cyberleninka.ru/"+str(url["href"]).strip() + "/pdf")`
- `databaseConnector.updateServiceState(serviceID, json.dumps(state))` → `ServiceStateRepository.update(serviceID, json.dumps(state))`
- `databaseConnector.removeServiceState(serviceID)` → `ServiceStateRepository.remove(serviceID)`

- [ ] **Step 9.13: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 9.14: Commit**

```
git add dags/build-graph-dag.py dags/build-graph-llm-v2-dag.py dags/build-graph-hierarchical-dag.py \
        dags/resolve-anaphora-dag.py dags/prepare-graph-construction-job-dag.py \
        dags/pdf-downloading-dag.py dags/start-graph-formation-job-dag.py \
        dags/get-proxies-dag-2.py dags/brightdata-proxy-update.py \
        dags/get-arxiv-pdf-urls-dag.py dags/get-springer-pdf-urls-dag.py dags/get-Lenin-pdf-urls-dag.py
git commit -m "refactor: all DAGs use domain repositories instead of databaseConnector"
```

---

## Task 10: Update root standalone scripts

**Files:**
- Modify: `get-proxies.py`, `get-proxies-2.py`, `hardcoded-proxy.py`
- Modify: `pdf-downloader.py`
- Modify: `graph-construction.py`
- Modify: `text-preparation-for-graph-construction.py`
- Modify: `prepare-graph-calculation-worker.py`
- Modify: `arxivWorker.py`, `springerWorker.py`, `cyberlenin-pdflinks-downloading.py`

Every root script gets this two-line header inserted at the very top, before any other imports:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))
```

Then update DB and connector imports using the substitution table from Task 9.

- [ ] **Step 10.1: Update `get-proxies.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
```
With:
```python
from repositories.proxy_repository import ProxyRepository
```
Replace `databaseConnector.addOrUpdateProxy(...)` → `ProxyRepository.add_or_update(...)`.

- [ ] **Step 10.2: Update `get-proxies-2.py`**

Add header. Replace:
```python
import json,urllib.request
import time
import pyodbc
import dbConnector
from dbConnector import databaseConnector
import requests
import bs4
import pendulum
```
With:
```python
import json, urllib.request
import requests
import bs4
import pendulum
from repositories.proxy_repository import ProxyRepository
```
Replace `databaseConnector.addOrUpdateProxy(...)` → `ProxyRepository.add_or_update(...)`.

- [ ] **Step 10.3: Update `hardcoded-proxy.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
```
With:
```python
from repositories.proxy_repository import ProxyRepository
```
Replace `databaseConnector.addOrUpdateProxy(...)` → `ProxyRepository.add_or_update(...)`.

- [ ] **Step 10.4: Update `pdf-downloader.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
```
With:
```python
from repositories.pdf_repository import PdfRepository
from repositories.proxy_repository import ProxyRepository
from ftpConnector import ftpConnector
```
Apply substitution table for all `databaseConnector` calls.

- [ ] **Step 10.5: Update `graph-construction.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
```
With:
```python
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
```
Replace `databaseConnector.getJobForExecution()` → `GraphJobRepository.get_job_for_execution()`.

- [ ] **Step 10.6: Update `text-preparation-for-graph-construction.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
import anaphoraResolverLapinLiass
from anaphoraResolverLapinLiass import BatchAnaphoraResolver, resolve_and_substitute
```
With:
```python
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
from anaphoraResolverLapinLiass import BatchAnaphoraResolver, resolve_and_substitute
```
Replace:
- `databaseConnector.getJobForPreparation()` → `GraphJobRepository.get_job_for_preparation()`
- `databaseConnector.getTextSourceForProcessing(jobId)` → `GraphJobRepository.get_text_source(jobId)`

- [ ] **Step 10.7: Update `prepare-graph-calculation-worker.py`**

Add header. Replace:
```python
import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
```
With:
```python
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
```
Apply substitution table:
- `databaseConnector.getJobForPreparation()` → `GraphJobRepository.get_job_for_preparation()`
- `databaseConnector.addFileSourceForGraphConstructionJob(file, jobId)` → `GraphJobRepository.add_file_source(file, jobId)`
- `databaseConnector.setErrorForPreparationJob(job[0], e)` → `GraphJobRepository.set_job_error(job[0], e)`

- [ ] **Step 10.8: Update `arxivWorker.py`, `springerWorker.py`, `cyberlenin-pdflinks-downloading.py`**

Add header to each. Replace `import dbConnector / from dbConnector import databaseConnector` with:
```python
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.service_state_repository import ServiceStateRepository
```
Apply substitution table for all DB calls in each file.

- [ ] **Step 10.9: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed.

- [ ] **Step 10.10: Commit**

```
git add get-proxies.py get-proxies-2.py hardcoded-proxy.py pdf-downloader.py \
        graph-construction.py text-preparation-for-graph-construction.py \
        prepare-graph-calculation-worker.py arxivWorker.py springerWorker.py \
        cyberlenin-pdflinks-downloading.py
git commit -m "refactor: root standalone scripts use dags/ repositories via sys.path"
```

---

## Task 11: Delete root-level duplicates and `dags/dbConnector.py`

**Files:**
- Delete: `configs.py` (root)
- Delete: `ftpConnector.py` (root)
- Delete: `dbConnector.py` (root)
- Delete: `anaphoraResolverLapinLiass.py` (root)
- Delete: `dags/dbConnector.py`

- [ ] **Step 11.1: Verify no remaining imports of root-level modules**

Run these greps and confirm zero results (excluding the files about to be deleted):

```
grep -r "from dbConnector import\|import dbConnector" dags/ --include="*.py"
grep -r "from dbConnector import\|import dbConnector" *.py
```

Both should return no matches except inside `dags/dbConnector.py` itself (the file to be deleted).

- [ ] **Step 11.2: Delete root-level duplicates**

```
git rm configs.py ftpConnector.py dbConnector.py anaphoraResolverLapinLiass.py
```

- [ ] **Step 11.3: Delete `dags/dbConnector.py`**

```
git rm dags/dbConnector.py
```

- [ ] **Step 11.4: Run full test suite**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 65 passed, 0 failures.

- [ ] **Step 11.5: Verify `dags/` is fully self-contained**

```
python -c "
import sys, os
sys.path.insert(0, 'dags')
from configs import getConfig
from ftpConnector import ftpConnector
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.latex_repository import LatexRepository
from repositories.graph_job_repository import GraphJobRepository
from repositories.service_state_repository import ServiceStateRepository
print('All imports OK — dags/ is self-contained')
"
```

Expected output: `All imports OK — dags/ is self-contained`

- [ ] **Step 11.6: Commit**

```
git commit -m "chore: delete root-level duplicate modules and dags/dbConnector.py"
```

---

## Done

After all tasks complete:
- `dags/` is fully self-contained (copyable to any Airflow installation)
- Each DB domain has its own repository class with a single responsibility
- Root standalone scripts use `dags/` modules via `sys.path`
- 65 tests pass, all covering the new infrastructure
- No root-level duplicate modules remain
