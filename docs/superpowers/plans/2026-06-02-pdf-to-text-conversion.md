# PDF-to-Text Conversion Bug Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix four bugs in the PDF text-extraction pipeline and eliminate code duplication by extracting shared logic into `dags/pdfConverter.py`.

**Architecture:** A new shared module `dags/pdfConverter.py` owns all conversion logic. `dags/pdf-conversion-dag.py` and `pdf-to-latex-converter.py` become thin wrappers that import and call `run_conversion()`. All bug fixes live in the shared module.

**Tech Stack:** Python 3.10+, pypdf, pytest, unittest.mock, Apache Airflow 2

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `dags/pdfConverter.py` | **Create** | All conversion logic — `run_conversion()` |
| `dags/tests/test_pdf_converter.py` | **Create** | Unit tests for `run_conversion()` |
| `dags/pdf-conversion-dag.py` | **Modify** | Thin Airflow DAG wrapper |
| `pdf-to-latex-converter.py` | **Modify** | Thin standalone dev-script wrapper |

---

## Task 1: Create `pdfConverter.py` with TDD

**Files:**
- Create: `dags/pdfConverter.py`
- Create: `dags/tests/test_pdf_converter.py`

- [ ] **Step 1.1: Write the failing tests**

Create `dags/tests/test_pdf_converter.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
import pdfConverter


def test_empty_queue_returns_zero():
    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', return_value=None):
        result = pdfConverter.run_conversion()
    assert result == 0


def test_converts_pdf_and_returns_count():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "hello world"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile') as mock_store, \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 1
    mock_file.seek.assert_called_once_with(0)
    assert mock_store.call_args[0][0] == 'arxiv/paper.tex'
    mock_save.assert_called_once_with('arxiv/paper.pdf', 'arxiv/paper.tex')


def test_failed_conversion_saves_na_and_loop_continues():
    mock_file = MagicMock()
    mock_file.seek.side_effect = Exception("FTP read error")

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['bad/file.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 0
    mock_save.assert_called_once_with('bad/file.pdf', 'NA')


def test_none_page_text_handled():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = None
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile'), \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation'):
        result = pdfConverter.run_conversion()

    assert result == 1
```

- [ ] **Step 1.2: Run tests to confirm they fail**

```
cd C:\Repositories\text-corpuses-processing
pytest dags/tests/test_pdf_converter.py -v
```

Expected: 4 errors — `ModuleNotFoundError: No module named 'pdfConverter'`

- [ ] **Step 1.3: Create `dags/pdfConverter.py`**

```python
from pypdf import PdfReader
import io
from ftpConnector import ftpConnector
from dbConnector import databaseConnector


def run_conversion() -> int:
    count = 0
    while True:
        url = databaseConnector.getPdfToConvertToLatex()
        if url is None:
            break
        try:
            file = ftpConnector.getFile(url)
            file.seek(0)
            reader = PdfReader(file)
            text = "".join(page.extract_text() or "" for page in reader.pages)
            tex_filename = url.replace('.pdf', '.tex')
            ftpConnector.storeFile(tex_filename, io.BytesIO(text.encode('utf-8')), 'Tex')
            databaseConnector.saveLatexFileLocation(url, tex_filename)
            count += 1
        except Exception as e:
            print(f"Failed to convert {url}: {e}")
            databaseConnector.saveLatexFileLocation(url, 'NA')
    return count
```

- [ ] **Step 1.4: Run tests to confirm they pass**

```
pytest dags/tests/test_pdf_converter.py -v
```

Expected output:
```
PASSED test_empty_queue_returns_zero
PASSED test_converts_pdf_and_returns_count
PASSED test_failed_conversion_saves_na_and_loop_continues
PASSED test_none_page_text_handled
4 passed
```

- [ ] **Step 1.5: Confirm existing tests still pass**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: all existing tests pass, plus the 4 new ones.

- [ ] **Step 1.6: Commit**

```
git add dags/pdfConverter.py dags/tests/test_pdf_converter.py
git commit -m "feat: extract PDF conversion logic into shared pdfConverter module"
```

---

## Task 2: Update the Airflow DAG

**Files:**
- Modify: `dags/pdf-conversion-dag.py`

- [ ] **Step 2.1: Replace the DAG task body**

Open `dags/pdf-conversion-dag.py`. Replace the entire `convertPdfFiles` function body (lines 17–45) so the file reads:

```python
import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_conversion",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["latexFiles"],
) as dag:

    @task()
    def convertPdfFiles():
        import pdfConverter
        converted = pdfConverter.run_conversion()
        print(f"Converted {converted} files")

    convertPdfFiles()
```

- [ ] **Step 2.2: Verify the import resolves**

```
python -c "import sys; sys.path.insert(0, 'dags'); import pdfConverter; print('OK')"
```

Expected: `OK`

- [ ] **Step 2.3: Commit**

```
git add dags/pdf-conversion-dag.py
git commit -m "refactor: simplify pdf-conversion DAG to delegate to pdfConverter module"
```

---

## Task 3: Update the standalone script

**Files:**
- Modify: `pdf-to-latex-converter.py`

- [ ] **Step 3.1: Replace the script body**

Replace the entire contents of `pdf-to-latex-converter.py` with:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

import pdfConverter

if __name__ == '__main__':
    converted = pdfConverter.run_conversion()
    print(f"Converted {converted} files")
```

- [ ] **Step 3.2: Verify the import resolves**

```
python -c "import sys; sys.path.insert(0, 'dags'); import pdfConverter; print('OK')"
```

Expected: `OK`

- [ ] **Step 3.3: Commit**

```
git add pdf-to-latex-converter.py
git commit -m "refactor: simplify standalone converter script to delegate to pdfConverter module"
```

---

## Done

All four bugs are fixed:
1. `None` guard — loop exits cleanly when queue is empty
2. Error handler safe — `url` is non-`None` when exception block is reached
3. No iteration cap — processes full queue per run
4. No duplication — single source of truth in `pdfConverter.py`
