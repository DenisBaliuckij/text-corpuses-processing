# PDF-to-Text Conversion — Bug Fix Design

**Date:** 2026-06-02
**Scope:** Fix bugs in `pdf-conversion-dag.py` and `pdf-to-latex-converter.py`; extract shared logic into `dags/pdfConverter.py`.

---

## Problem

The existing PDF-to-text conversion code has four bugs:

1. **No queue-empty guard** — `getPdfToConvertToLatex()` returns `None` when there are no pending files. `url.replace('.pdf', '.tex')` then raises `AttributeError`, crashing the loop.
2. **Broken error handler** — the `except` block calls `saveLatexFileLocation(urlToConvert, 'NA')` even when `urlToConvert` is `None`, causing a second crash.
3. **Hard 500-iteration cap** — the loop exits after 500 files regardless of queue size; Airflow restarts the task from scratch unnecessarily.
4. **Logic duplicated** — identical code lives in both `pdf-to-latex-converter.py` and `dags/pdf-conversion-dag.py`, making any future fix require two edits.

---

## Architecture

A new shared module `dags/pdfConverter.py` owns all conversion logic. Both consumers become thin wrappers.

```
pdf-to-latex-converter.py  ─┐
                              ├─ import ─► dags/pdfConverter.py
dags/pdf-conversion-dag.py ─┘              │
                                            ├─ databaseConnector
                                            └─ ftpConnector
```

---

## pdfConverter.py

```python
from pypdf import PdfReader
import io
from ftpConnector import ftpConnector
import dbConnector
from dbConnector import databaseConnector

def run_conversion() -> int:
    """Processes all pending PDFs. Returns count converted."""
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

**Bug fixes applied:**
- `url is None` check breaks cleanly when queue is empty
- `file.seek(0)` resets BytesIO position after FTP write before PdfReader reads it
- `page.extract_text() or ""` guards against pages returning `None`
- No iteration cap — processes the full queue; Airflow `@continuous` handles re-scheduling
- Error handler only reached when `url` is guaranteed non-`None`

---

## DAG update — pdf-conversion-dag.py

```python
@task()
def convertPdfFiles():
    import pdfConverter
    converted = pdfConverter.run_conversion()
    print(f"Converted {converted} files")
```

---

## Standalone script update — pdf-to-latex-converter.py

```python
import sys
sys.path.insert(0, 'dags')
import pdfConverter
pdfConverter.run_conversion()
```

---

## Files changed

| File | Change |
|------|--------|
| `dags/pdfConverter.py` | **New** — shared conversion module |
| `dags/pdf-conversion-dag.py` | Simplified to thin DAG wrapper |
| `pdf-to-latex-converter.py` | Simplified to thin standalone wrapper |

---

## Out of scope

- Actual LaTeX format output (equations, tables, formatting) — current plain-text extraction is intentional
- Airflow DAG scheduling changes
- Database schema changes
