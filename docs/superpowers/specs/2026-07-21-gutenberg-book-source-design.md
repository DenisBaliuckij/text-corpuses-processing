# Project Gutenberg Book Source Design

**Date:** 2026-07-21
**Project:** text-corpuses-processing
**Status:** Approved

## Goal

Add Project Gutenberg as a new URL-discovery source, split into per-genre subfolders, following the existing `paperDownloader.run_search` pattern used by arXiv/PubMed/Gujarati/Russian/English sources. Unlike every existing source, Gutenberg essentially never offers a native PDF — this is the first source where `pdf_downloading` must actually exercise its "download whatever format is available" fallback instead of assuming `.pdf`.

Context: this replaces an earlier request to integrate Twirpx/LibGen/Sci-Hub, which was declined — those redistribute copyrighted material without authorization (Sci-Hub via stolen institutional credentials). Project Gutenberg is exclusively public-domain, with a free official REST API (Gutendex), so it doesn't carry that risk.

---

## Research: Gutendex API

`https://gutendex.com/books/` — free, unauthenticated, no rate-limit documented (existing sources' proxy-pool usage covers this regardless per below).

Verified live via direct query (`?search=science`):

```json
{
  "count": 754,
  "next": "https://gutendex.com/books/?page=2&search=science",
  "results": [{
    "id": 41481,
    "title": "Astounding Stories of Super-Science January 1930",
    "subjects": ["Science fiction -- Periodicals", "..."],
    "bookshelves": ["Astounding Stories", "Category: Science-Fiction & Fantasy", "..."],
    "languages": ["en"],
    "formats": {
      "text/plain; charset=utf-8": "https://www.gutenberg.org/ebooks/41481.txt.utf-8",
      "text/html": "https://www.gutenberg.org/ebooks/41481.html.images",
      "application/epub+zip": "https://www.gutenberg.org/ebooks/41481.epub3.images",
      "application/x-mobipocket-ebook": "https://www.gutenberg.org/ebooks/41481.kf8.images",
      "application/rdf+xml": "https://www.gutenberg.org/ebooks/41481.rdf",
      "image/jpeg": "https://www.gutenberg.org/cache/epub/41481/pg41481.cover.medium.jpg",
      "application/octet-stream": "https://www.gutenberg.org/cache/epub/41481/pg41481-h.zip"
    }
  }]
}
```

Confirms: no PDF in `formats` for this (typical) item. Pagination is `page=`, 32 results/page, `next` is null on the last page. `count` is the total match count (usable the same way `numFound` is used for archive.org's `has_more`).

### Category counts (via `?search=<term>`)

| Category | Query term(s) | Approx. count |
|----------|---------------|---------------|
| `science` | `science` | 754 |
| `social_science` | `economics`, `sociology`, `political science` (3 rotating criteria — the single phrase `social science` only matches 11) | ~low hundreds combined |
| `law` | `law` | 532 |
| `history` | `history` | 2644 |
| `philosophy_religion` | `philosophy`, `religion` (2 rotating criteria) | 218 + 237 |
| `poetry_drama` | `poetry`, `drama` (2 rotating criteria) | 193 + 279 |
| `children` | `children` | 394 |
| `literature` | `fiction` | large (Gutenberg's core bulk; exact count not retrieved, request timed out, but this is the dominant category on Gutenberg) |

No `literature_modern` split: US public-domain cutoff is a rolling ~95 years, so as of 2026 Gutenberg tops out around works from ~1930-31. A `[1950 TO 2026]` filter (as used for `english_literature_modern`/`russian_literature_modern`) would structurally match ~nothing here, the same near-zero-yield outcome already observed for those two existing categories — not worth repeating. `literature` alone covers Gutenberg's fiction bulk.

---

## Architecture

```
dags/gutenbergDownloader.py                      ← new, Gutendex adapter (mirrors archiveOrgDownloader.py)
dags/download-gutenberg-science-dag.py           ← new
dags/download-gutenberg-social-science-dag.py    ← new
dags/download-gutenberg-law-dag.py               ← new
dags/download-gutenberg-history-dag.py           ← new
dags/download-gutenberg-philosophy-religion-dag.py ← new
dags/download-gutenberg-poetry-drama-dag.py      ← new
dags/download-gutenberg-children-dag.py          ← new
dags/download-gutenberg-literature-dag.py        ← new
dags/configs/search_configs.json                 ← modified, 8 new source keys
dags/pdf-downloading-dag.py                      ← modified, format-aware extension + gutenberg routing
Database/database-v0.28.sql                      ← new, GetPdfToDownload rotation patterns
```

### `gutenbergDownloader.py`

```python
_FORMAT_PRIORITY = [
    ('application/pdf', 'pdf'),
    ('application/epub+zip', 'epub'),
    ('text/html', 'html'),
    ('text/plain; charset=utf-8', 'txt'),
]

def search_books(query, page, proxies=None, tag=''):
    """Searches Gutendex for `query`. Returns (urls, has_more).
    Each URL has '#{tag}.{ext}' appended, ext being whichever format
    was actually selected (pdf if present — essentially never — else
    epub, else html, else txt; items with none of these are skipped)."""
```

Mirrors `archiveOrgDownloader.search_pdfs`'s shape (`query, page, proxies, tag` → `(urls, has_more)`), but page size is fixed by Gutendex at 32 (no `rows` param), and `has_more` is `bool(data['next'])` rather than a page/rows/total computation.

### DAGs

Same shape as `download-gujarati-law-dag.py`: one `@task`, inline `fetch_page` adapter calling `search_books`, `run_search(service_id=N, source='gutenberg_{category}', adapter_fn=fetch_page, use_proxy=True)`.

**`use_proxy=True`** — unlike the archive.org-based sources (`use_proxy=False`), per explicit instruction to route all new outbound requests through the existing proxy pool.

`service_id` assignments (next free after 27):

| service_id | category | search_configs.json criteria |
|---|---|---|
| 28 | science | 1: `science` |
| 29 | social_science | 3: `economics`, `sociology`, `political science` |
| 30 | law | 1: `law` |
| 31 | history | 1: `history` |
| 32 | philosophy_religion | 2: `philosophy`, `religion` |
| 33 | poetry_drama | 2: `poetry`, `drama` |
| 34 | children | 1: `children` |
| 35 | literature | 1: `fiction` |

All criteria `repeat: true`, `max_results: 5000` (matching the English-source convention, since Gutendex's own `count` is the real ceiling and will legitimately cap several of these categories well under 5000 — that's fine, it just means those categories exhaust and hit the existing `resume_at` backoff like any other thin category).

---

## `pdf_downloading` changes

### Extension handling

Currently `downloadOne()` unconditionally does `filename += '.pdf'`. Add a local `ext = 'pdf'` default, and for gutenberg URLs parse the real extension out of the tag instead of assuming PDF:

```python
ext = 'pdf'
...
if '#gutenberg_' in url:
    tag_part = url.rsplit('#', 1)[1]          # 'gutenberg_science.epub'
    category, ext = tag_part.rsplit('.', 1)   # 'gutenberg_science', 'epub'
    filename = f'gutenberg/{category[len("gutenberg_"):]}/'
    url = url.rsplit('#', 1)[0]
...
filename += str(uuid.uuid4())
filename += '.' + ext
```

This only changes behavior for gutenberg-tagged URLs; every existing source keeps hardcoded `.pdf` exactly as today (`ext` stays `'pdf'` for them, same resulting filename).

### FTP layout

```
gutenberg/science/
gutenberg/social_science/
gutenberg/law/
gutenberg/history/
gutenberg/philosophy_religion/
gutenberg/poetry_drama/
gutenberg/children/
gutenberg/literature/
```

---

## Database changes (`database-v0.28.sql`)

`ALTER PROCEDURE [dbo].[GetPdfToDownload]` — add 8 new pattern rows to the existing `@sources` table variable (the round-robin rotation list), one per category:

```sql
('%gutenberg_science%'),
('%gutenberg_social_science%'),
('%gutenberg_law%'),
('%gutenberg_history%'),
('%gutenberg_philosophy_religion%'),
('%gutenberg_poetry_drama%'),
('%gutenberg_children%'),
('%gutenberg_literature%'),
```

Appended after the existing `english_*` rows, before the `arxiv`/`lenin`/`ncbi` 3x-weighted rows — matching the existing ordering convention (each new source family's block added right before the high-volume-source block). No other schema change — `ServiceState` rows are created lazily by `load_state()`, same as every prior source addition.

---

## Error handling

Same as every existing source, no new cases:

| Situation | Behaviour |
|---|---|
| Gutendex request fails | Exception propagates out of `run_search`; Airflow marks task failed; state not advanced; retried next run |
| Item has none of pdf/epub/html/txt in `formats` | Skipped (rare — image-only or audio-book-only entries) |
| Category criterion exhausted (`repeat=true`, no sibling) | Existing `resume_at` 1h backoff (`_EXHAUSTION_BACKOFF_SECONDS`) applies unchanged |
| Proxy error during discovery | `mark_proxy_broken`, state not advanced (now applies to gutenberg too, via `use_proxy=True`) |

---

## Files to Create / Modify

| Action | File |
|---|---|
| Create | `dags/gutenbergDownloader.py` |
| Create | `dags/download-gutenberg-science-dag.py` |
| Create | `dags/download-gutenberg-social-science-dag.py` |
| Create | `dags/download-gutenberg-law-dag.py` |
| Create | `dags/download-gutenberg-history-dag.py` |
| Create | `dags/download-gutenberg-philosophy-religion-dag.py` |
| Create | `dags/download-gutenberg-poetry-drama-dag.py` |
| Create | `dags/download-gutenberg-children-dag.py` |
| Create | `dags/download-gutenberg-literature-dag.py` |
| Modify | `dags/configs/search_configs.json` — 8 new source keys |
| Modify | `dags/pdf-downloading-dag.py` — format-aware extension + 8 new routing branches |
| Create | `Database/database-v0.28.sql` |

## Explicitly out of scope (documented, not built)

- **HathiTrust** — public-domain access requires OAuth-gated Data API or per-item scan-to-PDF assembly; no simple bulk REST catalog like Gutendex.
- **Open Library** — much of its full-text resolves through Archive.org (overlaps existing sources) or is borrow-only, not a direct download.
- **Standard Ebooks** — clean catalog but only ~1-2k titles, low incremental value for a first pass.
- **Twirpx / LibGen / Sci-Hub** — declined outright; these redistribute copyrighted material without authorization.
