# Supporting Infrastructure — Design Spec

**Date:** 2026-06-02
**Sub-project:** 3 of 3 (Supporting infrastructure)
**Scope:** Root `requirements.txt`, git pre-push hook with install scripts, README.md architecture refresh in English and Russian.

---

## Components

### 1. `requirements.txt` (root)

A single all-inclusive file with four labeled sections. Shared packages appear once under the section that first needs them. The existing per-folder `dags/llm_v2/requirements.txt` and `dags/hierarchical_llm_version/requirements.txt` are kept unchanged — still valid for targeted installs.

```
# ── Core pipeline ────────────────────────────────────────────────────
apache-airflow
pyodbc
requests
beautifulsoup4
pypdf
spacy
nltk
networkx
pyvis
pendulum

# ── Testing ──────────────────────────────────────────────────────────
pytest

# ── LLM v2 (local HuggingFace — build_graph_llm_v2 DAG) ─────────────
torch>=2.0.0
transformers>=4.35.0
sentence-transformers>=2.2.0
pydantic>=2.0.0
pyyaml>=6.0
pymorphy3>=1.0.0
razdel>=0.5.0
numpy>=1.21.0
scikit-learn>=1.0.0
hdbscan>=0.8.33
tqdm>=4.62.0

# ── Hierarchical LLM (Yandex Cloud — build_graph_hierarchical DAG) ───
openai>=1.40.0
# (sentence-transformers, pydantic, pyyaml, numpy, scikit-learn,
#  hdbscan, razdel, nltk, tqdm listed above)
```

SpaCy models (`en_core_web_sm`, `en_core_web_lg`, `en_coreference_web_trf`) are not pip-installable and remain documented via `python -m spacy download` commands in README only.

---

### 2. Git pre-push hook + install scripts

**`hooks/pre-push`** — committed to the repo; copied to `.git/hooks/` by the install script:

```bash
#!/bin/sh
echo "Running tests before push..."
pytest dags/tests/ -k "not spacy_neural" -q
if [ $? -ne 0 ]; then
  echo "Tests failed. Push blocked."
  exit 1
fi
```

**`scripts/install-hooks.sh`** — for macOS/Linux/Git Bash:

```bash
#!/bin/sh
cp hooks/pre-push .git/hooks/pre-push
chmod +x .git/hooks/pre-push
echo "Git hooks installed."
```

**`scripts/install-hooks.bat`** — for Windows CMD:

```bat
@echo off
copy hooks\pre-push .git\hooks\pre-push
echo Git hooks installed. Run manually: pytest dags/tests/ -k "not spacy_neural" -q
```

Developers run the install script once after cloning. The hook blocks pushes if any test outside the `spacy_neural` group fails. The `.bat` cannot `chmod`, so it relies on git on Windows already treating the file as executable.

---

### 3. README.md refresh (English + Russian)

Both language sections receive identical structural updates (translated). Changes:

#### 3a. Setup section

Replace scattered `pip install` command blocks with a single command:

```bash
pip install -r requirements.txt
```

Followed by the spaCy model downloads and the hook install line:

```bash
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
sh scripts/install-hooks.sh   # Windows: scripts\install-hooks.bat
```

Optional installs (SpaCy neural) remain documented below the main block.

#### 3b. Architecture — new "Module structure" subsection

Add after the existing pipeline diagram:

| Module | Purpose |
|--------|---------|
| `configs.py` | Self-contained config loader (reads `dags/configs/configs.json` relative to `__file__`) |
| `repositories/` | 5 domain DB repositories: `ProxyRepository`, `PdfRepository`, `LatexRepository`, `GraphJobRepository`, `ServiceStateRepository` |
| `ftpConnector.py` | FTP upload / download / file listing |
| `paperDownloader.py` | Crawl state machine, proxy and URL helpers |
| `pdfConverter.py` | PDF → plain text extraction |
| `graphBuilder.py` | Rule-based SVO triplet extraction |
| `graphMetrics.py` | networkx graph statistics |
| `graphVisualizer.py` | pyvis interactive HTML visualization |
| `anaphoraResolver*.py` | Anaphora resolution dispatcher + LapinLiass + SpacyNeural backends |

#### 3c. Deploy DAGs note

Add one sentence to the existing deploy section:

> `dags/` is fully self-contained — copy it to any Airflow installation without any files from the repo root.

#### 3d. Testing section

Add:

```bash
pytest dags/tests/ -k "not spacy_neural" -v
```

With the current test count (105 tests, 1 deselected).

---

## Files changed

| File | Action |
|------|--------|
| `requirements.txt` | **Create** |
| `hooks/pre-push` | **Create** |
| `scripts/install-hooks.sh` | **Create** |
| `scripts/install-hooks.bat` | **Create** |
| `README.md` | **Modify** — setup, architecture, deploy, testing sections in both languages |

---

## Out of scope

- Pinning exact versions for core packages (apache-airflow, pyodbc, etc.) — too environment-specific
- CI/CD pipeline changes
- Any code changes
