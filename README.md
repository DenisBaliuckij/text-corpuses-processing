# Text Corpuses Processing Pipeline

An Apache Airflow-based pipeline for crawling scientific text corpuses, downloading PDFs, converting them to text, and building semantic knowledge graphs from the extracted content. Three graph-building backends are available: a fast rule-based NLP engine, a local HuggingFace LLM pipeline, and a hierarchical Yandex Cloud LLM pipeline.

---

## Overview

The pipeline automates the full journey from raw web sources to a structured semantic graph:

1. **Proxy management** — maintains a pool of HTTP proxies for scraping
2. **URL crawling** — discovers PDF links from arXiv, Springer, and CyberLeninka
3. **PDF downloading** — downloads discovered PDFs through proxies
4. **PDF-to-text conversion** — extracts plain text from PDFs
5. **Graph construction** — builds a semantic graph from text using one of three backends

Each stage is an independent Airflow DAG that does exactly one unit of work per run. All job and file state is tracked in SQL Server. File content (PDFs, text, graphs) is stored on an FTP server.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Apache Airflow Scheduler              │
└──────────┬──────────────────────────────────────────┘
           │
    ┌──────▼───────┐     ┌──────────────┐     ┌──────────────────┐
    │  get_proxies │     │ get_*_urls   │     │  pdf_downloading │
    │  (3 DAGs)    │     │ (3 DAGs)     │     │                  │
    └──────────────┘     └──────┬───────┘     └────────┬─────────┘
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
              │          finalize_job  →  job Status=30                                        │
              └────────────────────────────────────────────────────────────────────────────────┘
```

> **Note:** Enable exactly one graph-building DAG at a time (`build_graph`, `build_graph_llm_v2`, or `build_graph_hierarchical`). All three compete for the same file queue (Status=10 files) and produce output in different FTP sub-paths under `graphJobs/{jobId}/`.

---

## DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `get_proxies_for_calls` | `@continuous` | Fetches proxies from geonode.com |
| `get_proxies_for_calls_2` | `@continuous` | Fetches proxies from proxydb.net |
| `update-brightdata-proxy` | every 5 min | Keeps BrightData proxy entry current |
| `get_arxiv_urls` | `@continuous` | Crawls arXiv for PDF URLs |
| `get_springer_urls` | `@continuous` | Crawls Springer for PDF URLs |
| `get_lenin_urls` | `@continuous` | Crawls CyberLeninka for PDF URLs |
| `pdf_downloading` | `@continuous` | Downloads PDFs from discovered URLs |
| `pdf_conversion` | `@continuous` | Converts PDFs to plain text |
| `start_tree_formation_job` | manual trigger | Creates a new graph construction job |
| `prepare_graph_construction_job` | `@continuous` | Registers text files for a job |
| `resolve_anaphora` | `@continuous` | Resolves anaphora in one text file |
| `build_graph` | `@continuous` | Rule-based NLP (spaCy), incremental graph |
| `build_graph_llm_v2` | `@continuous` | Local HuggingFace LLM pipeline |
| `build_graph_hierarchical` | `@continuous` | Hierarchical Yandex Cloud LLM pipeline |
| `finalize_job` | `@continuous` | Marks a job complete when all files are done |

---

## Graph Construction Job Lifecycle

```
start_tree_formation_job (manual)
        │
        ▼ job status=0
prepare_graph_construction_job
        │ lists .tex files from FTP, registers in GraphConstructionFiles
        ▼ job status=10, files status=0
resolve_anaphora  (per file: 0→5→10)
        │
        ▼ files status=10  ← one of the three graph-building DAGs picks up here
build_graph / build_graph_llm_v2 / build_graph_hierarchical  (per file: 10→15→20)
        │
        ▼ all files status=20
finalize_job  →  job status=30 (completed)
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

## FTP Layout

```
/
├── arxiv/              ← downloaded arXiv PDFs
├── springer/           ← downloaded Springer PDFs
├── cyberleninka/       ← downloaded CyberLeninka PDFs
├── Tex/                ← converted plain text files
└── graphJobs/
    └── {jobId}/
        ├── anaphora/
        │   └── {fileId}.txt        ← anaphora-resolved text (written by resolve_anaphora)
        ├── graph.json              ← rule-based incremental graph (build_graph)
        ├── llm_v2/
        │   └── {fileId}/
        │       ├── raw_graph.json
        │       └── clustered_graph.json
        └── hierarchical/
            └── {fileId}/
                ├── raw_graph.json
                ├── clustered_graph.json
                └── hierarchy_tree.json
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
- `Database/database-v0.1.sql`
- `Database/database-v0.2-pdf-to-latex.sql`
- `Database/database-v0.3.sql`
- `Database/database-v0.4.sql`

---

## Setup

### Prerequisites

- Python 3.10+
- Apache Airflow 2 with `airflow.sdk`
- SQL Server Express with `TextCorpuses` database
- FTP server accessible at the address in `dags/configs/configs.json`

### Install base dependencies

```bash
pip install apache-airflow pyodbc requests beautifulsoup4 pypdf spacy nltk
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
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

Copy the `dags/` folder to your Airflow DAGs directory (or configure `dags_folder` in `airflow.cfg` to point here).

### Start a graph construction job

In the Airflow UI, trigger `start_tree_formation_job` with:
- **paths**: semicolon-separated FTP paths containing `.tex` files, e.g. `arxiv/;springer/`
- **textProcessorName**: `RuleBased` (use `build_graph`), `LLMv2` (use `build_graph_llm_v2`), or `Hierarchical` (use `build_graph_hierarchical`)

Enable only the graph-building DAG that matches your chosen processor.

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

Конвейер на базе Apache Airflow для краулинга научных текстовых корпусов, загрузки PDF-файлов, их конвертации в текст и построения семантических графов знаний. Доступны три бэкенда построения графа: быстрый движок на основе правил NLP, конвейер с локальной LLM (HuggingFace) и иерархический конвейер с облачной LLM (Яндекс Облако).

---

## Описание

Конвейер автоматизирует полный цикл — от сырых веб-источников до структурированного семантического графа:

1. **Управление прокси** — поддерживает пул HTTP-прокси для скрапинга
2. **Краулинг URL** — обнаруживает ссылки на PDF с arXiv, Springer и КиберЛенинки
3. **Загрузка PDF** — скачивает найденные PDF через прокси
4. **Конвертация PDF в текст** — извлекает текст из PDF-файлов
5. **Построение графа** — строит семантический граф из текста одним из трёх бэкендов

Каждый этап — отдельный Airflow DAG, выполняющий ровно одну единицу работы за запуск. Состояние всех заданий и файлов хранится в SQL Server. Содержимое файлов (PDF, тексты, графы) хранится на FTP-сервере.

---

## Архитектура

Конвейер состоит из **15 DAG**, разбитых на группы:

- **Прокси:** `get_proxies_for_calls`, `get_proxies_for_calls_2`, `update-brightdata-proxy`
- **Краулинг URL:** `get_arxiv_urls`, `get_springer_urls`, `get_lenin_urls`
- **Загрузка и конвертация:** `pdf_downloading`, `pdf_conversion`
- **Построение графа:** `start_tree_formation_job` (ручной запуск), `prepare_graph_construction_job`, `resolve_anaphora`, **`build_graph`**, **`build_graph_llm_v2`**, **`build_graph_hierarchical`**, `finalize_job`

> **Важно:** Единовременно должен быть включён только один DAG построения графа: `build_graph`, `build_graph_llm_v2` или `build_graph_hierarchical`. Все три конкурируют за одну очередь файлов (Status=10) и сохраняют результаты в разные подпапки FTP.

---

## Жизненный цикл задания построения графа

```
start_tree_formation_job (ручной запуск)
        │
        ▼ статус задания = 0
prepare_graph_construction_job
        │ перечисляет .tex-файлы с FTP, регистрирует в GraphConstructionFiles
        ▼ статус задания = 10, статус файлов = 0
resolve_anaphora  (для каждого файла: 0→5→10)
        │
        ▼ статус файлов = 10  ← один из трёх бэкендов подхватывает здесь
build_graph / build_graph_llm_v2 / build_graph_hierarchical  (файл: 10→15→20)
        │
        ▼ все файлы в статусе 20
finalize_job  →  статус задания = 30 (завершено)
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

## Структура FTP

```
/
├── arxiv/              ← загруженные PDF с arXiv
├── springer/           ← загруженные PDF со Springer
├── cyberleninka/       ← загруженные PDF с КиберЛенинки
├── Tex/                ← конвертированные текстовые файлы
└── graphJobs/
    └── {jobId}/
        ├── anaphora/
        │   └── {fileId}.txt          ← текст после разрешения анафоры
        ├── graph.json                ← инкрементальный граф (build_graph)
        ├── llm_v2/
        │   └── {fileId}/
        │       ├── raw_graph.json
        │       └── clustered_graph.json
        └── hierarchical/
            └── {fileId}/
                ├── raw_graph.json
                ├── clustered_graph.json
                └── hierarchy_tree.json
```

---

## База данных

SQL Server база данных `TextCorpuses` на `LAPTOP-I91584GB\SQLEXPRESS`.

Применяйте миграции по порядку в SSMS:
- `Database/database-v0.1.sql`
- `Database/database-v0.2-pdf-to-latex.sql`
- `Database/database-v0.3.sql`
- `Database/database-v0.4.sql`

---

## Установка

### Базовые зависимости

```bash
pip install apache-airflow pyodbc requests beautifulsoup4 pypdf spacy nltk
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg
python -c "import nltk; nltk.download('wordnet')"
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

### Конфигурация

Отредактируйте `dags/configs/configs.json`, указав строку подключения к SQL Server, адрес и учётные данные FTP-сервера.

### Развёртывание DAG

Скопируйте папку `dags/` в директорию DAG вашего Airflow (или настройте `dags_folder` в `airflow.cfg`).

### Запуск построения графа

В интерфейсе Airflow запустите DAG `start_tree_formation_job` с параметрами:
- **paths**: пути на FTP через точку с запятой, например `arxiv/;springer/`
- **textProcessorName**: `RuleBased` (→ `build_graph`), `LLMv2` (→ `build_graph_llm_v2`) или `Hierarchical` (→ `build_graph_hierarchical`)

Включите в Airflow только тот DAG построения графа, который соответствует выбранному процессору.

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
