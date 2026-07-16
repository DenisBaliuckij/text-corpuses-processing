# System Landscape — Text Corpuses Processing Pipeline

Full picture of the production deployment as of 2026-07-16: host/disk layout,
Docker topology, the 4-stage Airflow DAG pipeline, external data sources, the
proxy pool, and storage layout. Diagrams render natively on GitHub (Mermaid).

**Critical reminder:** the git repository (`~/Repositories/text-corpuses-processing/`
on the host) is *not* the deployment. The real, live stack runs from
`~/apache-airflow/` (hand-maintained, not a git repo) — DAG file changes must be
`docker cp`'d into the running containers to take effect. See `CLAUDE.md` /
project memory for the full deployment-topology gotcha.

---

## 1. Host & disk layout (172.21.128.103, `corpus-host`, 24 cores / 31GiB RAM)

```mermaid
flowchart TB
    subgraph HOST["Host: 172.21.128.103 — Ubuntu 24.04"]
        direction TB

        subgraph SDA["sda — 3.6TB (ext4, mounted /)"]
            direction TB
            OS["OS + systemd + docker.service"]
            FTP_STORE["FTP storage (FileZilla, port 21)<br/>arxiv/, springer/, cyberleninka/,<br/>gujarati/*, russian/*, english/*, Tex/, graphJobs/<br/>~200GB, ~16,700 files"]
            AF_BINDS["Airflow bind mounts<br/>~/apache-airflow/{dags,logs,config,plugins}<br/>(NOT affected by Docker data-root — plain host paths)"]
            SWAP["4x 8GB swapfiles = 32GB total<br/>/swap.img + /swap2.img + /swap3.img + /swap4.img<br/>(extended 2026-07-16, was 8GB)"]
        end

        subgraph NVME["nvme0n1 — 953.9GB (ext4, /mnt/nvme-mssql)<br/>was unpartitioned/unused until 2026-07-16"]
            direction TB
            MSSQL_DATA["mssql-data/, mssql-backups/<br/>SQL Server data+log files, uid 10001<br/>(migrated 2026-07-16 from sda)"]
            DOCKER_ROOT["docker-data/<br/>Docker storage root: image layers,<br/>container writable layers, named volumes<br/>(migrated 2026-07-16 from /var/lib/docker on sda)<br/>set via /etc/docker/daemon.json data-root"]
        end

        SDA -.->|"sda was 80-99% util, 170-450ms write latency<br/>before both migrations"| RESULT
        NVME -.->|"after both migrations:<br/>sda 1.6-4.8% util, 0-2.4ms latency<br/>nvme0n1 1.9-2.4% util, ~0.4ms latency"| RESULT["I/O bottleneck resolved"]
    end
```

---

## 2. Docker Compose topology (`~/apache-airflow/docker-compose.yaml`, CeleryExecutor)

```mermaid
flowchart TB
    subgraph EXT["External access"]
        USER["Denis / browser"]
        PORT5335["host port 5335"]
    end

    subgraph COMPOSE["docker compose project: apache-airflow"]
        NGINX["nginx-5335<br/>network_mode: host<br/>reverse proxy"]
        WEBUI["webui (custom-query UI)<br/>port 8090<br/>HTTP Basic Auth"]
        OPENWEBUI["open-webui (Ollama chat UI)<br/>internal PORT=5336<br/>UNRELATED to this pipeline<br/>~6.9GB image, biggest single Docker consumer"]

        APISERVER["airflow-apiserver<br/>port 8080"]
        SCHEDULER["airflow-scheduler"]
        DAGPROC["airflow-dag-processor"]
        WORKER["airflow-worker<br/>CeleryExecutor, WORKER_CONCURRENCY=64<br/>= up to 64 heavy OS processes at once<br/>(root cause of swap growth, unaddressed)"]
        TRIGGERER["airflow-triggerer"]

        MSSQL[("mssql<br/>SQL Server 2022 Express<br/>TextCorpuses DB<br/>restart: always")]
        POSTGRES[("postgres<br/>Airflow metadata DB")]
        REDIS[("redis<br/>Celery broker")]
    end

    USER --> PORT5335 --> NGINX
    NGINX -->|"/customquery/*"| WEBUI
    NGINX -->|"/report/*"| REPORT["static ops report<br/>generate_ops_report.py, every 15min cron"]
    NGINX -->|"everything else"| OPENWEBUI

    SCHEDULER --> REDIS
    WORKER --> REDIS
    DAGPROC --> POSTGRES
    SCHEDULER --> POSTGRES
    APISERVER --> POSTGRES
    WORKER --> MSSQL
    WEBUI --> MSSQL

    WORKER -.->|"host.docker.internal<br/>extra_hosts"| FTPHOST["FTP server<br/>native Ubuntu service<br/>port 21"]
```

---

## 3. Airflow DAG pipeline — 4 stages, ~35 DAGs

```mermaid
flowchart TB
    subgraph S1["Stage 1 — Proxy management"]
        GP1["get_proxies_for_calls<br/>(geonode.com)"]
        GP2["get_proxies_for_calls_2/3<br/>(proxydb.net-style sources)"]
        GP4["get_proxies_for_calls_4<br/>(free-proxy-list.net)"]
        BD["update-brightdata-proxy<br/>(paid, shared champion)"]
        VP["validate_proxies<br/>*/1 * * * * cron (NOT @continuous —<br/>hit a real Airflow continuous-timetable<br/>wedge bug when switched once)<br/>grace period added 2026-07-16:<br/>skips proxies <5min old"]
        VP -->|"GetTopProxiesForValidation<br/>@minAgeSeconds=300"| PROXYDB[("IPProxy table<br/>~19-33 rows, high churn<br/>SuccessCount rarely increments —<br/>proxies often die before a real<br/>download completes")]
        GP1 & GP2 & GP4 & BD -->|"validate_and_import<br/>(tests against arxiv.org first)"| PROXYDB
    end

    subgraph S2["Stage 2 — URL discovery (paperDownloader.py shared state machine)"]
        direction TB
        API["API downloaders (use_proxy=False)<br/>arxiv (svc 4), pubmed (svc 5),<br/>semantic_scholar (svc 6, PAUSED —<br/>institutional email required)"]
        SCRAPE["Web scrapers<br/>springer, cyberleninka, arxiv"]
        ARCHIVE["archiveOrgDownloader-based<br/>(21 sources, svc 8/12-26):<br/>gujarati_{literature,news,law,official,dictionary,science_archive}<br/>russian_{science,literature_modern/classic,news,law,social_science}<br/>english_{science,literature_modern/classic,news,law,social_science}<br/>broadened queries 2026-07-16:<br/>gujarati_law/dictionary/news, russian_literature_classic"]
        SHODH["shodhgangaDownloader-based (svc 9/10):<br/>gujarati_science_{natural,social}<br/>Shodhganga currently DOWN (connect timeout) —<br/>errors now correctly surface as failed tasks<br/>(fixed 2026-07-16, was silently 'success')"]
    end

    subgraph S3["Stage 3 — PDF & text"]
        PDFDL["pdf_downloading<br/>CONCURRENCY=64 (ThreadPoolExecutor)<br/>round-robin across ~24 source patterns<br/>QUEUE_EMPTY_BACKOFF=15min"]
        PDFCONV["pdf_conversion"]
    end

    subgraph S4["Stage 4 — Graph construction (manual trigger)"]
        START["start_tree_formation_job"]
        PREP["prepare_graph_construction_job"]
        ANAPH["resolve_anaphora<br/>LapinLiass (default) or SpacyNeural"]
        BUILD1["build_graph<br/>(rule-based spaCy)"]
        BUILD2["build_graph_llm_v2<br/>(local HuggingFace)"]
        BUILD3["build_graph_hierarchical<br/>(Yandex Cloud)"]
        FIN["finalize_job<br/>→ metrics.json + visualization.html"]
        START --> PREP --> ANAPH
        ANAPH --> BUILD1 & BUILD2 & BUILD3
        BUILD1 & BUILD2 & BUILD3 --> FIN
    end

    ARCHIVESITE(["archive.org<br/>advancedsearch.php"])
    SHODHSITE(["shodhganga.inflibnet.ac.in<br/>DOWN"])
    ARXIVSITE(["arxiv.org / pubmed / semanticscholar.org"])

    ARXIVSITE --> API
    ARCHIVESITE --> ARCHIVE
    SHODHSITE -.->|"timeout"| SHODH

    API & SCRAPE & ARCHIVE & SHODH -->|"save_urls / AddPdfUrl"| PDFURLS[("PdfDocuments table<br/>168k+ tracked URLs")]
    PDFURLS --> PDFDL
    PROXYDB -->|"GetLatestFreeProxy<br/>(random of top 20)"| PDFDL
    PDFDL -->|"downloaded PDFs"| FTPOUT["FTP: arxiv/, gujarati/*, russian/*, english/*, ..."]
    FTPOUT --> PDFCONV --> TEX["FTP: Tex/"]
    TEX -.->|"manual trigger"| START
```

---

## 4. Database — key tables (SQL Server, `TextCorpuses`, now on NVMe)

```mermaid
erDiagram
    PdfDocuments ||--o{ IPProxy : "downloaded via"
    ServiceState ||--|| PdfDocuments : "tracks discovery progress for"
    GraphConstructionJob ||--o{ GraphConstructionFiles : contains
    CustomQuery ||--o{ CustomQueryPdf : contains

    PdfDocuments {
        int ID PK
        string PDFUrl
        string LocationInFileSystem "NULL until downloaded"
        datetime ClaimedAt
        datetime InsertedAt "added v0.24"
    }
    IPProxy {
        int ID PK
        string IP
        int Port
        int LastChecked "unix epoch"
        bit IsBroken "unused — MarkProxyAsBroken deletes rows instead"
        int SuccessCount "rarely > 0, see proxy-churn note"
    }
    ServiceState {
        int ServiceId PK "4-26, per source"
        string State "JSON: criterion_index, page, done_criteria, resume_at"
    }
    GraphConstructionJob {
        int ID PK
        int Status "0/10/20/30/99"
        string ProcessorConfig "JSON: textProcessorName, anaphoraResolverName"
    }
```

---

## 5. This session's changes (2026-07-15/16), at a glance

```mermaid
flowchart LR
    A["Silent-failure masking fix<br/>paperDownloader.py"] --> E["Outages now visible<br/>as failed Airflow tasks"]
    B["Broadened queries<br/>4 thin categories"] --> F["russian_literature_classic<br/>339 → 2,554 matches (7.5x)"]
    C["Proxy validation<br/>grace period (300s)"] --> G["Reduces churn, but pool<br/>still thin (~19-33 rows)"]
    D["mssql + Docker root<br/>→ NVMe migration"] --> H["sda: 80-99% → 1.6-4.8% util<br/>THE throughput fix"]
    I["Swap 8GB → 32GB"] --> J["Headroom only —<br/>WORKER_CONCURRENCY=64<br/>root cause still unaddressed"]
    K["Fixed local .ssh<br/>permissions (Windows)"] --> L["SSH to corpus-host<br/>works from PowerShell/cmd"]
```

**Known unresolved items:**
- `AIRFLOW__CELERY__WORKER_CONCURRENCY=64` spawns too many heavy Celery worker
  processes for ~30 `@continuous` DAGs — the real driver of swap growth over
  time. Enlarging swap treats the symptom; reducing this or adding
  `worker_max_tasks_per_child` would address the cause.
- Proxy pool remains thin and high-churn even after the grace-period fix —
  free proxies are inherently short-lived; `SuccessCount` still rarely
  accumulates before a proxy dies.
- Shodhganga (`shodhganga.inflibnet.ac.in`) is down for an unknown duration —
  external, not fixable from our side. `gujarati_science_natural/social` will
  resume once it's back, now visibly (not silently) failing in the meantime.
- FTP storage and swap files remain on `sda` — candidates for further NVMe
  migration if disk pressure returns.
