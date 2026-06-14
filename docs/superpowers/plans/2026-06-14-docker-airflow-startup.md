# Docker Airflow Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the one DAG that requires user input to start paused, and create a self-contained Docker Compose setup (plus bash and PowerShell launch scripts) that installs all dependencies and starts Apache Airflow 3.x with the repo's DAGs.

**Architecture:** A custom `Dockerfile.airflow` builds on the official `apache/airflow:3.0.0-python3.10` image, installing the Microsoft SQL Server ODBC driver (for `pyodbc` on Linux), all pip packages from the three requirements files, plus `requests[socks]`, `dash`, `dash-cytoscape`, and the spaCy/NLTK models. A `docker-compose.yml` at repo root orchestrates four services: `postgres` (Airflow metadata DB), `airflow-init` (runs DB migration and creates the admin user, then exits), `airflow-webserver`, and `airflow-scheduler`. Two thin launch scripts — one bash, one PowerShell — verify Docker prerequisites, build the image, and call `docker compose up -d`.

**Tech Stack:** Docker Compose v2, Apache Airflow 3.0.0, Python 3.10, PostgreSQL 15, Microsoft ODBC Driver 18 for SQL Server, PyTorch (CPU), HuggingFace Transformers, OpenAI SDK, spaCy, NLTK, PySocks, Dash/Cytoscape.

---

## Files

| Action | Path | Purpose |
|--------|------|---------|
| Modify | `dags/start-graph-formation-job-dag.py` | Set `is_paused_upon_creation=True` — only DAG requiring user input |
| Create | `Dockerfile.airflow` | Custom Airflow image with all system + Python deps |
| Create | `docker-compose.yml` | Four-service Airflow stack |
| Create | `scripts/start-airflow.sh` | Bash launch script (Linux) |
| Create | `scripts/start-airflow.ps1` | PowerShell launch script (Windows) |

---

## Task 1: Fix DAG pausing for `start_tree_formation_job`

**Files:**
- Modify: `dags/start-graph-formation-job-dag.py:18`

`start_tree_formation_job` is the only DAG with `Param` fields (`paths`, `textProcessorName`, `anaphoraResolverName`) that must be filled in by the user before triggering. All other 17 DAGs are fully autonomous and should remain `is_paused_upon_creation=False`.

- [ ] **Step 1: Change the flag**

In `dags/start-graph-formation-job-dag.py`, line 18, change:
```python
    is_paused_upon_creation=False,
```
to:
```python
    is_paused_upon_creation=True,
```

- [ ] **Step 2: Verify no other DAG was changed**

Run:
```bash
grep -n "is_paused_upon_creation" dags/*.py
```

Expected: all DAGs show `False` except `start-graph-formation-job-dag.py` which shows `True`.

- [ ] **Step 3: Commit**

```bash
git add dags/start-graph-formation-job-dag.py
git commit -m "fix: pause start_tree_formation_job on creation — requires user input"
```

---

## Task 2: Create `Dockerfile.airflow`

**Files:**
- Create: `Dockerfile.airflow`

**Notes:**
- Base image `apache/airflow:3.0.0-python3.10` — Debian 12 (Bookworm).
- Root installs: Microsoft ODBC Driver 18 from Microsoft's Bookworm package repo (required by `pyodbc` on Linux), `unixodbc-dev` (ODBC headers), `build-essential` (C compiler for `hdbscan` and other C-extension packages).
- Torch is installed first with the CPU-only index URL (`https://download.pytorch.org/whl/cpu`) to avoid pulling the multi-GB CUDA build. The subsequent install of `requirements-llm-v2.txt` (which lists `torch>=2.0.0`) sees torch already satisfied and skips it.
- `PYTHONPATH=/opt/airflow/dags` makes `from repositories.xxx import xxx` resolve at scheduler/webserver startup (Airflow adds the dags folder to `sys.path` for task execution but not for the processes themselves at startup).

- [ ] **Step 1: Create the file**

Create `Dockerfile.airflow` at repo root with this exact content:

```dockerfile
FROM apache/airflow:3.0.0-python3.10

USER root

# Microsoft ODBC Driver 18 for SQL Server (required by pyodbc on Linux)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        apt-transport-https \
        unixodbc-dev \
        build-essential \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements-core.txt
COPY dags/llm_v2/requirements.txt /tmp/requirements-llm-v2.txt
COPY dags/hierarchical_llm_version/requirements.txt /tmp/requirements-hierarchical.txt

# Install PyTorch CPU-only first to avoid pulling the multi-GB CUDA build
RUN pip install --no-cache-dir \
        torch \
        --index-url https://download.pytorch.org/whl/cpu

# Install all remaining dependencies
RUN pip install --no-cache-dir \
        -r /tmp/requirements-core.txt \
        -r /tmp/requirements-llm-v2.txt \
        -r /tmp/requirements-hierarchical.txt \
        "requests[socks]" \
        pyodbc \
        dash \
        "dash-cytoscape"

# Download spaCy models and NLTK data
RUN python -m spacy download en_core_web_sm \
    && python -m spacy download en_core_web_lg \
    && python -c "import nltk; nltk.download('wordnet', quiet=True)"

# Make dags/ importable at process startup (scheduler, webserver)
ENV PYTHONPATH=/opt/airflow/dags
```

- [ ] **Step 2: Verify the file exists at repo root**

```bash
ls Dockerfile.airflow
```

Expected: file is listed.

---

## Task 3: Create `docker-compose.yml`

**Files:**
- Create: `docker-compose.yml`

**Notes:**
- Uses YAML anchors (`x-airflow-common`) to share build config and base environment across services without repetition.
- `airflow-init` runs `airflow db migrate` then creates the `admin` user (the `|| true` prevents failure if the user already exists on subsequent runs). It exits with code 0, which satisfies `condition: service_completed_successfully` on the dependent services.
- `configs.json` is volume-mounted into each Airflow container. **Before first run**, edit `dags/configs/configs.json` on the host and replace `LAPTOP-I91584GB\\SQLEXPRESS` with `host.docker.internal\\SQLEXPRESS` (Windows Docker Desktop) so the container can reach the host's SQL Server Express.
- DAGs volume is mounted read-write (not `:ro`) because Airflow writes `__pycache__` when importing DAGs.
- `AIRFLOW__CORE__FERNET_KEY` left empty — Airflow generates a key automatically. Set it explicitly if you need stable encrypted connection strings across restarts.

- [ ] **Step 1: Create the file**

Create `docker-compose.yml` at repo root with this exact content:

```yaml
x-airflow-common:
  &airflow-common
  build:
    context: .
    dockerfile: Dockerfile.airflow
  environment:
    &airflow-common-env
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__WEBSERVER__SECRET_KEY: 'local-dev-secret-change-for-production'
    PYTHONPATH: /opt/airflow/dags

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    environment:
      <<: *airflow-common-env
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dags/configs/configs.json:/opt/airflow/dags/configs/configs.json
      - airflow_logs:/opt/airflow/logs
    command: >
      bash -c "
        airflow db migrate &&
        (airflow users create
          --username admin
          --password admin
          --firstname Admin
          --lastname User
          --role Admin
          --email admin@example.com
        || true)
      "
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dags/configs/configs.json:/opt/airflow/dags/configs/configs.json
      - airflow_logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: webserver
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 10

  airflow-scheduler:
    <<: *airflow-common
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dags/configs/configs.json:/opt/airflow/dags/configs/configs.json
      - airflow_logs:/opt/airflow/logs
    command: scheduler
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres_data:
  airflow_logs:
```

- [ ] **Step 2: Validate compose file syntax**

```bash
docker compose config --quiet
```

Expected: no output, exit code 0.

---

## Task 4: Create `scripts/start-airflow.sh`

**Files:**
- Create: `scripts/start-airflow.sh`

- [ ] **Step 1: Create the file**

Create `scripts/start-airflow.sh` with this exact content:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

if ! command -v docker &>/dev/null; then
    echo "Error: Docker is not installed or not in PATH." >&2
    exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "Error: Docker daemon is not running." >&2
    exit 1
fi

echo "Building Airflow image (first run takes 10-20 minutes due to PyTorch and spaCy models)..."
docker compose build

echo "Starting all services..."
docker compose up -d

echo ""
echo "Airflow UI:  http://localhost:8080"
echo "Login:       admin / admin"
echo ""
echo "Useful commands:"
echo "  docker compose logs -f            # stream logs from all services"
echo "  docker compose logs -f airflow-scheduler  # scheduler logs only"
echo "  docker compose down               # stop and remove containers"
echo "  docker compose down -v            # stop and also delete the postgres volume"
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x scripts/start-airflow.sh
```

- [ ] **Step 3: Verify**

```bash
head -1 scripts/start-airflow.sh && ls -l scripts/start-airflow.sh | grep -o "^-rwx"
```

Expected: first line is `#!/usr/bin/env bash`, permissions start with `-rwx`.

---

## Task 5: Create `scripts/start-airflow.ps1`

**Files:**
- Create: `scripts/start-airflow.ps1`

- [ ] **Step 1: Create the file**

Create `scripts/start-airflow.ps1` with this exact content:

```powershell
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not in PATH."
    exit 1
}

docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker daemon is not running. Start Docker Desktop and try again."
    exit 1
}

Write-Host "Building Airflow image (first run takes 10-20 minutes due to PyTorch and spaCy models)..."
docker compose build
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose build failed"; exit 1 }

Write-Host "Starting all services..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up failed"; exit 1 }

Write-Host ""
Write-Host "Airflow UI:  http://localhost:8080"
Write-Host "Login:       admin / admin"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  docker compose logs -f                     # stream logs from all services"
Write-Host "  docker compose logs -f airflow-scheduler   # scheduler logs only"
Write-Host "  docker compose down                        # stop and remove containers"
Write-Host "  docker compose down -v                     # stop and also delete the postgres volume"
```

- [ ] **Step 2: Verify the file exists**

In PowerShell:
```powershell
Test-Path scripts\start-airflow.ps1
```

Expected: `True`

---

## Task 6: Commit all Docker infrastructure

- [ ] **Step 1: Stage and commit**

```bash
git add Dockerfile.airflow docker-compose.yml scripts/start-airflow.sh scripts/start-airflow.ps1 docs/superpowers/plans/2026-06-14-docker-airflow-startup.md
git commit -m "feat: add Docker Compose setup and launch scripts for Airflow 3.x

- Dockerfile.airflow: custom image with ODBC driver, all pip packages,
  spaCy models (en_core_web_sm/lg), NLTK wordnet
- docker-compose.yml: postgres + airflow-init + webserver + scheduler
- scripts/start-airflow.sh + start-airflow.ps1: build image and start stack
- configs.json volume-mounted; update Server to host.docker.internal before first run"
```

- [ ] **Step 2: Verify commit**

```bash
git log --oneline -3
```

Expected: two new commits at the top (one from Task 1, one from this task).

---

## Pre-flight Note for First Run

Before running the scripts, edit `dags/configs/configs.json` on the host:

Change:
```json
"ConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=LAPTOP-I91584GB\\SQLEXPRESS;..."
```
To:
```json
"ConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=host.docker.internal\\SQLEXPRESS;..."
```

`host.docker.internal` is the Docker Desktop DNS name that resolves to the Windows host from inside a container.
