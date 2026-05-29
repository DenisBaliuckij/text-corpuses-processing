# Graph Construction DAGs Design

**Date:** 2026-05-29  
**Project:** text-corpuses-processing  
**Status:** Approved

## Goal

Convert the existing graph construction pipeline scripts into Airflow DAGs. Each DAG does exactly one atomic unit of work. The SQL Server `TextCorpuses` database is the single source of truth for job and file state. FTP stores file content (source texts, resolved texts, and the incremental graph).

---

## Architecture Overview

```
[User triggers]
       │
       ▼
┌─────────────────────────┐
│  start_tree_formation   │  DAG 1 (exists)
│  _job                   │  Creates GraphConstructionJob at status=0
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  prepare_graph_         │  DAG 2 (new)
│  construction_job       │  Picks job at status=0
│                         │  Lists .tex files from FTP at job's paths
│                         │  Inserts each into GraphConstructionFiles
│                         │  Advances job to status=10
└────────────┬────────────┘
             │ (one file per run)
             ▼
┌─────────────────────────┐
│  resolve_anaphora       │  DAG 3 (new)
│                         │  Picks one file at status=0
│                         │  Locks it to status=5
│                         │  Reads raw .tex from FTP
│                         │  Resolves anaphora in memory
│                         │  Writes resolved text to FTP
│                         │  Sets file status=10
└────────────┬────────────┘
             │ (one file per run)
             ▼
┌─────────────────────────┐
│  build_graph            │  DAG 4 (new)
│                         │  Picks one file at status=10
│                         │  Locks it to status=15
│                         │  Reads resolved text from FTP
│                         │  Extracts NLP graph (spaCy)
│                         │  Loads existing job graph from FTP (or empty)
│                         │  Merges new edges into job graph
│                         │  Saves job graph back to FTP
│                         │  Sets file status=20
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  finalize_job           │  DAG 5 (new)
│                         │  Polls jobs at status=20
│                         │  Checks if all files are at status=20
│                         │  If yes: sets job status=30 (completed)
└─────────────────────────┘
```

DAGs 2–5 run `@continuous` with `max_active_runs=1`. DAGs 3 and 4 can run in parallel with each other — file status locking prevents collisions. Each DAG run picks exactly one item, processes it, and exits; Airflow immediately restarts it for the next item.

---

## State Machines

### `GraphConstructionJob.Status`

| Status | Meaning |
|--------|---------|
| 0 | Created, awaiting preparation |
| 5 | Preparation in progress |
| 10 | Ready — files registered, awaiting processing |
| 20 | Graph building in progress |
| 30 | Completed |
| 99 | Error |

### `GraphConstructionFiles.Status`

| Status | Meaning |
|--------|---------|
| 0 | Pending anaphora resolution |
| 5 | Anaphora resolution in progress |
| 10 | Anaphora done, ready for graph building |
| 15 | Graph building in progress |
| 20 | Graph building done |
| 99 | Error |

---

## Database Changes

### New column on `GraphConstructionFiles`

```sql
ALTER TABLE [dbo].[GraphConstructionFiles]
ADD [ResolvedFilePath] nvarchar(1000) NULL;
```

Stores the FTP path of the anaphora-resolved text so DAG 4 knows what to read.

### New stored procedures

#### `GetFileForAnaphoraResolution()`
Picks one `GraphConstructionFiles` row where `Status=0` and `GraphConstructionJobId` belongs to a job at status 10 or 20. Sets `Status=5`. Returns the row (fileId, filePath, jobId).

#### `MarkFileAnaphoraDone(@fileId int, @resolvedFilePath nvarchar(max))`
Sets `Status=10`, writes `ResolvedFilePath`. Only allowed from status=5.

#### `SetFileError(@fileId int, @error nvarchar(max))`
Sets `Status=99`. Stores error in a new `Error` column (nvarchar max, nullable) on `GraphConstructionFiles`.

#### `GetFileForGraphBuilding()`
Picks one `GraphConstructionFiles` row at `Status=10` from any job at status 10 or 20. Sets `Status=15`. Returns the row (fileId, resolvedFilePath, jobId).

#### `TransitionJobToExecution(@jobId int)`
Sets `GraphConstructionJob.Status=20` where `ID=@jobId` and current status is 10. No-op if status is already 20. This is called by DAG 4 on its first file for a given job to signal execution has started.

#### `MarkFileGraphDone(@fileId int)`
Sets `Status=20`. Only allowed from status=15.

#### `FinalizeCompletedJobs()`
Finds jobs at status=20 where no `GraphConstructionFiles` row has status other than 20. Sets job status=30. Returns the job ID if found, otherwise returns nothing.

### Missing `databaseConnector` methods (to add to `dags/dbConnector.py`)

The following methods are called by existing scripts but are absent from the class:
- `getJobForPreparation()` — wraps `GetJobForPreparation`
- `addFileSourceForGraphConstructionJob(location, jobId)` — wraps `AddTextSourceForProcessing`
- `setErrorForPreparationJob(jobId, error)` — wraps `SetErrorForGraphCreationJob`
- `getJobForExecution()` — wraps `GetJobForExecution` (used by existing DAG 1 only)
- `transitionJobToExecution(jobId)` — wraps `TransitionJobToExecution`
- `getFileForAnaphoraResolution()` — wraps `GetFileForAnaphoraResolution`
- `markFileAnaphoraDone(fileId, resolvedFilePath)` — wraps `MarkFileAnaphoraDone`
- `setFileError(fileId, error)` — wraps `SetFileError`
- `getFileForGraphBuilding()` — wraps `GetFileForGraphBuilding`
- `markFileGraphDone(fileId)` — wraps `MarkFileGraphDone`
- `finalizeCompletedJobs()` — wraps `FinalizeCompletedJobs`

---

## FTP Layout

```
graphJobs/
  {jobId}/
    graph.json          ← incremental job graph (JSON, grown on each file)
    anaphora/
      {fileId}.txt      ← resolved text per file (written by DAG 3, read by DAG 4)
```

Text source files already live under the paths specified in `GraphConstructionJob.IncludedPaths` (the Tex FTP root), e.g. `arxiv/`, `springer/`.

### `graph.json` format

```json
{
  "nodes": ["concept a", "concept b"],
  "edges": [
    {"agent_1": "concept a", "agent_2": "concept b", "meaning": "verb", "weight": 3}
  ]
}
```

Merging: if an edge `(agent_1, agent_2, meaning)` already exists, increment its weight. New edges are appended. New nodes are union-merged.

---

## DAG Designs

### DAG 2 — `prepare_graph_construction_job`

**File:** `dags/prepare-graph-construction-job-dag.py`

```
getJobForPreparation()                          → locks job 0→5, returns job row
IncludedPaths.split(';')                        → list of FTP path prefixes
for each path:
    ftpConnector.getFileList(path, 'Tex')       → list of .tex file paths
    for each file:
        addTextSourceForProcessing(path, jobId) → idempotent insert into GraphConstructionFiles
processGraphCreationJobToTextCopying(jobId)     → job 5→10
```

On exception: `setErrorForGraphCreationJob(jobId, error)` → job status=99.  
If no job at status=0: exit silently (Airflow restarts DAG).

### DAG 3 — `resolve_anaphora`

**File:** `dags/resolve-anaphora-dag.py`

```
getFileForAnaphoraResolution()                  → locks file 0→5, returns (fileId, filePath, jobId)
ftpConnector.getFile(filePath, 'Tex')           → raw text bytes
text = file.read().decode('utf-8')
output, _, _ = resolve_and_substitute(text)     → resolved text string (anaphoraResolverLapinLiass)
resolvedPath = f"graphJobs/{jobId}/anaphora/{fileId}.txt"
ftpConnector.storeFile(resolvedPath, BytesIO(output.encode('utf-8')), 'Graph')
markFileAnaphoraDone(fileId, resolvedPath)      → file 5→10
```

On exception: `setFileError(fileId, str(e))` → file status=99.  
If no file available: exit silently.

### DAG 4 — `build_graph`

**File:** `dags/build-graph-dag.py`

```
getFileForGraphBuilding()                       → locks file 10→15, returns (fileId, resolvedPath, jobId)
transitionJobToExecution(jobId)                 → job 10→20 (no-op if already 20)

ftpConnector.getFile(resolvedPath, 'Graph')     → resolved text
text = file.read().decode('utf-8')
new_edges = extract_graph_edges(text)           → spaCy NLP: list of (agent_1, agent_2, meaning)

graphPath = f"graphJobs/{jobId}/graph.json"
try:
    existing_file = ftpConnector.getFile(graphPath, 'Graph')
    graph = json.load(existing_file)
except FileNotFoundError:
    graph = {"nodes": [], "edges": []}

graph = merge_graph(graph, new_edges)
ftpConnector.storeFile(graphPath, BytesIO(json.dumps(graph).encode()), 'Graph')
markFileGraphDone(fileId)                       → file 15→20
```

`extract_graph_edges(text)` uses the spaCy pipeline from `make_eng_graphs.py` (rule-based NLP). For `processorName="AIBased"` jobs, this is a future extension point — the job config is available from the job row.

`merge_graph(graph, new_edges)`:
- Union-merge nodes
- For each new edge: if `(agent_1, agent_2, meaning)` already exists, increment weight; else append with weight=1

On exception: `setFileError(fileId, str(e))` → file status=99.  
If no file available: exit silently.

### DAG 5 — `finalize_job`

**File:** `dags/finalize-job-dag.py`

```
finalizeCompletedJobs()     → finds job at status=20 with all files at status=20
                              sets job status=30, returns job row or None
```

Always exits cleanly regardless of result. Airflow restarts immediately for the next check. No error handling needed — this is a pure read+update with no external dependencies.

---

## Error Handling Summary

| Layer | On error | Recovery |
|-------|----------|----------|
| Job preparation | Job → status=99 | Fix data, manually reset to status=0 and re-trigger |
| Anaphora resolution | File → status=99 | Fix, manually reset file to status=0 |
| Graph building | File → status=99 | Fix, manually reset file to status=0 |
| Finalize | No errors possible | — |

Files at status=99 do not block *other files* from being processed — DAGs 3 and 4 simply skip them and pick the next available file. However, a job with any file at status=99 will never reach status=30, since `FinalizeCompletedJobs` requires every file to be at status=20. Manual intervention (resetting the file to status=0) is required to recover.

---

## Files to Create / Modify

| Action | File |
|--------|------|
| Create | `dags/prepare-graph-construction-job-dag.py` |
| Create | `dags/resolve-anaphora-dag.py` |
| Create | `dags/build-graph-dag.py` |
| Create | `dags/finalize-job-dag.py` |
| Modify | `dags/dbConnector.py` — add missing methods |
| Modify | `Database/database-v0.3.sql` → new file `database-v0.4.sql` |
| Modify | `dags/configs/configs.json` — add Graph FTP bucket if not present |
