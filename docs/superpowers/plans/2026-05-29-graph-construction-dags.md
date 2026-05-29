# Graph Construction DAGs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the existing graph construction pipeline scripts into four new Airflow DAGs that build a semantic graph incrementally from text corpuses, with every step tracked in the SQL Server database and graph stored on FTP.

**Architecture:** Five Airflow DAGs (`@continuous`, `max_active_runs=1`) each do one atomic unit of work driven by status codes on `GraphConstructionJob` and `GraphConstructionFiles`. The graph is serialized as `graph.json` on FTP under `graphJobs/{jobId}/`, grown file by file. All DAG state lives in SQL Server `TextCorpuses` on `LAPTOP-I91584GB\SQLEXPRESS`.

**Tech Stack:** Python 3, Apache Airflow 2 (airflow.sdk), pyodbc, spaCy (`en_core_web_lg`), NLTK WordNetLemmatizer, ftplib, SQL Server, pytest.

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `Database/database-v0.4.sql` | Schema migration: new columns + 7 stored procedures |
| Modify | `dags/ftpConnector.py` | Add `getFileList` method |
| Modify | `dags/dbConnector.py` | Add 11 missing methods |
| Create | `dags/graphBuilder.py` | `extract_graph_edges`, `merge_graph` |
| Create | `dags/tests/test_graph_builder.py` | Unit tests for graphBuilder |
| Create | `dags/anaphoraResolverLapinLiass.py` | Copy of root-level anaphora resolver for Airflow access |
| Create | `dags/prepare-graph-construction-job-dag.py` | DAG 2: enumerate files, advance job to status=10 |
| Create | `dags/resolve-anaphora-dag.py` | DAG 3: resolve anaphora for one file |
| Create | `dags/build-graph-dag.py` | DAG 4: extract NLP edges, merge into job graph |
| Create | `dags/finalize-job-dag.py` | DAG 5: detect completion, set job status=30 |

---

## Task 1: SQL Migration — database-v0.4.sql

**Files:**
- Create: `Database/database-v0.4.sql`

This script adds two nullable columns to `GraphConstructionFiles` and creates seven new stored procedures. Run it in SSMS against the `TextCorpuses` database on `LAPTOP-I91584GB\SQLEXPRESS`.

- [ ] **Step 1: Create the migration script**

Create `Database/database-v0.4.sql` with the following content (save as UTF-8 in SSMS or any text editor, then run in SSMS):

```sql
USE [TextCorpuses]
GO

-- Add ResolvedFilePath and Error columns to GraphConstructionFiles
ALTER TABLE [dbo].[GraphConstructionFiles]
ADD [ResolvedFilePath] nvarchar(1000) NULL;
GO

ALTER TABLE [dbo].[GraphConstructionFiles]
ADD [Error] nvarchar(max) NULL;
GO

-- ============================================================
-- GetFileForAnaphoraResolution
-- Picks one file at Status=0 from an active job (Status 10 or 20),
-- locks it to Status=5, returns (ID, FilePath, GraphConstructionJobId).
-- ============================================================
CREATE PROCEDURE [dbo].[GetFileForAnaphoraResolution]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @fileId AS int;

    SELECT TOP 1 @fileId = f.ID
    FROM dbo.GraphConstructionFiles f
    INNER JOIN dbo.GraphConstructionJob j ON f.GraphConstructionJobId = j.ID
    WHERE f.Status = 0
    AND j.Status IN (10, 20);

    IF @fileId IS NULL
        RETURN;

    UPDATE dbo.GraphConstructionFiles
    SET Status = 5
    WHERE ID = @fileId;

    SELECT f.ID, f.FilePath, f.GraphConstructionJobId
    FROM dbo.GraphConstructionFiles f
    WHERE f.ID = @fileId;
END
GO

-- ============================================================
-- MarkFileAnaphoraDone
-- Transitions file from Status=5 to Status=10, stores resolved path.
-- ============================================================
CREATE PROCEDURE [dbo].[MarkFileAnaphoraDone]
    @fileId int,
    @resolvedFilePath nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 10, ResolvedFilePath = @resolvedFilePath
    WHERE ID = @fileId AND Status = 5;
END
GO

-- ============================================================
-- SetFileError
-- Sets file to Status=99 and stores error message.
-- ============================================================
CREATE PROCEDURE [dbo].[SetFileError]
    @fileId int,
    @error nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 99, Error = @error
    WHERE ID = @fileId;
END
GO

-- ============================================================
-- GetFileForGraphBuilding
-- Picks one file at Status=10 from an active job (Status 10 or 20),
-- locks it to Status=15, returns (ID, ResolvedFilePath, GraphConstructionJobId).
-- ============================================================
CREATE PROCEDURE [dbo].[GetFileForGraphBuilding]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @fileId AS int;

    SELECT TOP 1 @fileId = f.ID
    FROM dbo.GraphConstructionFiles f
    INNER JOIN dbo.GraphConstructionJob j ON f.GraphConstructionJobId = j.ID
    WHERE f.Status = 10
    AND j.Status IN (10, 20);

    IF @fileId IS NULL
        RETURN;

    UPDATE dbo.GraphConstructionFiles
    SET Status = 15
    WHERE ID = @fileId;

    SELECT f.ID, f.ResolvedFilePath, f.GraphConstructionJobId
    FROM dbo.GraphConstructionFiles f
    WHERE f.ID = @fileId;
END
GO

-- ============================================================
-- TransitionJobToExecution
-- Moves job from Status=10 to Status=20. No-op if already 20.
-- Called by build_graph DAG on first file of each job.
-- ============================================================
CREATE PROCEDURE [dbo].[TransitionJobToExecution]
    @jobId int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionJob
    SET Status = 20, LastStatusChangeAt = GETDATE()
    WHERE ID = @jobId AND Status = 10;
END
GO

-- ============================================================
-- MarkFileGraphDone
-- Transitions file from Status=15 to Status=20.
-- ============================================================
CREATE PROCEDURE [dbo].[MarkFileGraphDone]
    @fileId int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 20
    WHERE ID = @fileId AND Status = 15;
END
GO

-- ============================================================
-- FinalizeCompletedJobs
-- Finds a job at Status=20 where every file is at Status=20.
-- Sets that job to Status=30 (completed). Returns the job ID.
-- ============================================================
CREATE PROCEDURE [dbo].[FinalizeCompletedJobs]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @jobId AS int;

    SELECT TOP 1 @jobId = j.ID
    FROM dbo.GraphConstructionJob j
    WHERE j.Status = 20
    AND EXISTS (
        SELECT 1 FROM dbo.GraphConstructionFiles f
        WHERE f.GraphConstructionJobId = j.ID
    )
    AND NOT EXISTS (
        SELECT 1 FROM dbo.GraphConstructionFiles f
        WHERE f.GraphConstructionJobId = j.ID
        AND f.Status != 20
    );

    IF @jobId IS NULL
        RETURN;

    UPDATE dbo.GraphConstructionJob
    SET Status = 30, LastStatusChangeAt = GETDATE()
    WHERE ID = @jobId;

    SELECT @jobId AS ID;
END
GO
```

- [ ] **Step 2: Apply the migration in SSMS**

Open SSMS, connect to `LAPTOP-I91584GB\SQLEXPRESS`, open the script, and run it (F5). Verify no errors in the Messages panel.

- [ ] **Step 3: Verify the migration**

Run this query in SSMS to confirm all objects exist:

```sql
USE [TextCorpuses]
GO

-- Verify columns
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'GraphConstructionFiles'
  AND COLUMN_NAME IN ('ResolvedFilePath', 'Error');

-- Verify stored procedures (should return 7 rows)
SELECT name FROM sys.procedures
WHERE name IN (
    'GetFileForAnaphoraResolution',
    'MarkFileAnaphoraDone',
    'SetFileError',
    'GetFileForGraphBuilding',
    'TransitionJobToExecution',
    'MarkFileGraphDone',
    'FinalizeCompletedJobs'
);
```

Expected: 2 rows from the column query, 7 rows from the procedure query.

- [ ] **Step 4: Commit**

```bash
git add Database/database-v0.4.sql
git commit -m "feat: add schema migration for graph construction pipeline (v0.4)"
```

---

## Task 2: Add `getFileList` to `dags/ftpConnector.py`

**Files:**
- Modify: `dags/ftpConnector.py`

The root-level `ftpConnector.py` already has `getFileList` but `dags/ftpConnector.py` does not. DAG 2 needs it.

- [ ] **Step 1: Add the method**

Open `dags/ftpConnector.py`. The current file ends after `getFile`. Append `getFileList` so the full file reads:

```python
# -*- coding: utf-8 -*-

from configs import getConfig
import ftplib
import io

class ftpConnector:
    def storeFile(filename, file, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        server.storbinary(f"STOR {filename}", file)
        server.quit()
    def getFile(filePath, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        memfile = io.BytesIO()
        server.retrbinary("RETR " + filePath, memfile.write)
        server.quit()
        return memfile
    def getFileList(path, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        files = server.nlst(path)
        server.quit()
        return files
```

- [ ] **Step 2: Commit**

```bash
git add dags/ftpConnector.py
git commit -m "feat: add getFileList to dags ftpConnector"
```

---

## Task 3: Extend `dags/dbConnector.py` with 11 New Methods

**Files:**
- Modify: `dags/dbConnector.py`

The existing class has 12 methods. Add 11 more at the bottom of the class, before the final closing. Column indices for job rows from `GraphConstructionJob`: `[0]=ID, [1]=StartedAt, [2]=IncludedPaths, [3]=ProcessorConfig, [4]=Status, [5]=Error, [6]=LastStatusChangeAt`. Column indices for file rows from `GraphConstructionFiles`: `[0]=ID, [1]=FilePath or ResolvedFilePath, [2]=GraphConstructionJobId`.

- [ ] **Step 1: Add the 11 methods**

Open `dags/dbConnector.py`. The file currently ends with `insertGraphCreationJob`. Append these methods inside the `databaseConnector` class:

```python
    def getJobForPreparation():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForPreparation]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def addFileSourceForGraphConstructionJob(location, jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddTextSourceForProcessing] @location = ?, @jobId = ?", (location, jobId))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def processGraphCreationJobToTextCopying(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[ProcessGraphCreationJobToTextCopying] @jobId = ?", (jobId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def setErrorForPreparationJob(jobId, error):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SetErrorForGraphCreationJob] @id = ?, @error = ?", (jobId, str(error)))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def transitionJobToExecution(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[TransitionJobToExecution] @jobId = ?", (jobId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def getFileForAnaphoraResolution():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForAnaphoraResolution]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def markFileAnaphoraDone(fileId, resolvedFilePath):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileAnaphoraDone] @fileId = ?, @resolvedFilePath = ?", (fileId, resolvedFilePath))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def setFileError(fileId, error):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SetFileError] @fileId = ?, @error = ?", (fileId, str(error)))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def getFileForGraphBuilding():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForGraphBuilding]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def markFileGraphDone(fileId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileGraphDone] @fileId = ?", (fileId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def finalizeCompletedJobs():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[FinalizeCompletedJobs]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result
```

- [ ] **Step 2: Commit**

```bash
git add dags/dbConnector.py
git commit -m "feat: add 11 graph construction methods to databaseConnector"
```

---

## Task 4: Create `dags/graphBuilder.py` with Tests

**Files:**
- Create: `dags/graphBuilder.py`
- Create: `dags/tests/__init__.py`
- Create: `dags/tests/test_graph_builder.py`

`graphBuilder.py` encapsulates the spaCy NLP extraction (adapted from `C:\Repositories\concept-tree\make_eng_graphs.py`) and the pure graph merge logic. Requires: `pip install spacy nltk` and `python -m spacy download en_core_web_lg` and `python -c "import nltk; nltk.download('wordnet')"` in the Airflow environment.

- [ ] **Step 1: Write the failing tests first**

Create `dags/tests/__init__.py` (empty file), then create `dags/tests/test_graph_builder.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from graphBuilder import merge_graph, extract_graph_edges


def test_merge_graph_into_empty_adds_node_and_edge():
    graph = {"nodes": [], "edges": []}
    result = merge_graph(graph, [("cat", "dog", "chases")])
    assert "cat" in result["nodes"]
    assert "dog" in result["nodes"]
    assert result["edges"] == [
        {"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}
    ]


def test_merge_graph_increments_weight_on_duplicate_edge():
    graph = {
        "nodes": ["cat", "dog"],
        "edges": [{"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}]
    }
    result = merge_graph(graph, [("cat", "dog", "chases")])
    assert len(result["edges"]) == 1
    assert result["edges"][0]["weight"] == 2


def test_merge_graph_appends_new_edge():
    graph = {
        "nodes": ["cat", "dog"],
        "edges": [{"agent_1": "cat", "agent_2": "dog", "meaning": "chases", "weight": 1}]
    }
    result = merge_graph(graph, [("cat", "fish", "eats")])
    assert len(result["edges"]) == 2
    assert "fish" in result["nodes"]
    assert result["edges"][1] == {
        "agent_1": "cat", "agent_2": "fish", "meaning": "eats", "weight": 1
    }


def test_merge_graph_multiple_edges_in_one_call():
    graph = {"nodes": [], "edges": []}
    edges = [("a", "b", "rel"), ("a", "b", "rel"), ("b", "c", "rel2")]
    result = merge_graph(graph, edges)
    assert len(result["edges"]) == 2
    assert result["edges"][0]["weight"] == 2
    assert result["edges"][1]["weight"] == 1


def test_extract_graph_edges_returns_list_of_triples():
    edges = extract_graph_edges("The scientist studies the phenomenon.")
    assert isinstance(edges, list)
    for edge in edges:
        assert len(edge) == 3
        a1, a2, meaning = edge
        assert isinstance(a1, str) and isinstance(a2, str) and isinstance(meaning, str)


def test_extract_graph_edges_no_self_loops():
    edges = extract_graph_edges("The cat and the cat eat fish.")
    for a1, a2, _ in edges:
        assert a1 != a2
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd C:\Repositories\text-corpuses-processing
python -m pytest dags/tests/test_graph_builder.py -v
```

Expected output: `ImportError: No module named 'graphBuilder'` (or similar). Tests must fail before implementation.

- [ ] **Step 3: Create `dags/graphBuilder.py`**

```python
# -*- coding: utf-8 -*-
import spacy
from nltk.stem import WordNetLemmatizer

nlp = spacy.load("en_core_web_lg")
lemmatizer = WordNetLemmatizer()


def _get_syntactic_relations(doc):
    chunks = []
    relations = []
    subjects = {}
    conjunctions = {}
    chunk_to_text = {}

    for chunk in doc.noun_chunks:
        normalized = ' '.join([
            lemmatizer.lemmatize(token.text.lower(), pos='n')
            for token in chunk
            if token.text.lower() not in ['the', 'a', 'an']
        ])
        chunks.append((chunk.start_char, chunk.end_char, chunk, normalized, chunk.root.head, chunk.root.dep_))
        chunk_to_text[chunk.root] = normalized

    for token in doc:
        if token.dep_ == "conj" and token.head in chunk_to_text:
            head_text = chunk_to_text[token.head]
            conj_text = chunk_to_text.get(token)
            if head_text and conj_text:
                conjunctions.setdefault(head_text, []).append(conj_text)

    for token in doc:
        if token.dep_ == "conj" and token.head.pos_ == "NOUN":
            head_text = chunk_to_text.get(token.head, token.head.text.lower())
            conj_text = chunk_to_text.get(token, token.text.lower())
            relations.append((head_text, "and", conj_text))

    for chunk in chunks:
        if chunk[5] == 'nsubj':
            subject_text = chunk_to_text.get(chunk[2].root, chunk[3])
            subjects.setdefault(chunk[4], []).append(subject_text)
            if subject_text in conjunctions:
                subjects[chunk[4]].extend(conjunctions[subject_text])

    for i, chunk in enumerate(chunks):
        if chunk[4].pos_ == 'VERB' and chunk[5] != 'nsubj':
            subject_list = subjects.get(chunk[4], [])
            object_text = chunk_to_text.get(chunk[2].root, chunk[3])
            for subject in subject_list:
                relations.append((subject, chunk[4].text, object_text))
                if object_text in conjunctions:
                    for conj in conjunctions[object_text]:
                        relations.append((subject, chunk[4].text, conj))

        if chunk[4].pos_ == 'VERB' and i + 1 < len(chunks):
            next_chunk = chunks[i + 1]
            if next_chunk[4].pos_ == 'ADP':
                subject_list = subjects.get(chunk[4], [])
                relation_text = f"{chunk[4].text} {next_chunk[4].text}"
                object_text = chunk_to_text.get(next_chunk[2].root, next_chunk[3])
                for subject in subject_list:
                    relations.append((subject, relation_text, object_text))

    for token in doc:
        if token.dep_ == "prep" and token.head.pos_ == "NOUN":
            prep_text = token.text
            object_text = None
            for child in token.children:
                if child.dep_ == "pobj":
                    object_text = chunk_to_text.get(child, child.text.lower())
            if object_text:
                head_text = chunk_to_text.get(token.head, token.head.text.lower())
                subject_list = [head_text]
                if head_text in conjunctions:
                    subject_list.extend(conjunctions[head_text])
                for subject in subject_list:
                    relations.append((subject, prep_text, object_text))

    return relations


def extract_graph_edges(text):
    """Extract (agent_1, agent_2, meaning) triples from text using spaCy NLP.
    
    Returns list of tuples, no self-loops, no empty strings.
    """
    doc = nlp(text)
    relations = _get_syntactic_relations(doc)
    return [
        (a1, a2, meaning)
        for a1, a2, meaning in relations
        if a1 and a2 and a1 != a2
    ]


def merge_graph(graph, new_edges):
    """Merge new_edges into graph dict, incrementing weight on duplicates.

    Args:
        graph: dict with keys 'nodes' (list of str) and 'edges'
               (list of dicts with agent_1, agent_2, meaning, weight)
        new_edges: list of (agent_1, agent_2, meaning) tuples

    Returns:
        Updated graph dict (mutates and returns the same dict).
    """
    edge_index = {
        (e['agent_1'], e['agent_2'], e['meaning']): i
        for i, e in enumerate(graph['edges'])
    }
    nodes = set(graph['nodes'])

    for agent_1, agent_2, meaning in new_edges:
        nodes.add(agent_1)
        nodes.add(agent_2)
        key = (agent_1, agent_2, meaning)
        if key in edge_index:
            graph['edges'][edge_index[key]]['weight'] += 1
        else:
            idx = len(graph['edges'])
            graph['edges'].append({
                'agent_1': agent_1,
                'agent_2': agent_2,
                'meaning': meaning,
                'weight': 1
            })
            edge_index[key] = idx

    graph['nodes'] = list(nodes)
    return graph
```

- [ ] **Step 4: Run tests — all must pass**

```bash
python -m pytest dags/tests/test_graph_builder.py -v
```

Expected output:
```
test_merge_graph_into_empty_adds_node_and_edge PASSED
test_merge_graph_increments_weight_on_duplicate_edge PASSED
test_merge_graph_appends_new_edge PASSED
test_merge_graph_multiple_edges_in_one_call PASSED
test_extract_graph_edges_returns_list_of_triples PASSED
test_extract_graph_edges_no_self_loops PASSED

6 passed
```

If `en_core_web_lg` is not installed: `python -m spacy download en_core_web_lg` then retry.  
If `wordnet` is not downloaded: `python -c "import nltk; nltk.download('wordnet')"` then retry.

- [ ] **Step 5: Commit**

```bash
git add dags/graphBuilder.py dags/tests/__init__.py dags/tests/test_graph_builder.py
git commit -m "feat: add graphBuilder with NLP extraction and merge logic + tests"
```

---

## Task 5: Add Anaphora Resolver to `dags/`

**Files:**
- Create: `dags/anaphoraResolverLapinLiass.py`

Airflow imports from the `dags/` folder. The anaphora resolver lives at the repo root, so DAG 3 can't import it directly. Copy it to `dags/`.

- [ ] **Step 1: Copy the file**

```bash
copy C:\Repositories\text-corpuses-processing\anaphoraResolverLapinLiass.py ^
     C:\Repositories\text-corpuses-processing\dags\anaphoraResolverLapinLiass.py
```

- [ ] **Step 2: Verify the import works**

```bash
cd C:\Repositories\text-corpuses-processing\dags
python -c "from anaphoraResolverLapinLiass import resolve_and_substitute; print('OK')"
```

Expected: `OK`

If spaCy `en_core_web_sm` is missing: `python -m spacy download en_core_web_sm`

- [ ] **Step 3: Commit**

```bash
git add dags/anaphoraResolverLapinLiass.py
git commit -m "feat: add anaphora resolver to dags folder for Airflow access"
```

---

## Task 6: DAG 2 — `prepare_graph_construction_job`

**Files:**
- Create: `dags/prepare-graph-construction-job-dag.py`

Picks one `GraphConstructionJob` at status=0, lists `.tex` files from FTP for each path in `IncludedPaths`, registers each file in `GraphConstructionFiles`, then advances the job to status=10. One job per DAG run.

`GraphConstructionJob` column indices (from `SELECT *`): `[0]=ID, [1]=StartedAt, [2]=IncludedPaths, [3]=ProcessorConfig, [4]=Status`.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="prepare_graph_construction_job",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def prepare_job():
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector

        job = databaseConnector.getJobForPreparation()
        if job is None:
            return

        job_id = job[0]
        included_paths = job[2]

        try:
            paths = [p.strip() for p in included_paths.split(';') if p.strip()]
            for path in paths:
                file_list = ftpConnector.getFileList(path, 'Tex')
                for file_path in file_list:
                    databaseConnector.addFileSourceForGraphConstructionJob(file_path, job_id)

            databaseConnector.processGraphCreationJobToTextCopying(job_id)
        except Exception as e:
            databaseConnector.setErrorForPreparationJob(job_id, str(e))

    prepare_job()
```

- [ ] **Step 2: Verify Airflow can parse the DAG**

```bash
airflow dags list | findstr prepare_graph_construction_job
```

Expected: one row with `prepare_graph_construction_job`.

If Airflow is not on PATH, open the Airflow web UI and check the DAG appears without import errors.

- [ ] **Step 3: Commit**

```bash
git add dags/prepare-graph-construction-job-dag.py
git commit -m "feat: add prepare_graph_construction_job DAG"
```

---

## Task 7: DAG 3 — `resolve_anaphora`

**Files:**
- Create: `dags/resolve-anaphora-dag.py`

Picks one `GraphConstructionFiles` row at status=0 (from any active job), locks it to status=5, reads the raw `.tex` file from FTP, resolves anaphora in memory, writes resolved text to FTP under `graphJobs/{jobId}/anaphora/{fileId}.txt`, advances file to status=10.

`GraphConstructionFiles` row from `GetFileForAnaphoraResolution`: `[0]=ID, [1]=FilePath, [2]=GraphConstructionJobId`.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="resolve_anaphora",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def resolve_anaphora():
        import io
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector
        import anaphoraResolverLapinLiass
        from anaphoraResolverLapinLiass import resolve_and_substitute

        file_row = databaseConnector.getFileForAnaphoraResolution()
        if file_row is None:
            return

        file_id = file_row[0]
        file_path = file_row[1]
        job_id = file_row[2]

        try:
            raw_file = ftpConnector.getFile(file_path, 'Tex')
            raw_file.seek(0)
            text = raw_file.read().decode('utf-8', errors='replace')

            output, _, _ = resolve_and_substitute(text)

            resolved_path = f"graphJobs/{job_id}/anaphora/{file_id}.txt"
            ftpConnector.storeFile(
                resolved_path,
                io.BytesIO(output.encode('utf-8')),
                'Graph'
            )

            databaseConnector.markFileAnaphoraDone(file_id, resolved_path)
        except Exception as e:
            databaseConnector.setFileError(file_id, str(e))

    resolve_anaphora()
```

- [ ] **Step 2: Verify Airflow can parse the DAG**

```bash
airflow dags list | findstr resolve_anaphora
```

Expected: one row with `resolve_anaphora`.

- [ ] **Step 3: Commit**

```bash
git add dags/resolve-anaphora-dag.py
git commit -m "feat: add resolve_anaphora DAG"
```

---

## Task 8: DAG 4 — `build_graph`

**Files:**
- Create: `dags/build-graph-dag.py`

Picks one `GraphConstructionFiles` row at status=10 (anaphora resolved), locks it to status=15, transitions the job to status=20 (idempotent), reads the resolved text from FTP, extracts NLP edges, loads the existing `graph.json` for the job from FTP (or starts empty), merges new edges, saves back, marks file status=20.

`GraphConstructionFiles` row from `GetFileForGraphBuilding`: `[0]=ID, [1]=ResolvedFilePath, [2]=GraphConstructionJobId`.

`graph.json` format: `{"nodes": [...], "edges": [{"agent_1": str, "agent_2": str, "meaning": str, "weight": int}]}`.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="build_graph",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def build_graph():
        import io
        import json
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector
        import graphBuilder
        from graphBuilder import extract_graph_edges, merge_graph

        file_row = databaseConnector.getFileForGraphBuilding()
        if file_row is None:
            return

        file_id = file_row[0]
        resolved_path = file_row[1]
        job_id = file_row[2]

        try:
            databaseConnector.transitionJobToExecution(job_id)

            resolved_file = ftpConnector.getFile(resolved_path, 'Graph')
            resolved_file.seek(0)
            text = resolved_file.read().decode('utf-8', errors='replace')

            new_edges = extract_graph_edges(text)

            graph_path = f"graphJobs/{job_id}/graph.json"
            try:
                existing_file = ftpConnector.getFile(graph_path, 'Graph')
                existing_file.seek(0)
                graph = json.loads(existing_file.read().decode('utf-8'))
            except Exception:
                graph = {"nodes": [], "edges": []}

            graph = merge_graph(graph, new_edges)

            graph_bytes = json.dumps(graph, ensure_ascii=False).encode('utf-8')
            ftpConnector.storeFile(graph_path, io.BytesIO(graph_bytes), 'Graph')

            databaseConnector.markFileGraphDone(file_id)
        except Exception as e:
            databaseConnector.setFileError(file_id, str(e))

    build_graph()
```

- [ ] **Step 2: Verify Airflow can parse the DAG**

```bash
airflow dags list | findstr build_graph
```

Expected: one row with `build_graph`.

- [ ] **Step 3: Commit**

```bash
git add dags/build-graph-dag.py
git commit -m "feat: add build_graph DAG"
```

---

## Task 9: DAG 5 — `finalize_job`

**Files:**
- Create: `dags/finalize-job-dag.py`

Polls for jobs at status=20 where every `GraphConstructionFiles` row is at status=20. If found, sets the job to status=30 (completed). Always exits cleanly — no error handling needed, this is a pure DB read+update.

- [ ] **Step 1: Create the DAG file**

```python
# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

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
        import dbConnector
        from dbConnector import databaseConnector

        result = databaseConnector.finalizeCompletedJobs()
        if result is not None:
            print(f"Finalized job ID: {result[0]}")

    finalize_job()
```

- [ ] **Step 2: Verify Airflow can parse the DAG**

```bash
airflow dags list | findstr finalize_job
```

Expected: one row with `finalize_job`.

- [ ] **Step 3: Commit**

```bash
git add dags/finalize-job-dag.py
git commit -m "feat: add finalize_job DAG"
```

---

## End-to-End Smoke Test

After all tasks are complete, verify the full pipeline works:

- [ ] **Step 1: Enable all new DAGs in the Airflow UI**

Go to the Airflow web UI. Toggle on:
- `prepare_graph_construction_job`
- `resolve_anaphora`
- `build_graph`
- `finalize_job`

- [ ] **Step 2: Trigger a job**

Trigger `start_tree_formation_job` manually with:
- `paths`: a valid FTP path containing `.tex` files (e.g. `arxiv/`)
- `textProcessorName`: `RuleBased`

- [ ] **Step 3: Monitor status progression in SSMS**

Run this query repeatedly to watch state advance:

```sql
USE [TextCorpuses]
GO

SELECT j.ID, j.Status AS JobStatus, j.LastStatusChangeAt,
       COUNT(f.ID) AS TotalFiles,
       SUM(CASE WHEN f.Status = 0 THEN 1 ELSE 0 END) AS Pending,
       SUM(CASE WHEN f.Status = 5 THEN 1 ELSE 0 END) AS AnaphoraInProgress,
       SUM(CASE WHEN f.Status = 10 THEN 1 ELSE 0 END) AS AnaphDone,
       SUM(CASE WHEN f.Status = 15 THEN 1 ELSE 0 END) AS GraphInProgress,
       SUM(CASE WHEN f.Status = 20 THEN 1 ELSE 0 END) AS GraphDone,
       SUM(CASE WHEN f.Status = 99 THEN 1 ELSE 0 END) AS Errors
FROM dbo.GraphConstructionJob j
LEFT JOIN dbo.GraphConstructionFiles f ON f.GraphConstructionJobId = j.ID
GROUP BY j.ID, j.Status, j.LastStatusChangeAt
ORDER BY j.ID DESC;
```

Expected progression:
1. Job status=0 (just created)
2. Job status=5 (prepare DAG running)
3. Job status=10 + files at status=0 (prepare DAG done)
4. Files moving through 5→10→15→20 (anaphora + graph DAGs running)
5. Job status=20 (first graph DAG run)
6. Job status=30 + all files status=20 (finalize DAG done)

- [ ] **Step 4: Verify graph file on FTP**

Connect to the FTP server, navigate to `graphJobs/{jobId}/graph.json`, download and inspect it. It should contain a JSON object with `nodes` (list of strings) and `edges` (list of objects with `agent_1`, `agent_2`, `meaning`, `weight`).
