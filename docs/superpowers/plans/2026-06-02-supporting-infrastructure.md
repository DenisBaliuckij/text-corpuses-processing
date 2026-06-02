# Supporting Infrastructure — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create root `requirements.txt`, git pre-push hook with install scripts, and refresh README.md (EN + RU) with simplified setup, module structure table, deploy note, and testing section.

**Spec:** `docs/superpowers/specs/2026-06-02-supporting-infrastructure-design.md`

**Tech Stack:** Python 3.10+, pytest, git hooks, bash/bat scripts

---

## File Map

| File | Action |
|------|--------|
| `requirements.txt` | **Create** |
| `hooks/pre-push` | **Create** |
| `scripts/install-hooks.sh` | **Create** |
| `scripts/install-hooks.bat` | **Create** |
| `README.md` | **Modify** — setup, architecture, deploy, testing sections (EN + RU) |

---

## Task 1: Create `requirements.txt`

- [x] **Step 1.1:** Create `requirements.txt` at repo root with four labeled sections per spec.

---

## Task 2: Create git hook and install scripts

- [x] **Step 2.1:** Create `hooks/pre-push` — runs `pytest dags/tests/ -k "not spacy_neural" -q`, blocks push on failure.
- [x] **Step 2.2:** Create `scripts/install-hooks.sh` — copies hook and sets executable bit.
- [x] **Step 2.3:** Create `scripts/install-hooks.bat` — Windows CMD equivalent.

---

## Task 3: Refresh README.md

Both language sections receive identical structural updates (translated).

- [x] **Step 3.1 (3a):** Replace scattered pip install blocks with `pip install -r requirements.txt` + spaCy downloads + hook install line.
- [x] **Step 3.2 (3b):** Add "Module structure" table after the pipeline diagram (EN) / "Структура модулей" (RU).
- [x] **Step 3.3 (3c):** Add self-contained deploy note to the Deploy DAGs section (EN + RU).
- [x] **Step 3.4 (3d):** Add Testing / Тестирование section with current test count (105 tests, 1 deselected).

---

## Task 4: Commit

- [x] **Step 4.1:** Commit all supporting infrastructure files together.

---

## Done

After all tasks:
- Single `pip install -r requirements.txt` installs everything for the core pipeline and both LLM backends
- Pre-push hook blocks pushes when tests fail (install once with install-hooks script)
- README reflects current module structure, simplified setup, and accurate test count
