# Test Coverage — Design Spec

**Date:** 2026-06-02
**Sub-project:** 2 of 3 (Test coverage)
**Scope:** Unit and integration tests for the three untested business-logic modules: `ftpConnector`, `anaphoraResolverLapinLiass`, and the six repository-calling functions in `paperDownloader`.

---

## Problem

Three modules have zero test coverage after Sub-project 1:

1. **`dags/ftpConnector.py`** — 3 static methods, each making FTP calls. No tests.
2. **`dags/anaphoraResolverLapinLiass.py`** — core rule-based anaphora resolution algorithm with pure helper functions and spaCy-dependent pipeline. No tests.
3. **`dags/paperDownloader.py`** — 6 repository-calling functions (`load_state`, `save_state`, `clear_state`, `get_proxy`, `mark_proxy_broken`, `save_urls`) refactored in Sub-project 1 to use repositories. Not covered by the existing `test_paper_downloader.py` which only tests pure state-machine functions.

DAG files and root standalone scripts are explicitly out of scope — they are thin wrappers with no business logic.

---

## New Test Files

| File | Covers | Test count |
|------|--------|-----------|
| `dags/tests/test_ftp_connector.py` | `ftpConnector` — 3 methods + ftpPostfix parameter | ~4 tests |
| `dags/tests/test_anaphora_resolver_lapin_liass.py` | Pure helpers (unit) + `resolve_and_substitute` (integration) | ~15 tests |
| `dags/tests/test_paper_downloader_db.py` | 6 repo-calling functions in `paperDownloader` | ~8 tests |

All run under: `pytest dags/tests/ -k "not spacy_neural" -v`

The Lapin-Liass integration tests use real `en_core_web_sm` (already installed) — no new markers needed.

---

## `test_ftp_connector.py`

Mocks `ftplib.FTP` and `getConfig`. Verifies each method issues the correct FTP command and calls `quit()`.

```python
def test_store_file_sends_storbinary():
    # patches ftplib.FTP, getConfig
    # asserts storbinary('STOR filename', file) called
    # asserts quit() called

def test_get_file_returns_populated_bytesio():
    # patches ftplib.FTP, getConfig
    # asserts retrbinary('RETR path', callback) called
    # asserts returned object is BytesIO

def test_get_file_list_calls_nlst():
    # patches ftplib.FTP, getConfig
    # asserts nlst('path') called
    # asserts return value equals mock nlst result

def test_ftppostfix_uses_suffixed_config_keys():
    # calls storeFile with ftpPostfix='Graph'
    # asserts config['FtpHostGraph'] is used for connect()
```

---

## `test_anaphora_resolver_lapin_liass.py`

### Import strategy

`anaphoraResolverLapinLiass.py` calls `nlp = spacy.load("en_core_web_sm")` at module level. Unit tests must bypass this. Strategy: patch `spacy.load` before importing the module via `importlib.util`, loading it under a unique name so the real module is not cached.

Integration tests import the module normally (real spaCy loads).

### Part A — Unit tests (mocked spaCy tokens)

spaCy token attributes are set on `MagicMock` instances: `.pos_`, `.tag_`, `.dep_`, `.ent_type_`, `.text`, `.i`, `.idx`.

**`salience()` tests:**
- Distance 0 → recency score 100; distance 1 → 75; distance 4 → 0
- `dep == "nsubj"` adds 80; `dep == "dobj"` adds 50; `dep in ("pobj", "iobj")` adds 40
- `pos == "PROPN"` adds 30
- Combined: nsubj + PROPN + distance 0 → 210

**`compatible()` tests:**
- Gender mismatch → False (e.g. "he" vs female mention)
- Number mismatch → False (e.g. "they" vs singular mention)
- `"unknown"` gender is compatible with any gender
- Pronoun not in PRONOUNS dict → False

**`is_mention()` tests:**
- `pos_ == "NOUN"` → True; `pos_ == "PROPN"` → True; `pos_ == "VERB"` → False

**`detect_gender()` / `detect_number()` tests:**
- `ent_type_ == "PERSON"` → gender "unknown"
- `ent_type_ != "PERSON"` → gender "neutral"
- `tag_ == "NNS"` → number "plural"; otherwise → "sing"

**`build_substitutions()` tests:**
- Resolution with antecedent builds Substitution with correct `start`/`end` from token
- `mark=True` → replacement is `antecedent{original}`
- Resolution with `antecedent=None` is skipped
- Output is sorted descending by `start`

**`apply_substitutions()` tests:**
- Single substitution replaces correct slice
- Multiple substitutions applied right-to-left (no offset shift)
- No substitutions → original text unchanged

### Part B — Integration tests (real spaCy)

```python
def test_resolve_and_substitute_replaces_pronoun():
    text = "Alice went to the store. She bought milk."
    result, subs, _ = resolve_and_substitute(text)
    assert len(subs) > 0
    assert "Alice" in result

def test_resolve_and_substitute_mark_mode():
    text = "Alice went to the store. She bought milk."
    result, _, _ = resolve_and_substitute(text, mark=True)
    assert "{She}" in result

def test_resolve_and_substitute_no_pronouns():
    text = "Alice went to the store."
    result, subs, _ = resolve_and_substitute(text)
    assert result == text
    assert subs == []

def test_resolve_and_substitute_returns_correct_types():
    text = "The scientist published her findings."
    result, subs, resols = resolve_and_substitute(text)
    assert isinstance(result, str)
    assert isinstance(subs, list)
    assert isinstance(resols, list)
```

---

## `test_paper_downloader_db.py`

Patches repository methods on the `paperDownloader` module namespace.

```python
def test_load_state_returns_default_when_no_state():
    with patch('paperDownloader.ServiceStateRepository.get', return_value=None):
        result = load_state(4)
    assert result == {'criterion_index': 0, 'page': 1, 'done_criteria': []}

def test_load_state_parses_json_from_db():
    state = {'criterion_index': 1, 'page': 3, 'done_criteria': [0]}
    with patch('paperDownloader.ServiceStateRepository.get', return_value=(json.dumps(state),)):
        result = load_state(4)
    assert result == state

def test_save_state_serialises_and_calls_update():
    with patch('paperDownloader.ServiceStateRepository.update') as mock_update:
        save_state(4, {'criterion_index': 0, 'page': 1, 'done_criteria': []})
    mock_update.assert_called_once_with(4, '{"criterion_index": 0, "page": 1, "done_criteria": []}')

def test_clear_state_calls_remove():
    with patch('paperDownloader.ServiceStateRepository.remove') as mock_remove:
        clear_state(4)
    mock_remove.assert_called_once_with(4)

def test_get_proxy_returns_formatted_dict():
    fake = {'proxieIp': '1.2.3.4', 'proxiePort': 8080, 'proxieProtocol': 'http'}
    with patch('paperDownloader.ProxyRepository.get_latest', return_value=fake):
        result = get_proxy()
    assert result == {'ip': '1.2.3.4', 'port': 8080, 'protocol': 'http'}

def test_get_proxy_raises_when_no_proxy():
    with patch('paperDownloader.ProxyRepository.get_latest', return_value=None):
        with pytest.raises(RuntimeError):
            get_proxy()

def test_mark_proxy_broken_calls_repository():
    with patch('paperDownloader.ProxyRepository.mark_broken') as mock_mb:
        mark_proxy_broken('1.2.3.4')
    mock_mb.assert_called_once_with('1.2.3.4')

def test_save_urls_calls_add_url_for_each():
    with patch('paperDownloader.PdfRepository.add_url') as mock_add:
        save_urls(['http://a.com', 'http://b.com'])
    assert mock_add.call_count == 2
    mock_add.assert_any_call('http://a.com')
    mock_add.assert_any_call('http://b.com')
```

---

## Out of Scope

- DAG files
- Root standalone scripts
- `anaphoraResolverSpacyNeural.py` (already excluded via `-k "not spacy_neural"`)
- LLM pipeline modules (`llm_v2/`, `hierarchical_llm_version/`)
