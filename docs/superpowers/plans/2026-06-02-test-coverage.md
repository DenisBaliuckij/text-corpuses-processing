# Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add unit and integration tests for the three currently-untested business-logic modules: `ftpConnector`, `anaphoraResolverLapinLiass`, and the six repository-calling functions in `paperDownloader`.

**Architecture:** Three new test files under `dags/tests/`. `ftpConnector` and `paperDownloader` tests mock external dependencies (`ftplib.FTP` and repository classes respectively). `anaphoraResolverLapinLiass` gets unit tests with `MagicMock` tokens for the pure helpers, plus integration tests calling `resolve_and_substitute()` with real `en_core_web_sm`.

**Tech Stack:** Python 3.10+, pytest, unittest.mock, spaCy `en_core_web_sm`

---

## File Map

| File | Action |
|------|--------|
| `dags/tests/test_ftp_connector.py` | **Create** — 4 tests for `ftpConnector` |
| `dags/tests/test_anaphora_resolver_lapin_liass.py` | **Create** — ~15 unit + integration tests |
| `dags/tests/test_paper_downloader_db.py` | **Create** — 8 tests for repo-calling functions |

---

## Task 1: `test_ftp_connector.py`

**Files:**
- Create: `dags/tests/test_ftp_connector.py`

`ftpConnector.py` imports `from configs import getConfig` and `import ftplib`. Tests patch both. The `conftest.py` already mocks `sys.modules['configs']` globally, so each test patches `ftpConnector.getConfig` for precise control.

- [ ] **Step 1.1: Create `dags/tests/test_ftp_connector.py`**

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import io
from unittest.mock import patch, MagicMock
from ftpConnector import ftpConnector

_CFG = {
    'FtpHost': '127.0.0.1', 'FtpPort': 21,
    'FtpUser': 'user', 'FtpPassword': 'pass',
    'FtpHostGraph': '127.0.0.2', 'FtpPortGraph': 22,
    'FtpUserGraph': 'guser', 'FtpPasswordGraph': 'gpass',
}


def test_store_file_connects_logs_in_and_sends_storbinary():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        ftpConnector.storeFile('test.pdf', fake_file)
        mock_server = MockFTP.return_value
        mock_server.connect.assert_called_once_with('127.0.0.1', 21)
        mock_server.login.assert_called_once_with('user', 'pass')
        mock_server.storbinary.assert_called_once_with('STOR test.pdf', fake_file)
        mock_server.quit.assert_called_once()


def test_get_file_issues_retrbinary_and_returns_bytesio():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        result = ftpConnector.getFile('arxiv/paper.pdf')
        mock_server = MockFTP.return_value
        args = mock_server.retrbinary.call_args[0]
        assert args[0] == 'RETR arxiv/paper.pdf'
        assert isinstance(result, io.BytesIO)
        mock_server.quit.assert_called_once()


def test_get_file_list_calls_nlst_and_returns_result():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        MockFTP.return_value.nlst.return_value = ['file1.pdf', 'file2.pdf']
        result = ftpConnector.getFileList('arxiv/')
        MockFTP.return_value.nlst.assert_called_once_with('arxiv/')
        assert result == ['file1.pdf', 'file2.pdf']
        MockFTP.return_value.quit.assert_called_once()


def test_ftppostfix_uses_suffixed_config_keys():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        ftpConnector.storeFile('graph.json', fake_file, ftpPostfix='Graph')
        MockFTP.return_value.connect.assert_called_once_with('127.0.0.2', 22)
        MockFTP.return_value.login.assert_called_once_with('guser', 'gpass')
```

- [ ] **Step 1.2: Run tests**

```
pytest dags/tests/test_ftp_connector.py -v
```

Expected: 4 passed.

- [ ] **Step 1.3: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 69 passed (65 existing + 4 new).

- [ ] **Step 1.4: Commit**

```
git add dags/tests/test_ftp_connector.py
git commit -m "test: add unit tests for ftpConnector"
```

---

## Task 2: `test_anaphora_resolver_lapin_liass.py`

**Files:**
- Create: `dags/tests/test_anaphora_resolver_lapin_liass.py`

`anaphoraResolverLapinLiass.py` calls `nlp = spacy.load("en_core_web_sm")` at module level. Since `en_core_web_sm` is installed, it loads cleanly on import — no mocking of spaCy needed. Unit tests pass `MagicMock` objects with `.pos_`, `.tag_`, `.dep_`, `.ent_type_`, `.text`, `.i`, `.idx` set manually. Integration tests call `resolve_and_substitute()` with real text.

- [ ] **Step 2.1: Create `dags/tests/test_anaphora_resolver_lapin_liass.py`**

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from unittest.mock import MagicMock
from anaphoraResolverLapinLiass import (
    salience, compatible, is_mention, detect_gender, detect_number,
    build_substitutions, apply_substitutions,
    Mention, Resolution, Substitution, resolve_and_substitute,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def make_token(text='Alice', pos_='PROPN', tag_='NNP', dep_='nsubj',
               ent_type_='PERSON', i=0, idx=0):
    tok = MagicMock()
    tok.text = text
    tok.pos_ = pos_
    tok.tag_ = tag_
    tok.dep_ = dep_
    tok.ent_type_ = ent_type_
    tok.i = i
    tok.idx = idx
    return tok


def make_mention(text='Alice', token_index=0, sent_index=0, dep='nsubj',
                 pos='PROPN', gender='unknown', number='sing'):
    return Mention(text=text, token_index=token_index, sent_index=sent_index,
                   dep=dep, pos=pos, gender=gender, number=number)


# ── salience ─────────────────────────────────────────────────────────────────

def test_salience_distance_0_gives_100_recency():
    m = make_mention(sent_index=2, dep='nsubj', pos='NOUN')
    assert salience(m, current_sent_idx=2) == 100 + 80  # recency + nsubj


def test_salience_distance_1_gives_75_recency():
    m = make_mention(sent_index=1, dep='nsubj', pos='NOUN')
    assert salience(m, current_sent_idx=2) == 75 + 80


def test_salience_distance_4_zeroes_recency():
    m = make_mention(sent_index=0, dep='nsubj', pos='NOUN')
    assert salience(m, current_sent_idx=4) == 0 + 80  # max(0, 100-100)


def test_salience_dep_dobj_bonus():
    m = make_mention(dep='dobj', pos='NOUN', sent_index=0)
    assert salience(m, 0) == 100 + 50


def test_salience_dep_pobj_bonus():
    m = make_mention(dep='pobj', pos='NOUN', sent_index=0)
    assert salience(m, 0) == 100 + 40


def test_salience_propn_adds_30():
    m = make_mention(dep='nsubj', pos='PROPN', sent_index=0)
    assert salience(m, 0) == 100 + 80 + 30


# ── compatible ───────────────────────────────────────────────────────────────

def test_compatible_male_pronoun_male_mention():
    tok = make_token(text='he')
    m = make_mention(gender='male', number='sing')
    assert compatible(tok, m) is True


def test_compatible_gender_mismatch_returns_false():
    tok = make_token(text='he')
    m = make_mention(gender='female', number='sing')
    assert compatible(tok, m) is False


def test_compatible_number_mismatch_returns_false():
    tok = make_token(text='they')
    m = make_mention(gender='unknown', number='sing')
    assert compatible(tok, m) is False


def test_compatible_unknown_gender_is_wildcard():
    tok = make_token(text='it')   # neutral, sing
    m = make_mention(gender='unknown', number='sing')
    assert compatible(tok, m) is True


def test_compatible_non_pronoun_returns_false():
    tok = make_token(text='went')
    assert compatible(tok, make_mention()) is False


# ── is_mention, detect_gender, detect_number ─────────────────────────────────

def test_is_mention_noun_and_propn():
    assert is_mention(make_token(pos_='NOUN')) is True
    assert is_mention(make_token(pos_='PROPN')) is True


def test_is_mention_verb_returns_false():
    assert is_mention(make_token(pos_='VERB')) is False


def test_detect_gender_person_entity():
    assert detect_gender(make_token(ent_type_='PERSON')) == 'unknown'


def test_detect_gender_non_person():
    assert detect_gender(make_token(ent_type_='ORG')) == 'neutral'


def test_detect_number_nns_is_plural():
    assert detect_number(make_token(tag_='NNS')) == 'plural'


def test_detect_number_nn_is_singular():
    assert detect_number(make_token(tag_='NN')) == 'sing'


# ── build_substitutions ───────────────────────────────────────────────────────

def test_build_substitutions_produces_correct_substitution():
    mock_doc = MagicMock()
    mock_doc.__getitem__ = MagicMock(
        return_value=make_token(text='She', i=5, idx=25)
    )
    resolutions = [
        Resolution(pronoun='She', pronoun_index=5,
                   antecedent='Alice', antecedent_index=0, score=200)
    ]
    subs = build_substitutions(mock_doc, resolutions, mark=False)
    assert len(subs) == 1
    assert subs[0].replacement == 'Alice'
    assert subs[0].start == 25
    assert subs[0].end == 25 + len('She')


def test_build_substitutions_mark_mode_appends_original():
    mock_doc = MagicMock()
    mock_doc.__getitem__ = MagicMock(
        return_value=make_token(text='She', i=5, idx=25)
    )
    resolutions = [
        Resolution(pronoun='She', pronoun_index=5,
                   antecedent='Alice', antecedent_index=0, score=200)
    ]
    subs = build_substitutions(mock_doc, resolutions, mark=True)
    assert subs[0].replacement == 'Alice{She}'


def test_build_substitutions_skips_none_antecedent():
    mock_doc = MagicMock()
    resolutions = [
        Resolution(pronoun='She', pronoun_index=5,
                   antecedent=None, antecedent_index=None, score=-1)
    ]
    subs = build_substitutions(mock_doc, resolutions)
    assert subs == []


def test_build_substitutions_sorted_descending_by_start():
    tok_early = make_token(text='he', i=3, idx=10)
    tok_late = make_token(text='She', i=8, idx=30)

    def get_tok(index):
        return tok_early if index == 3 else tok_late

    mock_doc = MagicMock()
    mock_doc.__getitem__ = MagicMock(side_effect=get_tok)
    resolutions = [
        Resolution('he', 3, 'Bob', 0, 100),
        Resolution('She', 8, 'Alice', 0, 200),
    ]
    subs = build_substitutions(mock_doc, resolutions)
    assert subs[0].start > subs[1].start


# ── apply_substitutions ───────────────────────────────────────────────────────

def test_apply_substitutions_single_replacement():
    # "Alice went to the store. She bought milk."
    #  positions: S=25, h=26, e=27  →  She at [25, 28)
    text = "Alice went to the store. She bought milk."
    subs = [Substitution(start=25, end=28, original='She',
                         replacement='Alice', pronoun_index=5,
                         antecedent_index=0, score=200)]
    assert apply_substitutions(text, subs) == "Alice went to the store. Alice bought milk."


def test_apply_substitutions_empty_list():
    text = "Alice went."
    assert apply_substitutions(text, []) == text


def test_apply_substitutions_multiple_right_to_left():
    # "A B C D E"  →  replace D[6:7] with X, B[2:3] with Y
    text = "A B C D E"
    subs = [
        Substitution(start=6, end=7, original='D', replacement='X',
                     pronoun_index=3, antecedent_index=0, score=100),
        Substitution(start=2, end=3, original='B', replacement='Y',
                     pronoun_index=1, antecedent_index=0, score=100),
    ]
    assert apply_substitutions(text, subs) == "A Y C X E"


# ── integration tests (real spaCy en_core_web_sm) ────────────────────────────

def test_resolve_and_substitute_replaces_pronoun():
    text = "Alice went to the store. She bought milk."
    result, subs, _ = resolve_and_substitute(text)
    assert isinstance(result, str)
    assert len(subs) > 0
    assert 'Alice' in result


def test_resolve_and_substitute_mark_mode():
    text = "Alice went to the store. She bought milk."
    result, _, _ = resolve_and_substitute(text, mark=True)
    assert '{She}' in result


def test_resolve_and_substitute_no_pronouns_returns_original():
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

- [ ] **Step 2.2: Run tests**

```
pytest dags/tests/test_anaphora_resolver_lapin_liass.py -v
```

Expected: all tests pass. (The import loads `en_core_web_sm` once — expect ~1–2 s startup.)

- [ ] **Step 2.3: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: 69 + ~27 = ~96 passed.

- [ ] **Step 2.4: Commit**

```
git add dags/tests/test_anaphora_resolver_lapin_liass.py
git commit -m "test: add unit and integration tests for anaphoraResolverLapinLiass"
```

---

## Task 3: `test_paper_downloader_db.py`

**Files:**
- Create: `dags/tests/test_paper_downloader_db.py`

`paperDownloader.py` imports `ServiceStateRepository`, `ProxyRepository`, `PdfRepository` at module level. Patch them on the `paperDownloader` module namespace.

- [ ] **Step 3.1: Create `dags/tests/test_paper_downloader_db.py`**

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
import pytest
from unittest.mock import patch
from paperDownloader import (
    load_state, save_state, clear_state,
    get_proxy, mark_proxy_broken, save_urls,
)


def test_load_state_returns_default_when_no_state():
    with patch('paperDownloader.ServiceStateRepository.get', return_value=None):
        result = load_state(4)
    assert result == {'criterion_index': 0, 'page': 1, 'done_criteria': []}


def test_load_state_parses_json_from_db():
    state = {'criterion_index': 1, 'page': 3, 'done_criteria': [0]}
    row = (json.dumps(state),)
    with patch('paperDownloader.ServiceStateRepository.get', return_value=row):
        result = load_state(4)
    assert result == state


def test_save_state_serialises_and_calls_update():
    with patch('paperDownloader.ServiceStateRepository.update') as mock_update:
        save_state(4, {'criterion_index': 0, 'page': 1, 'done_criteria': []})
    mock_update.assert_called_once_with(
        4, '{"criterion_index": 0, "page": 1, "done_criteria": []}'
    )


def test_clear_state_calls_remove():
    with patch('paperDownloader.ServiceStateRepository.remove') as mock_remove:
        clear_state(4)
    mock_remove.assert_called_once_with(4)


def test_get_proxy_returns_formatted_dict():
    fake = {'proxieIp': ' 1.2.3.4 ', 'proxiePort': 8080, 'proxieProtocol': ' http '}
    with patch('paperDownloader.ProxyRepository.get_latest', return_value=fake):
        result = get_proxy()
    assert result == {'ip': '1.2.3.4', 'port': 8080, 'protocol': 'http'}


def test_get_proxy_raises_when_none():
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

- [ ] **Step 3.2: Run tests**

```
pytest dags/tests/test_paper_downloader_db.py -v
```

Expected: 8 passed.

- [ ] **Step 3.3: Confirm full suite passes**

```
pytest dags/tests/ -k "not spacy_neural" -v
```

Expected: ~104 passed, 0 failures.

- [ ] **Step 3.4: Commit**

```
git add dags/tests/test_paper_downloader_db.py
git commit -m "test: add tests for paperDownloader repository-calling functions"
```

---

## Done

After all three tasks, all business-logic modules have test coverage:
- `ftpConnector` — 4 tests covering all 3 methods and the `ftpPostfix` parameter
- `anaphoraResolverLapinLiass` — pure-function unit tests + integration tests via real spaCy
- `paperDownloader` repo-calling functions — 8 tests covering all 6 functions including edge cases
