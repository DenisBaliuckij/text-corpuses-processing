import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from paperDownloader import _next_active_index, advance_state

CRITERIA_SINGLE_REPEAT = [{"query": "a", "repeat": True}]
CRITERIA_SINGLE_ONCE   = [{"query": "a", "repeat": False}]
CRITERIA_TWO_REPEAT    = [{"query": "a", "repeat": True},  {"query": "b", "repeat": True}]
CRITERIA_TWO_ONCE      = [{"query": "a", "repeat": False}, {"query": "b", "repeat": False}]
CRITERIA_MIXED         = [{"query": "a", "repeat": True},  {"query": "b", "repeat": False}]


# ── _next_active_index ────────────────────────────────────────────────────────

def test_next_active_skips_done():
    criteria = [{"query": "a"}, {"query": "b"}, {"query": "c"}]
    assert _next_active_index(0, criteria, done={1}) == 2


def test_next_active_wraps_around():
    assert _next_active_index(1, [{"query": "a"}, {"query": "b"}], done=set()) == 0


def test_next_active_single_no_done_returns_self():
    assert _next_active_index(0, [{"query": "a"}], done=set()) == 0


def test_next_active_all_done_returns_none():
    assert _next_active_index(0, [{"query": "a"}, {"query": "b"}], done={0, 1}) is None


# ── advance_state: has_more=True ──────────────────────────────────────────────

def test_advance_increments_page_when_has_more():
    state = {"criterion_index": 0, "page": 3, "done_criteria": []}
    result = advance_state(state, CRITERIA_SINGLE_REPEAT, has_more=True)
    assert result == {"criterion_index": 0, "page": 4, "done_criteria": []}


def test_advance_has_more_does_not_change_criterion():
    state = {"criterion_index": 1, "page": 2, "done_criteria": [0]}
    result = advance_state(state, CRITERIA_TWO_ONCE, has_more=True)
    assert result["criterion_index"] == 1
    assert result["page"] == 3
    assert result["done_criteria"] == [0]


# ── advance_state: repeat=True, has_more=False ────────────────────────────────

def test_advance_repeat_moves_to_next_criterion():
    state = {"criterion_index": 0, "page": 5, "done_criteria": []}
    result = advance_state(state, CRITERIA_TWO_REPEAT, has_more=False)
    assert result == {"criterion_index": 1, "page": 1, "done_criteria": []}


def test_advance_repeat_single_wraps_to_self():
    state = {"criterion_index": 0, "page": 5, "done_criteria": []}
    result = advance_state(state, CRITERIA_SINGLE_REPEAT, has_more=False)
    assert result == {"criterion_index": 0, "page": 1, "done_criteria": []}


def test_advance_repeat_skips_done_on_next():
    criteria = [
        {"query": "a", "repeat": True},
        {"query": "b", "repeat": False},
        {"query": "c", "repeat": True},
    ]
    state = {"criterion_index": 0, "page": 2, "done_criteria": [1]}
    result = advance_state(state, criteria, has_more=False)
    assert result == {"criterion_index": 2, "page": 1, "done_criteria": [1]}


# ── advance_state: repeat=False, has_more=False ───────────────────────────────

def test_advance_once_adds_to_done_and_moves_to_next():
    state = {"criterion_index": 0, "page": 2, "done_criteria": []}
    result = advance_state(state, CRITERIA_TWO_ONCE, has_more=False)
    assert result["criterion_index"] == 1
    assert result["page"] == 1
    assert 0 in result["done_criteria"]


def test_advance_once_last_criterion_returns_none():
    state = {"criterion_index": 0, "page": 2, "done_criteria": []}
    assert advance_state(state, CRITERIA_SINGLE_ONCE, has_more=False) is None


def test_advance_once_second_returns_none_when_first_already_done():
    state = {"criterion_index": 1, "page": 2, "done_criteria": [0]}
    assert advance_state(state, CRITERIA_TWO_ONCE, has_more=False) is None


def test_advance_mixed_once_exhausted_moves_to_repeat():
    # criteria: [repeat=True, repeat=False]; once exhausted → move to repeat
    state = {"criterion_index": 1, "page": 2, "done_criteria": []}
    result = advance_state(state, CRITERIA_MIXED, has_more=False)
    assert result["criterion_index"] == 0
    assert result["page"] == 1
    assert 1 in result["done_criteria"]


def test_advance_once_preserves_existing_done():
    criteria = [
        {"query": "a", "repeat": False},
        {"query": "b", "repeat": False},
        {"query": "c", "repeat": False},
    ]
    state = {"criterion_index": 1, "page": 1, "done_criteria": [0]}
    result = advance_state(state, criteria, has_more=False)
    assert result["criterion_index"] == 2
    assert 0 in result["done_criteria"]
    assert 1 in result["done_criteria"]
