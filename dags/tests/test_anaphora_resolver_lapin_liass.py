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
    text = "Alice went to the store. She bought milk."
    subs = [Substitution(start=25, end=28, original='She',
                         replacement='Alice', pronoun_index=5,
                         antecedent_index=0, score=200)]
    assert apply_substitutions(text, subs) == "Alice went to the store. Alice bought milk."


def test_apply_substitutions_empty_list():
    text = "Alice went."
    assert apply_substitutions(text, []) == text


def test_apply_substitutions_multiple_right_to_left():
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
