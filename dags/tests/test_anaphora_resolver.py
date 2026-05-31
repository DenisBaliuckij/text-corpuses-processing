import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from anaphoraResolver import resolve_and_substitute

def test_dispatcher_defaults_to_lapin_liass():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text)
    assert isinstance(out, str)
    assert isinstance(subs, list)
    assert isinstance(ress, list)

def test_dispatcher_lapin_liass_explicit():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="LapinLiass")
    assert "Alice" in out

def test_dispatcher_unknown_name_falls_back_to_lapin_liass():
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="UnknownResolver")
    assert isinstance(out, str)

def test_dispatcher_spacy_neural_returns_correct_shape():
    # Requires: pip install "spacy[transformers]" && python -m spacy download en_coreference_web_trf
    text = "Alice studies biology. She publishes papers."
    out, subs, ress = resolve_and_substitute(text, resolver_name="SpacyNeural")
    assert isinstance(out, str)
    assert isinstance(subs, list)
    assert isinstance(ress, list)
    # Each substitution must have the same fields as Lapin-Liass Substitution
    for s in subs:
        assert hasattr(s, 'start')
        assert hasattr(s, 'end')
        assert hasattr(s, 'original')
        assert hasattr(s, 'replacement')
