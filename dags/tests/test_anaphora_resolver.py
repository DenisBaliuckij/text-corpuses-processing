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
