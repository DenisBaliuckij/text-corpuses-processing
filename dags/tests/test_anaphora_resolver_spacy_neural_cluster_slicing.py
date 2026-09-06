import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import spacy
from spacy.tokens import SpanGroup

import anaphoraResolverSpacyNeural as spacy_neural_module


def test_resolve_and_substitute_handles_spangroup_without_slice_support(monkeypatch):
    """
    spaCy's SpanGroup does not support slice indexing (cluster[1:] raises
    TypeError: an integer is required) - only integer indexing. A real
    coreference cluster of size >= 2 hit this on every document, since
    resolve_and_substitute iterated cluster[1:] directly. Regression test
    for that, using a real SpanGroup (not a mock) so it actually exercises
    the same __getitem__ behavior a real en_coreference_web_trf run would
    hit, without needing that heavy model installed.
    """
    blank_nlp = spacy.blank("en")
    doc = blank_nlp("Alice met Bob. She smiled at him.")
    doc.spans["coref_clusters_1"] = SpanGroup(doc, name="coref_clusters_1", spans=[doc[0:1], doc[4:5]])

    class _FakeNlp:
        def __call__(self, text):
            assert text == doc.text
            return doc

    monkeypatch.setattr(spacy_neural_module, "_get_nlp", lambda: _FakeNlp())

    out, subs, ress = spacy_neural_module.resolve_and_substitute(doc.text)

    assert len(subs) == 1
    assert subs[0].original == "She"
    assert subs[0].replacement == "Alice"
    assert len(ress) == 1
