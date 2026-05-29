from __future__ import annotations

from ..schemas.common import Sentence


def preprocess(text: str, language: str = "ru") -> list[Sentence]:
    text = text.strip()
    if not text:
        return []

    if language == "ru":
        from razdel import sentenize

        spans = list(sentenize(text))
        return [
            Sentence(id=i, text=span.text.strip())
            for i, span in enumerate(spans)
            if span.text.strip()
        ]
    else:
        import nltk

        try:
            sents = nltk.sent_tokenize(text, language="english")
        except LookupError:
            nltk.download("punkt_tab", quiet=True)
            sents = nltk.sent_tokenize(text, language="english")
        return [
            Sentence(id=i, text=s.strip())
            for i, s in enumerate(sents)
            if s.strip()
        ]
