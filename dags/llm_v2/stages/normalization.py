from __future__ import annotations

from ..config_schema import NormalizationConfig
from ..schemas.common import NormalizedTriplet, RawTriplet

_STOP_MODS_RU = {
    "некоторый", "данный", "этот", "такой", "различный",
    "определённый", "определенный", "свой", "весь",
}
_STOP_MODS_EN = {
    "some", "this", "these", "certain", "various", "the", "a", "an",
}


def normalize_triplets(
    triplets: list[RawTriplet],
    config: NormalizationConfig,
) -> list[NormalizedTriplet]:
    if not config.enabled:
        return [
            NormalizedTriplet(
                **t.model_dump(),
                norm_subject=t.subject,
                norm_relation=t.relation,
                norm_object=t.object,
            )
            for t in triplets
        ]

    _normalizer = _build_normalizer(config.language)

    results: list[NormalizedTriplet] = []
    for t in triplets:
        results.append(
            NormalizedTriplet(
                **t.model_dump(),
                norm_subject=_normalizer(t.subject),
                norm_relation=_normalizer(t.relation),
                norm_object=_normalizer(t.object),
            )
        )
    return results


def _build_normalizer(language: str):
    if language == "ru":
        import pymorphy3

        morph = pymorphy3.MorphAnalyzer()
        stop = _STOP_MODS_RU

        def _norm_ru(text: str) -> str:
            text = text.strip()
            if not text:
                return text
            if text.isupper() and len(text) <= 6:
                return text
            words = text.lower().split()
            lemmas = [morph.parse(w)[0].normal_form for w in words]
            lemmas = [l for l in lemmas if l not in stop]
            return " ".join(lemmas)

        return _norm_ru
    else:
        import spacy

        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            from spacy.cli import download

            download("en_core_web_sm")
            nlp = spacy.load("en_core_web_sm")
        stop = _STOP_MODS_EN

        def _norm_en(text: str) -> str:
            text = text.strip()
            if not text:
                return text
            if text.isupper() and len(text) <= 6:
                return text
            doc = nlp(text.lower())
            lemmas = [
                tok.text if tok.pos_ == "PROPN" else tok.lemma_
                for tok in doc
                if tok.text not in stop
            ]
            return " ".join(lemmas)

        return _norm_en
