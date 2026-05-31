# -*- coding: utf-8 -*-
import spacy
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Imported here for type compatibility with anaphoraResolverLapinLiass
from anaphoraResolverLapinLiass import Substitution, Resolution

_nlp = None

def _get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_coreference_web_trf")
    return _nlp


def resolve_and_substitute(text: str, mark: bool = False) -> Tuple[str, List[Substitution], List[Resolution]]:
    nlp = _get_nlp()
    doc = nlp(text)

    # Collect clusters: doc.spans keys like "coref_clusters_1", "coref_clusters_2", ...
    clusters = [
        spans for key, spans in doc.spans.items()
        if key.startswith("coref_clusters")
    ]

    # Build replacement map: character span -> replacement text
    # For each cluster, first span is the head antecedent
    replacements: List[Substitution] = []
    resolutions: List[Resolution] = []

    for cluster in clusters:
        if len(cluster) < 2:
            continue
        head_span = cluster[0]
        antecedent_text = head_span.text

        for mention_span in cluster[1:]:
            start = mention_span.start_char
            end = mention_span.end_char
            original = mention_span.text
            replacement = antecedent_text if not mark else f"{antecedent_text}{{{original}}}"
            replacements.append(Substitution(
                start=start,
                end=end,
                original=original,
                replacement=replacement,
                pronoun_index=mention_span.start,
                antecedent_index=head_span.start,
                score=1.0,
            ))
            resolutions.append(Resolution(
                pronoun=original,
                pronoun_index=mention_span.start,
                antecedent=antecedent_text,
                antecedent_index=head_span.start,
                score=1.0,
            ))

    # Apply in reverse order to preserve offsets
    replacements.sort(key=lambda s: s.start, reverse=True)
    out = text
    for s in replacements:
        out = out[:s.start] + s.replacement + out[s.end:]

    return out, replacements, resolutions
