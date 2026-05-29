


# -*- coding: utf-8 -*-
"""
Anaphora resolution + in-text substitution using spaCy.
- Uses the provided BatchAnaphoraResolver to select antecedents.
- Replaces each pronoun in the original text with the resolved antecedent surface form.
- Offers an option to mark substitutions for debugging.
Usage:
  python anaphora_subst.py input.txt > output.txt
  cat input.txt | python anaphora_subst.py - --mark
  python anaphora_subst.py input.txt --json
Requires:
  pip install spacy
  python -m spacy download en_core_web_sm
"""
import sys
import argparse
import pathlib
import json
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple
import spacy
# ---------------------------------------------------------
# NLP PIPELINE
# ---------------------------------------------------------
nlp = spacy.load("en_core_web_sm", disable=["lemmatizer"])
# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
PRONOUNS = {
    "he": ("male", "sing"),
    "him": ("male", "sing"),
    "his": ("male", "sing"),
    "she": ("female", "sing"),
    "her": ("female", "sing"),
    "it": ("neutral", "sing"),
    "its": ("neutral", "sing"),
    "they": ("unknown", "plural"),
    "them": ("unknown", "plural"),
    "their": ("unknown", "plural"),
}
# ---------------------------------------------------------
# DATA STRUCTURES
# ---------------------------------------------------------
@dataclass
class Mention:
    text: str
    token_index: int
    sent_index: int
    dep: str
    pos: str
    gender: str
    number: str
@dataclass
class Resolution:
    pronoun: str
    pronoun_index: int
    antecedent: Optional[str]
    antecedent_index: Optional[int]
    score: float
# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------
def detect_gender(token):
    # Basic heuristic: if spaCy says it's a PERSON, we don't force a gender
    if token.ent_type_ == "PERSON":
        return "unknown"
    return "neutral"
def detect_number(token):
    if token.tag_ in ("NNS", "NNPS"):
        return "plural"
    return "sing"
def is_mention(token):
    return token.pos_ in ("NOUN", "PROPN")
def compatible(pronoun_token, mention: Mention):
    p = pronoun_token.text.lower()
    if p not in PRONOUNS:
        return False
    p_gender, p_number = PRONOUNS[p]
    gender_ok = (
        p_gender == "unknown"
        or mention.gender == "unknown"
        or p_gender == mention.gender
    )
    number_ok = p_number == mention.number
    return gender_ok and number_ok
# ---------------------------------------------------------
# SALIENCE
# ---------------------------------------------------------
def salience(mention: Mention, current_sent_idx: int):
    score = 0
    # recency
    distance = current_sent_idx - mention.sent_index
    score += max(0, 100 - distance * 25)
    # grammatical role
    if mention.dep == "nsubj":
        score += 80
    elif mention.dep == "dobj":
        score += 50
    elif mention.dep in ("pobj", "iobj"):
        score += 40
    # proper noun
    if mention.pos == "PROPN":
        score += 30
    return score
# ---------------------------------------------------------
# RESOLVER
# ---------------------------------------------------------
class BatchAnaphoraResolver:
    def __init__(self):
        self.mentions: List[Mention] = []
    def reset(self):
        self.mentions = []
    def extract_mentions(self, doc):
        sentences = list(doc.sents)
        for sent_idx, sent in enumerate(sentences):
            for token in sent:
                if is_mention(token):
                    mention = Mention(
                        text=token.text,
                        token_index=token.i,
                        sent_index=sent_idx,
                        dep=token.dep_,
                        pos=token.pos_,
                        gender=detect_gender(token),
                        number=detect_number(token),
                    )
                    self.mentions.append(mention)
    def resolve_document(self, text: str) -> Dict:
        self.reset()
        doc = nlp(text)
        self.extract_mentions(doc)
        resolutions: List[Resolution] = []
        sentences = list(doc.sents)
        for sent_idx, sent in enumerate(sentences):
            for token in sent:
                lower = token.text.lower()
                if lower not in PRONOUNS:
                    continue
                best: Optional[Mention] = None
                best_score = -1.0
                for mention in self.mentions:
                    # antecedent must appear earlier
                    if mention.token_index >= token.i:
                        continue
                    # agreement
                    if not compatible(token, mention):
                        continue
                    score = salience(mention, sent_idx)
                    if score > best_score:
                        best_score = score
                        best = mention
                resolution = Resolution(
                    pronoun=token.text,
                    pronoun_index=token.i,
                    antecedent=(best.text if best else None),
                    antecedent_index=(best.token_index if best else None),
                    score=best_score
                )
                resolutions.append(resolution)
        return {
            "text": text,
            "doc": doc,  # keep doc for offsets
            "resolutions": resolutions
        }
    # ---------------------------------------------------------
# SUBSTITUTION LOGIC
# ---------------------------------------------------------
@dataclass
class Substitution:
    # character offsets in the original text for the pronoun token
    start: int
    end: int
    original: str
    replacement: str
    pronoun_index: int
    antecedent_index: Optional[int]
    score: float
def build_substitutions(doc, resolutions: List[Resolution], mark: bool = False) -> List[Substitution]:
    """
    Map token indices to character spans and build concrete substitutions.
    """
    subs: List[Substitution] = []
    for r in resolutions:
        if r.antecedent is None:
            continue
        # Map pronoun token index to its char span
        tok = doc[r.pronoun_index]
        start, end = tok.idx, tok.idx + len(tok.text)
        original = tok.text
        replacement = r.antecedent if not mark else f"{r.antecedent}{{{original}}}"
        subs.append(Substitution(
            start=start,
            end=end,
            original=original,
            replacement=replacement,
            pronoun_index=r.pronoun_index,
            antecedent_index=r.antecedent_index,
            score=r.score
        ))
    # Sort by start descending to avoid offset shifts on replacement
    subs.sort(key=lambda s: s.start, reverse=True)
    return subs
def apply_substitutions(text: str, substitutions: List[Substitution]) -> str:
    out = text
    for s in substitutions:
        out = out[:s.start] + s.replacement + out[s.end:]
    return out
# ---------------------------------------------------------
# PIPELINE ENTRY
# ---------------------------------------------------------
def resolve_and_substitute(text: str, mark: bool = False) -> Tuple[str, List[Substitution], List[Resolution]]:
    resolver = BatchAnaphoraResolver()
    result = resolver.resolve_document(text)
    doc = result["doc"]
    resolutions: List[Resolution] = result["resolutions"]
    substitutions = build_substitutions(doc, resolutions, mark=mark)
    output = apply_substitutions(text, substitutions)
    return output, substitutions, resolutions
