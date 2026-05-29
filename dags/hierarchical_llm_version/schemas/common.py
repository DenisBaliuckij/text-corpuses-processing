from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel


class Sentence(BaseModel):
    id: int
    text: str


class Chunk(BaseModel):
    id: str
    text: str
    sentence_ids: list[int]


class Mention(BaseModel):
    text: str
    chunk_id: str | None = None


class KeyConcept(BaseModel):
    name: str
    importance: Literal["core", "supporting", "peripheral"] = "supporting"


class ExtractedEntity(BaseModel):
    mention_text: str
    canonical_name: str
    entity_type: str = ""
    importance: float = 0.5
    rationale: str = ""
    chunk_id: Optional[str] = None


class ExtractedTriplet(BaseModel):
    subject: str
    predicate: str
    object: str
    importance: float = 0.5
    confidence: float = 1.0
    evidence: str = ""
    chunk_id: Optional[str] = None
