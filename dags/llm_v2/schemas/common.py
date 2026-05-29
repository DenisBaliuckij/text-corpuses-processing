from __future__ import annotations

from pydantic import BaseModel


class Sentence(BaseModel):
    id: int
    text: str


class Mention(BaseModel):
    text: str
    chunk_id: str | None = None


class Chunk(BaseModel):
    id: str
    text: str
    sentence_ids: list[int]


class RawTriplet(BaseModel):
    subject: str
    relation: str
    object: str
    chunk_id: str | None = None


class NormalizedTriplet(BaseModel):
    subject: str
    relation: str
    object: str
    chunk_id: str | None = None
    norm_subject: str = ""
    norm_relation: str = ""
    norm_object: str = ""
