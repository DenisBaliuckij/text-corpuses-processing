from __future__ import annotations

from pydantic import BaseModel

from .common import Mention, Chunk


class RawGraphMeta(BaseModel):
    created_at: str
    source_text: str
    pipeline_version: str = "0.2.0"
    config_summary: dict = {}
    stats: dict = {}


class RawNode(BaseModel):
    id: str
    label: str
    mentions: list[Mention] = []


class RawEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str
    weight: int = 1
    mentions: list[Mention] = []


class RawGraph(BaseModel):
    meta: RawGraphMeta
    chunks: list[Chunk] = []
    nodes: list[RawNode] = []
    edges: list[RawEdge] = []
