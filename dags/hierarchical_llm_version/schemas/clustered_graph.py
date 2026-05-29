from __future__ import annotations

from pydantic import BaseModel

from .common import Mention


class ClusteredGraphMeta(BaseModel):
    created_at: str
    source_text: str
    pipeline_version: str = "hierarchical-0.1.0"
    config_summary: dict = {}
    stats: dict = {}


class ClusteredNode(BaseModel):
    id: str
    label: str
    members: list[str] = []
    size: int = 1
    embedding: list[float] = []
    mentions: list[Mention] = []
    importance: float = 0.0


class ClusteredEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str
    members: list[str] = []
    size: int = 1
    embedding: list[float] = []
    importance: float = 0.0


class ClusteredGraph(BaseModel):
    meta: ClusteredGraphMeta
    nodes: list[ClusteredNode] = []
    edges: list[ClusteredEdge] = []


class MethodResult(BaseModel):
    param_labels: list[str]
    graphs: dict[str, ClusteredGraph]


class MultiClusteredGraph(BaseModel):
    meta: ClusteredGraphMeta
    methods: dict[str, MethodResult]
