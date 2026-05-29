from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from .common import KeyConcept


class HierarchyNode(BaseModel):
    id: str
    level: int
    summary: str = ""
    topic: str = ""
    key_concepts: list[KeyConcept] = []
    subtopics: list[str] = []

    parent_id: Optional[str] = None
    children_ids: list[str] = []

    chunk_id: Optional[str] = None

    source_chunk_ids: list[str] = []

    input_tokens: int = 0
    output_tokens: int = 0


class HierarchyTree(BaseModel):
    meta: dict = {}
    root_id: str
    nodes: dict[str, HierarchyNode] = {}
    levels: dict[int, list[str]] = {}

    def get(self, node_id: str) -> HierarchyNode:
        return self.nodes[node_id]

    def path_to_root(self, node_id: str) -> list[str]:
        """[node_id, parent_id, ..., root_id]."""
        path: list[str] = []
        cur: Optional[str] = node_id
        while cur is not None and cur in self.nodes:
            path.append(cur)
            cur = self.nodes[cur].parent_id
        return path
