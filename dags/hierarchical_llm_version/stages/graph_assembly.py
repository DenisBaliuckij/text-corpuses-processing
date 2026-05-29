"""Build a llm_v2-shaped RawGraph from extracted entities + triplets."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from ..config_schema import PipelineConfig
from ..schemas.common import Chunk, ExtractedEntity, ExtractedTriplet, Mention
from ..schemas.raw_graph import RawEdge, RawGraph, RawGraphMeta, RawNode


def assemble_raw_graph(
    entities: list[ExtractedEntity],
    triplets: list[ExtractedTriplet],
    chunks: list[Chunk],
    source_text: str,
    config: PipelineConfig,
) -> RawGraph:
    """Group entities/triplets by canonical_name and produce a RawGraph.

    Node importance = max over (entity importances with this canonical_name,
    triplet importances of incident edges). This makes a node "survive" the
    importance filter if any of its mentions OR any of its participations
    were judged important.

    Edge importance = max over triplets that produced this edge.
    """
    node_map: dict[str, RawNode] = {}
    node_mentions: dict[str, list[Mention]] = defaultdict(list)
    node_importance: dict[str, float] = defaultdict(float)

    def _ensure_node(canon: str, mention_text: str, chunk_id: str | None,
                     importance: float = 0.0) -> str:
        canon = canon.strip()
        if not canon:
            return ""
        if canon not in node_map:
            node_map[canon] = RawNode(
                id=f"n{len(node_map)}",
                label=canon,
                mentions=[],
            )
        node_mentions[canon].append(Mention(text=mention_text, chunk_id=chunk_id))
        if importance > node_importance[canon]:
            node_importance[canon] = importance
        return node_map[canon].id

    # entities first — gives nodes their original mention texts + importance
    for e in entities:
        _ensure_node(
            e.canonical_name,
            e.mention_text or e.canonical_name,
            e.chunk_id,
            importance=e.importance,
        )

    edge_map: dict[tuple[str, str, str], RawEdge] = {}
    edge_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    edge_importance: dict[tuple[str, str, str], float] = defaultdict(float)

    for t in triplets:
        rel = t.predicate.strip()
        if not rel:
            continue
        sid = _ensure_node(t.subject, t.subject, t.chunk_id, importance=t.importance)
        oid = _ensure_node(t.object, t.object, t.chunk_id, importance=t.importance)
        if not (sid and oid):
            continue

        ekey = (sid, oid, rel)
        edge_counts[ekey] += 1
        if t.importance > edge_importance[ekey]:
            edge_importance[ekey] = t.importance

        ev = t.evidence or rel
        if ekey not in edge_map:
            edge_map[ekey] = RawEdge(
                id=f"e{len(edge_map)}",
                source=sid,
                target=oid,
                label=rel,
                weight=1,
                mentions=[Mention(text=ev, chunk_id=t.chunk_id)],
            )
        else:
            edge_map[ekey].mentions.append(Mention(text=ev, chunk_id=t.chunk_id))

    for label, node in node_map.items():
        node.mentions = node_mentions[label]
        node.importance = node_importance[label]
    for ekey, edge in edge_map.items():
        edge.weight = edge_counts[ekey]
        edge.importance = edge_importance[ekey]

    meta = RawGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=source_text[:500],
        config_summary={
            "pipeline": "hierarchical_llm_version",
            "extraction_model": config.llm.extraction.model_id,
            "embedding_model": config.embedding.model_name,
            "language": config.preprocessing.language,
            "grouping": config.pass1.grouping.method,
            "entity_resolution": config.entity_resolution.enabled,
        },
        stats={
            "total_triplets": len(triplets),
            "nodes": len(node_map),
            "edges": len(edge_map),
        },
    )

    return RawGraph(
        meta=meta,
        chunks=chunks,
        nodes=list(node_map.values()),
        edges=list(edge_map.values()),
    )
