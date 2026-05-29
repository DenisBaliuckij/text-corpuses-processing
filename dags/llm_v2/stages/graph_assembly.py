from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from ..config_schema import PipelineConfig
from ..schemas.common import Chunk, Mention, NormalizedTriplet
from ..schemas.raw_graph import RawEdge, RawGraph, RawGraphMeta, RawNode


def assemble_graph(
    triplets: list[NormalizedTriplet],
    chunks: list[Chunk],
    source_text: str,
    config: PipelineConfig,
) -> RawGraph:
    node_map: dict[str, RawNode] = {}
    node_mentions: dict[str, list[Mention]] = defaultdict(list)

    edge_map: dict[tuple[str, str, str], RawEdge] = {}
    edge_counts: dict[tuple[str, str, str], int] = defaultdict(int)

    def _get_or_create_node(norm_label: str, raw_text: str, chunk_id: str | None) -> str:
        if norm_label not in node_map:
            node_map[norm_label] = RawNode(
                id=f"n{len(node_map)}",
                label=norm_label,
                mentions=[],
            )
        node_mentions[norm_label].append(Mention(text=raw_text, chunk_id=chunk_id))
        return node_map[norm_label].id

    for t in triplets:
        src_id = _get_or_create_node(t.norm_subject, t.subject, t.chunk_id)
        tgt_id = _get_or_create_node(t.norm_object, t.object, t.chunk_id)

        ekey = (src_id, tgt_id, t.norm_relation)
        edge_counts[ekey] += 1

        if ekey not in edge_map:
            edge_map[ekey] = RawEdge(
                id=f"e{len(edge_map)}",
                source=src_id,
                target=tgt_id,
                label=t.norm_relation,
                weight=1,
                mentions=[Mention(text=t.relation, chunk_id=t.chunk_id)],
            )
        else:
            edge_map[ekey].mentions.append(
                Mention(text=t.relation, chunk_id=t.chunk_id)
            )

    for label, node in node_map.items():
        node.mentions = node_mentions[label]

    for ekey, edge in edge_map.items():
        edge.weight = edge_counts[ekey]

    n_triplets = len(triplets)
    n_nodes = len(node_map)
    n_edges = len(edge_map)

    meta = RawGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=source_text[:500],
        config_summary={
            "llm_model": config.llm.model_name,
            "embedding_model": config.embedding.model_name,
            "normalization": config.normalization.language if config.normalization.enabled else "off",
        },
        stats={
            "total_triplets": n_triplets,
            "nodes": n_nodes,
            "edges": n_edges,
        },
    )

    return RawGraph(
        meta=meta,
        chunks=chunks,
        nodes=list(node_map.values()),
        edges=list(edge_map.values()),
    )
