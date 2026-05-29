from __future__ import annotations

import logging
from datetime import datetime, timezone
from itertools import product
from typing import Iterable

from ..config_schema import ImportanceFilteringConfig, ImportanceFilteringMultiConfig
from ..schemas.clustered_graph import (
    ClusteredEdge,
    ClusteredGraph,
    ClusteredGraphMeta,
    ClusteredNode,
    MethodResult,
    MultiClusteredGraph,
)
from ..schemas.raw_graph import RawEdge, RawGraph, RawNode

logger = logging.getLogger(__name__)


def filter_raw_by_importance(
    raw: RawGraph,
    entity_threshold: float,
    relation_threshold: float,
    drop_orphan_nodes: bool,
) -> tuple[list[RawNode], list[RawEdge]]:
    surviving_node_ids = {n.id for n in raw.nodes if n.importance >= entity_threshold}
    edges = [
        e for e in raw.edges
        if e.source in surviving_node_ids
        and e.target in surviving_node_ids
        and e.importance >= relation_threshold
    ]
    if drop_orphan_nodes:
        connected: set[str] = set()
        for e in edges:
            connected.add(e.source)
            connected.add(e.target)
        surviving_node_ids &= connected
    nodes = [n for n in raw.nodes if n.id in surviving_node_ids]
    return nodes, edges


def build_clustered_from_filter(
    raw: RawGraph,
    entity_threshold: float,
    relation_threshold: float,
    drop_orphan_nodes: bool,
) -> ClusteredGraph:
    """Filter + 1:1 promotion to ClusteredGraph."""
    raw_nodes, raw_edges = filter_raw_by_importance(
        raw, entity_threshold, relation_threshold, drop_orphan_nodes,
    )
    return _promote(raw, raw_nodes, raw_edges, {
        "filter": "importance",
        "entity_threshold": entity_threshold,
        "relation_threshold": relation_threshold,
        "drop_orphan_nodes": drop_orphan_nodes,
    })


def build_multi_clustered(
    raw: RawGraph, multi_cfg: ImportanceFilteringMultiConfig,
) -> tuple[MultiClusteredGraph, str]:
    pairs = list(_iter_pairs(multi_cfg))
    graphs: dict[str, ClusteredGraph] = {}
    param_labels: list[str] = []

    for t_e, t_r in pairs:
        label = f"e={t_e:.2f},r={t_r:.2f}"
        param_labels.append(label)
        graphs[label] = build_clustered_from_filter(
            raw, t_e, t_r, multi_cfg.drop_orphan_nodes,
        )

    default_label = param_labels[len(param_labels) // 2]
    default_graph = graphs[default_label]

    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={
            "filter": "importance",
            "sweep_mode": multi_cfg.sweep_mode,
            "entity_thresholds": list(multi_cfg.entity_thresholds),
            "relation_thresholds": list(multi_cfg.relation_thresholds),
            "drop_orphan_nodes": multi_cfg.drop_orphan_nodes,
            "raw_nodes": len(raw.nodes),
            "raw_edges": len(raw.edges),
        },
        stats=default_graph.meta.stats,
    )
    multi = MultiClusteredGraph(
        meta=meta,
        methods={"importance_filter": MethodResult(
            param_labels=param_labels, graphs=graphs,
        )},
    )
    return multi, default_label


def _iter_pairs(cfg: ImportanceFilteringMultiConfig) -> Iterable[tuple[float, float]]:
    if cfg.sweep_mode == "cross_product":
        for t_e, t_r in product(cfg.entity_thresholds, cfg.relation_thresholds):
            yield float(t_e), float(t_r)
    elif cfg.sweep_mode == "paired":
        for t_e, t_r in zip(cfg.entity_thresholds, cfg.relation_thresholds):
            yield float(t_e), float(t_r)
    else:  # validated upstream, but be defensive
        raise ValueError(f"unknown sweep_mode: {cfg.sweep_mode}")


def _promote(
    raw: RawGraph,
    raw_nodes: list[RawNode],
    raw_edges: list[RawEdge],
    config_summary: dict,
) -> ClusteredGraph:
    nodes: list[ClusteredNode] = []
    raw_to_cluster: dict[str, str] = {}
    for i, n in enumerate(raw_nodes):
        cid = f"c{i}"
        nodes.append(ClusteredNode(
            id=cid,
            label=n.label,
            members=[n.id],
            size=1,
            embedding=[],
            mentions=list(n.mentions),
            importance=n.importance,
        ))
        raw_to_cluster[n.id] = cid

    edges: list[ClusteredEdge] = []
    for i, e in enumerate(raw_edges):
        edges.append(ClusteredEdge(
            id=f"ce{i}",
            source=raw_to_cluster[e.source],
            target=raw_to_cluster[e.target],
            label=e.label,
            members=[e.id],
            size=1,
            embedding=[],
            importance=e.importance,
        ))

    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary=config_summary,
        stats={"nodes": len(nodes), "edges": len(edges)},
    )
    return ClusteredGraph(meta=meta, nodes=nodes, edges=edges)


def build_clustered_unfiltered(raw: RawGraph) -> ClusteredGraph:
    return _promote(raw, list(raw.nodes), list(raw.edges), {"filter": "off"})
