"""Build a ClusteredGraph (llm_v2-shaped) from the RawGraph.

Default behaviour (`clustering.enabled=False`): every canonical entity becomes
its own singleton cluster — entity resolution is treated as the merging step.

If `clustering.enabled=True`, run agglomerative clustering on entity
embeddings (cosine distance, average linkage) on top of the raw graph and
collapse parallel edges as in `llm_v2`.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

from ..config_schema import ClusteringConfig
from ..models.embedder import Embedder
from ..schemas.clustered_graph import ClusteredEdge, ClusteredGraph, ClusteredGraphMeta, ClusteredNode
from ..schemas.common import Mention
from ..schemas.raw_graph import RawGraph

logger = logging.getLogger(__name__)


def build_clustered_graph(
    raw: RawGraph,
    config: ClusteringConfig,
    embedder: Embedder | None = None,
) -> ClusteredGraph:
    if not config.enabled or len(raw.nodes) <= 1:
        return _promote(raw)
    if embedder is None:
        logger.warning("[5] clustering.enabled=True but no embedder provided; falling back to promotion")
        return _promote(raw)
    return _agglomerative(raw, config, embedder)


def _promote(raw: RawGraph) -> ClusteredGraph:
    """1:1 promotion — each raw node = singleton cluster."""
    nodes = [
        ClusteredNode(
            id=f"c{i}",
            label=n.label,
            members=[n.id],
            size=1,
            embedding=[],
            mentions=list(n.mentions),
            importance=n.importance,
        )
        for i, n in enumerate(raw.nodes)
    ]
    raw_to_cluster = {n.id: nodes[i].id for i, n in enumerate(raw.nodes)}
    edges = [
        ClusteredEdge(
            id=f"ce{i}",
            source=raw_to_cluster[e.source],
            target=raw_to_cluster[e.target],
            label=e.label,
            members=[e.id],
            size=1,
            embedding=[],
            importance=e.importance,
        )
        for i, e in enumerate(raw.edges)
    ]
    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={"clustering": "promoted_from_raw"},
        stats={"nodes": len(nodes), "edges": len(edges)},
    )
    return ClusteredGraph(meta=meta, nodes=nodes, edges=edges)


def _agglomerative(
    raw: RawGraph, config: ClusteringConfig, embedder: Embedder
) -> ClusteredGraph:
    from sklearn.cluster import AgglomerativeClustering
    from sklearn.metrics.pairwise import cosine_similarity

    labels = [n.label for n in raw.nodes]
    embs = embedder.encode_batch(labels)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embs = embs / norms

    model = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=config.threshold,
        linkage="average",
    )
    cluster_labels = model.fit_predict(embs)

    groups: dict[int, list[int]] = defaultdict(list)
    for i, cl in enumerate(cluster_labels):
        groups[int(cl)].append(i)

    raw_to_cluster: dict[str, str] = {}
    clustered_nodes: list[ClusteredNode] = []

    for ci, (_, members_idx) in enumerate(sorted(groups.items())):
        member_embs = embs[members_idx]
        centroid = member_embs.mean(axis=0).reshape(1, -1)
        best = int(np.argmax(cosine_similarity(member_embs, centroid).flatten()))
        label = labels[members_idx[best]]
        member_ids = [raw.nodes[i].id for i in members_idx]
        mentions: list[Mention] = []
        for i in members_idx:
            mentions.extend(raw.nodes[i].mentions)
        max_imp = max((raw.nodes[i].importance for i in members_idx), default=0.0)
        cid = f"c{ci}"
        clustered_nodes.append(ClusteredNode(
            id=cid,
            label=label,
            members=member_ids,
            size=len(member_ids),
            embedding=[],
            mentions=mentions,
            importance=max_imp,
        ))
        for mid in member_ids:
            raw_to_cluster[mid] = cid

    clustered_edges: list[ClusteredEdge] = []
    if config.cluster_relations:
        edge_groups: dict[tuple[str, str], list[int]] = defaultdict(list)
        for ei, e in enumerate(raw.edges):
            ns = raw_to_cluster.get(e.source, e.source)
            nt = raw_to_cluster.get(e.target, e.target)
            edge_groups[(ns, nt)].append(ei)

        for (src, tgt), idxs in edge_groups.items():
            if src == tgt:
                continue
            edge_labels = [raw.edges[i].label for i in idxs]
            member_ids = [raw.edges[i].id for i in idxs]
            if len(edge_labels) == 1:
                lbl = edge_labels[0]
            else:
                eembs = embedder.encode_batch(edge_labels)
                en = np.linalg.norm(eembs, axis=1, keepdims=True)
                en[en == 0] = 1.0
                eembs = eembs / en
                cent = eembs.mean(axis=0).reshape(1, -1)
                best = int(np.argmax(cosine_similarity(eembs, cent).flatten()))
                lbl = edge_labels[best]
            max_imp = max((raw.edges[i].importance for i in idxs), default=0.0)
            clustered_edges.append(ClusteredEdge(
                id=f"ce{len(clustered_edges)}",
                source=src,
                target=tgt,
                label=lbl,
                members=member_ids,
                size=len(member_ids),
                embedding=[],
                importance=max_imp,
            ))
    else:
        for e in raw.edges:
            ns = raw_to_cluster.get(e.source, e.source)
            nt = raw_to_cluster.get(e.target, e.target)
            if ns == nt:
                continue
            clustered_edges.append(ClusteredEdge(
                id=e.id,
                source=ns,
                target=nt,
                label=e.label,
                members=[],
                size=1,
                embedding=[],
                importance=e.importance,
            ))

    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={
            "clustering": "agglomerative",
            "threshold": config.threshold,
        },
        stats={"nodes": len(clustered_nodes), "edges": len(clustered_edges)},
    )
    return ClusteredGraph(meta=meta, nodes=clustered_nodes, edges=clustered_edges)
