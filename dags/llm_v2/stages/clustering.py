from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.metrics.pairwise import cosine_similarity

from ..config_schema import ClusteringConfig, PipelineConfig
from ..models.embedder import Embedder
from ..models.llm_client import LLMClient
from ..schemas.common import Mention
from ..schemas.clustered_graph import (
    ClusteredEdge,
    ClusteredGraph,
    ClusteredGraphMeta,
    ClusteredNode,
    MethodResult,
    MultiClusteredGraph,
)
from ..schemas.raw_graph import RawGraph


def _compute_embeddings(raw: RawGraph, embedder: Embedder) -> np.ndarray:
    chunk_map = {c.id: c.text for c in raw.chunks}

    texts: list[str] = []
    text_idx: dict[str, int] = {}
    per_node_idxs: list[list[int]] = []

    def _intern(s: str) -> int:
        i = text_idx.get(s)
        if i is None:
            i = len(texts)
            text_idx[s] = i
            texts.append(s)
        return i

    for node in raw.nodes:
        idxs: list[int] = []
        for m in node.mentions:
            if m.chunk_id and m.chunk_id in chunk_map:
                idxs.append(_intern(f"{m.text}: {chunk_map[m.chunk_id]}"))
        if not idxs:
            idxs.append(_intern(node.label))
        per_node_idxs.append(idxs)

    if not per_node_idxs:
        return np.empty((0, embedder.dim))

    all_embs = embedder.encode_batch(texts)

    node_embs = np.stack([all_embs[idxs].mean(axis=0) for idxs in per_node_idxs])
    norms = np.linalg.norm(node_embs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return node_embs / norms


def _name_cluster(member_labels: list[str], llm: LLMClient, prompt_template: str) -> str:
    entities_str = ", ".join(member_labels)
    prompt = prompt_template.replace("{entities}", entities_str)
    result = llm.generate(prompt).strip()
    name = result.split("\n")[0].strip().strip('"').strip("'")
    return name if name else member_labels[0]


def cluster_graph(
    raw: RawGraph,
    embedder: Embedder,
    config: PipelineConfig,
    *,
    llm: LLMClient | None = None,
    prompt_template: str | None = None,
    precomputed_node_embs: np.ndarray | None = None,
    override_threshold: float | None = None,
    override_method: str | None = None,
    override_n_clusters: int | None = None,
    override_min_cluster_size: int | None = None,
    override_min_samples: int | None = None,
) -> ClusteredGraph:
    cc = config.clustering

    node_labels = [n.label for n in raw.nodes]
    node_ids = [n.id for n in raw.nodes]

    if precomputed_node_embs is not None:
        node_embs = precomputed_node_embs
    else:
        node_embs = _compute_embeddings(raw, embedder)

    effective_threshold = override_threshold if override_threshold is not None else cc.threshold
    node_cluster_labels = _run_clustering(
        node_embs, cc,
        override_threshold=effective_threshold,
        override_method=override_method,
        override_n_clusters=override_n_clusters,
        override_min_cluster_size=override_min_cluster_size,
        override_min_samples=override_min_samples,
    )

    cluster_groups: dict[int, list[int]] = defaultdict(list)
    for idx, cl in enumerate(node_cluster_labels):
        cluster_groups[cl].append(idx)

    clustered_nodes: list[ClusteredNode] = []
    old_to_new: dict[str, str] = {}

    for ci, (_, members_idx) in enumerate(sorted(cluster_groups.items())):
        member_ids = [node_ids[i] for i in members_idx]
        member_embs = node_embs[members_idx]

        centroid = member_embs.mean(axis=0).reshape(1, -1)

        if cc.llm_naming and llm is not None and prompt_template is not None and len(members_idx) > 1:
            member_labels = [node_labels[i] for i in members_idx]
            label = _name_cluster(member_labels, llm, prompt_template)
        else:
            best = int(np.argmax(cosine_similarity(member_embs, centroid).flatten()))
            label = node_labels[members_idx[best]]

        mentions: list[Mention] = []
        for i in members_idx:
            mentions.extend(raw.nodes[i].mentions)

        cid = f"c{ci}"
        clustered_nodes.append(
            ClusteredNode(
                id=cid,
                label=label,
                members=member_ids,
                size=len(member_ids),
                embedding=centroid.flatten().tolist() if cc.include_embeddings else [],
                mentions=mentions,
            )
        )
        for mid in member_ids:
            old_to_new[mid] = cid

    clustered_edges: list[ClusteredEdge] = []

    if cc.cluster_relations:
        edge_groups: dict[tuple[str, str], list[int]] = defaultdict(list)
        for ei, e in enumerate(raw.edges):
            new_src = old_to_new.get(e.source, e.source)
            new_tgt = old_to_new.get(e.target, e.target)
            edge_groups[(new_src, new_tgt)].append(ei)

        for (src, tgt), edge_idxs in edge_groups.items():
            if src == tgt:
                continue

            edge_labels = [raw.edges[i].label for i in edge_idxs]
            edge_member_ids = [raw.edges[i].id for i in edge_idxs]

            if len(edge_labels) == 1:
                label = edge_labels[0]
                emb: list[float] = []
            else:
                embs = embedder.encode_batch(edge_labels)
                enorms = np.linalg.norm(embs, axis=1, keepdims=True)
                enorms[enorms == 0] = 1.0
                embs = embs / enorms
                cent = embs.mean(axis=0).reshape(1, -1)
                best = int(np.argmax(cosine_similarity(embs, cent).flatten()))
                label = edge_labels[best]
                emb = cent.flatten().tolist() if cc.include_embeddings else []

            clustered_edges.append(
                ClusteredEdge(
                    id=f"ce{len(clustered_edges)}",
                    source=src,
                    target=tgt,
                    label=label,
                    members=edge_member_ids,
                    size=len(edge_member_ids),
                    embedding=emb,
                )
            )
    else:
        for e in raw.edges:
            new_src = old_to_new.get(e.source, e.source)
            new_tgt = old_to_new.get(e.target, e.target)
            if new_src == new_tgt:
                continue
            clustered_edges.append(
                ClusteredEdge(
                    id=e.id,
                    source=new_src,
                    target=new_tgt,
                    label=e.label,
                    members=[],
                    size=1,
                )
            )

    method_used = override_method or cc.method
    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={
            "clustering_method": method_used,
            "threshold": effective_threshold,
            "n_clusters": override_n_clusters or cc.n_clusters,
            "raw_nodes": len(raw.nodes),
            "raw_edges": len(raw.edges),
        },
        stats={
            "nodes": len(clustered_nodes),
            "edges": len(clustered_edges),
        },
    )

    return ClusteredGraph(meta=meta, nodes=clustered_nodes, edges=clustered_edges)


def cluster_graph_multi(
    raw: RawGraph,
    embedder: Embedder,
    config: PipelineConfig,
    *,
    llm: LLMClient | None = None,
    prompt_template: str | None = None,
) -> MultiClusteredGraph:
    """Backward-compatible: agglomerative-only multi-threshold."""
    cc = config.clustering
    thresholds = cc.threshold_values

    node_embs = _compute_embeddings(raw, embedder)

    graphs: dict[str, ClusteredGraph] = {}
    param_labels: list[str] = []
    for t in thresholds:
        label = f"{t:.3f}"
        param_labels.append(label)
        g = cluster_graph(
            raw, embedder, config,
            llm=llm,
            prompt_template=prompt_template,
            precomputed_node_embs=node_embs,
            override_threshold=t,
        )
        graphs[label] = g

    mid_label = param_labels[len(param_labels) // 2]
    mid_graph = graphs[mid_label]

    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={
            "clustering_method": cc.method,
            "threshold_min": cc.threshold_min,
            "threshold_max": cc.threshold_max,
            "threshold_steps": cc.threshold_steps,
            "raw_nodes": len(raw.nodes),
            "raw_edges": len(raw.edges),
        },
        stats=mid_graph.meta.stats,
    )

    return MultiClusteredGraph(
        meta=meta,
        methods={
            "agglomerative": MethodResult(param_labels=param_labels, graphs=graphs),
        },
    )


def cluster_graph_all_methods(
    raw: RawGraph,
    embedder: Embedder,
    config: PipelineConfig,
    *,
    llm: LLMClient | None = None,
    prompt_template: str | None = None,
) -> MultiClusteredGraph:
    """Run all methods (agglomerative, kmeans, hdbscan) with parameter sweeps."""
    cc = config.clustering
    node_embs = _compute_embeddings(raw, embedder)

    methods: dict[str, MethodResult] = {}

    agg_labels: list[str] = []
    agg_graphs: dict[str, ClusteredGraph] = {}
    for t in cc.threshold_values:
        label = f"{t:.3f}"
        agg_labels.append(label)
        agg_graphs[label] = cluster_graph(
            raw, embedder, config,
            llm=llm,
            prompt_template=prompt_template,
            precomputed_node_embs=node_embs,
            override_threshold=t,
            override_method="agglomerative",
        )
    methods["agglomerative"] = MethodResult(param_labels=agg_labels, graphs=agg_graphs)

    km_labels: list[str] = []
    km_graphs: dict[str, ClusteredGraph] = {}
    for k in cc.k_values:
        label = f"k={k}"
        km_labels.append(label)
        km_graphs[label] = cluster_graph(
            raw, embedder, config,
            llm=llm,
            prompt_template=prompt_template,
            precomputed_node_embs=node_embs,
            override_method="kmeans",
            override_n_clusters=k,
        )
    methods["kmeans"] = MethodResult(param_labels=km_labels, graphs=km_graphs)

    hdb_labels: list[str] = []
    hdb_graphs: dict[str, ClusteredGraph] = {}
    for mcs, ms in cc.hdbscan_param_grid:
        label = f"mcs={mcs},ms={ms}"
        hdb_labels.append(label)
        hdb_graphs[label] = cluster_graph(
            raw, embedder, config,
            llm=llm,
            prompt_template=prompt_template,
            precomputed_node_embs=node_embs,
            override_method="hdbscan",
            override_min_cluster_size=mcs,
            override_min_samples=ms,
        )
    methods["hdbscan"] = MethodResult(param_labels=hdb_labels, graphs=hdb_graphs)

    mid_label = agg_labels[len(agg_labels) // 2]
    mid_graph = agg_graphs[mid_label]

    meta = ClusteredGraphMeta(
        created_at=datetime.now(timezone.utc).isoformat(),
        source_text=raw.meta.source_text,
        config_summary={
            "multi_method": True,
            "threshold_min": cc.threshold_min,
            "threshold_max": cc.threshold_max,
            "threshold_steps": cc.threshold_steps,
            "k_min": cc.k_min,
            "k_max": cc.k_max,
            "k_step": cc.k_step,
            "hdbscan_min_cluster_sizes": cc.hdbscan_min_cluster_sizes,
            "hdbscan_min_samples": cc.hdbscan_min_samples,
            "raw_nodes": len(raw.nodes),
            "raw_edges": len(raw.edges),
        },
        stats=mid_graph.meta.stats,
    )

    return MultiClusteredGraph(meta=meta, methods=methods)


def _run_clustering(
    embeddings: np.ndarray,
    cc: ClusteringConfig,
    *,
    override_threshold: float | None = None,
    override_method: str | None = None,
    override_n_clusters: int | None = None,
    override_min_cluster_size: int | None = None,
    override_min_samples: int | None = None,
) -> list[int]:
    n = embeddings.shape[0]
    if n <= 1:
        return list(range(n))

    method = override_method or cc.method

    if method == "agglomerative":
        threshold = override_threshold if override_threshold is not None else cc.threshold
        n_clusters = override_n_clusters or cc.n_clusters
        if n_clusters:
            model = AgglomerativeClustering(n_clusters=n_clusters, linkage="average")
        else:
            model = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=threshold,
                linkage="average",
            )
        return model.fit_predict(embeddings).tolist()

    elif method == "kmeans":
        k = override_n_clusters or cc.n_clusters or max(2, int(np.sqrt(n / 2)))
        k = min(k, n)
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        return model.fit_predict(embeddings).tolist()

    elif method == "hdbscan":
        import hdbscan

        mcs = override_min_cluster_size or cc.min_cluster_size
        ms = override_min_samples or (cc.min_samples if cc.min_samples else mcs)
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=mcs,
            min_samples=ms,
            metric="euclidean",
            cluster_selection_method="eom",
        )
        labels = clusterer.fit_predict(embeddings)
        next_label = int(labels.max()) + 1 if len(labels) > 0 else 0
        result = []
        for lb in labels:
            if lb == -1:
                result.append(next_label)
                next_label += 1
            else:
                result.append(int(lb))
        return result

    else:
        raise ValueError(f"Unknown clustering method: {method}")
