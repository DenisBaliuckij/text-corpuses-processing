"""Step 1.2 — group nodes for next-level aggregation."""
from __future__ import annotations

import logging

import numpy as np

from ..config_schema import GroupingConfig
from ..models.embedder import Embedder
from ..schemas.hierarchy import HierarchyNode

logger = logging.getLogger(__name__)


def group_nodes(
    nodes: list[HierarchyNode],
    config: GroupingConfig,
    embedder: Embedder | None = None,
) -> list[list[int]]:
    if not nodes:
        return []
    if config.method == "sequential":
        return _group_sequential(len(nodes), config.group_size)
    if config.method == "semantic":
        if embedder is None:
            raise ValueError("semantic grouping requires an Embedder")
        return _group_semantic(nodes, embedder, config)
    raise ValueError(f"unknown grouping method: {config.method}")


def _group_sequential(n: int, group_size: int) -> list[list[int]]:
    if group_size < 1:
        raise ValueError("group_size must be >= 1")
    return [list(range(i, min(i + group_size, n))) for i in range(0, n, group_size)]


def _group_semantic(
    nodes: list[HierarchyNode], embedder: Embedder, config: GroupingConfig
) -> list[list[int]]:
    import hdbscan

    texts = [n.summary or n.topic or "" for n in nodes]
    embs = embedder.encode_batch(texts)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embs = embs / norms

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=max(2, config.min_cluster_size),
        min_samples=max(1, config.min_samples),
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(embs)

    groups: dict[int, list[int]] = {}
    next_label = (int(labels.max()) + 1) if (labels >= 0).any() else 0
    for i, lb in enumerate(labels):
        if lb == -1:
            groups.setdefault(next_label, []).append(i)
            next_label += 1
        else:
            groups.setdefault(int(lb), []).append(i)

    return [sorted(g) for g in groups.values()]
