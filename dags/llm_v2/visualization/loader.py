from __future__ import annotations

import json
from pathlib import Path

from ..schemas.clustered_graph import ClusteredGraph, MultiClusteredGraph


def load_graph(path: str | Path) -> ClusteredGraph:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return ClusteredGraph(**raw)


def load_multi_graph(path: str | Path) -> MultiClusteredGraph:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return MultiClusteredGraph(**raw)


def load_auto(path: str | Path) -> ClusteredGraph | MultiClusteredGraph:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if "methods" in raw:
        return MultiClusteredGraph(**raw)
    if "thresholds" in raw:
        from ..schemas.clustered_graph import ClusteredGraphMeta, MethodResult
        thresholds = raw["thresholds"]
        param_labels = [str(t) for t in thresholds]
        graphs = {str(t): ClusteredGraph(**g) for t, g in zip(thresholds, raw["graphs"].values())}
        meta = ClusteredGraphMeta(**raw["meta"])
        return MultiClusteredGraph(
            meta=meta,
            methods={"agglomerative": MethodResult(param_labels=param_labels, graphs=graphs)},
        )
    return ClusteredGraph(**raw)
