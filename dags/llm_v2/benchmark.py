from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import numpy as np

from .models.embedder import Embedder
from .schemas.clustered_graph import ClusteredGraph



def _find_occurrences(haystack: str, needle: str) -> list[int]:
    """Case-insensitive substring search; returns all start indices."""
    if not needle:
        return []
    h = haystack.lower()
    n = needle.lower()
    out: list[int] = []
    start = 0
    while True:
        idx = h.find(n, start)
        if idx < 0:
            break
        out.append(idx)
        start = idx + max(1, len(n))
    return out


def _window(text: str, lo: int, hi: int, half: int) -> str:
    return text[max(0, lo - half) : min(len(text), hi + half)]


def _l2_normalize(arr: np.ndarray) -> np.ndarray:
    if arr.size == 0:
        return arr
    if arr.ndim == 1:
        n = float(np.linalg.norm(arr))
        return arr / n if n > 0 else arr
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return arr / norms



def embed_node(
    label: str,
    mention_texts: Iterable[str],
    source_text: str,
    embedder: Embedder,
    window_chars: int = 300,
    max_occurrences: int = 8,
) -> np.ndarray:
    seen: set[str] = set()
    candidates: list[str] = []
    for cand in [label, *mention_texts]:
        c = (cand or "").strip()
        if c and c not in seen:
            seen.add(c)
            candidates.append(c)

    snippets: list[str] = []
    for cand in candidates:
        for start in _find_occurrences(source_text, cand):
            ctx = _window(source_text, start, start + len(cand), window_chars)
            snippets.append(f"{label}: {ctx}")
            if len(snippets) >= max_occurrences:
                break
        if len(snippets) >= max_occurrences:
            break

    if not snippets:
        return embedder.encode(label)
    return embedder.encode_batch(snippets).mean(axis=0)


def embed_edge(
    src_label: str,
    relation: str,
    tgt_label: str,
    source_text: str,
    embedder: Embedder,
    window_chars: int = 400,
    max_occurrences: int = 6,
) -> np.ndarray:
    sentence = f"{src_label} {relation} {tgt_label}".strip()

    src_occ = _find_occurrences(source_text, src_label)
    tgt_occ = _find_occurrences(source_text, tgt_label)

    snippets: list[str] = []
    for s in src_occ:
        for t in tgt_occ:
            if abs(s - t) <= window_chars * 2:
                lo, hi = min(s, t), max(s, t) + max(len(src_label), len(tgt_label))
                ctx = _window(source_text, lo, hi, window_chars // 2)
                snippets.append(f"{sentence}: {ctx}")
                if len(snippets) >= max_occurrences:
                    break
        if len(snippets) >= max_occurrences:
            break

    if not snippets:
        per_side = max(1, max_occurrences // 2)
        for occ_list, lbl in ((src_occ, src_label), (tgt_occ, tgt_label)):
            for start in occ_list[:per_side]:
                ctx = _window(source_text, start, start + len(lbl), window_chars)
                snippets.append(f"{sentence}: {ctx}")

    if not snippets:
        return embedder.encode(sentence)
    return embedder.encode_batch(snippets).mean(axis=0)



def embed_graph_nodes(
    graph: ClusteredGraph,
    source_text: str,
    embedder: Embedder,
    window_chars: int = 300,
) -> np.ndarray:
    embs: list[np.ndarray] = []
    for node in graph.nodes:
        mention_texts = [m.text for m in node.mentions]
        embs.append(
            embed_node(node.label, mention_texts, source_text, embedder, window_chars)
        )
    if not embs:
        return np.empty((0, embedder.dim))
    return _l2_normalize(np.stack(embs))


def embed_graph_edges(
    graph: ClusteredGraph,
    source_text: str,
    embedder: Embedder,
    window_chars: int = 400,
) -> np.ndarray:
    nodes_by_id = {n.id: n for n in graph.nodes}
    embs: list[np.ndarray] = []
    for edge in graph.edges:
        src = nodes_by_id.get(edge.source)
        tgt = nodes_by_id.get(edge.target)
        if src is None or tgt is None:
            embs.append(embedder.encode(edge.label))
            continue
        embs.append(
            embed_edge(
                src.label, edge.label, tgt.label,
                source_text, embedder, window_chars,
            )
        )
    if not embs:
        return np.empty((0, embedder.dim))
    return _l2_normalize(np.stack(embs))



@dataclass
class SideMetrics:
    precision_sim: float
    recall_sim: float
    f1_sim: float
    precision_dist: float
    recall_dist: float
    pred_count: int
    gt_count: int
    pred_to_gt: list[tuple[int, float]] = field(default_factory=list)
    gt_to_pred: list[tuple[int, float]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "precision_sim": self.precision_sim,
            "recall_sim": self.recall_sim,
            "f1_sim": self.f1_sim,
            "precision_dist": self.precision_dist,
            "recall_dist": self.recall_dist,
            "pred_count": self.pred_count,
            "gt_count": self.gt_count,
        }


def _side_metrics(pred_embs: np.ndarray, gt_embs: np.ndarray) -> SideMetrics:
    n_pred, n_gt = len(pred_embs), len(gt_embs)
    if n_pred == 0 or n_gt == 0:
        return SideMetrics(
            precision_sim=0.0, recall_sim=0.0, f1_sim=0.0,
            precision_dist=1.0, recall_dist=1.0,
            pred_count=n_pred, gt_count=n_gt,
        )

    sim = pred_embs @ gt_embs.T

    p_best_gt = sim.argmax(axis=1)
    p_best_sim = sim.max(axis=1)
    g_best_pred = sim.argmax(axis=0)
    g_best_sim = sim.max(axis=0)

    p_sim = float(p_best_sim.mean())
    r_sim = float(g_best_sim.mean())
    f1 = (2 * p_sim * r_sim / (p_sim + r_sim)) if (p_sim + r_sim) > 0 else 0.0

    return SideMetrics(
        precision_sim=p_sim,
        recall_sim=r_sim,
        f1_sim=f1,
        precision_dist=1.0 - p_sim,
        recall_dist=1.0 - r_sim,
        pred_count=n_pred,
        gt_count=n_gt,
        pred_to_gt=[(int(j), float(s)) for j, s in zip(p_best_gt, p_best_sim)],
        gt_to_pred=[(int(i), float(s)) for i, s in zip(g_best_pred, g_best_sim)],
    )


@dataclass
class GraphMetrics:
    nodes: SideMetrics
    edges: SideMetrics

    def summary(self) -> dict:
        return {"nodes": self.nodes.as_dict(), "edges": self.edges.as_dict()}


def evaluate_graph(
    pred: ClusteredGraph,
    gt: ClusteredGraph,
    source_text: str,
    embedder: Embedder,
    node_window: int = 300,
    edge_window: int = 400,
) -> GraphMetrics:
    pred_node_embs = embed_graph_nodes(pred, source_text, embedder, node_window)
    gt_node_embs = embed_graph_nodes(gt, source_text, embedder, node_window)
    nodes = _side_metrics(pred_node_embs, gt_node_embs)

    pred_edge_embs = embed_graph_edges(pred, source_text, embedder, edge_window)
    gt_edge_embs = embed_graph_edges(gt, source_text, embedder, edge_window)
    edges = _side_metrics(pred_edge_embs, gt_edge_embs)

    return GraphMetrics(nodes=nodes, edges=edges)



def print_metrics(metrics: GraphMetrics) -> None:
    s = metrics.summary()
    n, e = s["nodes"], s["edges"]
    print(f"Nodes (pred={n['pred_count']}, gt={n['gt_count']}):")
    print(f"  precision  sim={n['precision_sim']:.4f}   dist={n['precision_dist']:.4f}")
    print(f"  recall     sim={n['recall_sim']:.4f}   dist={n['recall_dist']:.4f}")
    print(f"  F1         sim={n['f1_sim']:.4f}")
    print(f"Edges (pred={e['pred_count']}, gt={e['gt_count']}):")
    print(f"  precision  sim={e['precision_sim']:.4f}   dist={e['precision_dist']:.4f}")
    print(f"  recall     sim={e['recall_sim']:.4f}   dist={e['recall_dist']:.4f}")
    print(f"  F1         sim={e['f1_sim']:.4f}")


def show_node_alignments(
    pred: ClusteredGraph,
    gt: ClusteredGraph,
    metrics: GraphMetrics,
    top_k: int = 10,
    direction: str = "pred_to_gt",
) -> None:
    pred_labels = [n.label for n in pred.nodes]
    gt_labels = [n.label for n in gt.nodes]
    if direction == "pred_to_gt":
        items = [
            (pred_labels[i], gt_labels[j], sim)
            for i, (j, sim) in enumerate(metrics.nodes.pred_to_gt)
        ]
        title = "predicted → nearest GT"
    else:
        items = [
            (gt_labels[j], pred_labels[i], sim)
            for j, (i, sim) in enumerate(metrics.nodes.gt_to_pred)
        ]
        title = "GT → nearest predicted"

    items.sort(key=lambda x: x[2], reverse=True)
    print(f"Top {top_k} ({title}):")
    for src, dst, sim in items[:top_k]:
        print(f"  [{sim:.3f}]  {src!r}  →  {dst!r}")
    print(f"\nBottom {top_k} ({title}):")
    for src, dst, sim in items[-top_k:]:
        print(f"  [{sim:.3f}]  {src!r}  →  {dst!r}")


def show_edge_alignments(
    pred: ClusteredGraph,
    gt: ClusteredGraph,
    metrics: GraphMetrics,
    top_k: int = 10,
    direction: str = "pred_to_gt",
) -> None:
    def fmt(g: ClusteredGraph) -> list[str]:
        by_id = {n.id: n.label for n in g.nodes}
        return [
            f"{by_id.get(e.source, e.source)} —[{e.label}]→ {by_id.get(e.target, e.target)}"
            for e in g.edges
        ]

    pred_strs = fmt(pred)
    gt_strs = fmt(gt)

    if direction == "pred_to_gt":
        items = [
            (pred_strs[i], gt_strs[j], sim)
            for i, (j, sim) in enumerate(metrics.edges.pred_to_gt)
        ]
        title = "predicted → nearest GT"
    else:
        items = [
            (gt_strs[j], pred_strs[i], sim)
            for j, (i, sim) in enumerate(metrics.edges.gt_to_pred)
        ]
        title = "GT → nearest predicted"

    items.sort(key=lambda x: x[2], reverse=True)
    print(f"Top {top_k} ({title}):")
    for src, dst, sim in items[:top_k]:
        print(f"  [{sim:.3f}]  {src}\n           →  {dst}")
    print(f"\nBottom {top_k} ({title}):")
    for src, dst, sim in items[-top_k:]:
        print(f"  [{sim:.3f}]  {src}\n           →  {dst}")



def load_clustered_graph(path: str | Path) -> ClusteredGraph:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return ClusteredGraph(**raw)
