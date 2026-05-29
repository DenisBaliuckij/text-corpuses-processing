"""Step 2.1 — assemble per-chunk global context from the hierarchy tree.

The size of the context is **constant** in expectation: root summary +
O(depth) intermediate summaries + a deduplicated concept list +
O(neighbor_window) neighbouring chunk snippets. Hard-capped by
`max_context_chars`.
"""
from __future__ import annotations

from ..config_schema import Pass2Config
from ..schemas.common import Chunk
from ..schemas.hierarchy import HierarchyTree


def build_context(
    chunk: Chunk,
    chunk_to_leaf_id: dict[str, str],
    tree: HierarchyTree,
    chunks: list[Chunk],
    chunk_index: dict[str, int],
    config: Pass2Config,
) -> str:
    leaf_id = chunk_to_leaf_id.get(chunk.id)
    if leaf_id is None:
        return _root_only(tree)

    path_bottom_up = tree.path_to_root(leaf_id)            # [leaf, ..., root]
    path_top_down = list(reversed(path_bottom_up))         # [root, ..., leaf]
    middle = path_top_down[1:-1] if len(path_top_down) >= 2 else []

    parts: list[str] = []

    root = tree.get(tree.root_id)
    parts.append(f"=== Глобальный обзор корпуса (уровень {root.level}) ===")
    if root.topic:
        parts.append(f"Тематика: {root.topic}")
    if root.summary:
        parts.append(f"Summary: {root.summary}")
    if root.subtopics:
        parts.append(f"Подтемы: {', '.join(root.subtopics)}")

    for node_id in middle:
        node = tree.get(node_id)
        parts.append(f"\n--- Раздел уровня {node.level} ---")
        if node.topic:
            parts.append(f"Тематика: {node.topic}")
        if node.summary:
            parts.append(f"Summary: {node.summary}")
        if node.subtopics:
            parts.append(f"Подтемы: {', '.join(node.subtopics)}")

    # deduplicated concepts collected along the whole path, sorted by Pass-1
    # discrete importance (core > supporting > peripheral) and truncated to
    # `max_concepts_in_context` to keep the context size bounded.
    _IMP_RANK = {"core": 0, "supporting": 1, "peripheral": 2}
    collected: list[tuple[int, int, str, str]] = []  # (rank, first_seen, name, importance)
    seen: set[str] = set()
    pos = 0
    for node_id in path_top_down:
        node = tree.get(node_id)
        for kc in node.key_concepts:
            key = kc.name.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            imp = kc.importance if kc.importance in _IMP_RANK else "supporting"
            collected.append((_IMP_RANK[imp], pos, kc.name, imp))
            pos += 1

    collected.sort(key=lambda x: (x[0], x[1]))
    cap = config.max_concepts_in_context
    if cap and cap > 0:
        collected = collected[:cap]

    if collected:
        concepts_by_imp: dict[str, list[str]] = {"core": [], "supporting": [], "peripheral": []}
        for _, _, name, imp in collected:
            concepts_by_imp[imp].append(name)
        parts.append("\n--- Ключевые концепты (по важности) ---")
        for imp in ("core", "supporting", "peripheral"):
            items = concepts_by_imp[imp]
            if items:
                parts.append(f"  {imp}: {', '.join(items)}")

    if config.include_neighbors and config.neighbor_window > 0:
        idx = chunk_index.get(chunk.id, -1)
        if idx >= 0:
            lo = max(0, idx - config.neighbor_window)
            hi = min(len(chunks), idx + config.neighbor_window + 1)
            neighbors = [chunks[j] for j in range(lo, hi) if j != idx]
            if neighbors:
                parts.append("\n--- Соседние фрагменты ---")
                for n in neighbors:
                    snippet = n.text[:200].replace("\n", " ")
                    parts.append(f"  [{n.id}] {snippet}{'...' if len(n.text) > 200 else ''}")

    text = "\n".join(parts)
    if len(text) > config.max_context_chars:
        text = text[: config.max_context_chars] + "\n[...context truncated...]"
    return text


def _root_only(tree: HierarchyTree) -> str:
    root = tree.get(tree.root_id)
    bits = ["=== Глобальный обзор корпуса ==="]
    if root.topic:
        bits.append(f"Тематика: {root.topic}")
    if root.summary:
        bits.append(f"Summary: {root.summary}")
    return "\n".join(bits)
