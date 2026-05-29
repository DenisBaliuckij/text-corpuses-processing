from __future__ import annotations

import logging
from collections import defaultdict

import numpy as np

from ..config_schema import EntityResolutionConfig
from ..models.embedder import Embedder
from ..schemas.common import ExtractedEntity, ExtractedTriplet

logger = logging.getLogger(__name__)


def resolve_entities(
    entities: list[ExtractedEntity],
    triplets: list[ExtractedTriplet],
    embedder: Embedder | None,
    config: EntityResolutionConfig,
) -> tuple[dict[str, str], list[ExtractedEntity], list[ExtractedTriplet]]:
    if not config.enabled or not (entities or triplets):
        return {}, list(entities), list(triplets)

    unique_names: list[str] = []
    seen_lower: set[str] = set()

    def _consume(name: str) -> None:
        n = name.strip()
        key = n.lower()
        if n and key not in seen_lower:
            seen_lower.add(key)
            unique_names.append(n)

    for e in entities:
        _consume(e.canonical_name)
    for t in triplets:
        _consume(t.subject)
        _consume(t.object)

    if len(unique_names) <= 1:
        return {}, list(entities), list(triplets)

    norm_groups: dict[str, list[int]] = defaultdict(list)
    for i, name in enumerate(unique_names):
        norm_groups[name.lower()].append(i)

    group_keys = list(norm_groups.keys())
    n_groups = len(group_keys)
    rep_names = [unique_names[norm_groups[k][0]] for k in group_keys]

    if config.use_embeddings and embedder is not None and n_groups > 1:
        embs = embedder.encode_batch(rep_names)
        norms = np.linalg.norm(embs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        embs = embs / norms

        parent = list(range(n_groups))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

        sim = embs @ embs.T
        for i in range(n_groups):
            for j in range(i + 1, n_groups):
                if sim[i, j] >= config.similarity_threshold:
                    union(i, j)

        cluster_to_members: dict[int, list[int]] = defaultdict(list)
        for i in range(n_groups):
            cluster_to_members[find(i)].append(i)

        sizes_sorted = sorted((len(m) for m in cluster_to_members.values()), reverse=True)
        if sizes_sorted:
            logger.info("[2.3] ER cluster sizes (top): %s", sizes_sorted[:10])
            biggest = sizes_sorted[0]
            if biggest > max(3, int(0.3 * n_groups)):
                biggest_cluster = max(cluster_to_members.values(), key=len)
                sample = [rep_names[i] for i in biggest_cluster[:8]]
                logger.warning(
                    "[2.3] Suspicious ER collapse: largest cluster has %d/%d names. "
                    "Sample: %s. Likely causes: (a) embedding model needs an input "
                    "prefix (e5/bge -> 'query: '); (b) similarity_threshold (%.2f) "
                    "too low; (c) transitive union-find chain.",
                    biggest, n_groups, sample, config.similarity_threshold,
                )

        pos_to_resolved: dict[int, str] = {}
        for members in cluster_to_members.values():
            best = min(members, key=lambda m: (len(rep_names[m]), rep_names[m].lower()))
            for m in members:
                pos_to_resolved[m] = rep_names[best]
    else:
        pos_to_resolved = {i: rep_names[i] for i in range(n_groups)}

    mapping_ci: dict[str, str] = {}
    for g_pos, key in enumerate(group_keys):
        mapping_ci[key] = pos_to_resolved[g_pos]

    def _resolve(name: str) -> str:
        return mapping_ci.get(name.strip().lower(), name)

    new_entities = [
        ExtractedEntity(
            mention_text=e.mention_text,
            canonical_name=_resolve(e.canonical_name),
            entity_type=e.entity_type,
            importance=e.importance,
            rationale=e.rationale,
            chunk_id=e.chunk_id,
        )
        for e in entities
    ]
    new_triplets = [
        ExtractedTriplet(
            subject=_resolve(t.subject),
            predicate=t.predicate,
            object=_resolve(t.object),
            confidence=t.confidence,
            evidence=t.evidence,
            chunk_id=t.chunk_id,
        )
        for t in triplets
    ]

    display: dict[str, str] = {}
    for nm in unique_names:
        resolved = _resolve(nm)
        if resolved != nm:
            display[nm] = resolved

    n_unique_after = len({_resolve(nm) for nm in unique_names})
    logger.info(
        "[2.3] Entity resolution: %d unique names -> %d after merge (%d rewrites)",
        len(unique_names), n_unique_after, len(display),
    )
    return display, new_entities, new_triplets
