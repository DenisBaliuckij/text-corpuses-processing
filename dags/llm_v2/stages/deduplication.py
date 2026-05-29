from __future__ import annotations

import numpy as np

from ..config_schema import DeduplicationConfig
from ..models.embedder import Embedder
from ..schemas.common import NormalizedTriplet


def deduplicate_triplets(
    triplets: list[NormalizedTriplet],
    embedder: Embedder,
    config: DeduplicationConfig,
) -> list[NormalizedTriplet]:
    if not config.enabled or len(triplets) <= 1:
        return list(triplets)

    texts = [
        f"{t.norm_subject} {t.norm_relation} {t.norm_object}" for t in triplets
    ]
    embeddings = embedder.encode_batch(texts)

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms

    keep = [True] * len(triplets)
    for i in range(len(triplets)):
        if not keep[i]:
            continue
        for j in range(i + 1, len(triplets)):
            if not keep[j]:
                continue
            sim = float(np.dot(embeddings[i], embeddings[j]))
            if sim > config.threshold:
                if len(triplets[j].norm_relation) < len(triplets[i].norm_relation):
                    keep[i] = False
                    break
                else:
                    keep[j] = False

    return [t for t, k in zip(triplets, keep) if k]
