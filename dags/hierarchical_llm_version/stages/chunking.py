from __future__ import annotations

from ..config_schema import PreprocessingConfig
from ..schemas.common import Chunk, Sentence


def build_chunks(sentences: list[Sentence], config: PreprocessingConfig) -> list[Chunk]:
    """Create overlapping sentence chunks."""
    if not sentences:
        return []

    size = config.chunk_size
    step = max(1, size - config.overlap_size)
    chunks: list[Chunk] = []

    for start in range(0, len(sentences), step):
        window = sentences[start : start + size]
        if not window:
            break
        chunks.append(
            Chunk(
                id=f"chunk_{len(chunks)}",
                text=" ".join(s.text for s in window),
                sentence_ids=[s.id for s in window],
            )
        )
        if window[-1].id == sentences[-1].id:
            break

    return chunks
