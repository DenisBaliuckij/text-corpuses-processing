from __future__ import annotations

import numpy as np
from sentence_transformers import SentenceTransformer

from ..config_schema import EmbeddingConfig


class Embedder:
    def __init__(self, config: EmbeddingConfig):
        self.model = SentenceTransformer(config.model_name, device=config.device)
        self.dim = self.model.get_sentence_embedding_dimension()

    def encode(self, text: str) -> np.ndarray:
        if not text or not text.strip():
            return np.zeros(self.dim)
        return self.model.encode(text, convert_to_numpy=True)

    def encode_batch(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, self.dim))
        return self.model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
