from __future__ import annotations

import logging

import numpy as np
from sentence_transformers import SentenceTransformer

from ..config_schema import EmbeddingConfig

logger = logging.getLogger(__name__)


def _auto_prefix(model_name: str) -> str:
    name = (model_name or "").lower()
    if "e5" in name or "/bge-" in name or name.startswith("bge"):
        return "query: "
    return ""


class Embedder:
    def __init__(self, config: EmbeddingConfig):
        self.model = SentenceTransformer(config.model_name, device=config.device)
        self.dim = self.model.get_sentence_embedding_dimension()
        if config.prefix is None:
            self.prefix = _auto_prefix(config.model_name)
        else:
            self.prefix = config.prefix
        if self.prefix:
            logger.info(
                "Embedder using input prefix %r (required by %s family)",
                self.prefix, config.model_name,
            )

    def _with_prefix(self, text: str) -> str:
        return f"{self.prefix}{text}" if self.prefix else text

    def encode(self, text: str) -> np.ndarray:
        if not text or not text.strip():
            return np.zeros(self.dim)
        return self.model.encode(self._with_prefix(text), convert_to_numpy=True)

    def encode_batch(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, self.dim))
        prepped = [self._with_prefix(t) for t in texts]
        return self.model.encode(prepped, convert_to_numpy=True, show_progress_bar=False)
