from __future__ import annotations

from itertools import product
from pathlib import Path
from typing import Literal

import numpy as np
import yaml
from pydantic import BaseModel, model_validator


class LLMConfig(BaseModel):
    model_name: str = "Qwen/Qwen2-1.5B-Instruct"
    device: str = "cpu"
    load_in_8bit: bool = False
    max_new_tokens: int = 256
    temperature: float = 0.3


class EmbeddingConfig(BaseModel):
    model_name: str = "all-mpnet-base-v2"
    device: str = "cpu"


class CoreferenceConfig(BaseModel):
    enabled: bool = True
    prompt_file: str = "prompts/coreference_ru.txt"
    context_sentences: int = 3
    window_sentences: int = 5


class ExtractionConfig(BaseModel):
    prompt_file: str = "prompts/extraction_ru.txt"
    chunk_size: int = 3
    overlap_size: int = 1


class NormalizationConfig(BaseModel):
    enabled: bool = True
    language: Literal["ru", "en"] = "ru"


class DeduplicationConfig(BaseModel):
    enabled: bool = True
    threshold: float = 0.92


class ClusteringConfig(BaseModel):
    method: Literal["agglomerative", "kmeans", "hdbscan"] = "agglomerative"
    threshold: float = 0.5
    n_clusters: int | None = None
    min_cluster_size: int = 5
    min_samples: int | None = None
    cluster_relations: bool = True
    include_embeddings: bool = False
    cluster_naming_prompt: str = "prompts/cluster_naming_ru.txt"
    llm_naming: bool = False

    multi_method: bool = False

    threshold_min: float | None = None
    threshold_max: float | None = None
    threshold_steps: int | None = None

    k_min: int = 2
    k_max: int = 10
    k_step: int = 1

    hdbscan_min_cluster_sizes: list[int] = [3, 5, 10]
    hdbscan_min_samples: list[int] = [1, 3, 5]

    @property
    def is_multi_threshold(self) -> bool:
        return all(v is not None for v in (self.threshold_min, self.threshold_max, self.threshold_steps))

    @property
    def threshold_values(self) -> list[float]:
        if not self.is_multi_threshold:
            return [self.threshold]
        return np.linspace(self.threshold_min, self.threshold_max, self.threshold_steps).tolist()

    @property
    def k_values(self) -> list[int]:
        return list(range(self.k_min, self.k_max + 1, self.k_step))

    @property
    def hdbscan_param_grid(self) -> list[tuple[int, int]]:
        return list(product(self.hdbscan_min_cluster_sizes, self.hdbscan_min_samples))

    @model_validator(mode="after")
    def _validate_multi_threshold(self):
        fields = (self.threshold_min, self.threshold_max, self.threshold_steps)
        set_count = sum(v is not None for v in fields)
        if set_count not in (0, 3):
            raise ValueError("threshold_min, threshold_max, and threshold_steps must all be set together")
        if self.is_multi_threshold:
            if self.threshold_steps < 2:
                raise ValueError("threshold_steps must be >= 2")
            if self.threshold_min >= self.threshold_max:
                raise ValueError("threshold_min must be < threshold_max")
            if not self.multi_method and self.method != "agglomerative":
                raise ValueError("multi-threshold without multi_method only supports agglomerative clustering")
            if self.n_clusters is not None:
                raise ValueError("n_clusters must not be set with multi-threshold")
        if self.multi_method:
            if not self.is_multi_threshold:
                raise ValueError("multi_method requires threshold_min, threshold_max, and threshold_steps")
            if self.k_min >= self.k_max:
                raise ValueError("k_min must be < k_max")
            if self.k_step < 1:
                raise ValueError("k_step must be >= 1")
            if not self.hdbscan_min_cluster_sizes:
                raise ValueError("hdbscan_min_cluster_sizes must not be empty")
            if not self.hdbscan_min_samples:
                raise ValueError("hdbscan_min_samples must not be empty")
        return self


class PathsConfig(BaseModel):
    input_text: str = ""
    output_dir: str = "output"


class PipelineConfig(BaseModel):
    llm: LLMConfig = LLMConfig()
    embedding: EmbeddingConfig = EmbeddingConfig()
    coreference: CoreferenceConfig = CoreferenceConfig()
    extraction: ExtractionConfig = ExtractionConfig()
    normalization: NormalizationConfig = NormalizationConfig()
    deduplication: DeduplicationConfig = DeduplicationConfig()
    clustering: ClusteringConfig = ClusteringConfig()
    paths: PathsConfig = PathsConfig()


def load_config(path: str | Path) -> PipelineConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return PipelineConfig(**raw)
