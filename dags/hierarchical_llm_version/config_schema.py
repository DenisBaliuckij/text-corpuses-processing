from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field, model_validator


class LLMModelConfig(BaseModel):

    model_id: str = "deepseek-v32/latest"
    temperature: float = 0.3
    max_output_tokens: int = 600
    instructions: str = ""


class LLMConfig(BaseModel):
    api_key_env: str = "YANDEX_CLOUD_API_KEY"
    api_key: Optional[str] = None
    base_url: str = "https://ai.api.cloud.yandex.net/v1"
    folder: str = ""

    cheap: LLMModelConfig = LLMModelConfig(max_output_tokens=600)
    strong: LLMModelConfig = LLMModelConfig(max_output_tokens=1500)
    extraction: LLMModelConfig = LLMModelConfig(max_output_tokens=2500)

    max_concurrency: int = 8
    max_retries: int = 3
    retry_base_delay: float = 1.0


class EmbeddingConfig(BaseModel):
    model_name: str = "all-mpnet-base-v2"
    device: str = "cpu"
    prefix: Optional[str] = None


class PreprocessingConfig(BaseModel):
    language: Literal["ru", "en"] = "ru"
    chunk_size: int = 5
    overlap_size: int = 1


class GroupingConfig(BaseModel):

    method: Literal["sequential", "semantic"] = "sequential"
    group_size: int = 5
    min_cluster_size: int = 3
    min_samples: int = 1


class Pass1Config(BaseModel):
    summary_prompt: str = "prompts/chunk_summary_ru.txt"
    aggregation_prompt: str = "prompts/group_aggregation_ru.txt"
    root_prompt: str = "prompts/root_aggregation_ru.txt"

    grouping: GroupingConfig = GroupingConfig()

    max_chars_per_aggregation: int = 12000

    max_top_level_nodes: int = 20

    max_levels: int = 10


class Pass2Config(BaseModel):
    extraction_prompt: str = "prompts/extraction_with_context_ru.txt"
    include_neighbors: bool = True
    neighbor_window: int = 1
    max_context_chars: int = 6000
    max_concepts_in_context: int = 30


class ImportanceFilteringMultiConfig(BaseModel):

    enabled: bool = False
    entity_thresholds: list[float] = [0.2, 0.4, 0.6, 0.8]
    relation_thresholds: list[float] = [0.2, 0.4, 0.6, 0.8]
    sweep_mode: Literal["cross_product", "paired"] = "cross_product"
    drop_orphan_nodes: bool = True


class ImportanceFilteringConfig(BaseModel):
    enabled: bool = True
    default_entity_threshold: float = 0.5
    default_relation_threshold: float = 0.5
    drop_orphan_nodes: bool = True
    multi: ImportanceFilteringMultiConfig = ImportanceFilteringMultiConfig()


class EntityResolutionConfig(BaseModel):
    enabled: bool = True
    similarity_threshold: float = 0.85
    use_embeddings: bool = True


class ClusteringConfig(BaseModel):
    enabled: bool = False
    threshold: float = 0.4
    cluster_relations: bool = True
    cluster_naming_prompt: str = "prompts/cluster_naming_ru.txt"


class PathsConfig(BaseModel):
    input_text: str = ""
    output_dir: str = "output"


class PipelineConfig(BaseModel):
    llm: LLMConfig = LLMConfig()
    embedding: EmbeddingConfig = EmbeddingConfig()
    preprocessing: PreprocessingConfig = PreprocessingConfig()
    pass1: Pass1Config = Pass1Config()
    pass2: Pass2Config = Pass2Config()
    entity_resolution: EntityResolutionConfig = EntityResolutionConfig()
    importance_filtering: ImportanceFilteringConfig = ImportanceFilteringConfig()
    clustering: ClusteringConfig = ClusteringConfig()
    paths: PathsConfig = PathsConfig()

    @model_validator(mode="after")
    def _validate(self):
        if self.preprocessing.chunk_size < 1:
            raise ValueError("preprocessing.chunk_size must be >= 1")
        if self.preprocessing.overlap_size >= self.preprocessing.chunk_size:
            raise ValueError("preprocessing.overlap_size must be < chunk_size")
        if self.pass1.grouping.group_size < 1:
            raise ValueError("pass1.grouping.group_size must be >= 1")
        if self.pass1.max_top_level_nodes < 1:
            raise ValueError("pass1.max_top_level_nodes must be >= 1")
        if self.pass1.max_levels < 1:
            raise ValueError("pass1.max_levels must be >= 1")
        if self.llm.max_concurrency < 1:
            raise ValueError("llm.max_concurrency must be >= 1")

        ifc = self.importance_filtering
        for t in (ifc.default_entity_threshold, ifc.default_relation_threshold):
            if not (0.0 <= t <= 1.0):
                raise ValueError("importance_filtering thresholds must be in [0, 1]")
        if ifc.multi.enabled:
            if not ifc.multi.entity_thresholds or not ifc.multi.relation_thresholds:
                raise ValueError("importance_filtering.multi: thresholds must not be empty")
            for arr in (ifc.multi.entity_thresholds, ifc.multi.relation_thresholds):
                for t in arr:
                    if not (0.0 <= t <= 1.0):
                        raise ValueError("importance_filtering.multi thresholds must be in [0, 1]")
            if ifc.multi.sweep_mode == "paired":
                if len(ifc.multi.entity_thresholds) != len(ifc.multi.relation_thresholds):
                    raise ValueError(
                        "importance_filtering.multi.sweep_mode='paired' requires "
                        "equal-length entity_thresholds and relation_thresholds"
                    )
        return self


def load_config(path: str | Path) -> PipelineConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return PipelineConfig(**raw)
