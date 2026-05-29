from __future__ import annotations

import logging
from pathlib import Path

from .config_schema import PipelineConfig
from .models.embedder import Embedder
from .models.llm_client import LLMClient
from .schemas.clustered_graph import ClusteredGraph
from .schemas.raw_graph import RawGraph
from .utils.io import load_prompt, load_text, save_json, save_text

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: PipelineConfig, base_dir: Path | None = None):
        self.config = config
        self.base_dir = base_dir or Path.cwd()

        logger.info("Loading LLM: %s", config.llm.model_name)
        self.llm = LLMClient(config.llm)

        logger.info("Loading embedder: %s", config.embedding.model_name)
        self.embedder = Embedder(config.embedding)

    def run(self, text: str | None = None) -> dict:
        cfg = self.config
        out = Path(cfg.paths.output_dir)
        out.mkdir(parents=True, exist_ok=True)

        if text is None:
            input_path = Path(cfg.paths.input_text)
            if not input_path.is_absolute():
                input_path = self.base_dir / input_path
            text = load_text(input_path)
        logger.info("Input text length: %d chars", len(text))

        from .stages.preprocessing import preprocess

        lang = cfg.normalization.language
        sentences = preprocess(text, language=lang)
        logger.info("[0] Preprocessing: %d sentences", len(sentences))

        from .stages.coreference import resolve_coreferences

        resolved_text, sentences = resolve_coreferences(
            sentences, self.llm, cfg.coreference, base_dir=self.base_dir
        )
        save_text(resolved_text, out / "coreference_resolved.txt")
        logger.info("[1] Coreference resolved, %d sentences", len(sentences))

        from .stages.chunking import build_chunks

        chunks = build_chunks(sentences, cfg.extraction)
        logger.info("[1.5] Built %d chunks", len(chunks))

        from .stages.extraction import extract_triplets

        raw_triplets = extract_triplets(
            chunks, self.llm, cfg.extraction, base_dir=self.base_dir
        )
        logger.info("[2] Extracted %d raw triplets", len(raw_triplets))

        from .stages.normalization import normalize_triplets

        norm_triplets = normalize_triplets(raw_triplets, cfg.normalization)
        logger.info("[3] Normalized %d triplets", len(norm_triplets))

        from .stages.deduplication import deduplicate_triplets

        dedup_triplets = deduplicate_triplets(
            norm_triplets, self.embedder, cfg.deduplication
        )
        logger.info(
            "[4] Deduplication: %d -> %d triplets",
            len(norm_triplets),
            len(dedup_triplets),
        )

        from .stages.graph_assembly import assemble_graph

        raw_graph: RawGraph = assemble_graph(dedup_triplets, chunks, text, cfg)
        save_json(raw_graph.model_dump(), out / "raw_graph.json")
        logger.info(
            "[5] Raw graph: %d nodes, %d edges",
            len(raw_graph.nodes),
            len(raw_graph.edges),
        )

        from .stages.clustering import (
            cluster_graph,
            cluster_graph_all_methods,
            cluster_graph_multi,
        )

        naming_prompt_path = Path(cfg.clustering.cluster_naming_prompt)
        if not naming_prompt_path.is_absolute():
            naming_prompt_path = self.base_dir / naming_prompt_path
        naming_prompt = load_prompt(naming_prompt_path) if naming_prompt_path.exists() else None

        result: dict = {
            "sentences": sentences,
            "chunks": chunks,
            "raw_triplets": raw_triplets,
            "norm_triplets": norm_triplets,
            "dedup_triplets": dedup_triplets,
            "raw_graph": raw_graph,
        }

        if cfg.clustering.multi_method:
            multi = cluster_graph_all_methods(
                raw_graph, self.embedder, cfg,
                llm=self.llm, prompt_template=naming_prompt,
            )
            save_json(multi.model_dump(), out / "multi_clustered_graph.json")
            method_counts = {m: len(r.param_labels) for m, r in multi.methods.items()}
            logger.info("[6] Multi-method clustering: %s", method_counts)

            agg = multi.methods["agglomerative"]
            mid_label = agg.param_labels[len(agg.param_labels) // 2]
            clustered = agg.graphs[mid_label]
            save_json(clustered.model_dump(), out / "clustered_graph.json")
            logger.info(
                "[6] Default clustered graph (agglomerative t=%s): %d nodes, %d edges",
                mid_label, len(clustered.nodes), len(clustered.edges),
            )
            result["multi_clustered_graph"] = multi
            result["clustered_graph"] = clustered

        elif cfg.clustering.is_multi_threshold:
            multi = cluster_graph_multi(
                raw_graph, self.embedder, cfg,
                llm=self.llm, prompt_template=naming_prompt,
            )
            save_json(multi.model_dump(), out / "multi_clustered_graph.json")
            agg = multi.methods["agglomerative"]
            logger.info(
                "[6] Multi-threshold clustering: %d thresholds", len(agg.param_labels)
            )

            mid_label = agg.param_labels[len(agg.param_labels) // 2]
            clustered = agg.graphs[mid_label]
            save_json(clustered.model_dump(), out / "clustered_graph.json")
            logger.info(
                "[6] Default clustered graph (t=%s): %d nodes, %d edges",
                mid_label, len(clustered.nodes), len(clustered.edges),
            )
            result["multi_clustered_graph"] = multi
            result["clustered_graph"] = clustered
        else:
            clustered: ClusteredGraph = cluster_graph(
                raw_graph, self.embedder, cfg,
                llm=self.llm, prompt_template=naming_prompt,
            )
            save_json(clustered.model_dump(), out / "clustered_graph.json")
            logger.info(
                "[6] Clustered graph: %d nodes, %d edges",
                len(clustered.nodes),
                len(clustered.edges),
            )
            result["clustered_graph"] = clustered

        return result
