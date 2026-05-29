from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .config_schema import PipelineConfig
from .models.embedder import Embedder
from .models.llm_client import LLMClient
from .schemas.clustered_graph import ClusteredGraph
from .schemas.hierarchy import HierarchyTree
from .schemas.raw_graph import RawGraph
from .stages.build_clustered import build_clustered_graph
from .stages.chunking import build_chunks
from .stages.entity_resolution import resolve_entities
from .stages.graph_assembly import assemble_raw_graph
from .stages.importance_filter import (
    build_clustered_from_filter,
    build_multi_clustered,
)
from .stages.pass1_hierarchy import build_hierarchy
from .stages.pass1_summarize import summarize_chunks
from .stages.pass2_extraction import extract_with_context
from .stages.preprocessing import preprocess
from .utils.io import load_text, save_json

logger = logging.getLogger(__name__)


def _needs_embedder(cfg: PipelineConfig) -> bool:
    if cfg.entity_resolution.enabled and cfg.entity_resolution.use_embeddings:
        return True
    if cfg.pass1.grouping.method == "semantic":
        return True
    if cfg.clustering.enabled:
        return True
    return False


class Pipeline:
    def __init__(self, config: PipelineConfig, base_dir: Path | None = None):
        self.config = config
        self.base_dir = base_dir or Path.cwd()

        logger.info("Initializing LLM client (base_url=%s)", config.llm.base_url)
        self.llm = LLMClient(config.llm)

        if _needs_embedder(config):
            logger.info("Loading embedder: %s", config.embedding.model_name)
            self.embedder: Embedder | None = Embedder(config.embedding)
        else:
            self.embedder = None

    async def run_async(self, text: str | None = None) -> dict:
        cfg = self.config
        out = Path(cfg.paths.output_dir)
        if not out.is_absolute():
            out = self.base_dir / out
        out.mkdir(parents=True, exist_ok=True)

        if text is None:
            input_path = Path(cfg.paths.input_text)
            if not input_path.is_absolute():
                input_path = self.base_dir / input_path
            text = load_text(input_path)
        logger.info("Input text: %d chars", len(text))


        sentences = preprocess(text, language=cfg.preprocessing.language)
        chunks = build_chunks(sentences, cfg.preprocessing)
        logger.info("[0] %d sentences -> %d chunks", len(sentences), len(chunks))


        leaf_nodes = await summarize_chunks(
            chunks, self.llm, cfg.pass1, base_dir=self.base_dir,
        )
        tree: HierarchyTree = await build_hierarchy(
            leaf_nodes, self.llm, cfg.pass1,
            embedder=self.embedder, base_dir=self.base_dir,
        )
        logger.info(
            "[1] Hierarchy: %d nodes, %d levels, root=%s",
            len(tree.nodes), max(tree.levels.keys()), tree.root_id,
        )
        save_json(tree.model_dump(), out / "hierarchy_tree.json")


        entities, triplets = await extract_with_context(
            chunks, tree, self.llm, cfg.pass2, base_dir=self.base_dir,
        )
        save_json(
            {
                "entities": [e.model_dump() for e in entities],
                "triplets": [t.model_dump() for t in triplets],
            },
            out / "extracted_raw.json",
        )


        mapping: dict[str, str] = {}
        if cfg.entity_resolution.enabled:
            mapping, entities, triplets = resolve_entities(
                entities, triplets, self.embedder, cfg.entity_resolution,
            )
            save_json(
                {
                    "mapping": mapping,
                    "entities": [e.model_dump() for e in entities],
                    "triplets": [t.model_dump() for t in triplets],
                },
                out / "extracted_resolved.json",
            )


        raw_graph: RawGraph = assemble_raw_graph(entities, triplets, chunks, text, cfg)
        save_json(raw_graph.model_dump(), out / "raw_graph.json")
        logger.info("[4] Raw graph: %d nodes, %d edges", len(raw_graph.nodes), len(raw_graph.edges))


        ifc = cfg.importance_filtering
        multi_clustered = None
        default_label: str | None = None

        if ifc.enabled and ifc.multi.enabled:
            multi_clustered, default_label = build_multi_clustered(raw_graph, ifc.multi)
            save_json(multi_clustered.model_dump(), out / "multi_clustered_graph.json")
            method = multi_clustered.methods["importance_filter"]
            logger.info(
                "[5] Multi-clustered (importance_filter): %d variants, default=%s",
                len(method.param_labels), default_label,
            )
            clustered: ClusteredGraph = method.graphs[default_label]
        elif ifc.enabled:
            clustered = build_clustered_from_filter(
                raw_graph,
                entity_threshold=ifc.default_entity_threshold,
                relation_threshold=ifc.default_relation_threshold,
                drop_orphan_nodes=ifc.drop_orphan_nodes,
            )
            logger.info(
                "[5] Importance-filtered (e>=%.2f, r>=%.2f, drop_orphans=%s)",
                ifc.default_entity_threshold,
                ifc.default_relation_threshold,
                ifc.drop_orphan_nodes,
            )
        else:
            clustered = build_clustered_graph(
                raw_graph, cfg.clustering, embedder=self.embedder,
            )
            logger.info("[5] Clustering (importance filter off): method=%s",
                        "agglomerative" if cfg.clustering.enabled else "promote")

        save_json(clustered.model_dump(), out / "clustered_graph.json")
        logger.info(
            "[5] Clustered graph: %d nodes, %d edges",
            len(clustered.nodes), len(clustered.edges),
        )


        token_stats = self.llm.stats.to_dict()
        save_json(token_stats, out / "token_stats.json")

        hierarchy_stats = {
            "n_levels": max(tree.levels.keys()),
            "n_nodes": len(tree.nodes),
            "level_sizes": {str(k): len(v) for k, v in sorted(tree.levels.items())},
            "n_chunks": len(chunks),
            "n_sentences": len(sentences),
            "n_entities_extracted": len(entities),
            "n_triplets_extracted": len(triplets),
            "n_entity_rewrites": len(mapping),
        }
        save_json(hierarchy_stats, out / "hierarchy_stats.json")

        logger.info(
            "Tokens: input=%d output=%d calls=%d failures=%d",
            token_stats["total_input_tokens"],
            token_stats["total_output_tokens"],
            token_stats["total_calls"],
            token_stats["total_failures"],
        )
        logger.info("Hierarchy levels: %s", hierarchy_stats["level_sizes"])

        return {
            "sentences": sentences,
            "chunks": chunks,
            "hierarchy_tree": tree,
            "entities": entities,
            "triplets": triplets,
            "entity_mapping": mapping,
            "raw_graph": raw_graph,
            "clustered_graph": clustered,
            "multi_clustered_graph": multi_clustered,
            "default_param_label": default_label,
            "token_stats": token_stats,
            "hierarchy_stats": hierarchy_stats,
        }

    def run(self, text: str | None = None) -> dict:
        return asyncio.run(self.run_async(text))
