from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..config_schema import Pass2Config
from ..models.llm_client import LLMClient
from ..schemas.common import Chunk, ExtractedEntity, ExtractedTriplet
from ..schemas.hierarchy import HierarchyTree
from ..utils.io import load_prompt
from ..utils.json_parsing import extract_json_partial, looks_truncated
from .pass2_context import build_context

logger = logging.getLogger(__name__)

_INSTRUCTIONS = (
    "Ты помощник по извлечению знаний. Извлекай сущности и триплеты из ФРАГМЕНТА, "
    "учитывая ГЛОБАЛЬНЫЙ КОНТЕКСТ корпуса. Возвращай ТОЛЬКО валидный JSON "
    "без markdown-форматирования и пояснений."
)


async def extract_with_context(
    chunks: list[Chunk],
    tree: HierarchyTree,
    llm: LLMClient,
    config: Pass2Config,
    base_dir: Path | None = None,
) -> tuple[list[ExtractedEntity], list[ExtractedTriplet]]:
    if not chunks:
        return [], []

    prompt_path = Path(config.extraction_prompt)
    if base_dir and not prompt_path.is_absolute():
        prompt_path = base_dir / prompt_path
    template = load_prompt(prompt_path)

    chunk_to_leaf: dict[str, str] = {
        node.chunk_id: node.id
        for node in tree.nodes.values()
        if node.level == 1 and node.chunk_id
    }
    chunk_index = {c.id: i for i, c in enumerate(chunks)}

    tasks = [
        _extract_one(chunk, tree, chunk_to_leaf, chunks, chunk_index, llm, template, config)
        for chunk in chunks
    ]
    results = await asyncio.gather(*tasks)

    all_entities: list[ExtractedEntity] = []
    all_triplets: list[ExtractedTriplet] = []
    per_chunk_summary: list[str] = []
    n_truncated = 0
    n_empty = 0
    for chunk, (ents, trips, diag) in zip(chunks, results):
        all_entities.extend(ents)
        all_triplets.extend(trips)
        per_chunk_summary.append(f"{chunk.id}: e={len(ents)} t={len(trips)}")
        if diag.get("truncated"):
            n_truncated += 1
        if diag.get("empty"):
            n_empty += 1

    logger.info(
        "[2.2] Extracted %d entities, %d triplets across %d chunks "
        "(truncated=%d, empty=%d)",
        len(all_entities), len(all_triplets), len(chunks), n_truncated, n_empty,
    )
    if n_empty > 0 or n_truncated > 0:
        logger.info("[2.2] Per-chunk: %s", " | ".join(per_chunk_summary))
    return all_entities, all_triplets


async def _extract_one(
    chunk: Chunk,
    tree: HierarchyTree,
    chunk_to_leaf: dict[str, str],
    chunks: list[Chunk],
    chunk_index: dict[str, int],
    llm: LLMClient,
    template: str,
    config: Pass2Config,
) -> tuple[list[ExtractedEntity], list[ExtractedTriplet], dict]:
    context = build_context(chunk, chunk_to_leaf, tree, chunks, chunk_index, config)
    prompt = (
        template
        .replace("{global_context}", context)
        .replace("{text}", chunk.text)
    )

    raw = await llm.generate_async(
        llm.config.extraction, _INSTRUCTIONS, prompt, stage="pass2_extraction",
    )
    truncated = looks_truncated(raw)
    parsed = extract_json_partial(raw) or {}

    entities: list[ExtractedEntity] = []
    for e in parsed.get("entities") or []:
        if not isinstance(e, dict):
            continue
        canonical = (e.get("canonical_name") or e.get("mention_text") or "").strip()
        mention = (e.get("mention_text") or canonical).strip()
        if not canonical:
            continue
        entities.append(ExtractedEntity(
            mention_text=mention,
            canonical_name=canonical,
            entity_type=str(e.get("entity_type", "")).strip(),
            importance=_coerce_importance(e.get("importance")),
            rationale=str(e.get("rationale", "")).strip(),
            chunk_id=chunk.id,
        ))

    triplets: list[ExtractedTriplet] = []
    for t in parsed.get("triplets") or []:
        if not isinstance(t, dict):
            continue
        s = str(t.get("subject", "")).strip()
        p = str(t.get("predicate", "")).strip()
        o = str(t.get("object", "")).strip()
        if not (s and p and o):
            continue
        triplets.append(ExtractedTriplet(
            subject=s,
            predicate=p,
            object=o,
            importance=_coerce_importance(t.get("importance")),
            confidence=_coerce_unit(t.get("confidence"), default=1.0),
            evidence=str(t.get("evidence", "")).strip(),
            chunk_id=chunk.id,
        ))

    diag = {
        "raw_len": len(raw or ""),
        "truncated": truncated,
        "empty": (len(entities) == 0 and len(triplets) == 0),
        "n_entities": len(entities),
        "n_triplets": len(triplets),
    }
    if diag["empty"] or truncated:
        head = (raw or "")[:400].replace("\n", " ")
        tail = (raw or "")[-200:].replace("\n", " ") if raw else ""
        logger.warning(
            "[2.2] %s: e=%d t=%d truncated=%s raw_len=%d\n  HEAD: %r\n  TAIL: %r",
            chunk.id, len(entities), len(triplets), truncated, diag["raw_len"], head, tail,
        )
    return entities, triplets, diag


_DISCRETE_IMPORTANCE = {
    "core": 1.0,
    "section_specific": 0.7,
    "supporting": 0.5,
    "peripheral": 0.2,
}


def _coerce_importance(value) -> float:
    if isinstance(value, (int, float)):
        return max(0.0, min(1.0, float(value)))
    if isinstance(value, str):
        key = value.strip().lower()
        if key in _DISCRETE_IMPORTANCE:
            return _DISCRETE_IMPORTANCE[key]
        try:
            return max(0.0, min(1.0, float(key)))
        except ValueError:
            return 0.5
    return 0.5


def _coerce_unit(value, default: float = 1.0) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default
