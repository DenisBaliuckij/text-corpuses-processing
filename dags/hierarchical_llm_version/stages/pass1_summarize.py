"""Step 1.1 — chunk summaries (level-1 leaves of the hierarchy)."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..config_schema import Pass1Config
from ..models.llm_client import LLMClient
from ..schemas.common import Chunk, KeyConcept
from ..schemas.hierarchy import HierarchyNode
from ..utils.io import load_prompt
from ..utils.json_parsing import extract_json_partial, looks_truncated

logger = logging.getLogger(__name__)

_INSTRUCTIONS = (
    "Ты помощник по анализу текстов. Твоя задача — выдать краткое summary, "
    "тематику и ключевые концепты фрагмента. Возвращай ТОЛЬКО валидный JSON, "
    "без markdown-форматирования и пояснений."
)


async def summarize_chunks(
    chunks: list[Chunk],
    llm: LLMClient,
    config: Pass1Config,
    base_dir: Path | None = None,
) -> list[HierarchyNode]:
    """Build level-1 HierarchyNode for each chunk in parallel."""
    if not chunks:
        return []

    prompt_path = Path(config.summary_prompt)
    if base_dir and not prompt_path.is_absolute():
        prompt_path = base_dir / prompt_path
    template = load_prompt(prompt_path)

    tasks = [
        _summarize_one(chunk, idx, llm, template)
        for idx, chunk in enumerate(chunks)
    ]
    nodes = await asyncio.gather(*tasks)
    logger.info("[1.1] Summarized %d chunks", len(nodes))
    return list(nodes)


async def _summarize_one(
    chunk: Chunk, idx: int, llm: LLMClient, template: str
) -> HierarchyNode:
    prompt = template.replace("{text}", chunk.text)

    in_before = llm.stats.input_tokens
    out_before = llm.stats.output_tokens

    raw = await llm.generate_async(
        llm.config.cheap, _INSTRUCTIONS, prompt, stage="pass1_summarize"
    )
    truncated = looks_truncated(raw)
    parsed = extract_json_partial(raw) or {}

    summary = (parsed.get("summary") or chunk.text[:240]).strip()
    topic = str(parsed.get("topic") or "").strip()
    if not parsed or (not parsed.get("summary") and not parsed.get("key_concepts")):
        head = (raw or "")[:300].replace("\n", " ")
        logger.warning(
            "[1.1] %s: parse-empty (summary missing). truncated=%s raw_len=%d head=%r",
            chunk.id, truncated, len(raw or ""), head,
        )
    elif truncated:
        logger.info(
            "[1.1] %s: response truncated but recovered (summary=%d chars, concepts=%d)",
            chunk.id, len(summary), len(parsed.get("key_concepts") or []),
        )

    concepts: list[KeyConcept] = []
    for c in parsed.get("key_concepts") or []:
        if isinstance(c, str):
            if c.strip():
                concepts.append(KeyConcept(name=c.strip()))
        elif isinstance(c, dict) and c.get("name"):
            imp = c.get("importance", "supporting")
            if imp not in ("core", "supporting", "peripheral"):
                imp = "supporting"
            concepts.append(KeyConcept(name=str(c["name"]).strip(), importance=imp))

    in_after = llm.stats.input_tokens
    out_after = llm.stats.output_tokens

    return HierarchyNode(
        id=f"h1_{idx}",
        level=1,
        summary=summary,
        topic=topic,
        key_concepts=concepts,
        chunk_id=chunk.id,
        source_chunk_ids=[chunk.id],
        input_tokens=in_after - in_before,
        output_tokens=out_after - out_before,
    )
