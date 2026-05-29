from __future__ import annotations

import logging

from ..config_schema import LLMModelConfig
from ..models.llm_client import LLMClient
from ..schemas.common import KeyConcept
from ..schemas.hierarchy import HierarchyNode
from ..utils.json_parsing import extract_json_partial, looks_truncated

logger = logging.getLogger(__name__)

_INSTRUCTIONS = (
    "Ты помощник по агрегации. Объединяй summaries дочерних узлов в более "
    "общий обзор. Возвращай ТОЛЬКО валидный JSON без markdown-форматирования."
)


def format_children(children: list[HierarchyNode]) -> str:
    parts: list[str] = []
    for i, c in enumerate(children, 1):
        kc = ", ".join(f"{k.name} ({k.importance})" for k in c.key_concepts)
        block = (
            f"[{i}] topic: {c.topic}\n"
            f"summary: {c.summary}"
        )
        if kc:
            block += f"\nkey_concepts: {kc}"
        if c.subtopics:
            block += f"\nsubtopics: {', '.join(c.subtopics)}"
        parts.append(block)
    return "\n\n".join(parts)


async def aggregate_group(
    children: list[HierarchyNode],
    new_id: str,
    new_level: int,
    llm: LLMClient,
    model_cfg: LLMModelConfig,
    template: str,
    max_chars: int,
    stage: str = "pass1_aggregate",
) -> HierarchyNode:
    serialized = format_children(children)
    if len(serialized) <= max_chars or len(children) <= 2:
        node = await _aggregate_direct(
            children, serialized, new_id, new_level,
            llm, model_cfg, template, stage,
        )
        return node

    mid = len(children) // 2
    left, right = children[:mid], children[mid:]
    sub_left = await aggregate_group(
        left, f"{new_id}.a", new_level, llm, model_cfg, template, max_chars, stage,
    )
    sub_right = await aggregate_group(
        right, f"{new_id}.b", new_level, llm, model_cfg, template, max_chars, stage,
    )

    merged_serialized = format_children([sub_left, sub_right])
    final = await _aggregate_direct(
        [sub_left, sub_right], merged_serialized, new_id, new_level,
        llm, model_cfg, template, stage,
    )
    final.children_ids = [c.id for c in children]
    src: list[str] = []
    seen: set[str] = set()
    for c in children:
        for cid in c.source_chunk_ids:
            if cid not in seen:
                src.append(cid)
                seen.add(cid)
    final.source_chunk_ids = src

    final.input_tokens += sub_left.input_tokens + sub_right.input_tokens
    final.output_tokens += sub_left.output_tokens + sub_right.output_tokens
    return final


async def _aggregate_direct(
    children: list[HierarchyNode],
    serialized: str,
    new_id: str,
    new_level: int,
    llm: LLMClient,
    model_cfg: LLMModelConfig,
    template: str,
    stage: str,
) -> HierarchyNode:
    prompt = template.replace("{children}", serialized)

    in_before = llm.stats.input_tokens
    out_before = llm.stats.output_tokens

    raw = await llm.generate_async(model_cfg, _INSTRUCTIONS, prompt, stage=stage)
    truncated = looks_truncated(raw)
    parsed = extract_json_partial(raw) or {}

    fallback_summary = " ".join(c.summary for c in children)[:1500]
    summary = (parsed.get("summary") or fallback_summary).strip()
    if not parsed.get("summary"):
        head = (raw or "")[:300].replace("\n", " ")
        logger.warning(
            "[%s] %s: parse-empty (summary missing). truncated=%s raw_len=%d head=%r",
            stage, new_id, truncated, len(raw or ""), head,
        )
    topic = str(parsed.get("topic") or "").strip()

    raw_subs = parsed.get("subtopics") or []
    if not isinstance(raw_subs, list):
        raw_subs = []
    subtopics = [str(s).strip() for s in raw_subs if str(s).strip()]

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

    src: list[str] = []
    seen: set[str] = set()
    for c in children:
        for cid in c.source_chunk_ids:
            if cid not in seen:
                src.append(cid)
                seen.add(cid)

    in_after = llm.stats.input_tokens
    out_after = llm.stats.output_tokens

    return HierarchyNode(
        id=new_id,
        level=new_level,
        summary=summary,
        topic=topic,
        subtopics=subtopics,
        key_concepts=concepts,
        children_ids=[c.id for c in children],
        source_chunk_ids=src,
        input_tokens=in_after - in_before,
        output_tokens=out_after - out_before,
    )
