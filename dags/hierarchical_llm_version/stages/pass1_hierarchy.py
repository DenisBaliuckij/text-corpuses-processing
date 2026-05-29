"""Steps 1.4-1.5 — bottom-up hierarchy construction + root aggregation."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..config_schema import Pass1Config
from ..models.embedder import Embedder
from ..models.llm_client import LLMClient
from ..schemas.common import KeyConcept
from ..schemas.hierarchy import HierarchyNode, HierarchyTree
from ..utils.io import load_prompt
from ..utils.json_parsing import extract_json_partial, looks_truncated
from .pass1_aggregate import aggregate_group, format_children
from .pass1_group import group_nodes

logger = logging.getLogger(__name__)

_ROOT_INSTRUCTIONS = (
    "Ты помощник по высокоуровневой агрегации. На вход даны summaries "
    "верхнеуровневых разделов корпуса. Сформируй один глобальный обзор "
    "всего корпуса. Возвращай ТОЛЬКО валидный JSON."
)


async def build_hierarchy(
    leaf_nodes: list[HierarchyNode],
    llm: LLMClient,
    config: Pass1Config,
    embedder: Embedder | None = None,
    base_dir: Path | None = None,
) -> HierarchyTree:
    if not leaf_nodes:
        raise ValueError("build_hierarchy: leaf_nodes is empty")

    agg_template = _load(config.aggregation_prompt, base_dir)
    root_template = _load(config.root_prompt, base_dir)

    all_nodes: dict[str, HierarchyNode] = {n.id: n for n in leaf_nodes}
    levels: dict[int, list[str]] = {1: [n.id for n in leaf_nodes]}

    current = list(leaf_nodes)
    level = 2

    while len(current) > config.max_top_level_nodes and level <= config.max_levels:
        groups = group_nodes(current, config.grouping, embedder)
        logger.info(
            "[1.4] level %d: %d nodes -> %d groups",
            level, len(current), len(groups),
        )

        # aggregate each group in parallel; concurrency is bounded inside LLMClient
        tasks = []
        for gi, idxs in enumerate(groups):
            children = [current[i] for i in idxs]
            tasks.append(
                aggregate_group(
                    children, f"h{level}_{gi}", level,
                    llm, llm.config.strong, agg_template,
                    config.max_chars_per_aggregation,
                    stage=f"pass1_aggregate_l{level}",
                )
            )
        new_nodes: list[HierarchyNode] = list(await asyncio.gather(*tasks))

        for new_node in new_nodes:
            for cid in new_node.children_ids:
                all_nodes[cid].parent_id = new_node.id
            all_nodes[new_node.id] = new_node

        levels[level] = [n.id for n in new_nodes]
        current = new_nodes
        level += 1

    # ── root ──────────────────────────────────────────────────────────────
    if len(current) == 1:
        # corpus collapses to one node already; treat it as the root.
        root_id = current[0].id
        return HierarchyTree(
            meta={"n_levels": max(levels.keys()), "n_nodes": len(all_nodes)},
            root_id=root_id,
            nodes=all_nodes,
            levels=levels,
        )

    root_id = "root"
    root = await _aggregate_root(
        current, root_id, level, llm, root_template,
        config.max_chars_per_aggregation,
    )
    for c in current:
        all_nodes[c.id].parent_id = root_id
    all_nodes[root_id] = root
    levels[level] = [root_id]

    return HierarchyTree(
        meta={"n_levels": max(levels.keys()), "n_nodes": len(all_nodes)},
        root_id=root_id,
        nodes=all_nodes,
        levels=levels,
    )


async def _aggregate_root(
    children: list[HierarchyNode],
    root_id: str,
    level: int,
    llm: LLMClient,
    root_template: str,
    max_chars: int,
) -> HierarchyNode:
    """Same recursive-splitting logic as aggregate_group but with root prompt."""
    serialized = format_children(children)

    if len(serialized) > max_chars and len(children) > 2:
        mid = len(children) // 2
        left, right = children[:mid], children[mid:]
        # use generic aggregation prompt for intermediate halves... but root
        # prompt is the only one we have here. Re-use it: it produces the
        # same JSON shape, just framed at corpus level — acceptable.
        sub_l = await _aggregate_root(left, f"{root_id}.a", level, llm, root_template, max_chars)
        sub_r = await _aggregate_root(right, f"{root_id}.b", level, llm, root_template, max_chars)
        serialized = format_children([sub_l, sub_r])
        node = await _aggregate_root_direct([sub_l, sub_r], serialized, root_id, level, llm, root_template)
        node.children_ids = [c.id for c in children]
        src: list[str] = []
        seen: set[str] = set()
        for c in children:
            for cid in c.source_chunk_ids:
                if cid not in seen:
                    src.append(cid)
                    seen.add(cid)
        node.source_chunk_ids = src
        node.input_tokens += sub_l.input_tokens + sub_r.input_tokens
        node.output_tokens += sub_l.output_tokens + sub_r.output_tokens
        return node

    return await _aggregate_root_direct(children, serialized, root_id, level, llm, root_template)


async def _aggregate_root_direct(
    children: list[HierarchyNode],
    serialized: str,
    root_id: str,
    level: int,
    llm: LLMClient,
    root_template: str,
) -> HierarchyNode:
    prompt = root_template.replace("{children}", serialized)

    in_before = llm.stats.input_tokens
    out_before = llm.stats.output_tokens

    raw = await llm.generate_async(
        llm.config.strong, _ROOT_INSTRUCTIONS, prompt, stage="pass1_root",
    )
    truncated = looks_truncated(raw)
    parsed = extract_json_partial(raw) or {}

    summary = (parsed.get("summary") or " ".join(c.summary for c in children)[:2000]).strip()
    if not parsed.get("summary"):
        head = (raw or "")[:300].replace("\n", " ")
        logger.warning(
            "[pass1_root] parse-empty. truncated=%s raw_len=%d head=%r",
            truncated, len(raw or ""), head,
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
        id=root_id,
        level=level,
        summary=summary,
        topic=topic,
        subtopics=subtopics,
        key_concepts=concepts,
        children_ids=[c.id for c in children],
        source_chunk_ids=src,
        input_tokens=in_after - in_before,
        output_tokens=out_after - out_before,
    )


def _load(rel: str, base_dir: Path | None) -> str:
    p = Path(rel)
    if base_dir and not p.is_absolute():
        p = base_dir / p
    return load_prompt(p)
