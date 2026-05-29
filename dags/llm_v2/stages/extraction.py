from __future__ import annotations

import re
from pathlib import Path

from tqdm import tqdm

from ..config_schema import ExtractionConfig
from ..models.llm_client import LLMClient
from ..schemas.common import Chunk, RawTriplet
from ..utils.io import load_prompt


def extract_triplets(
    chunks: list[Chunk],
    llm: LLMClient,
    config: ExtractionConfig,
    base_dir: Path | None = None,
) -> list[RawTriplet]:
    prompt_path = Path(config.prompt_file)
    if base_dir and not prompt_path.is_absolute():
        prompt_path = base_dir / prompt_path
    template = load_prompt(prompt_path)

    triplets: list[RawTriplet] = []

    for chunk in tqdm(chunks, desc="Extracting triplets"):
        prompt = template.format(text=chunk.text)
        output = llm.generate(prompt)
        parsed = _parse_pipe_output(output, chunk.id)
        triplets.extend(parsed)

    return triplets


_PIPE_RE = re.compile(r"^(.+?)\s*\|\s*(.+?)\s*\|\s*(.+)$")


def _parse_pipe_output(text: str, chunk_id: str) -> list[RawTriplet]:
    results: list[RawTriplet] = []
    for line in text.strip().splitlines():
        line = line.strip().lstrip("- ").lstrip("0123456789.)").strip()
        if not line:
            continue
        m = _PIPE_RE.match(line)
        if m:
            subj, rel, obj = (g.strip().strip("\"'") for g in m.groups())
            if subj and rel and obj:
                results.append(
                    RawTriplet(
                        subject=subj, relation=rel, object=obj, chunk_id=chunk_id
                    )
                )
    return results
