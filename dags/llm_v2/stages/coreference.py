from __future__ import annotations

from pathlib import Path

from ..config_schema import CoreferenceConfig
from ..models.llm_client import LLMClient
from ..schemas.common import Sentence
from ..utils.io import load_prompt


def resolve_coreferences(
    sentences: list[Sentence],
    llm: LLMClient,
    config: CoreferenceConfig,
    base_dir: Path | None = None,
) -> tuple[str, list[Sentence]]:
    if not config.enabled or not sentences:
        full_text = " ".join(s.text for s in sentences)
        return full_text, sentences

    prompt_path = Path(config.prompt_file)
    if base_dir and not prompt_path.is_absolute():
        prompt_path = base_dir / prompt_path
    template = load_prompt(prompt_path)

    ctx = config.context_sentences
    win = config.window_sentences
    resolved_parts: list[str] = []
    idx = 0

    while idx < len(sentences):
        context_texts = [
            resolved_parts[i] if i < len(resolved_parts) else sentences[i].text
            for i in range(max(0, idx - ctx), idx)
        ]
        context_str = " ".join(context_texts) if context_texts else "(начало текста)"

        window = sentences[idx : idx + win]
        window_text = " ".join(s.text for s in window)

        prompt = template.format(context=context_str, text=window_text)
        output = llm.generate(prompt).strip()

        output = output.split("\n\n")[0].strip()
        if not output:
            output = window_text

        resolved_parts.extend(
            [s.strip() for s in _split_roughly(output, len(window))]
        )
        idx += win

    resolved_parts = resolved_parts[: len(sentences)]

    resolved_text = " ".join(resolved_parts)

    from .preprocessing import preprocess

    lang = "ru" if "НЕ переписывай" in template else "en"
    new_sentences = preprocess(resolved_text, language=lang)
    return resolved_text, new_sentences


def _split_roughly(text: str, expected: int) -> list[str]:
    import re

    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    if len(parts) >= expected:
        return parts[:expected]
    return parts if parts else [text]
