from __future__ import annotations

import json
from pathlib import Path


def load_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def save_text(text: str, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def save_json(data, path: str | Path, indent: int = 2) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=indent), encoding="utf-8")


def load_json(path: str | Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_prompt(path: str | Path) -> str:
    return load_text(path)
