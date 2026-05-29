from __future__ import annotations

import json
import re

_FENCE_RE = re.compile(r"```(?:json)?\s*(.+?)\s*```", re.DOTALL | re.IGNORECASE)


def extract_json(text: str):
    """Strict JSON parse from LLM output. Returns dict/list or None."""
    if not text:
        return None

    candidates: list[str] = []
    m = _FENCE_RE.search(text)
    if m:
        candidates.append(m.group(1).strip())
    candidates.append(text.strip())
    sliced = _slice_first_object(text)
    if sliced:
        candidates.append(sliced)

    for s in candidates:
        if not s:
            continue
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            continue
    return None


def extract_json_partial(text: str) -> dict | None:
    if not text:
        return None

    direct = extract_json(text)
    if isinstance(direct, dict):
        return direct
    if isinstance(direct, list):
        return {"_list": direct}
    return _recover_partial_object(text)


def looks_truncated(text: str) -> bool:
    if not text:
        return False
    s = text.rstrip().rstrip("`").rstrip()
    return not s.endswith("}") and not s.endswith("]")


_ARRAY_KEYS = ("entities", "triplets", "key_concepts", "subtopics")
_STRING_KEYS = ("summary", "topic")


def _recover_partial_object(text: str) -> dict | None:
    result: dict = {}
    for key in _ARRAY_KEYS:
        items = _extract_array_items(text, key)
        if items:
            result[key] = items
    for key in _STRING_KEYS:
        val = _extract_string_field(text, key)
        if val is not None:
            result[key] = val
    return result if result else None


def _extract_array_items(text: str, key: str) -> list:
    pattern = re.compile(rf'"{re.escape(key)}"\s*:\s*\[')
    m = pattern.search(text)
    if not m:
        return []

    i = m.end()
    n = len(text)
    items: list = []

    while i < n:
        while i < n and text[i] in " \t\r\n,":
            i += 1
        if i >= n or text[i] == "]":
            break
        ch = text[i]

        if ch == "{":
            end = _find_matching(text, i, "{", "}")
            if end is None:
                break
            try:
                items.append(json.loads(text[i:end]))
            except json.JSONDecodeError:
                repaired = _try_repair_object(text[i:end])
                if repaired is not None:
                    items.append(repaired)
            i = end

        elif ch == "[":
            end = _find_matching(text, i, "[", "]")
            if end is None:
                break
            try:
                items.append(json.loads(text[i:end]))
            except json.JSONDecodeError:
                pass
            i = end

        elif ch == '"':
            end = _find_string_close(text, i + 1)
            if end is None:
                break
            try:
                items.append(json.loads(text[i:end + 1]))
            except json.JSONDecodeError:
                pass
            i = end + 1

        else:
            j = i
            while j < n and text[j] not in ",]} \t\r\n":
                j += 1
            try:
                items.append(json.loads(text[i:j]))
            except json.JSONDecodeError:
                pass
            i = j

    return items


def _extract_string_field(text: str, key: str) -> str | None:
    pattern = re.compile(rf'"{re.escape(key)}"\s*:\s*"')
    m = pattern.search(text)
    if not m:
        return None
    open_quote = m.end() - 1
    close = _find_string_close(text, open_quote + 1)
    if close is None:
        return _best_effort_string(text[open_quote + 1:])
    try:
        return json.loads(text[open_quote:close + 1])
    except json.JSONDecodeError:
        return _best_effort_string(text[open_quote + 1:close])


def _find_matching(text: str, start: int, open_ch: str, close_ch: str) -> int | None:
    """Index *after* the matching close bracket, or None if truncated."""
    depth = 0
    in_str = False
    esc = False
    n = len(text)
    for i in range(start, n):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return i + 1
    return None


def _find_string_close(text: str, start: int) -> int | None:
    n = len(text)
    esc = False
    for i in range(start, n):
        ch = text[i]
        if esc:
            esc = False
        elif ch == "\\":
            esc = True
        elif ch == '"':
            return i
    return None


def _try_repair_object(s: str):
    cleaned = s.rstrip()
    if cleaned.endswith(","):
        cleaned = cleaned[:-1]
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def _best_effort_string(s: str) -> str:
    try:
        return json.loads('"' + s.replace('"', '\\"') + '"')
    except json.JSONDecodeError:
        return s


def _slice_first_object(text: str) -> str | None:
    start = -1
    open_ch = None
    for i, ch in enumerate(text):
        if ch in "{[":
            start = i
            open_ch = ch
            break
    if start < 0:
        return None
    close_ch = "}" if open_ch == "{" else "]"
    end = _find_matching(text, start, open_ch, close_ch)
    if end is None:
        return None
    return text[start:end]
