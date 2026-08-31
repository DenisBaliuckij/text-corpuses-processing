"""Strips LaTeX markup from sciparse's .tex conversion output, leaving clean prose
text suitable as a langembed training corpus. Not exhaustive LaTeX parsing -- targets
the constructs sciparse itself emits rather than being a general-purpose LaTeX engine.

Deliberate small duplicate of langembed/src/langembed/data/tex_to_text.py -- kept here
so sciparse_bridge.py has no cross-repo runtime dependency for this ~20-line pure
function. See that file's plan task for the reasoning.
"""

from __future__ import annotations

import re

_COMMENT_RE = re.compile(r"(?<!\\)%.*$", re.MULTILINE)
_DISPLAY_MATH_RE = re.compile(r"\$\$.*?\$\$|\\\[.*?\\\]", re.DOTALL)
_INLINE_MATH_RE = re.compile(r"\$[^$]*\$")
_COMMAND_WITH_ARG_RE = re.compile(r"\\[a-zA-Z]+\*?(\[[^\]]*\])?\{([^{}]*)\}")
_BARE_COMMAND_RE = re.compile(r"\\[a-zA-Z]+\*?")
_WHITESPACE_RE = re.compile(r"[ \t]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")


def tex_to_text(tex: str) -> str:
    text = _COMMENT_RE.sub("", tex)
    text = _DISPLAY_MATH_RE.sub(" ", text)
    text = _INLINE_MATH_RE.sub(" ", text)

    prev = None
    while prev != text:
        prev = text
        text = _COMMAND_WITH_ARG_RE.sub(lambda m: m.group(2), text)

    text = _BARE_COMMAND_RE.sub(" ", text)
    text = text.replace("{", " ").replace("}", " ")
    text = _WHITESPACE_RE.sub(" ", text)
    text = _BLANK_LINES_RE.sub("\n\n", text)
    return text.strip()
