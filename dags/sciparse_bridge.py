"""Bridges langembed's document-normalization output into sciparse's existing LaTeX
conversion queue: uploads a normalized PDF to FTP, registers it via
LatexRepository.register_for_conversion, polls until pdf_conversion (the existing
Airflow DAG) has processed it, fetches the resulting .tex, and strips it to plain
text. Runs natively inside the airflow-worker container (not via SSH/docker run) --
this is pure DB/FTP orchestration with no GPU/heavy-compute cost, and needs direct
access to this repo's own configs.py/ftpConnector.py/repositories. See "Why SSH, not
DockerOperator" in langembed's
docs/superpowers/specs/2026-08-31-full-pipeline-dag-design.md for why the
compute-heavy tasks go through SSH while this one stays in-worker.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from ftpConnector import ftpConnector
from repositories.latex_repository import LatexRepository
from tex_to_text import tex_to_text

BRIDGE_FTP_PREFIX = "langembed_bridge"


class ConversionTimeoutError(RuntimeError):
    pass


def register_and_wait(
    pdf_path: Path, lang: str, poll_interval_s: int = 15, timeout_s: int = 3600
) -> str:
    """Uploads `pdf_path` to FTP, registers it for LaTeX conversion, and blocks until
    pdf_conversion has produced a .tex file for it (or raises ConversionTimeoutError
    after `timeout_s`). Returns the .tex file's plain-text content (already stripped
    via tex_to_text)."""
    remote_pdf_path = f"{BRIDGE_FTP_PREFIX}/{lang}/{pdf_path.name}"
    with pdf_path.open("rb") as f:
        ftpConnector.storeFile(remote_pdf_path, f)

    LatexRepository.register_for_conversion(
        pdf_url=f"langembed-bridge:{lang}/{pdf_path.name}",
        location_in_filesystem=remote_pdf_path,
    )

    deadline = time.monotonic() + timeout_s
    latex_location = None
    while time.monotonic() < deadline:
        latex_location = LatexRepository.get_latex_location(remote_pdf_path)
        if latex_location:
            break
        time.sleep(poll_interval_s)

    if not latex_location:
        raise ConversionTimeoutError(f"{remote_pdf_path} was not converted within {timeout_s}s")

    tex_bytes = ftpConnector.getFile(latex_location, "Tex")
    tex_content = tex_bytes.getvalue().decode("utf-8", errors="replace")
    return tex_to_text(tex_content)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--pdf", required=True, type=Path)
    ap.add_argument("--lang", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--poll-interval-s", type=int, default=15)
    ap.add_argument("--timeout-s", type=int, default=3600)
    args = ap.parse_args()

    text = register_and_wait(args.pdf, args.lang, args.poll_interval_s, args.timeout_s)
    args.out.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
