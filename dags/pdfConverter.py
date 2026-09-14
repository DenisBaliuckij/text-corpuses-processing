import os
os.environ.setdefault('OMP_NUM_THREADS', '2')
os.environ.setdefault('MKL_NUM_THREADS', '2')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '2')
os.environ.setdefault('NUMEXPR_NUM_THREADS', '2')

import io
import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

from ftpConnector import ftpConnector
from repositories.latex_repository import LatexRepository
from sciparse.pipeline import load_config, run
from sciparse.substitute import linearize

CONCURRENCY = 2


def _convert_one(phase: int | None = None):
    """Returns None if the queue is empty, else True/False for success/failure."""
    url = (
        LatexRepository.get_next_to_convert_filtered(phase)
        if phase is not None
        else LatexRepository.get_next_to_convert()
    )
    if url is None:
        return None
    try:
        file = ftpConnector.getFile(url)
        file.seek(0)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
            tmp_pdf.write(file.read())
            tmp_pdf_path = tmp_pdf.name
        out_dir = tempfile.mkdtemp(prefix="sciparse_conv_")
        try:
            cfg = load_config()
            cfg.layout_backend = "pymupdf"
            cfg.ocr_backend = "tesseract"
            cfg.escalation_mode = "ollama"
            cfg.escalation_ollama_model = "qwen3:14b"
            cfg.escalation_ollama_host = "http://host.docker.internal:11434"
            cfg.formula_desc_model = "qwen3:14b"
            cfg.formula_desc_ollama_host = "http://host.docker.internal:11434"
            cfg.formula_desc_batch_size = 5
            cfg.formula_primary = "pix2tex"
            cfg.formula_secondary = "ruleocr"
            cfg.table_primary = "ollama"
            cfg.figure_describer = "ollama"
            cfg.figure_vlm_model = "moondream"
            cfg.figure_vlm_host = "http://host.docker.internal:11434"
            # Groundedness (number/entity/off-topic-phrase checking) is the
            # only reliably-discriminating signal for a VLM-primary
            # description: chart_replot is structurally neutral once we
            # stop trusting the bar/pie de-render heuristic for non-chart
            # imagery, and regeneration_agreement's raw-token-overlap check
            # against OCR/Tesseract text chronically underscores even
            # verifiably accurate descriptions (different vocabulary between
            # natural prose and literal OCR fragments). Reproduced live: a
            # confirmed-accurate, fully-grounded figure description capped
            # at confidence ~0.74-0.86 under the default weights, just
            # short of the 0.80 accept threshold, purely from those two
            # weak signals. Hard-fail veto on groundedness still applies
            # before this weighting, so real hallucinations are unaffected.
            cfg.figure_weights = {
                "groundedness": 0.60,
                "chart_replot": 0.15,
                "regeneration_agreement": 0.10,
                "caption_entailment": 0.15,
            }
            document = run(tmp_pdf_path, out_dir, config=cfg)
            text = linearize(document)
        finally:
            os.remove(tmp_pdf_path)
            shutil.rmtree(out_dir, ignore_errors=True)

        tex_filename = os.path.splitext(url)[0] + '.tex'
        ftpConnector.storeFile(tex_filename, io.BytesIO(text.encode('utf-8')), 'Tex')
        LatexRepository.save_location(url, tex_filename)
        return True
    except Exception as e:
        print(f"Failed to convert {url}: {e}")
        LatexRepository.save_location(url, 'NA')
        return False


def run_conversion(phase: int | None = None) -> int:
    count_lock = threading.Lock()
    count = 0

    def worker():
        nonlocal count
        while True:
            result = _convert_one(phase)
            if result is None:
                return
            if result:
                with count_lock:
                    count += 1

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(worker) for _ in range(CONCURRENCY)]
        for future in futures:
            future.result()

    return count
