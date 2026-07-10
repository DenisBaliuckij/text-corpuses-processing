from pypdf import PdfReader
import io
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from ftpConnector import ftpConnector
from repositories.latex_repository import LatexRepository

CONCURRENCY = 8


def _convert_one():
    """Returns None if the queue is empty, else True/False for success/failure."""
    url = LatexRepository.get_next_to_convert()
    if url is None:
        return None
    try:
        file = ftpConnector.getFile(url)
        file.seek(0)
        reader = PdfReader(file)
        text = "".join(page.extract_text() or "" for page in reader.pages)
        tex_filename = os.path.splitext(url)[0] + '.tex'
        ftpConnector.storeFile(tex_filename, io.BytesIO(text.encode('utf-8')), 'Tex')
        LatexRepository.save_location(url, tex_filename)
        return True
    except Exception as e:
        print(f"Failed to convert {url}: {e}")
        LatexRepository.save_location(url, 'NA')
        return False


def run_conversion() -> int:
    count_lock = threading.Lock()
    count = 0

    def worker():
        nonlocal count
        while True:
            result = _convert_one()
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
