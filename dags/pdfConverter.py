from pypdf import PdfReader
import io
import os
from ftpConnector import ftpConnector
from dbConnector import databaseConnector


def run_conversion() -> int:
    count = 0
    while True:
        url = databaseConnector.getPdfToConvertToLatex()
        if url is None:
            break
        try:
            file = ftpConnector.getFile(url)
            file.seek(0)
            reader = PdfReader(file)
            text = "".join(page.extract_text() or "" for page in reader.pages)
            tex_filename = os.path.splitext(url)[0] + '.tex'
            ftpConnector.storeFile(tex_filename, io.BytesIO(text.encode('utf-8')), 'Tex')
            databaseConnector.saveLatexFileLocation(url, tex_filename)
            count += 1
        except Exception as e:
            print(f"Failed to convert {url}: {e}")
            databaseConnector.saveLatexFileLocation(url, 'NA')
    return count
