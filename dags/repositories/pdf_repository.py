import time
import pyodbc
from configs import getConfig

_DEADLOCK_RETRIES = 3


def _exec_write(sql, params=()):
    # SQL Server error 40001 = deadlock victim; retry with back-off.
    # Without this, a transient deadlock during save_location() silently
    # loses the result of an already-completed FTP upload: the file exists
    # on disk, but LocationInFileSystem never gets recorded, leaving the
    # row "pending" forever and the file an orphan.
    for attempt in range(_DEADLOCK_RETRIES):
        try:
            cnxn = pyodbc.connect(getConfig()['ConnectionString'])
            cursor = cnxn.cursor()
            cursor.execute(sql, params)
            cnxn.commit()
            cursor.close()
            cnxn.close()
            return
        except pyodbc.Error as e:
            if e.args[0] == '40001' and attempt < _DEADLOCK_RETRIES - 1:
                time.sleep(0.1 * (attempt + 1))
            else:
                raise


def _exec_read_one(sql, params=()):
    # Same retry semantics as _exec_write. GetPdfToDownload is itself a
    # write (it atomically claims a row), so it can deadlock too.
    for attempt in range(_DEADLOCK_RETRIES):
        try:
            cnxn = pyodbc.connect(getConfig()['ConnectionString'])
            cursor = cnxn.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            cnxn.commit()
            cursor.close()
            cnxn.close()
            return row
        except pyodbc.Error as e:
            if e.args[0] == '40001' and attempt < _DEADLOCK_RETRIES - 1:
                time.sleep(0.1 * (attempt + 1))
            else:
                raise


class PdfRepository:
    @staticmethod
    def add_url(url):
        _exec_write("execute [dbo].[AddPdfUrl] @url = ?", (url,))

    @staticmethod
    def add_urls(urls: list) -> None:
        if not urls:
            return
        for url in urls:
            _exec_write("execute [dbo].[AddPdfUrl] @url = ?", (url,))

    @staticmethod
    def get_next_to_download() -> str | None:
        row = _exec_read_one("execute [dbo].[GetPdfToDownload]")
        return row[0] if row else None

    @staticmethod
    def save_location(url, location):
        _exec_write(
            "execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?",
            (url, location)
        )
