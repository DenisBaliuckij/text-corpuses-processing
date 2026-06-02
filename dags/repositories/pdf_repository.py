import pyodbc
from configs import getConfig


class PdfRepository:
    @staticmethod
    def add_url(url):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddPdfUrl] @url = ?", (url,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_next_to_download() -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPdfToDownload]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def save_location(url, location):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?",
            (url, location)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()
