import pyodbc
from configs import getConfig


class LatexRepository:
    @staticmethod
    def get_next_to_convert() -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPDFLocationForLatexConvertation]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def get_next_to_convert_filtered(phase: int) -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[GetPDFLocationForLatexConvertationFiltered] @phase = ?",
            (phase,)
        )
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def save_location(url, location):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SaveLatexDocumentLocation] @pdfUrl = ?, @latexLocation=?",
            (url, location)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def register_for_conversion(pdf_url: str, location_in_filesystem: str) -> None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[RegisterPdfForLatexConversion] @pdfUrl = ?, @locationInFileSystem = ?",
            (pdf_url, location_in_filesystem)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_latex_location(pdf_location: str) -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT LatexLocation FROM dbo.LatexDocuments WHERE PDFLocation = ?",
            (pdf_location,)
        )
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        if row is None or not row[0]:
            return None
        return row[0]
