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
