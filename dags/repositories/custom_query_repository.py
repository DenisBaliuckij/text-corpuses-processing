import pyodbc
from configs import getConfig


class CustomQueryRepository:
    @staticmethod
    def create(source_name, criterion_json, folder_name) -> int:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[CreateCustomQuery] @sourceName = ?, @criterionJson = ?, @folderName = ?",
            (source_name, criterion_json, folder_name)
        )
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return int(row[0])

    @staticmethod
    def get_pending() -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPendingCustomQueries]")
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows

    @staticmethod
    def add_pdf(custom_query_id, pdf_url) -> int:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddCustomQueryPdf] @customQueryId = ?, @pdfUrl = ?",
            (custom_query_id, pdf_url)
        )
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return int(row[0])

    @staticmethod
    def find_existing_download(base_url) -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[FindExistingDownload] @baseUrl = ?", (base_url,))
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def mark_pdf_status(pdf_id, status, destination_path=None):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[MarkCustomQueryPdfStatus] @id = ?, @status = ?, @destinationPath = ?",
            (pdf_id, status, destination_path)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def update_query_status(query_id, status, error=None):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[UpdateCustomQueryStatus] @id = ?, @status = ?, @error = ?",
            (query_id, status, error)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_status(query_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetCustomQueryStatus] @id = ?", (query_id,))
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def get_pdfs(query_id) -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetCustomQueryPdfs] @customQueryId = ?", (query_id,))
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows

    @staticmethod
    def get_recent() -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetRecentCustomQueries]")
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows

    @staticmethod
    def get_pending_downloads(query_id) -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPendingCustomQueryDownloads] @customQueryId = ?", (query_id,))
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows
