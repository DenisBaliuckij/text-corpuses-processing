import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_conversion",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs = 1,
    tags=["latexFiles"],
) as dag:

    @task()
    def convertPdfFiles():
        # -*- coding: utf-8 -*-
        from pypdf import PdfReader
        import ftpConnector
        from ftpConnector import ftpConnector
        import dbConnector
        from dbConnector import databaseConnector
        import io
        
        i = 0
        while True:   
            i+=1
            urlToConvert = databaseConnector.getPdfToConvertToLatex()
            try:
                file = ftpConnector.getFile(urlToConvert)
                reader = PdfReader(file)
                text = ""
                for page in reader.pages:
                     text += page.extract_text()
                result = io.StringIO(text)
                filename = urlToConvert.replace('.pdf', '.tex')
                ftpConnector.storeFile(filename, io.BytesIO(result.read().encode('utf8')), 'Tex')
                print(urlToConvert)
                print(filename)
                databaseConnector.saveLatexFileLocation(urlToConvert, filename)
                if i>500:
                    break
            except Exception as e:
                databaseConnector.saveLatexFileLocation(urlToConvert, 'NA')


        
    convertPdfFiles()
