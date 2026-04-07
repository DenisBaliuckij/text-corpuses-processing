import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_downloading",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs = 1,
    tags=["pdfFiles"],
) as dag:

    @task()
    def downloadPdfFiles():
# -*- coding: utf-8 -*-
        from pathlib import Path
        import requests
        import pyodbc 
        import ftplib
        import uuid
        import io
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector
        i = 0
        def storeFile(initialUrl,filename, file):
            ftpConnector.storeFile(filename, file)
            databaseConnector.savePdfFileLocation(initialUrl, filename)
        
        
        while i<500:
            try:
                url = databaseConnector.getPdfToDownload()
                initialUrl = url
        
                if 'support' in url:
                    databaseConnector.savePdfFileLocation(initialUrl, "NA")
                    continue
                filename=""
                if 'arxiv' in url:
                    filename = 'arxiv/'
                if 'springer' in url:
                    filename = 'springer/'
                    url = url.replace('/article', 'content/pdf')
                filename+=str(uuid.uuid4())
                filename+='.pdf'
                proxieResult = databaseConnector.getLatestProxy()
                proxieIp = proxieResult["proxieIp"]
                proxiePort = proxieResult["proxiePort"]
                proxieProtocol = proxieResult["proxieProtocol"]
                proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                           'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}
                print(proxies)
                response = requests.get(url, 
                                    data=None, 
                                    headers={
                                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                        }, 
                                    proxies=proxies, 
                                    timeout=30)
                if response.status_code == 200:
                    file = io.BytesIO(response.content)
                    storeFile(initialUrl, filename, file)
        
                else:
                    url = url.replace('.pdf', '_reference.pdf')
                    response = requests.get(url,
                              data=None, 
                              headers={
                                  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                  }, 
                              proxies=proxies, 
                              timeout=30)
                    if response.status_code == 200:
                        file = io.BytesIO(response.content)
                        storeFile(initialUrl, filename, file)
                    else:
                        databaseConnector.savePdfFileLocation(initialUrl, "NA")
            except Exception as e:
                print(e)
                databaseConnector.markProxyAsBroken(str(proxieIp).strip())
                continue
            i+=1


        
    downloadPdfFiles()
