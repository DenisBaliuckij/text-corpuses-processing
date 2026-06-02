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
        import requests
        import uuid
        import io
        from repositories.pdf_repository import PdfRepository
        from repositories.proxy_repository import ProxyRepository
        import ftpConnector
        from ftpConnector import ftpConnector
        i = 0
        def storeFile(initialUrl,filename, file):
            ftpConnector.storeFile(filename, file)
            PdfRepository.save_location(initialUrl, filename)


        while i<500:
            try:
                url = PdfRepository.get_next_to_download()
                initialUrl = url

                if 'support' in url:
                    PdfRepository.save_location(initialUrl, "NA")
                    continue
                filename=""
                if 'arxiv' in url:
                    filename = 'arxiv/'
                if 'lenin' in url:
                    filename = 'cyberleninka/'
                if 'springer' in url:
                    filename = 'springer/'
                    url = url.replace('/article', 'content/pdf')
                filename+=str(uuid.uuid4())
                filename+='.pdf'
                proxieResult = ProxyRepository.get_latest()
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
                    urdockl = url.replace('.pdf', '_reference.pdf')
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
                        PdfRepository.save_location(initialUrl, "NA")
            except Exception as e:
                print(e)
                ProxyRepository.mark_broken(str(proxieIp).strip())
                continue
            i+=1


        
    downloadPdfFiles()
