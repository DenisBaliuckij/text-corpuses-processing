import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_downloading",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["pdfFiles"],
) as dag:

    @task()
    def downloadPdfFiles():
        # -*- coding: utf-8 -*-
        import requests
        import uuid
        import io
        from concurrent.futures import ThreadPoolExecutor
        from repositories.pdf_repository import PdfRepository
        from repositories.proxy_repository import ProxyRepository
        import ftpConnector
        from ftpConnector import ftpConnector

        TOTAL_URLS = 500
        # Every worker independently calls ProxyRepository.get_latest(), which
        # always returns the single shared paid proxy (BrightData) while its
        # row exists. Too much concurrency here means many workers open
        # simultaneous tunnels through that one proxy at once, which exceeds
        # its plan's concurrent-connection limit and causes real ProxyErrors
        # that get the proxy marked broken (and deleted) for everyone.
        CONCURRENCY = 3

        def storeFile(initialUrl, filename, file):
            ftpConnector.storeFile(filename, file)
            PdfRepository.save_location(initialUrl, filename)

        def downloadOne():
            proxieIp = None
            try:
                url = PdfRepository.get_next_to_download()
                if url is None:
                    return
                initialUrl = url

                if 'support' in url:
                    PdfRepository.save_location(initialUrl, "NA")
                    return
                if 'springer' in url:
                    # Excluded for now due to a known Springer-specific issue.
                    # Left pending (not marked NA) so it resumes automatically
                    # once re-enabled.
                    return
                filename = ""
                if 'arxiv' in url:
                    filename = 'arxiv/'
                if 'lenin' in url:
                    filename = 'cyberleninka/'
                if 'springer' in url:
                    filename = 'springer/'
                    url = url.replace('/article', 'content/pdf')
                if '#gujarati_literature' in url:
                    filename = 'gujarati/literature/'
                    url = url.split('#')[0]
                if '#gujarati_news' in url:
                    filename = 'gujarati/news/'
                    url = url.split('#')[0]
                if '#gujarati_science_natural' in url:
                    filename = 'gujarati/science_natural/'
                    url = url.split('#')[0]
                if '#gujarati_science_social' in url:
                    filename = 'gujarati/science_social/'
                    url = url.split('#')[0]
                filename += str(uuid.uuid4())
                filename += '.pdf'
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
                    ProxyRepository.mark_success(str(proxieIp).strip())

                else:
                    response = requests.get(url.replace('.pdf', '_reference.pdf'),
                              data=None,
                              headers={
                                  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                  },
                              proxies=proxies,
                              timeout=30)
                    if response.status_code == 200:
                        file = io.BytesIO(response.content)
                        storeFile(initialUrl, filename, file)
                        ProxyRepository.mark_success(str(proxieIp).strip())
                    else:
                        PdfRepository.save_location(initialUrl, "NA")
            except Exception as e:
                print(e)
                if proxieIp and isinstance(e, requests.exceptions.ProxyError):
                    ProxyRepository.mark_broken(str(proxieIp).strip())

        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = [executor.submit(downloadOne) for _ in range(TOTAL_URLS)]
            for future in futures:
                future.result()

    downloadPdfFiles()
