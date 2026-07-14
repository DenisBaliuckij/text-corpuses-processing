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
        # deterministically returns the single highest-SuccessCount proxy -
        # so many workers can still concentrate load on one "champion" free
        # proxy at once. Raised from 8 to 16 (2026-07-14) after the Gujarati/
        # Russian/English source expansion turned most of the 500 per-run
        # slots into real (slow) downloads instead of instant no-ops, which
        # collapsed hourly throughput; keep an eye on whether the current
        # champion proxy starts failing under this load before raising further.
        # Being stepped up further (2026-07-14 evening) towards a ~85% host
        # utilization target, watching load average/memory/docker stats
        # between steps - see project memory for the step log.
        CONCURRENCY = 32

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
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_news' in url:
                    filename = 'gujarati/news/'
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_science_natural' in url:
                    filename = 'gujarati/science_natural/'
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_science_social' in url:
                    filename = 'gujarati/science_social/'
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_law' in url:
                    filename = 'gujarati/law/'
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_official' in url:
                    filename = 'gujarati/official/'
                    url = url.rsplit('#', 1)[0]
                if '#gujarati_dictionary' in url:
                    filename = 'gujarati/dictionary/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_science' in url:
                    filename = 'russian/science/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_literature_modern' in url:
                    filename = 'russian/literature_modern/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_literature_classic' in url:
                    filename = 'russian/literature_classic/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_news' in url:
                    filename = 'russian/news/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_law' in url:
                    filename = 'russian/law/'
                    url = url.rsplit('#', 1)[0]
                if '#russian_social_science' in url:
                    filename = 'russian/social_science/'
                    url = url.rsplit('#', 1)[0]
                if '#english_science' in url:
                    filename = 'english/science/'
                    url = url.rsplit('#', 1)[0]
                if '#english_literature_modern' in url:
                    filename = 'english/literature_modern/'
                    url = url.rsplit('#', 1)[0]
                if '#english_literature_classic' in url:
                    filename = 'english/literature_classic/'
                    url = url.rsplit('#', 1)[0]
                if '#english_news' in url:
                    filename = 'english/news/'
                    url = url.rsplit('#', 1)[0]
                if '#english_law' in url:
                    filename = 'english/law/'
                    url = url.rsplit('#', 1)[0]
                if '#english_social_science' in url:
                    filename = 'english/social_science/'
                    url = url.rsplit('#', 1)[0]
                if '#customquery_' in url:
                    tag = url.rsplit('#customquery_', 1)[1]
                    filename = f'custom/{tag}/'
                    url = url.rsplit('#', 1)[0]
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
