from datetime import timedelta

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

    # Guaranteed recovery for the 2026-07-17 stuck-task incidents: a
    # ThreadPoolExecutor worker can hang forever in a blocking syscall
    # (observed with SOCKS4/5 proxies not honoring their timeout) despite
    # downloadOne()'s internal ~30s timeouts, and Python cannot forcibly
    # kill a thread stuck mid-syscall - the process itself would just sit
    # there indefinitely, occupying the one max_active_runs=1 slot, with
    # no automatic recovery. execution_timeout operates at the OS process
    # level (Airflow sends SIGTERM/SIGKILL from outside the process), so
    # it terminates the task regardless of what state Python's threads are
    # in. Set well above the observed legitimate run range (35-98 min) so
    # it only fires on a genuine hang, never a merely-slow real batch.
    @task(execution_timeout=timedelta(hours=3))
    def downloadPdfFiles():
        # -*- coding: utf-8 -*-
        import requests
        import time
        import uuid
        import io
        from concurrent.futures import ThreadPoolExecutor
        from repositories.pdf_repository import PdfRepository
        from repositories.proxy_repository import ProxyRepository
        import ftpConnector
        from ftpConnector import ftpConnector

        TOTAL_URLS = 500
        QUEUE_EMPTY_BACKOFF_SECONDS = 15 * 60
        # Raised from 8 to 16 (2026-07-14) after the Gujarati/Russian/English
        # source expansion, then held at 32 for a while because raising it
        # further wasn't helping - GetLatestProxy deterministically returned
        # one "champion" proxy to every worker, and Airflow's global
        # core.parallelism (default 32) was fully saturated by the ~30
        # continuous discovery DAGs, so pdf_downloading couldn't even get
        # enough executor slots to benefit from more in-task threads. Both
        # fixed same day (proxy selection now spreads across top 20
        # candidates; core.parallelism raised to 64) - stepping up again
        # towards ~85% host utilization, watching load/mem/throughput
        # between steps. See project memory for the step log.
        CONCURRENCY = 64

        def storeFile(initialUrl, filename, file):
            ftpConnector.storeFile(filename, file)
            PdfRepository.save_location(initialUrl, filename)

        def downloadOne():
            # Returns whether a URL was actually claimed from the queue (True),
            # as opposed to the queue having nothing left to give out (False) -
            # used after the batch to detect a fully-empty download queue.
            proxieIp = None
            got_url = False
            try:
                url = PdfRepository.get_next_to_download()
                if url is None:
                    return False
                got_url = True
                initialUrl = url

                if 'support' in url:
                    PdfRepository.save_location(initialUrl, "NA")
                    return True
                if 'springer' in url:
                    # Excluded for now due to a known Springer-specific issue.
                    # Left pending (not marked NA) so it resumes automatically
                    # once re-enabled.
                    return True
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
            return got_url

        # Root cause of the 2026-07-17 stuck-task incidents: downloadOne()
        # bounds each individual request/FTP call at ~30s, but that isn't
        # airtight for every code path (observed: SOCKS4/5 proxies, which
        # don't always honor the timeout passed through requests/PySocks
        # on a stalled handshake). Python cannot forcibly kill a thread
        # blocked in a syscall, so a single such hang permanently strands
        # that worker for the rest of the batch. future.result() with no
        # timeout then blocks the *main* thread forever too, since results
        # are collected in submission order - even though up to 63 other
        # workers may have finished, the whole task just sits there,
        # never crashing, never exiting, indefinitely occupying the one
        # max_active_runs=1 slot until someone manually kills it.
        # PER_FUTURE_TIMEOUT_SECONDS is generous (some legitimate PDFs are
        # multi-GB and can take minutes even without hanging) but bounded,
        # so a hung future is treated as "claimed nothing" instead of
        # stalling the batch forever.
        PER_FUTURE_TIMEOUT_SECONDS = 300
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = [executor.submit(downloadOne) for _ in range(TOTAL_URLS)]
            results = []
            for future in futures:
                try:
                    results.append(future.result(timeout=PER_FUTURE_TIMEOUT_SECONDS))
                except TimeoutError:
                    print(f'[pdf_downloading] a download hung past {PER_FUTURE_TIMEOUT_SECONDS}s '
                          f'(worker thread lost, not the whole batch) - treating as unclaimed')
                    results.append(False)

        if not any(results):
            # Every single attempt in this batch found nothing to claim -
            # the download queue is empty across all sources right now.
            # Back off instead of having @continuous re-trigger an
            # immediate identical no-op batch over and over.
            print(f'[pdf_downloading] queue empty (0/{TOTAL_URLS} claimed); '
                  f'backing off {QUEUE_EMPTY_BACKOFF_SECONDS}s')
            time.sleep(QUEUE_EMPTY_BACKOFF_SECONDS)

    downloadPdfFiles()
