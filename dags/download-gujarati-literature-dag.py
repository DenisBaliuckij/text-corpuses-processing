# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gujarati_literature",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gujarati"],
) as dag:

    @task()
    def download_gujarati_literature():
        import paperDownloader
        from paperDownloader import run_search
        from archiveOrgDownloader import search_pdfs

        ROWS = 50

        def fetch_page(criterion, page, proxy):
            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            } if proxy else None

            urls, has_more = search_pdfs(
                criterion['query'], page, ROWS,
                proxies=proxies, tag='gujarati_literature',
            )
            max_results = criterion.get('max_results', 1000)
            has_more = has_more and (page * ROWS) < max_results
            return urls, has_more

        run_search(service_id=7, source='gujarati_literature', adapter_fn=fetch_page, use_proxy=False)

    download_gujarati_literature()
