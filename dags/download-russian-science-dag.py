# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_russian_science",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "russian"],
) as dag:

    @task()
    def download_russian_science():
        import paperDownloader
        from paperDownloader import run_search
        from archiveOrgDownloader import search_pdfs

        ROWS = 50

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_pdfs(
                criterion['query'], page, ROWS,
                proxies=None, tag='russian_science',
            )
            max_results = criterion.get('max_results', 3000)
            has_more = has_more and (page * ROWS) < max_results
            return urls, has_more

        run_search(service_id=15, source='russian_science', adapter_fn=fetch_page, use_proxy=False)

    download_russian_science()
