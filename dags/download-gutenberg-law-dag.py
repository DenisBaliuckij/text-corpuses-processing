# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gutenberg_law",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gutenberg"],
) as dag:

    @task()
    def download_gutenberg_law():
        import paperDownloader
        from paperDownloader import run_search
        from gutenbergDownloader import search_books

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_books(
                criterion['query'], page, proxy=proxy, tag='gutenberg_law',
            )
            max_results = criterion.get('max_results', 5000)
            has_more = has_more and (page * 32) < max_results
            return urls, has_more

        run_search(service_id=30, source='gutenberg_law', adapter_fn=fetch_page, use_proxy=True)

    download_gutenberg_law()
