# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_arxiv_scientific",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_arxiv_scientific():
        import paperDownloader
        from paperDownloader import run_search
        from arxivApiDownloader import fetch_page

        run_search(service_id=4, source='arxiv', adapter_fn=fetch_page, use_proxy=True)

    download_arxiv_scientific()
