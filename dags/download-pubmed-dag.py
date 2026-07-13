# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_pubmed",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_pubmed():
        import paperDownloader
        from paperDownloader import run_search
        from pubmedDownloader import fetch_page

        run_search(service_id=5, source='pubmed', adapter_fn=fetch_page, use_proxy=False)

    download_pubmed()
