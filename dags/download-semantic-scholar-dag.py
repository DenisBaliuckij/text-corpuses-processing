# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_semantic_scholar",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_semantic_scholar():
        import paperDownloader
        from paperDownloader import run_search
        from semanticScholarDownloader import fetch_page

        run_search(service_id=6, source='semantic_scholar', adapter_fn=fetch_page, use_proxy=False)

    download_semantic_scholar()
