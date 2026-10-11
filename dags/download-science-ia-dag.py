# -*- coding: utf-8 -*-
# Added 2026-10-11: multilingual scientific texts (Internet Archive scientific texts and journals in ~35 languages). URL discovery only - files
# are fetched by pdf_downloading into science3/<source>_<language>/ (see scienceDownloader.py).
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_science_ia",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "science", "multilingual"],
) as dag:

    @task()
    def download_science_ia():
        from paperDownloader import run_search
        from scienceDownloader import fetch_ia

        run_search(service_id=45, source='science_ia', adapter_fn=fetch_ia, use_proxy=False)

    download_science_ia()
