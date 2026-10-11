# -*- coding: utf-8 -*-
# Added 2026-10-11: medical articles (PMC Open Access plain text, modern and historical). URL discovery only - files are
# fetched by pdf_downloading into medical3/pmc/ (see medicalDownloader.py).
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_medical_pmc",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "medical"],
) as dag:

    @task()
    def download_medical_pmc():
        from paperDownloader import run_search
        from medicalDownloader import fetch_pmc

        run_search(service_id=40, source='medical_pmc', adapter_fn=fetch_pmc, use_proxy=False)

    download_medical_pmc()
