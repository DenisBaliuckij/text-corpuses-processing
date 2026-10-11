# -*- coding: utf-8 -*-
# Added 2026-10-11: medical articles (CyberLeninka medical rubrics). URL discovery only - files are
# fetched by pdf_downloading into medical3/cyberleninka/ (see medicalDownloader.py).
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_medical_cyberleninka",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "medical"],
) as dag:

    @task()
    def download_medical_cyberleninka():
        from paperDownloader import run_search
        from medicalDownloader import fetch_cyberleninka

        run_search(service_id=44, source='medical_cyberleninka', adapter_fn=fetch_cyberleninka, use_proxy=True)

    download_medical_cyberleninka()
