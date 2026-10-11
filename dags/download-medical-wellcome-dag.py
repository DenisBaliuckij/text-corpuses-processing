# -*- coding: utf-8 -*-
# Added 2026-10-11: medical articles (Wellcome Collection digitised medical books). URL discovery only - files are
# fetched by pdf_downloading into medical3/wellcome/ (see medicalDownloader.py).
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_medical_wellcome",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "medical"],
) as dag:

    @task()
    def download_medical_wellcome():
        import time
        import diskGuard
        if not diskGuard.heavy_allowed():
            # free-space guard (2026-10-11): large-file source, paused while the download disk is low
            print(f'[diskGuard] download disk {diskGuard.describe()} - discovery paused for 30 min')
            time.sleep(30 * 60)
            return
        from paperDownloader import run_search
        from medicalDownloader import fetch_wellcome

        run_search(service_id=41, source='medical_wellcome', adapter_fn=fetch_wellcome, use_proxy=False)

    download_medical_wellcome()
