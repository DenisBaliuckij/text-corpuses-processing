# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_gujarati_science_social",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "gujarati"],
) as dag:

    @task()
    def download_gujarati_science_social():
        import paperDownloader
        from paperDownloader import run_search
        from shodhgangaDownloader import search_pdfs

        RPP = 50

        def fetch_page(criterion, page, proxy):
            urls, has_more = search_pdfs(
                criterion['subject'], page, RPP,
                proxies=None, tag='gujarati_science_social',
            )
            return urls, has_more

        run_search(service_id=10, source='gujarati_science_social', adapter_fn=fetch_page, use_proxy=False)

    download_gujarati_science_social()
