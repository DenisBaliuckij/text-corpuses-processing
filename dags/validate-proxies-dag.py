import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="validate_proxies",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfFiles", "proxies"],
) as dag:

    @task()
    def validateTopProxies():
        # -*- coding: utf-8 -*-
        import time
        from concurrent.futures import ThreadPoolExecutor

        import requests

        from repositories.proxy_repository import ProxyRepository

        TOP_N = 50
        CONCURRENCY = 20
        TEST_URL = "https://archive.org/"
        TEST_TIMEOUT_SECONDS = 10

        def check(candidate):
            ip, port, protocol = candidate
            proxy_url = f"{protocol}://{ip}:{port}"
            try:
                response = requests.get(
                    TEST_URL,
                    proxies={'http': proxy_url, 'https': proxy_url},
                    timeout=TEST_TIMEOUT_SECONDS,
                )
                if response.status_code != 200:
                    ProxyRepository.mark_broken(str(ip).strip())
            except Exception:
                ProxyRepository.mark_broken(str(ip).strip())

        candidates = ProxyRepository.get_top_candidates(TOP_N)
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = [executor.submit(check, candidate) for candidate in candidates]
            for future in futures:
                future.result()

    validateTopProxies()
