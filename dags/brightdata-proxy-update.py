# -*- coding: utf-8 -*-
"""
Created on Mon May 11 07:50:03 2026

@author: denis
"""

import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="update-brightdata-proxy",
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def update_brightdata_proxy():
        from repositories.proxy_repository import ProxyRepository

        ProxyRepository.add_or_update('brd-customer-hl_68e14c58-zone-isp_proxy1:sgpoqre858ru@brd.superproxy.io', 33335, 2094097452, 'http')
        
    update_brightdata_proxy()
