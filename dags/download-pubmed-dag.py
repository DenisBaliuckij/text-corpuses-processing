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
        import time
        import requests
        import xml.etree.ElementTree as ET
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 50
            retstart = (page - 1) * PAGE_SIZE

            query_parts = [criterion['query']]
            if criterion.get('date_from') or criterion.get('date_to'):
                d_from = criterion.get('date_from', '1900/01/01').replace('-', '/')
                d_to = criterion.get('date_to', '2099/12/31').replace('-', '/')
                query_parts.append(f'"{d_from}"[dp]:"{d_to}"[dp]')
            if criterion.get('open_access_only', False):
                query_parts.append('"pmc open access"[filter]')

            query = ' AND '.join(query_parts)

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }
            headers = {'User-Agent': 'Mozilla/5.0'}

            esearch = requests.get(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
                params={
                    'db': 'pubmed',
                    'term': query,
                    'retstart': retstart,
                    'retmax': PAGE_SIZE,
                    'retmode': 'xml',
                },
                proxies=proxies,
                timeout=30,
                headers=headers,
            )
            esearch.raise_for_status()

            root = ET.fromstring(esearch.content)
            total_count = int(root.findtext('Count') or 0)
            pmids = [el.text for el in root.findall('.//Id')]

            urls = []
            if pmids:
                time.sleep(0.35)  # NCBI rate limit: max 3 req/s

                efetch = requests.get(
                    'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
                    params={
                        'db': 'pubmed',
                        'id': ','.join(pmids),
                        'rettype': 'xml',
                        'retmode': 'xml',
                    },
                    proxies=proxies,
                    timeout=30,
                    headers=headers,
                )
                efetch.raise_for_status()

                efetch_root = ET.fromstring(efetch.content)
                for article_id in efetch_root.findall('.//ArticleId'):
                    if article_id.get('IdType') == 'pmc':
                        pmc_id = article_id.text
                        if pmc_id:
                            urls.append(
                                f'https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/'
                            )

            max_results = criterion.get('max_results', 1000)
            has_more = (page * PAGE_SIZE) < min(total_count, max_results)
            return urls, has_more

        run_search(service_id=5, source='pubmed', adapter_fn=fetch_page)

    download_pubmed()
