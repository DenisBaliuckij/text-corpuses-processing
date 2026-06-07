# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="download_arxiv_scientific",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "scientific"],
) as dag:

    @task()
    def download_arxiv_scientific():
        import requests
        import xml.etree.ElementTree as ET
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 50
            start = (page - 1) * PAGE_SIZE

            parts = []
            if criterion.get('query'):
                parts.append(f"all:{criterion['query']}")
            if criterion.get('categories'):
                cat_expr = ' OR '.join(f"cat:{c}" for c in criterion['categories'])
                parts.append(f"({cat_expr})")
            if criterion.get('date_from') or criterion.get('date_to'):
                d_from = criterion.get('date_from', '2000-01-01').replace('-', '') + '0000'
                d_to = criterion.get('date_to', '2099-12-31').replace('-', '') + '2359'
                parts.append(f"submittedDate:[{d_from} TO {d_to}]")

            search_query = ' AND '.join(parts) if parts else 'all:*'

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }

            resp = requests.get(
                'http://export.arxiv.org/api/query',
                params={
                    'search_query': search_query,
                    'start': start,
                    'max_results': PAGE_SIZE,
                    'sortBy': 'submittedDate',
                    'sortOrder': 'descending',
                },
                proxies=proxies,
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0'},
            )
            resp.raise_for_status()

            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'opensearch': 'http://a9.com/-/spec/opensearch/1.1/',
            }
            root = ET.fromstring(resp.content)
            total = int(root.findtext('opensearch:totalResults', namespaces=ns) or 0)
            max_results = criterion.get('max_results', 1000)

            urls = []
            for entry in root.findall('atom:entry', ns):
                for link in entry.findall('atom:link', ns):
                    if link.get('title') == 'pdf':
                        href = link.get('href', '')
                        if href:
                            if not href.startswith('http'):
                                href = 'https://arxiv.org' + href
                            urls.append(href)

            has_more = (page * PAGE_SIZE) < min(total, max_results)
            return urls, has_more

        run_search(service_id=4, source='arxiv', adapter_fn=fetch_page)

    download_arxiv_scientific()
