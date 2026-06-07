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
        import requests
        import configs
        from configs import getConfig
        import paperDownloader
        from paperDownloader import run_search

        def fetch_page(criterion, page, proxy):
            PAGE_SIZE = 100
            offset = (page - 1) * PAGE_SIZE

            params = {
                'query': criterion['query'],
                'fields': 'openAccessPdf,citationCount,year',
                'limit': PAGE_SIZE,
                'offset': offset,
            }

            if criterion.get('fields_of_study'):
                params['fieldsOfStudy'] = ','.join(criterion['fields_of_study'])

            if criterion.get('date_from') or criterion.get('date_to'):
                year_from = criterion.get('date_from', '2000-01-01')[:4]
                year_to = criterion.get('date_to', '2099-12-31')[:4]
                params['year'] = f'{year_from}-{year_to}'

            proxies = {
                'http': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
                'https': f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}",
            }

            headers = {'User-Agent': 'Mozilla/5.0'}
            api_key = getConfig().get('SemanticScholarApiKey', '')
            if api_key:
                headers['x-api-key'] = api_key

            resp = requests.get(
                'https://api.semanticscholar.org/graph/v1/paper/search',
                params=params,
                proxies=proxies,
                timeout=30,
                headers=headers,
            )
            resp.raise_for_status()

            data = resp.json()
            papers = data.get('data', [])
            next_token = data.get('next')

            min_citations = criterion.get('min_citations', 0)
            open_access_only = criterion.get('open_access_only', False)
            max_results = criterion.get('max_results', 1000)

            urls = []
            for paper in papers:
                if paper.get('citationCount', 0) < min_citations:
                    continue
                oa_pdf = paper.get('openAccessPdf')
                if open_access_only and not oa_pdf:
                    continue
                if oa_pdf and oa_pdf.get('url'):
                    urls.append(oa_pdf['url'])

            has_more = bool(next_token) and (page * PAGE_SIZE) < max_results
            return urls, has_more

        run_search(service_id=6, source='semantic_scholar', adapter_fn=fetch_page)

    download_semantic_scholar()
