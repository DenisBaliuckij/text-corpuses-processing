# -*- coding: utf-8 -*-
import requests
from configs import getConfig


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
    } if proxy else None

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
