# -*- coding: utf-8 -*-
import requests

_SEARCH_URL = 'https://archive.org/advancedsearch.php'
_METADATA_URL = 'https://archive.org/metadata/{identifier}'
_DOWNLOAD_URL = 'https://archive.org/download/{identifier}/{filename}'


def search_pdfs(query, page, rows, proxies=None, tag=''):
    """Searches archive.org for items matching `query` (a Lucene-style query
    string, e.g. "mediatype:(texts) AND language:(guj)"). Returns
    (urls, has_more). Each URL has '#{tag}' appended so downstream consumers
    can recover the category without a DB schema change."""
    params = {
        'q': query,
        'fl[]': 'identifier',
        'rows': rows,
        'page': page,
        'output': 'json',
    }
    resp = requests.get(_SEARCH_URL, params=params, proxies=proxies, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
    resp.raise_for_status()
    data = resp.json()
    docs = data.get('response', {}).get('docs', [])
    total = data.get('response', {}).get('numFound', 0)

    urls = []
    for doc in docs:
        identifier = doc.get('identifier')
        if not identifier:
            continue
        try:
            pdf_filename = _find_pdf_filename(identifier, proxies)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            # The metadata host itself is unreachable (not just this one
            # identifier) - retrying the same broken host for up to `rows`
            # remaining items would silently burn ~rows*30s with zero
            # progress (this previously hung process_custom_queries for
            # 4+ minutes on a single page). Stop this page immediately and
            # keep whatever URLs were already found instead of retrying.
            has_more = False
            return urls, has_more
        except requests.exceptions.RequestException:
            # Other errors (e.g. a bad response for this one identifier)
            # don't indicate the host is down - skip just this item.
            continue
        if not pdf_filename:
            continue
        url = _DOWNLOAD_URL.format(identifier=identifier, filename=pdf_filename)
        if tag:
            url += '#' + tag
        urls.append(url)

    has_more = (page * rows) < total
    return urls, has_more


def _find_pdf_filename(identifier, proxies=None):
    resp = requests.get(_METADATA_URL.format(identifier=identifier), proxies=proxies, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
    if resp.status_code != 200:
        return None
    data = resp.json()
    for f in data.get('files', []):
        name = f.get('name', '')
        if name.lower().endswith('.pdf'):
            return name
    return None
