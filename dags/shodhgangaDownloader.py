# -*- coding: utf-8 -*-
import urllib.parse

import bs4
import requests

_BASE = 'https://shodhganga.inflibnet.ac.in'
_SEARCH_PATH = '/simple-search'


def search_pdfs(subject_filter, page, rpp, proxies=None, tag='', language='Gujarati'):
    """Searches Shodhganga (INFLIBNET's Indian ETD repository) for theses
    whose language equals `language` and whose subject contains
    `subject_filter`. Returns (urls, has_more). Each URL has '#{tag}'
    appended so downstream consumers can recover the category without a
    DB schema change."""
    start = (page - 1) * rpp
    params = {
        'query': '',
        'filter_field_1': 'language',
        'filter_type_1': 'equals',
        'filter_value_1': language,
        'filter_field_2': 'subject',
        'filter_type_2': 'contains',
        'filter_value_2': subject_filter,
        'rpp': rpp,
        'start': start,
        'sort_by': 0,
        'order': 'asc',
    }
    resp = requests.get(_BASE + _SEARCH_PATH, params=params, proxies=proxies, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
    resp.raise_for_status()
    soup = bs4.BeautifulSoup(resp.text, 'html.parser')

    item_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/handle/10603/' in href and href.count('/') <= 4:
            item_links.append(urllib.parse.urljoin(_BASE, href))
    item_links = list(dict.fromkeys(item_links))

    urls = []
    for item_url in item_links:
        for pdf_url in _find_pdf_links(item_url, proxies):
            if tag:
                pdf_url += '#' + tag
            urls.append(pdf_url)

    has_more = len(item_links) >= rpp
    return urls, has_more


def _find_pdf_links(item_url, proxies=None):
    """Theses on Shodhganga are split into one PDF per chapter (title page,
    declaration, chapter01, chapter02, ..., bibliography) rather than a
    single combined file, so every bitstream PDF is collected."""
    resp = requests.get(item_url, proxies=proxies, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
    if resp.status_code != 200:
        return []
    soup = bs4.BeautifulSoup(resp.text, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/bitstream/' in href and href.lower().endswith('.pdf'):
            links.append(urllib.parse.urljoin(item_url, href))
    return list(dict.fromkeys(links))
