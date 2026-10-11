# -*- coding: utf-8 -*-
"""URL discovery for the medical article sources (added 2026-10-11).

Every adapter follows paperDownloader.run_search's contract:
    fetch_<source>(criterion, page, proxy) -> (list[str], has_more: bool)
and tags each URL with '#medical_<folder>.<ext>' so pdf_downloading routes the file
to 'medical3/<folder>/' with the right extension (see parse_medical_tag) and
GetPdfToDownload can rotate it as its own source ('%medical_<folder>%').

Sources (all checked 2026-10-11 to return real files from the corpus host,
unlike the NCBI/Europe PMC/medRxiv PDF links, which return bot-check pages):
  - PMC Open Access on AWS (pmc-oa-opendata S3 bucket): plain text per article,
    ~8.3M articles incl. ~280k digitised 1800-1950 back-issues (CC0).
    Enumerated with NCBI ESearch (db=pmc) over date windows, because ESearch
    returns at most 9,999 ids per query.
  - Wellcome Collection: digitised historical medical books as IIIF PDFs. The
    catalogue API returns at most 10,000 works per query, so criteria are date windows.
  - Internet Archive (Medical Heritage Library, Russian medical texts): items with a PDF.
  - Project Gutenberg (Gutendex topics): public-domain medical books.
  - CyberLeninka medical rubrics (Russian articles).
"""
import datetime as dt
import re
import time

import requests

_UA = {'User-Agent': 'Mozilla/5.0 (corpus-host medical article harvester)'}
_ESEARCH = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
_ESEARCH_MAX = 9999
_PMC_S3 = 'https://pmc-oa-opendata.s3.amazonaws.com/PMC{id}.1/PMC{id}.1.txt'
_WELLCOME_API = 'https://api.wellcomecollection.org/catalogue/v2/works'
_WELLCOME_PDF = 'https://iiif.wellcomecollection.org/pdf/{bnumber}'
_WELLCOME_PAGE_SIZE = 100
_WELLCOME_MAX_RESULTS = 10000
_IA_ROWS = 50
_IA_MAX_RESULTS = 10000
_GUTENDEX = 'https://gutendex.com/books/'
_CYBERLENINKA = 'https://cyberleninka.ru/article/c/{rubric}/{page}'


def parse_medical_tag(url: str):
    """'<url>#medical_<folder>[.<ext>]' -> (folder, ext, url_without_tag), else None.
    ext defaults to 'pdf'."""
    if '#medical_' not in url:
        return None
    clean_url, tag = url.rsplit('#', 1)
    name = tag[len('medical_'):]
    folder, _, ext = name.partition('.')
    return folder, (ext or 'pdf'), clean_url


def _proxies(proxy):
    if not proxy:
        return None
    proxy_url = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
    return {'http': proxy_url, 'https': proxy_url}


def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


# ----------------------------------------------------------------------------- PMC
def _pmc_ids(term: str, start: dt.date, end: dt.date, proxies) -> list:
    """All PMC ids for `term` published in [start, end]; windows holding more than
    ESearch's 9,999-id cap are split in half recursively."""
    query = f'{term} AND "{start:%Y/%m/%d}"[pdat]:"{end:%Y/%m/%d}"[pdat]'
    resp = requests.get(_ESEARCH, params={'db': 'pmc', 'term': query, 'retmax': _ESEARCH_MAX,
                                          'retmode': 'json'},
                        proxies=proxies, timeout=60, headers=_UA)
    resp.raise_for_status()
    result = resp.json()['esearchresult']
    time.sleep(0.4)  # NCBI: max 3 requests/s without an API key
    count = int(result.get('count', 0))
    if count > _ESEARCH_MAX and start < end:
        mid = start + (end - start) // 2
        return (_pmc_ids(term, start, mid, proxies)
                + _pmc_ids(term, mid + dt.timedelta(days=1), end, proxies))
    if count > _ESEARCH_MAX:
        print(f'[medical_pmc] {count} ids on {start} exceed the ESearch cap; '
              f'only the first {_ESEARCH_MAX} are taken')
    return result.get('idlist', [])


def fetch_pmc(criterion, page, proxy):
    """criterion: {date_from, date_to, window_days, folder ('pmc' | 'pmc_historical'),
    query (default 'open access[filter]')}. Page n = the n-th date window."""
    date_from, date_to = _date(criterion['date_from']), _date(criterion['date_to'])
    window = dt.timedelta(days=criterion.get('window_days', 30))
    start = date_from + (page - 1) * window
    if start > date_to:
        return [], False
    end = min(start + window - dt.timedelta(days=1), date_to)
    term = criterion.get('query', 'open access[filter]')
    tag = f"#medical_{criterion.get('folder', 'pmc')}.txt"
    ids = _pmc_ids(term, start, end, _proxies(proxy))
    print(f'[medical_pmc] {start}..{end}: {len(ids)} articles')
    return [_PMC_S3.format(id=i) + tag for i in ids], end < date_to


# ----------------------------------------------------------------------------- Wellcome
def fetch_wellcome(criterion, page, proxy):
    """criterion: {date_from, date_to} (production dates; either may be omitted).
    Digitised books ('workType=a') that are available online, as IIIF PDFs."""
    params = {'availabilities': 'online', 'workType': criterion.get('work_type', 'a'),
              'pageSize': _WELLCOME_PAGE_SIZE, 'page': page, 'include': 'items'}
    if criterion.get('date_from'):
        params['production.dates.from'] = criterion['date_from']
    if criterion.get('date_to'):
        params['production.dates.to'] = criterion['date_to']
    resp = requests.get(_WELLCOME_API, params=params, proxies=_proxies(proxy), timeout=60,
                        headers=_UA)
    resp.raise_for_status()
    data = resp.json()
    total = data.get('totalResults', 0)
    if page == 1 and total > _WELLCOME_MAX_RESULTS:
        print(f"[medical_wellcome] {criterion} has {total} works; the API serves only the "
              f"first {_WELLCOME_MAX_RESULTS} - split this window")
    urls = []
    for work in data.get('results', []):
        for item in work.get('items', []):
            for loc in item.get('locations', []):
                m = re.search(r'/presentation/v\d/(b\d+\w*)', loc.get('url') or '')
                if m:
                    urls.append(_WELLCOME_PDF.format(bnumber=m.group(1)) + '#medical_wellcome.pdf')
    urls = list(dict.fromkeys(urls))
    has_more = page < data.get('totalPages', 0) and page * _WELLCOME_PAGE_SIZE < _WELLCOME_MAX_RESULTS
    return urls, has_more


# ----------------------------------------------------------------------------- Internet Archive
def fetch_ia(criterion, page, proxy):
    """criterion: {query (Lucene), folder}. Items without a PDF are skipped; the PDF
    name comes from each item's metadata (names vary, e.g. '<id>.pdf' vs '101489241.pdf').
    Unlike archiveOrgDownloader.search_pdfs, a metadata-host timeout raises (run_search
    then retries the same page) instead of reporting 'no more pages', which would
    end the criterion early."""
    from archiveOrgDownloader import _SEARCH_URL, _DOWNLOAD_URL, _find_pdf_filename
    proxies = _proxies(proxy)
    resp = requests.get(_SEARCH_URL, params={'q': criterion['query'], 'fl[]': 'identifier',
                                             'rows': _IA_ROWS, 'page': page, 'output': 'json'},
                        proxies=proxies, timeout=60, headers=_UA)
    resp.raise_for_status()
    found = resp.json().get('response', {})
    # tag_prefix: 'medical' (default) or 'science' (scienceDownloader reuses this adapter)
    tag = f"#{criterion.get('tag_prefix', 'medical')}_{criterion.get('folder', 'ia')}.pdf"
    urls = []
    for doc in found.get('docs', []):
        identifier = doc.get('identifier')
        if not identifier:
            continue
        try:
            pdf_name = _find_pdf_filename(identifier, proxies)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            raise
        except requests.exceptions.RequestException:
            continue
        if pdf_name:
            urls.append(_DOWNLOAD_URL.format(identifier=identifier, filename=pdf_name) + tag)
    total = min(found.get('numFound', 0), _IA_MAX_RESULTS)
    return urls, page * _IA_ROWS < total


# ----------------------------------------------------------------------------- Gutenberg
def fetch_gutenberg(criterion, page, proxy):
    """criterion: {topic, languages (optional, e.g. 'de'), folder, tag_prefix} - Gutendex
    'topic' matches subjects and bookshelves."""
    from gutenbergDownloader import _pick_format
    params = {'topic': criterion['topic'], 'page': page}
    if criterion.get('languages'):
        params['languages'] = criterion['languages']
    resp = requests.get(_GUTENDEX, params=params, proxies=_proxies(proxy), timeout=60, headers=_UA)
    resp.raise_for_status()
    data = resp.json()
    prefix, folder = criterion.get('tag_prefix', 'medical'), criterion.get('folder', 'gutenberg')
    urls = []
    for book in data.get('results', []):
        picked = _pick_format(book.get('formats', {}))
        if picked:
            url, ext = picked
            urls.append(f'{url}#{prefix}_{folder}.{ext}')
    return urls, bool(data.get('next'))


# ----------------------------------------------------------------------------- CyberLeninka
def fetch_cyberleninka(criterion, page, proxy):
    """criterion: {rubric} - a CyberLeninka rubric slug, e.g. 'clinical-medicine'."""
    resp = requests.get(_CYBERLENINKA.format(rubric=criterion['rubric'], page=page),
                        proxies=_proxies(proxy), timeout=30, headers=_UA)
    resp.raise_for_status()
    slugs = list(dict.fromkeys(re.findall(r'href="(/article/n/[^"#?]+)"', resp.text)))
    urls = [f'https://cyberleninka.ru{s}/pdf#medical_cyberleninka.pdf' for s in slugs]
    return urls, bool(slugs)
