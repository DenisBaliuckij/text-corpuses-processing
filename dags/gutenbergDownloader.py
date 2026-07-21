# -*- coding: utf-8 -*-
import requests

_SEARCH_URL = 'https://gutendex.com/books/'

_FORMAT_PRIORITY = [
    ('application/pdf', 'pdf'),
    ('application/epub+zip', 'epub'),
    ('text/html', 'html'),
]


def _pick_format(formats: dict):
    """Returns (url, ext) for the highest-priority format present in
    `formats` (a Gutendex book's mime-type -> URL dict), or None if none
    of the known formats are present. Checks pdf, epub, and html by exact
    mime-type match, then falls back to any 'text/plain' variant (Gutendex
    exposes different charset variants, e.g. 'text/plain; charset=utf-8'
    vs 'text/plain; charset=us-ascii', as distinct keys - matching only
    the utf-8 one would silently skip a book that only has the ascii
    variant)."""
    for mime, ext in _FORMAT_PRIORITY:
        if mime in formats:
            return formats[mime], ext
    for mime, url in formats.items():
        if mime.startswith('text/plain'):
            return url, 'txt'
    return None


def search_books(query, page, proxy=None, tag=''):
    """Searches Gutendex for `query`. Returns (urls, has_more).
    Each URL has '#{tag}.{ext}' appended (if tag is non-empty), ext being
    whichever format was actually selected: pdf if present (essentially
    never on Gutenberg), else epub, else html, else txt. Items with none
    of these formats are skipped.

    proxy, if given, is {'ip', 'port', 'protocol'} (as returned by
    paperDownloader.get_proxy()) - converted internally to requests'
    proxies dict so every caller doesn't have to repeat that conversion."""
    proxies = None
    if proxy:
        proxy_url = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
        proxies = {'http': proxy_url, 'https': proxy_url}

    resp = requests.get(
        _SEARCH_URL, params={'search': query, 'page': page}, proxies=proxies,
        timeout=30, headers={'User-Agent': 'Mozilla/5.0'},
    )
    resp.raise_for_status()
    data = resp.json()

    urls = []
    for book in data.get('results', []):
        picked = _pick_format(book.get('formats', {}))
        if picked is None:
            continue
        url, ext = picked
        if tag:
            url += f'#{tag}.{ext}'
        urls.append(url)

    has_more = bool(data.get('next'))
    return urls, has_more
