import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
from gutenbergDownloader import search_books


def _fake_response(json_data):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def test_search_books_picks_epub_when_no_pdf():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 1, 'formats': {
                'text/html': 'https://www.gutenberg.org/ebooks/1.html.images',
                'application/epub+zip': 'https://www.gutenberg.org/ebooks/1.epub3.images',
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/1.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, has_more = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/1.epub3.images#gutenberg_science.epub']
    assert has_more is False


def test_search_books_prefers_pdf_when_present():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 2, 'formats': {
                'application/pdf': 'https://www.gutenberg.org/files/2/2.pdf',
                'application/epub+zip': 'https://www.gutenberg.org/ebooks/2.epub3.images',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/files/2/2.pdf#gutenberg_science.pdf']


def test_search_books_falls_back_to_html_when_no_epub():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 3, 'formats': {
                'text/html': 'https://www.gutenberg.org/ebooks/3.html.images',
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/3.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/3.html.images#gutenberg_science.html']


def test_search_books_falls_back_to_txt_when_only_txt():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 4, 'formats': {
                'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/4.txt.utf-8',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == ['https://www.gutenberg.org/ebooks/4.txt.utf-8#gutenberg_science.txt']


def test_search_books_skips_item_with_no_usable_format():
    data = {
        'count': 1, 'next': None,
        'results': [{
            'id': 5, 'formats': {
                'image/jpeg': 'https://www.gutenberg.org/cache/epub/5/pg5.cover.medium.jpg',
                'application/rdf+xml': 'https://www.gutenberg.org/ebooks/5.rdf',
            }
        }]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='gutenberg_science')
    assert urls == []


def test_search_books_has_more_true_when_next_present():
    data = {'count': 100, 'next': 'https://gutendex.com/books/?page=2&search=science', 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        _, has_more = search_books('science', 1)
    assert has_more is True


def test_search_books_has_more_false_when_next_null():
    data = {'count': 5, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        _, has_more = search_books('science', 1)
    assert has_more is False


def test_search_books_passes_query_and_page_params():
    data = {'count': 0, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('social science', 3)
    _, kwargs = mock_get.call_args
    assert kwargs['params'] == {'search': 'social science', 'page': 3}


def test_search_books_converts_proxy_dict_to_requests_proxies():
    data = {'count': 0, 'next': None, 'results': []}
    proxy = {'ip': '1.2.3.4', 'port': 8080, 'protocol': 'http'}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('science', 1, proxy=proxy)
    _, kwargs = mock_get.call_args
    assert kwargs['proxies'] == {'http': 'http://1.2.3.4:8080', 'https': 'http://1.2.3.4:8080'}


def test_search_books_no_proxy_when_proxy_none():
    data = {'count': 0, 'next': None, 'results': []}
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)) as mock_get:
        search_books('science', 1, proxy=None)
    _, kwargs = mock_get.call_args
    assert kwargs['proxies'] is None


def test_search_books_no_tag_appends_nothing():
    data = {
        'count': 1, 'next': None,
        'results': [{'id': 6, 'formats': {'text/plain; charset=utf-8': 'https://www.gutenberg.org/ebooks/6.txt.utf-8'}}]
    }
    with patch('gutenbergDownloader.requests.get', return_value=_fake_response(data)):
        urls, _ = search_books('science', 1, tag='')
    assert urls == ['https://www.gutenberg.org/ebooks/6.txt.utf-8']
