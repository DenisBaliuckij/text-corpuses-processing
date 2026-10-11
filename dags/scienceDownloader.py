# -*- coding: utf-8 -*-
"""URL discovery for multilingual scientific texts (added 2026-10-11), a separate process
from the medical sources but built on the same adapters (medicalDownloader).

URLs are tagged '#science_<folder>.<ext>' (folder = '<source>_<language>', e.g. 'ia_ger');
pdf_downloading stores them in 'science3/<folder>/' and GetPdfToDownload rotates
'%#science_ia%' and '%#science_gutenberg%' as their own sources.

What the other medical sources already cover in all languages (2026-10-11 counts):
  - PMC Open Access: download_medical_pmc has no language filter, so its ~62k
    non-English articles (pt 16.6k, de 13.9k, es 13.4k, fr 11k, zh 7.2k, ko 1.6k, ru 442)
    are collected there.
  - CyberLeninka: get_lenin_urls crawls all 37 subject rubrics (Russian, all fields).
  - Wellcome Collection is medical history only.
"""
import medicalDownloader


def parse_science_tag(url: str):
    """'<url>#science_<folder>[.<ext>]' -> (folder, ext, url_without_tag), else None."""
    if '#science_' not in url:
        return None
    clean_url, tag = url.rsplit('#', 1)
    folder, _, ext = tag[len('science_'):].partition('.')
    return folder, (ext or 'pdf'), clean_url


def fetch_ia(criterion, page, proxy):
    """Internet Archive texts with a PDF; criterion: {query, folder}."""
    return medicalDownloader.fetch_ia({**criterion, 'tag_prefix': 'science'}, page, proxy)


def fetch_gutenberg(criterion, page, proxy):
    """Project Gutenberg books by topic and language; criterion: {topic, languages, folder}."""
    return medicalDownloader.fetch_gutenberg({**criterion, 'tag_prefix': 'science'}, page, proxy)
