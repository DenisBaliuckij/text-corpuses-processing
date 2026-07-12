# -*- coding: utf-8 -*-
"""Shared helper for the free-proxy-source DAGs: tests each candidate proxy
against a real HTTPS target before trusting it, instead of blindly
importing whatever a public proxy list claims is available.

Uses arxiv.org itself (the actual target the pipeline needs to reach)
rather than a generic test endpoint, with a latency cutoff and a content
sanity check. Since requests verifies certificates by default, this also
naturally rejects proxies that intercept/MITM traffic with a self-signed
certificate - a failure mode observed repeatedly in the download logs."""
from concurrent.futures import ThreadPoolExecutor
import time

import requests

_TEST_URL = "https://arxiv.org/"
_TEST_TIMEOUT = 8
_MAX_LATENCY_SECONDS = 5
_EXPECTED_CONTENT = "arXiv"
_VALIDATION_CONCURRENCY = 50


def validate_and_import(candidates, timestamp):
    """candidates: list of (ip, port, protocol) tuples.

    Tests each concurrently through a real request to arxiv.org; only
    imports (via ProxyRepository.add_or_update) proxies that respond
    correctly and fast enough. Returns the number successfully imported.
    """
    from repositories.proxy_repository import ProxyRepository

    def check(candidate):
        ip, port, protocol = candidate
        proxy_url = f"{protocol}://{ip}:{port}"
        start = time.monotonic()
        try:
            r = requests.get(
                _TEST_URL,
                proxies={'http': proxy_url, 'https': proxy_url},
                timeout=_TEST_TIMEOUT,
            )
            elapsed = time.monotonic() - start
            if elapsed > _MAX_LATENCY_SECONDS:
                return None
            if r.status_code != 200 or _EXPECTED_CONTENT not in r.text:
                return None
            return candidate
        except Exception:
            return None

    imported = 0
    with ThreadPoolExecutor(max_workers=_VALIDATION_CONCURRENCY) as executor:
        for result in executor.map(check, candidates):
            if result is None:
                continue
            ip, port, protocol = result
            try:
                ProxyRepository.add_or_update(ip, port, timestamp, protocol)
                imported += 1
            except Exception as e:
                print(e)
    return imported
