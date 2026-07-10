# -*- coding: utf-8 -*-
"""Shared helper for the free-proxy-source DAGs: tests each candidate proxy
with a quick real request before trusting it, instead of blindly importing
whatever a public proxy list claims is available."""
from concurrent.futures import ThreadPoolExecutor

import requests

_TEST_URL = "https://httpbin.org/ip"
_TEST_TIMEOUT = 5
_VALIDATION_CONCURRENCY = 50


def validate_and_import(candidates, timestamp):
    """candidates: list of (ip, port, protocol) tuples.

    Tests each concurrently through a real request; only imports (via
    ProxyRepository.add_or_update) the ones that actually respond. Returns
    the number successfully imported.
    """
    from repositories.proxy_repository import ProxyRepository

    def check(candidate):
        ip, port, protocol = candidate
        proxy_url = f"{protocol}://{ip}:{port}"
        try:
            r = requests.get(
                _TEST_URL,
                proxies={'http': proxy_url, 'https': proxy_url},
                timeout=_TEST_TIMEOUT,
            )
            return candidate if r.status_code == 200 else None
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
