# -*- coding: utf-8 -*-
import json
import os

from repositories.service_state_repository import ServiceStateRepository
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository

_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
_SEARCH_CONFIG_PATH = os.path.join(_DAG_FOLDER, 'configs', 'search_configs.json')


def load_search_config(source: str) -> list:
    """Reads search_configs.json and returns the criteria list for the given source."""
    with open(_SEARCH_CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f).get(source, [])


def load_state(service_id: int) -> dict:
    """Reads crawl state from ServiceState. Returns a fresh default state if none exists."""
    result = ServiceStateRepository.get(service_id)
    if result is None:
        return {'criterion_index': 0, 'page': 1, 'done_criteria': []}
    return json.loads(result[0])


def save_state(service_id: int, state: dict) -> None:
    """Persists crawl state to ServiceState as a JSON string."""
    ServiceStateRepository.update(service_id, json.dumps(state))


def clear_state(service_id: int) -> None:
    """Deletes crawl state from ServiceState (all criteria exhausted)."""
    ServiceStateRepository.remove(service_id)


def get_proxy() -> dict:
    """Returns {'ip', 'port', 'protocol'} from the proxy pool.
    Raises RuntimeError if no proxy is available."""
    result = ProxyRepository.get_latest()
    if result is None:
        raise RuntimeError('No proxy available')
    return {
        'ip': str(result['proxieIp']).strip(),
        'port': result['proxiePort'],
        'protocol': str(result['proxieProtocol']).strip(),
    }


def mark_proxy_broken(ip: str) -> None:
    """Marks a proxy as broken in the DB."""
    ProxyRepository.mark_broken(ip)


def save_urls(urls: list) -> None:
    """Calls PdfRepository.add_url() for each URL. Idempotent."""
    for url in urls:
        PdfRepository.add_url(url)


def _next_active_index(current: int, criteria: list, done: set):
    """Return the next criterion index not in done, wrapping around.
    Returns None if every index is in done."""
    n = len(criteria)
    for offset in range(1, n + 1):
        idx = (current + offset) % n
        if idx not in done:
            return idx
    return None


def advance_state(state: dict, criteria: list, has_more: bool):
    """Pure function. Computes the next state after one page of one criterion.

    Returns the new state dict, or None if all criteria are exhausted
    (all repeat=false and all in done_criteria).
    """
    current = state['criterion_index']
    done = set(state['done_criteria'])

    if has_more:
        return {**state, 'page': state['page'] + 1}

    criterion = criteria[current]
    if criterion.get('repeat', False):
        next_idx = _next_active_index(current, criteria, done)
        return {
            'criterion_index': next_idx if next_idx is not None else current,
            'page': 1,
            'done_criteria': list(done),
        }
    else:
        done = done | {current}
        next_idx = _next_active_index(current, criteria, done)
        if next_idx is None:
            return None
        return {'criterion_index': next_idx, 'page': 1, 'done_criteria': list(done)}


def run_search(service_id: int, source: str, adapter_fn, use_proxy: bool = True) -> None:
    """Main entry point called by each DAG task.

    Loads criteria and state, picks the current criterion, calls adapter_fn
    to fetch one page of URLs, saves them, advances state, and persists.

    adapter_fn(criterion, page, proxy) -> (list[str], bool)
      criterion — one dict from search_configs.json
      page      — current 1-based page number
      proxy     — {'ip': str, 'port': int, 'protocol': str}, or None when use_proxy=False
      returns   — (list of URL strings, has_more bool)

    use_proxy=False skips proxy lookup entirely (suitable for public APIs that
    don't require IP rotation, e.g. arXiv, PubMed, Semantic Scholar).
    """
    criteria = load_search_config(source)
    if not criteria:
        return

    state = load_state(service_id)
    done = set(state['done_criteria'])

    # Recover if state points at an already-done criterion
    current = state['criterion_index']
    if current in done:
        current = _next_active_index(current, criteria, done)
        if current is None:
            clear_state(service_id)
            return
        state = {'criterion_index': current, 'page': 1, 'done_criteria': list(done)}

    proxy = None
    if use_proxy:
        try:
            proxy = get_proxy()
        except RuntimeError:
            return  # no proxy; exit without changing state

    try:
        urls, has_more = adapter_fn(criteria[state['criterion_index']], state['page'], proxy)
    except Exception as e:
        print(f'[paperDownloader] adapter error: {e}')
        if proxy and any(w in str(e).lower() for w in ('proxy', 'connect', 'timeout', 'ssl')):
            mark_proxy_broken(proxy['ip'])
        return  # state NOT advanced; Airflow retries on next run

    save_urls(urls)

    new_state = advance_state(state, criteria, has_more)
    if new_state is None:
        clear_state(service_id)
    else:
        save_state(service_id, new_state)
