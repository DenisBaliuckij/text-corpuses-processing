import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
import pytest
from unittest.mock import patch
from paperDownloader import (
    load_state, save_state, clear_state,
    get_proxy, mark_proxy_broken, save_urls,
)


def test_load_state_returns_default_when_no_state():
    with patch('paperDownloader.ServiceStateRepository.get', return_value=None):
        result = load_state(4)
    assert result == {'criterion_index': 0, 'page': 1, 'done_criteria': [], 'resume_at': None}


def test_load_state_parses_json_from_db():
    state = {'criterion_index': 1, 'page': 3, 'done_criteria': [0]}
    row = (json.dumps(state),)
    with patch('paperDownloader.ServiceStateRepository.get', return_value=row):
        result = load_state(4)
    assert result == {**state, 'resume_at': None}


def test_load_state_preserves_existing_resume_at():
    state = {'criterion_index': 0, 'page': 1, 'done_criteria': [], 'resume_at': 12345.0}
    row = (json.dumps(state),)
    with patch('paperDownloader.ServiceStateRepository.get', return_value=row):
        result = load_state(4)
    assert result['resume_at'] == 12345.0


def test_save_state_serialises_and_calls_update():
    with patch('paperDownloader.ServiceStateRepository.update') as mock_update:
        save_state(4, {'criterion_index': 0, 'page': 1, 'done_criteria': []})
    mock_update.assert_called_once_with(
        4, '{"criterion_index": 0, "page": 1, "done_criteria": []}'
    )


def test_clear_state_calls_remove():
    with patch('paperDownloader.ServiceStateRepository.remove') as mock_remove:
        clear_state(4)
    mock_remove.assert_called_once_with(4)


def test_get_proxy_returns_formatted_dict():
    fake = {'proxieIp': ' 1.2.3.4 ', 'proxiePort': 8080, 'proxieProtocol': ' http '}
    with patch('paperDownloader.ProxyRepository.get_latest', return_value=fake):
        result = get_proxy()
    assert result == {'ip': '1.2.3.4', 'port': 8080, 'protocol': 'http'}


def test_get_proxy_raises_when_none():
    with patch('paperDownloader.ProxyRepository.get_latest', return_value=None):
        with pytest.raises(RuntimeError):
            get_proxy()


def test_mark_proxy_broken_calls_repository():
    with patch('paperDownloader.ProxyRepository.mark_broken') as mock_mb:
        mark_proxy_broken('1.2.3.4')
    mock_mb.assert_called_once_with('1.2.3.4')


def test_save_urls_calls_add_url_for_each():
    with patch('paperDownloader.PdfRepository.add_url') as mock_add:
        save_urls(['http://a.com', 'http://b.com'])
    assert mock_add.call_count == 2
    mock_add.assert_any_call('http://a.com')
    mock_add.assert_any_call('http://b.com')
