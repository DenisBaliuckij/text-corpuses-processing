import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
from repositories.proxy_repository import ProxyRepository

_CFG = {'ConnectionString': 'Driver={SQL Server};Server=test;'}


def test_proxy_add_or_update_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.add_or_update('1.2.3.4', 8080, 12345, 'http')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            ('1.2.3.4', 8080, 12345, 'http')
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_mark_broken_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.mark_broken('1.2.3.4')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[MarkProxyAsBroken] @ip = ?", ('1.2.3.4',)
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_get_latest_returns_dict():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        mock_cur.fetchone.return_value = ('1.2.3.4', 8080, 'http')
        result = ProxyRepository.get_latest()
        assert result == {'proxieIp': '1.2.3.4', 'proxiePort': 8080, 'proxieProtocol': 'http'}
