import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import io
from unittest.mock import patch, MagicMock
from ftpConnector import ftpConnector

_CFG = {
    'FtpHost': '127.0.0.1', 'FtpPort': 21,
    'FtpUser': 'user', 'FtpPassword': 'pass',
    'FtpHostGraph': '127.0.0.2', 'FtpPortGraph': 22,
    'FtpUserGraph': 'guser', 'FtpPasswordGraph': 'gpass',
}


def test_store_file_connects_logs_in_and_sends_storbinary():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        ftpConnector.storeFile('test.pdf', fake_file)
        mock_server = MockFTP.return_value
        mock_server.connect.assert_called_once_with('127.0.0.1', 21)
        mock_server.login.assert_called_once_with('user', 'pass')
        mock_server.storbinary.assert_called_once_with('STOR test.pdf', fake_file)
        mock_server.quit.assert_called_once()


def test_get_file_issues_retrbinary_and_returns_bytesio():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        result = ftpConnector.getFile('arxiv/paper.pdf')
        mock_server = MockFTP.return_value
        args = mock_server.retrbinary.call_args[0]
        assert args[0] == 'RETR arxiv/paper.pdf'
        assert isinstance(result, io.BytesIO)
        mock_server.quit.assert_called_once()


def test_get_file_list_calls_nlst_and_returns_result():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        MockFTP.return_value.nlst.return_value = ['file1.pdf', 'file2.pdf']
        result = ftpConnector.getFileList('arxiv/')
        MockFTP.return_value.nlst.assert_called_once_with('arxiv/')
        assert result == ['file1.pdf', 'file2.pdf']
        MockFTP.return_value.quit.assert_called_once()


def test_ftppostfix_uses_suffixed_config_keys():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        ftpConnector.storeFile('graph.json', fake_file, ftpPostfix='Graph')
        MockFTP.return_value.connect.assert_called_once_with('127.0.0.2', 22)
        MockFTP.return_value.login.assert_called_once_with('guser', 'gpass')
