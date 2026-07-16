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


# Regression coverage for the 2026-07-16 incident: a failed storbinary/
# retrbinary/nlst left the FTP session open server-side (quit() was never
# reached), and under CONCURRENCY=64 those leaked sessions accumulated on
# the FileZilla server until it silently stopped accepting new passive
# data connections (systemd still reported it "active"). Every connection
# must be closed even when the transfer itself raises.

def test_store_file_closes_connection_even_if_storbinary_raises():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        mock_server.storbinary.side_effect = TimeoutError('data connection timed out')
        try:
            ftpConnector.storeFile('test.pdf', fake_file)
        except TimeoutError:
            pass
        else:
            raise AssertionError('expected the original TimeoutError to propagate')
        mock_server.quit.assert_called_once()


def test_store_file_falls_back_to_close_if_quit_itself_fails():
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        # quit() sends a QUIT command over a socket that storbinary() just
        # broke - it can raise too. Must not leak the connection or crash.
        mock_server.storbinary.side_effect = ConnectionResetError('broken pipe')
        mock_server.quit.side_effect = OSError('socket already closed')
        try:
            ftpConnector.storeFile('test.pdf', fake_file)
        except ConnectionResetError:
            pass
        else:
            raise AssertionError('expected the original ConnectionResetError to propagate')
        mock_server.quit.assert_called_once()
        mock_server.close.assert_called_once()


def test_get_file_closes_connection_even_if_retrbinary_raises():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        mock_server.retrbinary.side_effect = TimeoutError('data connection timed out')
        try:
            ftpConnector.getFile('arxiv/paper.pdf')
        except TimeoutError:
            pass
        else:
            raise AssertionError('expected the original TimeoutError to propagate')
        mock_server.quit.assert_called_once()


def test_get_file_list_closes_connection_even_if_nlst_raises():
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        mock_server.nlst.side_effect = TimeoutError('data connection timed out')
        try:
            ftpConnector.getFileList('arxiv/')
        except TimeoutError:
            pass
        else:
            raise AssertionError('expected the original TimeoutError to propagate')
        mock_server.quit.assert_called_once()


def test_store_file_closes_connection_even_if_login_raises():
    # A failure before the transfer even starts (e.g. bad credentials,
    # connect timeout) must not leave the socket open either.
    fake_file = io.BytesIO(b'data')
    with patch('ftpConnector.getConfig', return_value=_CFG), \
         patch('ftpConnector.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        mock_server.login.side_effect = OSError('connection refused')
        try:
            ftpConnector.storeFile('test.pdf', fake_file)
        except OSError:
            pass
        else:
            raise AssertionError('expected the original OSError to propagate')
        mock_server.quit.assert_called_once()
