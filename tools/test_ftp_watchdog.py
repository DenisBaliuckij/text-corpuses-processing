import io
import subprocess
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(__file__))

import ftp_watchdog


def test_probe_returns_ok_when_data_connect_succeeds():
    with patch('ftp_watchdog.ftplib.FTP') as MockFTP, \
         patch('ftp_watchdog.socket.create_connection') as mock_connect:
        mock_server = MockFTP.return_value
        mock_server.sendcmd.return_value = '227 Entering Passive Mode (127,0,0,1,195,80)'
        ok, detail = ftp_watchdog.probe_passive_data_connection()
        assert ok is True
        mock_connect.assert_called_once_with(('127.0.0.1', 195 * 256 + 80), timeout=ftp_watchdog.DATA_TIMEOUT)
        mock_server.quit.assert_called_once()


def test_probe_fails_when_data_connect_times_out():
    with patch('ftp_watchdog.ftplib.FTP') as MockFTP, \
         patch('ftp_watchdog.socket.create_connection', side_effect=TimeoutError('timed out')):
        mock_server = MockFTP.return_value
        mock_server.sendcmd.return_value = '227 Entering Passive Mode (127,0,0,1,195,80)'
        ok, detail = ftp_watchdog.probe_passive_data_connection()
        assert ok is False
        assert 'timed out' in detail
        mock_server.quit.assert_called_once()  # still closes the control connection


def test_probe_fails_when_login_raises():
    with patch('ftp_watchdog.ftplib.FTP') as MockFTP:
        mock_server = MockFTP.return_value
        mock_server.login.side_effect = OSError('connection refused')
        ok, detail = ftp_watchdog.probe_passive_data_connection()
        assert ok is False
        assert 'connection refused' in detail
        mock_server.quit.assert_called_once()


def test_probe_closes_connection_even_if_quit_fails():
    with patch('ftp_watchdog.ftplib.FTP') as MockFTP, \
         patch('ftp_watchdog.socket.create_connection') as mock_connect:
        mock_server = MockFTP.return_value
        mock_server.sendcmd.return_value = '227 Entering Passive Mode (127,0,0,1,195,80)'
        mock_server.quit.side_effect = OSError('already closed')
        ok, detail = ftp_watchdog.probe_passive_data_connection()
        assert ok is True
        mock_server.close.assert_called_once()


def test_restart_service_success():
    fake_result = subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')
    with patch('ftp_watchdog.subprocess.run', return_value=fake_result) as mock_run:
        ok, output = ftp_watchdog.restart_service()
        assert ok is True
        mock_run.assert_called_once_with(
            ['sudo', 'systemctl', 'restart', 'filezilla-server'],
            capture_output=True, text=True, timeout=30,
        )


def test_restart_service_failure_reports_output():
    fake_result = subprocess.CompletedProcess(args=[], returncode=1, stdout='', stderr='permission denied')
    with patch('ftp_watchdog.subprocess.run', return_value=fake_result):
        ok, output = ftp_watchdog.restart_service()
        assert ok is False
        assert 'permission denied' in output


def test_main_does_nothing_when_healthy():
    with patch('ftp_watchdog.FTP_USER', 'user'), \
         patch('ftp_watchdog.FTP_PASSWORD', 'pass'), \
         patch('ftp_watchdog.probe_passive_data_connection', return_value=(True, 'ok')), \
         patch('ftp_watchdog.restart_service') as mock_restart, \
         patch('ftp_watchdog.log') as mock_log:
        ftp_watchdog.main()
        mock_restart.assert_not_called()
        mock_log.assert_not_called()


def test_main_restarts_and_reprobes_when_unhealthy():
    with patch('ftp_watchdog.FTP_USER', 'user'), \
         patch('ftp_watchdog.FTP_PASSWORD', 'pass'), \
         patch('ftp_watchdog.probe_passive_data_connection', side_effect=[(False, 'data connect timed out'), (True, 'ok')]), \
         patch('ftp_watchdog.restart_service', return_value=(True, '')) as mock_restart, \
         patch('ftp_watchdog.time.sleep'), \
         patch('ftp_watchdog.log') as mock_log:
        ftp_watchdog.main()
        mock_restart.assert_called_once()
        logged = ' '.join(str(c.args[0]) for c in mock_log.call_args_list)
        assert 'FAILED' in logged
        assert 'Restarted' in logged
        assert 'Post-restart probe: OK' in logged


def test_main_logs_when_restart_itself_fails():
    with patch('ftp_watchdog.FTP_USER', 'user'), \
         patch('ftp_watchdog.FTP_PASSWORD', 'pass'), \
         patch('ftp_watchdog.probe_passive_data_connection', return_value=(False, 'down')), \
         patch('ftp_watchdog.restart_service', return_value=(False, 'permission denied')), \
         patch('ftp_watchdog.log') as mock_log:
        ftp_watchdog.main()
        logged = ' '.join(str(c.args[0]) for c in mock_log.call_args_list)
        assert 'Failed to restart' in logged


def test_main_skips_check_without_credentials():
    with patch('ftp_watchdog.FTP_USER', None), \
         patch('ftp_watchdog.FTP_PASSWORD', None), \
         patch('ftp_watchdog.probe_passive_data_connection') as mock_probe, \
         patch('ftp_watchdog.log') as mock_log:
        try:
            ftp_watchdog.main()
        except SystemExit as e:
            assert e.code == 1
        else:
            raise AssertionError('expected SystemExit(1)')
        mock_probe.assert_not_called()
