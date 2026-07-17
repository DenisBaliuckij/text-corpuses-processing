# -*- coding: utf-8 -*-
"""
Functional health check + auto-heal for the FileZilla FTP server.

Why this exists (incident 2026-07-16): a failed passive-mode data
transfer (originally caused by download/proxy errors) could leave an FTP
session open server-side (see the ftpConnector.py fix in the same commit
range). Under CONCURRENCY=64 those leaked sessions accumulated until
FileZilla silently stopped accepting *new* passive data connections -
while `systemctl is-active` and Docker-style health checks kept reporting
it as perfectly healthy the whole time. Control-channel commands (USER/
PASS/PASV) kept getting answered; only the actual data connection was
dead. That combination makes this outage invisible to any check that
doesn't attempt a real data transfer.

This script does a real PASV + data-connect probe (using the same
credentials/host the pipeline itself uses) and restarts the service if
the probe fails. It does NOT fix the root cause (that's the
ftpConnector.py try/finally fix) - it's a safety net for the next time
something else leaks a session, or the leak fix has a gap we haven't
found yet.

Deployment (see README/deployment notes): copy to
/home/s939/apache-airflow/scripts/ftp_watchdog.py, run every 5 minutes
via cron sourcing report.env for credentials (same pattern as
generate_ops_report.py), and needs a narrowly-scoped NOPASSWD sudoers
rule limited to this exact restart command - see deployment notes for
the exact line, don't grant broader sudo access.
"""
import ftplib
import os
import re
import socket
import subprocess
import sys
import time

FTP_HOST = '127.0.0.1'
FTP_PORT = 21
FTP_USER = os.environ.get('REPORT_FTP_USER')
FTP_PASSWORD = os.environ.get('REPORT_FTP_PASSWORD')
CONTROL_TIMEOUT = 8
DATA_TIMEOUT = 8
SERVICE_NAME = 'filezilla-server'
LOG_PATH = '/home/s939/apache-airflow/scripts/ftp_watchdog.log'


def log(message):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line)
    try:
        with open(LOG_PATH, 'a') as f:
            f.write(line + '\n')
    except OSError:
        pass  # logging failure shouldn't block the health check/restart


def probe_passive_data_connection():
    """Returns (ok: bool, detail: str)."""
    server = ftplib.FTP(timeout=CONTROL_TIMEOUT)
    try:
        server.connect(FTP_HOST, FTP_PORT)
        server.login(FTP_USER, FTP_PASSWORD)
        resp = server.sendcmd('PASV')
        m = re.search(r'\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)', resp)
        if not m:
            return False, f"could not parse PASV response: {resp!r}"
        ip = '.'.join(m.groups()[:4])
        port = int(m.group(5)) * 256 + int(m.group(6))
        t0 = time.time()
        try:
            sock = socket.create_connection((ip, port), timeout=DATA_TIMEOUT)
            sock.close()
        except OSError as e:
            return False, f"data connect to {ip}:{port} failed after {time.time()-t0:.1f}s: {e}"
        return True, "ok"
    except Exception as e:
        return False, f"control-channel step failed: {type(e).__name__}: {e}"
    finally:
        try:
            server.quit()
        except Exception:
            try:
                server.close()
            except Exception:
                pass


def restart_service():
    result = subprocess.run(
        ['sudo', 'systemctl', 'restart', SERVICE_NAME],
        capture_output=True, text=True, timeout=30,
    )
    return result.returncode == 0, (result.stdout + result.stderr).strip()


def main():
    if not FTP_USER or not FTP_PASSWORD:
        log("ERROR: REPORT_FTP_USER/REPORT_FTP_PASSWORD not set in environment - skipping check")
        sys.exit(1)

    ok, detail = probe_passive_data_connection()
    if ok:
        return  # healthy - stay quiet, don't spam the log every 5 minutes

    log(f"FTP passive-data probe FAILED: {detail}")
    restarted, output = restart_service()
    if restarted:
        log(f"Restarted {SERVICE_NAME} successfully")
        # Give it a moment, then re-probe so a failed restart is visible in
        # this same log line rather than only surfacing on the next tick.
        # A single 3s wait proved too short in production (2026-07-17):
        # the service hadn't finished binding its passive-port range yet,
        # so every restart logged a false "still failing" even though it
        # came up fine a few seconds later. Retry with backoff instead of
        # one fixed-delay check.
        ok2, detail2 = False, None
        for wait_s in (3, 5, 7):
            time.sleep(wait_s)
            ok2, detail2 = probe_passive_data_connection()
            if ok2:
                break
        if ok2:
            log("Post-restart probe: OK")
        else:
            log(f"Post-restart probe STILL FAILING: {detail2}")
    else:
        log(f"Failed to restart {SERVICE_NAME}: {output}")


if __name__ == '__main__':
    main()
