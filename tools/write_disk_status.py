#!/usr/bin/env python3
"""Writes the free space of the Airflow download disk for dags/diskGuard.py (added 2026-10-11).
Runs from cron every 5 minutes; the worker container can't see the disk itself (it uploads via FTP)."""
import json
import os
import shutil
import time

DISK = '/mnt/text-corpus'  # the '<source>3/' FTP folders (/opt/pdfs/*3 -> /mnt/text-corpus/ftp_pdfs)
OUT = '/home/s939/apache-airflow/dags/configs/disk_status.json'

usage = shutil.disk_usage(DISK)
status = {
    'disk': DISK,
    'free_pct': round(usage.free * 100 / usage.total, 2),
    'free_bytes': usage.free,
    'total_bytes': usage.total,
    'checked_at': int(time.time()),
}
tmp = OUT + '.tmp'
with open(tmp, 'w', encoding='utf-8') as f:
    json.dump(status, f)
os.replace(tmp, OUT)
