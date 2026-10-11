# -*- coding: utf-8 -*-
"""Free-space guard for Airflow downloads (added 2026-10-11).

All new downloads land on the 20 TB drive (/mnt/text-corpus, the '<source>3/' FTP folders),
but the worker writes over FTP and can't see that disk. The host cron script
scripts/write_disk_status.py writes its free space to configs/disk_status.json every
5 minutes; this module reads it.

  - below HEAVY_MIN_FREE_PCT: the large-file sources (Wellcome Collection books of
    50-110 MB, Internet Archive medical/science scans) stop discovering and downloading;
  - below ALL_MIN_FREE_PCT: pdf_downloading stops altogether.

A missing or stale status file (the cron isn't running) blocks the heavy sources only.
Downloading resumes on its own once space is freed.
"""
import json
import os
import time

STATUS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', 'disk_status.json')
HEAVY_MIN_FREE_PCT = 15.0
ALL_MIN_FREE_PCT = 5.0
STALE_AFTER_SECONDS = 2 * 60 * 60
# URL tags of the large-file sources (see medicalDownloader.py / scienceDownloader.py)
HEAVY_TAGS = ('#medical_wellcome', '#medical_ia', '#science_ia')


def free_pct():
    """Free space of the download disk in percent, or None if the status is missing/stale."""
    try:
        with open(STATUS_PATH, encoding='utf-8') as f:
            status = json.load(f)
        if time.time() - status['checked_at'] > STALE_AFTER_SECONDS:
            return None
        return float(status['free_pct'])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def heavy_allowed(pct=None):
    pct = free_pct() if pct is None else pct
    return pct is not None and pct >= HEAVY_MIN_FREE_PCT


def all_allowed(pct=None):
    pct = free_pct() if pct is None else pct
    return pct is None or pct >= ALL_MIN_FREE_PCT


def is_heavy_url(url):
    return any(tag in url for tag in HEAVY_TAGS)


def describe():
    pct = free_pct()
    return 'unknown (status file missing or stale)' if pct is None else f'{pct:.1f}% free'
