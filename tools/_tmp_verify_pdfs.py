# -*- coding: utf-8 -*-
"""
One-off PDF structural-integrity audit. Run inside the airflow-worker
container (has pypdf + the app's own configs/ftpConnector modules).

Redesigned 2026-07-16 after the MLSD/NLST-based version hit persistent
(100%) passive-mode listing failures on this FTP server, even paced and
with a single reused connection. Directory listing (MLSD/NLST) turned out
to be far less reliable here than the single-file transfers (RETR/STOR)
production actually uses. Rather than keep fighting listing, this version
skips it entirely: PdfDocuments.LocationInFileSystem already stores the
exact FTP path for every successfully downloaded file, so a true random
sample can be pulled straight from the database (ORDER BY NEWID()) and
fetched with RETR only - the exact same operation the pipeline itself
performs successfully thousands of times a day.

For each of ~1000 sampled files, checks:
  - starts with b'%PDF-'
  - pypdf.PdfReader can open it without raising
  - page count > 0
  - at least one of the first 3 pages yields non-trivial extractable text

Reuses a single FTP control connection for the whole run, with a small
per-file delay as a courtesy since this shares the server with live
production traffic.
"""
import io
import sys
import time

sys.path.insert(0, '/opt/airflow/dags')

import ftplib
import pyodbc
from configs import getConfig
from pypdf import PdfReader

SAMPLE_SIZE = 1000


def get_random_sample():
    """Pull a true random sample of already-downloaded file paths
    straight from the DB - no FTP directory listing involved."""
    config = getConfig()
    cnxn = pyodbc.connect(config['ConnectionString'])
    cursor = cnxn.cursor()
    cursor.execute(
        f"SELECT TOP ({SAMPLE_SIZE}) LocationInFileSystem "
        "FROM dbo.PdfDocuments "
        "WHERE LocationInFileSystem NOT IN ('', 'NA') "
        "ORDER BY NEWID()"
    )
    rows = [r[0] for r in cursor.fetchall()]
    cursor.close()
    cnxn.close()
    return rows


def ftp_connect():
    config = getConfig()
    server = ftplib.FTP(timeout=20)
    server.connect(config["FtpHost"], config["FtpPort"])
    server.login(config["FtpUser"], config["FtpPassword"])
    return server


def safe_close(server):
    try:
        server.quit()
    except Exception:
        try:
            server.close()
        except Exception:
            pass


def check_pdf(data):
    if len(data) == 0:
        return False, "zero-byte file", 0, False
    if not data[:1024].lstrip(b'\x00').startswith(b'%PDF-'):
        return False, "missing %PDF- header", 0, False
    has_eof = b'%%EOF' in data[-2048:]
    try:
        reader = PdfReader(io.BytesIO(data), strict=False)
        pages = len(reader.pages)
    except Exception as e:
        return False, f"pypdf could not parse: {type(e).__name__}: {e}", 0, False
    if pages == 0:
        return False, "0 pages", 0, False
    has_text = False
    try:
        for p in reader.pages[:min(3, pages)]:
            text = p.extract_text() or ""
            if len(text.strip()) > 40:
                has_text = True
                break
    except Exception:
        pass
    if not has_eof:
        return True, "parsed OK but no %%EOF trailer near end (lenient)", pages, has_text
    return True, "ok", pages, has_text


def main():
    print("Pulling random sample from PdfDocuments...", flush=True)
    sample = get_random_sample()
    print(f"Sampled {len(sample)} already-downloaded file paths from the DB.\n", flush=True)

    results = {'ok_clean': 0, 'ok_no_eof': 0, 'ok_no_text': 0, 'fail': 0}
    failures = []
    no_text_examples = []
    sizes_ok = []

    # ftp connection is held in a single-item list (not a bare variable) so
    # the nested helper below can rebind it via closure without `nonlocal`
    # gymnastics, and a broken/None connection is always visible to the
    # next iteration rather than silently retried on a stale object.
    ftp_box = [None]

    def ensure_connected():
        if ftp_box[0] is None:
            ftp_box[0] = ftp_connect()
        return ftp_box[0]

    def reconnect():
        if ftp_box[0] is not None:
            safe_close(ftp_box[0])
        ftp_box[0] = None
        time.sleep(1)
        try:
            ftp_box[0] = ftp_connect()
        except Exception:
            pass  # leave as None; next call to ensure_connected() retries

    ensure_connected()
    for i, path in enumerate(sample):
        time.sleep(0.3)  # courtesy pacing - shares the server with live production traffic
        data = None
        try:
            for attempt in range(2):
                try:
                    server = ensure_connected()
                    buf = io.BytesIO()
                    server.retrbinary(f"RETR {path}", buf.write)
                    data = buf.getvalue()
                    break
                except Exception as e:
                    if attempt == 1:
                        results['fail'] += 1
                        failures.append((path, f"FTP fetch error: {type(e).__name__}: {e}"))
                    else:
                        reconnect()
        except Exception as e:
            # Final safety net: whatever went wrong, record it and move on -
            # a one-off audit script must never crash mid-run over a single
            # file (see the 2026-07-17 incident where an unhandled
            # ConnectionRefusedError on reconnect killed the whole run).
            results['fail'] += 1
            failures.append((path, f"unexpected error: {type(e).__name__}: {e}"))
        if data is None:
            continue

        ok, reason, pages, has_text = check_pdf(data)
        if not ok:
            results['fail'] += 1
            failures.append((path, reason))
        else:
            sizes_ok.append(len(data))
            if "no %%EOF" in reason:
                results['ok_no_eof'] += 1
            elif not has_text:
                results['ok_no_text'] += 1
                no_text_examples.append((path, pages))
            else:
                results['ok_clean'] += 1

        if (i + 1) % 50 == 0:
            print(f"  ...{i + 1}/{len(sample)} checked (fail={results['fail']})", flush=True)

    if ftp_box[0] is not None:
        safe_close(ftp_box[0])

    print("\n" + "=" * 60, flush=True)
    print("RESULTS", flush=True)
    print("=" * 60, flush=True)
    total = len(sample)
    print(f"Sampled: {total}", flush=True)
    print(f"  Clean (parses, has extractable text): {results['ok_clean']} ({results['ok_clean']/total:.1%})", flush=True)
    print(f"  OK but no extractable text (likely scans): {results['ok_no_text']} ({results['ok_no_text']/total:.1%})", flush=True)
    print(f"  OK but missing %%EOF trailer (lenient parse): {results['ok_no_eof']} ({results['ok_no_eof']/total:.1%})", flush=True)
    print(f"  FAILED structural checks: {results['fail']} ({results['fail']/total:.1%})", flush=True)
    if sizes_ok:
        print(f"  Avg size of OK files: {sum(sizes_ok)/len(sizes_ok)/1024/1024:.2f} MB", flush=True)

    if failures:
        print(f"\nFirst {min(30, len(failures))} failures:", flush=True)
        for path, reason in failures[:30]:
            print(f"  {path}: {reason}", flush=True)

    if no_text_examples:
        print(f"\nFirst 10 no-extractable-text examples (may be legit scans):", flush=True)
        for path, pages in no_text_examples[:10]:
            print(f"  {path}: {pages} pages", flush=True)


if __name__ == '__main__':
    main()
