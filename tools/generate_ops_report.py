#!/usr/bin/env python3
"""Regenerates the pipeline ops report as a static HTML file.

Runs directly on the deployment host (not inside a container) so it can
shell out to `docker exec` against the mssql/postgres containers and
connect to the host-published FTP port directly. Intended to be invoked
by cron every 15 minutes; see the accompanying crontab entry.

Output is written to REPORT_OUTPUT_PATH, which nginx (the nginx-5335
service in docker-compose.yaml) serves as a static file at /report/.
"""
import ftplib
import html
import os
import re
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone

MSSQL_CONTAINER = 'apache-airflow-mssql-1'
POSTGRES_CONTAINER = 'apache-airflow-postgres-1'
FTP_HOST = '127.0.0.1'
REPORT_OUTPUT_PATH = '/home/s939/apache-airflow/reports/index.html'

# Credentials are read from the environment only - never hardcoded here.
# The cron job sources a local, non-git-tracked env file before running
# this script (see the deployment notes alongside this file).
MSSQL_PASSWORD = os.environ.get('MSSQL_SA_PASSWORD')
FTP_USER = os.environ.get('REPORT_FTP_USER')
FTP_PASSWORD = os.environ.get('REPORT_FTP_PASSWORD')

if not all([MSSQL_PASSWORD, FTP_USER, FTP_PASSWORD]):
    sys.exit(
        'Missing required environment variables: MSSQL_SA_PASSWORD, '
        'REPORT_FTP_USER, REPORT_FTP_PASSWORD must all be set.'
    )

SOURCE_PATTERNS = [
    ('gujarati_literature', '%gujarati_literature%'),
    ('gujarati_news', '%gujarati_news%'),
    ('gujarati_science_natural', '%gujarati_science_natural%'),
    ('gujarati_science_social', '%gujarati_science_social%'),
    ('gujarati_law', '%gujarati_law%'),
    ('gujarati_official', '%gujarati_official%'),
    ('gujarati_dictionary', '%gujarati_dictionary%'),
    ('russian_science', '%russian_science%'),
    ('russian_literature_modern', '%russian_literature_modern%'),
    ('russian_literature_classic', '%russian_literature_classic%'),
    ('russian_news', '%russian_news%'),
    ('russian_law', '%russian_law%'),
    ('russian_social_science', '%russian_social_science%'),
    ('english_science', '%english_science%'),
    ('english_literature_modern', '%english_literature_modern%'),
    ('english_literature_classic', '%english_literature_classic%'),
    ('english_news', '%english_news%'),
    ('english_law', '%english_law%'),
    ('english_social_science', '%english_social_science%'),
    ('arxiv', '%arxiv%'),
    ('cyberleninka', '%lenin%'),
    ('pubmed', '%ncbi%'),
    ('semantic_scholar', '%semanticscholar%'),
    ('springer', '%springer%'),
]

FTP_FOLDERS = [
    'arxiv', 'cyberleninka', 'springer',
    'gujarati/literature', 'gujarati/news', 'gujarati/science_natural',
    'gujarati/science_social', 'gujarati/law', 'gujarati/official',
    'gujarati/dictionary',
    'russian/science', 'russian/literature_modern', 'russian/literature_classic',
    'russian/news', 'russian/law', 'russian/social_science',
    'english/science', 'english/literature_modern', 'english/literature_classic',
    'english/news', 'english/law', 'english/social_science',
]

DAG_IDS = [
    'download_arxiv_scientific', 'download_pubmed', 'download_semantic_scholar',
    'download_gujarati_literature', 'download_gujarati_news',
    'download_gujarati_science_natural', 'download_gujarati_science_social',
    'download_gujarati_science_archive', 'download_gujarati_law',
    'download_gujarati_official', 'download_gujarati_dictionary',
    'download_russian_science', 'download_russian_literature_modern',
    'download_russian_literature_classic', 'download_russian_news',
    'download_russian_law', 'download_russian_social_science',
    'download_english_science', 'download_english_literature_modern',
    'download_english_literature_classic', 'download_english_news',
    'download_english_law', 'download_english_social_science',
]


def run_sqlcmd(query: str) -> list[list[str]]:
    cmd = [
        'docker', 'exec', MSSQL_CONTAINER,
        '/opt/mssql-tools18/bin/sqlcmd',
        '-S', 'localhost', '-U', 'sa', '-P', MSSQL_PASSWORD, '-C',
        '-h', '-1', '-W', '-s', '|',
        '-Q', f'SET NOCOUNT ON; {query}',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith('---') or line.startswith('('):
            continue
        rows.append([c.strip() for c in line.split('|')])
    return rows


def run_psql(query: str) -> list[list[str]]:
    cmd = [
        'docker', 'exec', POSTGRES_CONTAINER,
        'psql', '-U', 'airflow', '-d', 'airflow', '-t', '-A', '-F', ',',
        '-c', query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(line.split(','))
    return rows


def get_source_breakdown() -> list[dict]:
    case_expr = 'CASE ' + ' '.join(
        f"WHEN PDFUrl LIKE '{pattern}' THEN '{name}'" for name, pattern in SOURCE_PATTERNS
    ) + " ELSE 'other' END"
    query = f"""
        SELECT {case_expr} AS Source, COUNT(*) AS Total,
          SUM(CASE WHEN LocationInFileSystem NOT IN ('','NA') THEN 1 ELSE 0 END) AS Downloaded,
          SUM(CASE WHEN LocationInFileSystem = '' THEN 1 ELSE 0 END) AS Pending,
          SUM(CASE WHEN LocationInFileSystem = 'NA' THEN 1 ELSE 0 END) AS NotAvailable
        FROM TextCorpuses.dbo.PdfDocuments
        GROUP BY {case_expr}
        ORDER BY Downloaded DESC;
    """
    rows = run_sqlcmd(query)
    return [
        {'source': r[0], 'total': int(r[1]), 'downloaded': int(r[2]),
         'pending': int(r[3]), 'na': int(r[4])}
        for r in rows if len(r) == 5
    ]


def get_grand_total() -> dict:
    rows = run_sqlcmd(
        "SELECT COUNT(*), SUM(CASE WHEN LocationInFileSystem NOT IN ('','NA') THEN 1 ELSE 0 END) "
        "FROM TextCorpuses.dbo.PdfDocuments;"
    )
    total, downloaded = rows[0]
    return {'total': int(total), 'downloaded': int(downloaded)}


def get_24h_inserted() -> int:
    """Count of URLs added in the last 24h, from InsertedAt (added
    database-v0.24.sql). Rows inserted before that migration have a NULL
    InsertedAt and are excluded, so this undercounts until 24h of history
    has accumulated after the column was added."""
    rows = run_sqlcmd(
        "SELECT COUNT(*) FROM TextCorpuses.dbo.PdfDocuments "
        "WHERE InsertedAt >= DATEADD(HOUR, -24, SYSUTCDATETIME());"
    )
    return int(rows[0][0]) if rows and rows[0][0].isdigit() else 0


def get_pdf_downloading_runs() -> dict:
    """pdf_downloading's own success/failed run counts in the last 24h.

    Added 2026-07-16: this DAG was never tracked anywhere in the report -
    only the 23 upstream *discovery* DAGs (DAG_IDS below) were, even
    though pdf_downloading is where actual PDF throughput lives and where
    that day's incidents (proxy-pool exhaustion, an FTP-server wedge)
    both manifested as failed/slow runs. A 24h rolling "PDF loaded" total
    barely dips from a 10-30 minute outage, so this was invisible without
    manually querying dag_run.
    """
    rows = run_psql(
        "SELECT SUM(CASE WHEN state='success' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) "
        "FROM dag_run WHERE dag_id='pdf_downloading' "
        "AND start_date >= NOW() - INTERVAL '24 hours';"
    )
    if rows and len(rows[0]) == 2:
        success = int(rows[0][0]) if rows[0][0] else 0
        failed = int(rows[0][1]) if rows[0][1] else 0
        return {'success': success, 'failed': failed}
    return {'success': 0, 'failed': 0}


def get_recent_throughput(hours: int = 4, bucket_minutes: int = 15) -> list[dict]:
    """PDF download counts in fixed-size recent buckets, from
    PdfDocuments.ClaimedAt (set only on a successful download - see
    save_location()/downloadOne() in pdf-downloading-dag.py).

    Added 2026-07-16 alongside get_pdf_downloading_runs(): the report
    previously had no time-series view at all, only 24h rolling totals,
    so a short outage was mathematically invisible in it. Buckets with no
    successful downloads are explicitly filled with 0 (not omitted) so a
    gap renders as a visible zero bar, not a silently-missing row.
    """
    rows = run_sqlcmd(f"""
        SELECT CONVERT(varchar, DATEADD(minute, (DATEDIFF(minute, 0, ClaimedAt)/{bucket_minutes})*{bucket_minutes}, 0), 120) AS Bucket,
               COUNT(*) AS Cnt
        FROM TextCorpuses.dbo.PdfDocuments
        WHERE ClaimedAt > DATEADD(hour, -{hours}, GETUTCDATE())
        GROUP BY DATEADD(minute, (DATEDIFF(minute, 0, ClaimedAt)/{bucket_minutes})*{bucket_minutes}, 0);
    """)
    counts = {}
    for r in rows:
        if len(r) == 2:
            try:
                counts[r[0]] = int(r[1])
            except ValueError:
                continue

    now = datetime.now(timezone.utc)
    now_bucket = now.replace(
        minute=(now.minute // bucket_minutes) * bucket_minutes, second=0, microsecond=0,
    )
    n_buckets = hours * 60 // bucket_minutes
    buckets = []
    for i in range(n_buckets, -1, -1):
        bucket_time = now_bucket - timedelta(minutes=bucket_minutes * i)
        key = bucket_time.strftime('%Y-%m-%d %H:%M:%S')
        buckets.append({'label': bucket_time.strftime('%H:%M'), 'count': counts.get(key, 0)})
    return buckets


def get_24h_dag_runs() -> dict:
    dag_list = ",".join(f"'{d}'" for d in DAG_IDS)
    rows = run_psql(
        "SELECT dag_id, "
        "SUM(CASE WHEN state='success' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) "
        f"FROM dag_run WHERE dag_id IN ({dag_list}) "
        "AND start_date >= NOW() - INTERVAL '24 hours' GROUP BY dag_id;"
    )
    return {r[0]: {'success': int(r[1]), 'failed': int(r[2])} for r in rows if len(r) == 3}


def get_dag_paused_states() -> dict:
    dag_list = ",".join(f"'{d}'" for d in DAG_IDS)
    rows = run_psql(f"SELECT dag_id, is_paused FROM dag WHERE dag_id IN ({dag_list});")
    return {r[0]: r[1] == 't' for r in rows if len(r) == 2}


def ftp_connect() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, 21, timeout=20)
    ftp.login(FTP_USER, FTP_PASSWORD)
    return ftp


def get_ftp_stats() -> dict:
    ftp = ftp_connect()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y%m%d%H%M%S')
    folders = {}
    for folder in FTP_FOLDERS:
        try:
            total_size = 0
            total_files = 0
            recent_files = 0
            for name, facts in ftp.mlsd(folder):
                if facts.get('type') != 'file':
                    continue
                total_files += 1
                total_size += int(facts.get('size', 0))
                if facts.get('modify', '')[:14] >= cutoff:
                    recent_files += 1
            folders[folder] = {'files': total_files, 'size_mb': total_size / 1024 / 1024,
                                'recent_24h': recent_files}
        except ftplib.error_perm:
            folders[folder] = {'files': 0, 'size_mb': 0.0, 'recent_24h': 0}
    ftp.quit()
    return folders


def check_shodhganga_reachable() -> bool:
    try:
        socket.create_connection(('shodhganga.inflibnet.ac.in', 443), timeout=8).close()
        return True
    except OSError:
        return False


def get_host_resources() -> dict:
    free_out = subprocess.run(['free', '-b'], capture_output=True, text=True).stdout
    mem_line = [l for l in free_out.splitlines() if l.startswith('Mem:')][0].split()
    swap_line = [l for l in free_out.splitlines() if l.startswith('Swap:')][0].split()
    mem_total, mem_used = int(mem_line[1]), int(mem_line[2])
    swap_total, swap_used = int(swap_line[1]), int(swap_line[2])

    uptime_out = subprocess.run(['uptime'], capture_output=True, text=True).stdout
    load_match = re.search(r'load average:\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)', uptime_out)
    load = load_match.groups() if load_match else ('?', '?', '?')

    nproc = int(subprocess.run(['nproc'], capture_output=True, text=True).stdout.strip())

    return {
        'mem_used_gb': mem_used / 1024 ** 3, 'mem_total_gb': mem_total / 1024 ** 3,
        'swap_used_gb': swap_used / 1024 ** 3, 'swap_total_gb': swap_total / 1024 ** 3,
        'load': load, 'nproc': nproc,
    }


def get_disk_stats() -> list[dict]:
    """Per-drive capacity + a live I/O utilization snapshot.

    Added 2026-07-16 after migrating mssql's data/log files and Docker's
    storage root from sda (which was measured at 80-99% utilization,
    170-450ms write latency - the throughput bottleneck at the time) onto
    a previously-unused NVMe drive. Tracks both going forward so a
    regression back toward sda saturation is visible here, not just
    discovered ad hoc.
    """
    drives = [
        {'device': 'sda', 'label': 'sda — ОС, FTP, подкачка', 'mount': '/'},
        {'device': 'nvme0n1', 'label': 'nvme0n1 — mssql + Docker root', 'mount': '/mnt/nvme-mssql'},
    ]

    for d in drives:
        df_out = subprocess.run(['df', '-B1', d['mount']], capture_output=True, text=True).stdout
        fields = df_out.splitlines()[1].split()
        d['used_gb'] = int(fields[2]) / 1024 ** 3
        d['total_gb'] = int(fields[1]) / 1024 ** 3

    # Two samples 1s apart; the first iostat table is a since-boot cumulative
    # average, not current activity - only the second (live) sample is used.
    try:
        iostat_out = subprocess.run(
            ['iostat', '-dx', '1', '2'], capture_output=True, text=True, timeout=15,
        ).stdout
        device_lines = {}
        for line in iostat_out.splitlines():
            parts = line.split()
            if parts and parts[0] in ('sda', 'nvme0n1'):
                device_lines[parts[0]] = parts  # last occurrence wins = 2nd sample
        for d in drives:
            parts = device_lines.get(d['device'])
            # columns: Device r/s rkB/s rrqm/s %rrqm r_await rareq-sz w/s wkB/s
            #          wrqm/s %wrqm w_await wareq-sz d/s dkB/s drqm/s %drqm
            #          d_await dareq-sz f/s f_await aqu-sz %util
            if parts and len(parts) >= 22:
                d['w_await_ms'] = float(parts[10])
                d['util_pct'] = float(parts[21])
            else:
                d['w_await_ms'] = None
                d['util_pct'] = None
    except (subprocess.SubprocessError, OSError, ValueError):
        for d in drives:
            d['w_await_ms'] = None
            d['util_pct'] = None

    return drives


def get_container_stats() -> list[dict]:
    out = subprocess.run(
        ['docker', 'stats', '--no-stream', '--format', '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}'],
        capture_output=True, text=True, timeout=30,
    ).stdout
    containers = []
    for line in out.splitlines():
        parts = line.split('\t')
        if len(parts) == 4:
            containers.append({'name': parts[0], 'cpu': parts[1], 'mem': parts[2], 'mem_pct': parts[3]})
    containers.sort(key=lambda c: float(c['mem_pct'].rstrip('%') or 0), reverse=True)
    return containers


CSS = """
  :root {
    --bg: #f3f5f4; --surface: #ffffff; --surface-2: #eaeeed; --border: #d8dedd;
    --text: #1b2426; --text-dim: #5c6b6e; --accent: #1f7d8c; --accent-soft: #dcecee;
    --good: #2f8f52; --good-soft: #e3f2e7; --warn: #a6741c; --warn-soft: #f6ecd8;
    --bad: #b8443d; --bad-soft: #fbe7e5;
    --mono: ui-monospace, "Cascadia Code", "SF Mono", "Consolas", "Liberation Mono", monospace;
    --sans: "Segoe UI", -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1416; --surface: #161e21; --surface-2: #1d2528; --border: #2a3438;
      --text: #e8edee; --text-dim: #93a3a8; --accent: #5fc4d1; --accent-soft: #17363c;
      --good: #6fcb8c; --good-soft: #16311f; --warn: #e0b355; --warn-soft: #3a2e14;
      --bad: #e58179; --bad-soft: #3a1a17;
    }
  }
  :root[data-theme="dark"] {
    --bg: #0f1416; --surface: #161e21; --surface-2: #1d2528; --border: #2a3438;
    --text: #e8edee; --text-dim: #93a3a8; --accent: #5fc4d1; --accent-soft: #17363c;
    --good: #6fcb8c; --good-soft: #16311f; --warn: #e0b355; --warn-soft: #3a2e14;
    --bad: #e58179; --bad-soft: #3a1a17;
  }
  :root[data-theme="light"] {
    --bg: #f3f5f4; --surface: #ffffff; --surface-2: #eaeeed; --border: #d8dedd;
    --text: #1b2426; --text-dim: #5c6b6e; --accent: #1f7d8c; --accent-soft: #dcecee;
    --good: #2f8f52; --good-soft: #e3f2e7; --warn: #a6741c; --warn-soft: #f6ecd8;
    --bad: #b8443d; --bad-soft: #fbe7e5;
  }
  * { box-sizing: border-box; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); margin: 0; padding: 2.5rem 1.25rem 5rem; }
  .page { max-width: 980px; margin: 0 auto; display: flex; flex-direction: column; gap: 2.25rem; }
  header.masthead { display: flex; flex-direction: column; gap: 0.4rem; border-bottom: 1px solid var(--border); padding-bottom: 1.5rem; }
  .eyebrow { font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.12em; text-transform: uppercase; color: var(--accent); }
  h1 { font-family: var(--mono); font-size: 1.9rem; font-weight: 600; margin: 0; text-wrap: balance; letter-spacing: -0.01em; }
  .subtitle { color: var(--text-dim); font-size: 0.95rem; max-width: 62ch; }
  .timestamp { font-family: var(--mono); font-size: 0.8rem; color: var(--text-dim); }
  section { display: flex; flex-direction: column; gap: 0.9rem; }
  h2 { font-family: var(--mono); font-size: 1.05rem; font-weight: 600; margin: 0; display: flex; align-items: baseline; gap: 0.6rem; }
  h2 .section-note { font-family: var(--sans); font-weight: 400; font-size: 0.82rem; color: var(--text-dim); }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 0.85rem; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 1rem 1.1rem; display: flex; flex-direction: column; gap: 0.3rem; }
  .stat-label { font-size: 0.72rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-dim); }
  .stat-value { font-family: var(--mono); font-size: 1.55rem; font-variant-numeric: tabular-nums; font-weight: 600; }
  .stat-value.accent { color: var(--accent); }
  .stat-sub { font-size: 0.78rem; color: var(--text-dim); }
  .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 6px; background: var(--surface); }
  table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
  th, td { padding: 0.6rem 0.8rem; text-align: left; white-space: nowrap; }
  th { font-family: var(--mono); font-size: 0.7rem; letter-spacing: 0.06em; text-transform: uppercase; color: var(--text-dim); border-bottom: 1px solid var(--border); font-weight: 500; }
  tbody tr:not(:last-child) td { border-bottom: 1px solid var(--surface-2); }
  td.num, th.num { text-align: right; font-family: var(--mono); font-variant-numeric: tabular-nums; }
  td.name { font-weight: 600; }
  .bar-cell { display: flex; align-items: center; gap: 0.5rem; min-width: 160px; }
  .bar-track { flex: 1; height: 7px; border-radius: 4px; background: var(--surface-2); overflow: hidden; display: flex; min-width: 90px; }
  .bar-fill.good { background: var(--good); height: 100%; }
  .bar-fill.warn { background: var(--warn); height: 100%; }
  .bar-fill.bad { background: var(--bad); height: 100%; }
  .bar-pct { font-family: var(--mono); font-size: 0.76rem; color: var(--text-dim); width: 3.2em; text-align: right; }
  .pill { display: inline-flex; align-items: center; gap: 0.35rem; font-family: var(--mono); font-size: 0.72rem; padding: 0.15rem 0.55rem; border-radius: 99px; font-weight: 600; white-space: nowrap; }
  .pill.good { background: var(--good-soft); color: var(--good); }
  .pill.warn { background: var(--warn-soft); color: var(--warn); }
  .pill.bad { background: var(--bad-soft); color: var(--bad); }
  .pill.neutral { background: var(--surface-2); color: var(--text-dim); }
  .meter-grid { display: flex; flex-direction: column; gap: 0.6rem; }
  .meter-row { display: grid; grid-template-columns: 160px 1fr 4.5em; align-items: center; gap: 0.75rem; font-size: 0.85rem; }
  .meter-row .meter-value { font-family: var(--mono); text-align: right; color: var(--text-dim); font-variant-numeric: tabular-nums; }
  .meter-track { height: 9px; border-radius: 5px; background: var(--surface-2); overflow: hidden; }
  .meter-fill { height: 100%; border-radius: 5px; }
  .issue-list { display: flex; flex-direction: column; gap: 0.7rem; }
  .issue { background: var(--surface); border: 1px solid var(--border); border-left: 3px solid var(--border); border-radius: 6px; padding: 0.85rem 1rem; display: flex; flex-direction: column; gap: 0.3rem; }
  .issue.bad { border-left-color: var(--bad); }
  .issue.warn { border-left-color: var(--warn); }
  .issue.good { border-left-color: var(--good); }
  .issue-head { display: flex; align-items: center; gap: 0.6rem; }
  .issue-title { font-weight: 600; font-size: 0.92rem; }
  .issue-body { font-size: 0.86rem; color: var(--text-dim); line-height: 1.5; }
  code { font-family: var(--mono); background: var(--surface-2); padding: 0.1rem 0.35rem; border-radius: 4px; font-size: 0.85em; }
  footer { border-top: 1px solid var(--border); padding-top: 1.25rem; font-size: 0.8rem; color: var(--text-dim); display: flex; flex-direction: column; gap: 0.3rem; }
"""


def bar(pct: float) -> str:
    cls = 'good' if pct < 90 else 'warn'
    return (f'<div class="bar-cell"><div class="bar-track">'
            f'<div class="bar-fill {cls}" style="width:{pct:.1f}%"></div></div>'
            f'<span class="bar-pct">{pct:.1f}%</span></div>')


def throughput_bar(count: int, max_count: int) -> str:
    """Unlike bar() above (where a full bar is a warning - disk/CPU
    saturation), here a full bar is good: more downloads is better. A
    zero-count bucket renders as a full-width red bar specifically so a
    gap can't be missed while skimming the report."""
    if count == 0:
        return (f'<div class="bar-cell"><div class="bar-track">'
                f'<div class="bar-fill bad" style="width:100%"></div></div>'
                f'<span class="bar-pct">0</span></div>')
    pct = max((count / max_count * 100) if max_count else 0, 4)
    return (f'<div class="bar-cell"><div class="bar-track">'
            f'<div class="bar-fill good" style="width:{pct:.1f}%"></div></div>'
            f'<span class="bar-pct">{count:,}</span></div>')


def meter(label: str, used: float, total: float, unit: str, warn_pct: float = 80) -> str:
    pct = (used / total * 100) if total else 0
    color = 'var(--bad)' if pct >= warn_pct else ('var(--warn)' if pct >= 60 else 'var(--good)')
    return (f'<div class="meter-row"><span class="meter-label">{html.escape(label)}</span>'
            f'<div class="meter-track"><div class="meter-fill" style="width:{pct:.0f}%; background:{color};"></div></div>'
            f'<span class="meter-value">{pct:.0f}%</span></div>')


def render(sources, grand_total, dag_runs, ftp_stats, host, containers,
           shodhganga_up, paused_states, generated_at, inserted_24h, disks,
           pdf_downloading_runs, recent_throughput) -> str:
    total_ftp_files = sum(f['files'] for f in ftp_stats.values())
    total_ftp_size_gb = sum(f['size_mb'] for f in ftp_stats.values()) / 1024
    total_24h_downloads = sum(f['recent_24h'] for f in ftp_stats.values())
    total_dag_success = sum(d['success'] for d in dag_runs.values())
    total_dag_failed = sum(d['failed'] for d in dag_runs.values())
    downloaded_pct = (grand_total['downloaded'] / grand_total['total'] * 100) if grand_total['total'] else 0

    source_rows = []
    for s in sources:
        pct = (s['downloaded'] / s['total'] * 100) if s['total'] else 0
        is_paused = paused_states.get(f"download_{s['source']}", None)
        if s['total'] == 0:
            status = '<span class="pill bad">нет данных</span>'
        elif is_paused:
            status = '<span class="pill neutral">приостановлено</span>'
        elif pct >= 95:
            status = '<span class="pill good">завершено</span>'
        else:
            status = '<span class="pill good">в норме</span>'
        source_rows.append(
            f'<tr><td class="name">{html.escape(s["source"])}</td>'
            f'<td class="num">{s["total"]:,}</td><td class="num">{s["downloaded"]:,}</td>'
            f'<td>{bar(pct)}</td><td class="num">{s["pending"]:,}</td>'
            f'<td class="num">{s["na"]:,}</td><td>{status}</td></tr>'
        )

    dag_rows = []
    for dag_id in DAG_IDS:
        source_name = dag_id.replace('download_', '')
        d = dag_runs.get(dag_id, {'success': 0, 'failed': 0})
        dag_rows.append(
            f'<tr><td class="name">{html.escape(source_name)}</td>'
            f'<td class="num">{d["success"]:,}</td><td class="num">{d["failed"]:,}</td></tr>'
        )

    ftp_rows = []
    for folder, f in sorted(ftp_stats.items(), key=lambda kv: -kv[1]['size_mb']):
        ftp_rows.append(
            f'<tr><td class="name">{html.escape(folder)}</td><td class="num">{f["files"]:,}</td>'
            f'<td class="num">{f["size_mb"]:,.1f} MB</td><td class="num">{f["recent_24h"]:,}</td></tr>'
        )

    disk_rows = []
    for d in disks:
        cap_pct = (d['used_gb'] / d['total_gb'] * 100) if d['total_gb'] else 0
        if d['util_pct'] is None:
            util_cell = '<span class="pill neutral">н/д</span>'
            latency_cell = '—'
        else:
            util_cell = bar(d['util_pct'])
            latency_cell = f"{d['w_await_ms']:.1f} мс"
        disk_rows.append(
            f'<tr><td class="name">{html.escape(d["label"])}</td>'
            f'<td class="num">{d["used_gb"]:.0f} / {d["total_gb"]:.0f} ГБ ({cap_pct:.0f}%)</td>'
            f'<td>{util_cell}</td><td class="num">{latency_cell}</td></tr>'
        )

    max_throughput = max((b['count'] for b in recent_throughput), default=0)
    throughput_rows = []
    for b in recent_throughput:
        throughput_rows.append(
            f'<tr><td class="name">{html.escape(b["label"])}</td>'
            f'<td>{throughput_bar(b["count"], max_throughput)}</td></tr>'
        )
    # The most recent bucket is usually still partially in-progress at
    # generation time - a low/zero count there is expected, not a gap.
    # Only buckets that had a full window to fill count for the alert.
    zero_buckets = [b['label'] for b in recent_throughput[:-1] if b['count'] == 0]

    container_rows = []
    for c in containers:
        container_rows.append(
            f'<tr><td class="name">{html.escape(c["name"])}</td><td class="num">{html.escape(c["cpu"])}</td>'
            f'<td class="num">{html.escape(c["mem"])}</td><td class="num">{html.escape(c["mem_pct"])}</td></tr>'
        )

    issues = []
    if zero_buckets:
        issues.append(('bad', f'Разрыв в загрузках PDF ({len(zero_buckets)} окон по 0)',
                        'Загрузки полностью останавливались в следующих 15-минутных окнах (UTC): '
                        + ', '.join(zero_buckets) + '. Проверьте состояние FTP-сервера '
                        '(systemctl status filezilla-server, реально ли принимает passive-соединения, '
                        'не только "active") и пул прокси (IPProxy).'))
    if pdf_downloading_runs['failed'] > 0:
        issues.append(('warn', f"pdf_downloading: {pdf_downloading_runs['failed']} неудачных запусков за 24ч",
                        'Проверьте логи последних неудачных запусков в Airflow - типичные причины: '
                        'истощение пула прокси (IPProxy.SuccessCount) или проблемы с FTP-сервером.'))
    if paused_states.get('download_semantic_scholar'):
        issues.append(('warn', 'Semantic Scholar приостановлен',
                        'API отклоняет запросы ключа с бесплатных/личных почтовых доменов; DAG приостановлен и исключён из round-robin ротации до появления корпоративной почты.'))
    if not shodhganga_up:
        issues.append(('bad', 'Shodhganga недоступна',
                        'shodhganga.inflibnet.ac.in не принимает соединения сейчас, что блокирует поиск для gujarati_science_social.'))
    swap_pct = (host['swap_used_gb'] / host['swap_total_gb'] * 100) if host['swap_total_gb'] else 0
    if swap_pct >= 50:
        issues.append(('warn', f'Повышенное использование подкачки (swap) ({swap_pct:.0f}%)',
                        'Коррелирует с более медленным холодным стартом контейнеров на этом сервере из-за конкуренции за дисковый ввод-вывод. Не срочно, но стоит наблюдать.'))
    if not issues:
        issues.append(('good', 'Активных проблем не обнаружено', 'Все отслеживаемые показатели в норме.'))

    issue_html = ''.join(
        f'<div class="issue {level}"><div class="issue-head"><span class="issue-title">{html.escape(title)}</span></div>'
        f'<div class="issue-body">{html.escape(body)}</div></div>'
        for level, title, body in issues
    )

    return f"""<meta charset="utf-8">
<title>Конвейер обработки корпусов — Отчёт</title>
<style>{CSS}</style>
<div class="page">
  <header class="masthead">
    <span class="eyebrow">Обработка текстовых корпусов — Инфраструктура</span>
    <h1>Отчёт о работе конвейера</h1>
    <p class="subtitle">Формируется автоматически каждые 15 минут на основе данных PdfDocuments, FTP, Airflow и метрик сервера.</p>
    <span class="timestamp">Сформирован {generated_at} UTC · сервер 172.21.128.103 · обновляется каждые 15 минут через cron</span>
  </header>

  <section>
    <div class="stat-grid">
      <div class="stat-card"><span class="stat-label">Отслеживается URL PDF</span><span class="stat-value">{grand_total['total']:,}</span><span class="stat-sub">источников: {len(sources)}</span></div>
      <div class="stat-card"><span class="stat-label">Загружено</span><span class="stat-value accent">{grand_total['downloaded']:,}</span><span class="stat-sub">{downloaded_pct:.1f}% от общей очереди</span></div>
      <div class="stat-card"><span class="stat-label">Занято места на FTP</span><span class="stat-value">{total_ftp_size_gb:.1f} ГБ</span><span class="stat-sub">~{total_ftp_files:,} файлов</span></div>
      <div class="stat-card"><span class="stat-label">Загрузка сервера ({host['nproc']} ядер)</span><span class="stat-value">{'/'.join(host['load'])}</span><span class="stat-sub">среднее за 1/5/15 мин</span></div>
    </div>
  </section>

  <section>
    <h2>Скорость загрузки по источникам <span class="section-note">PdfDocuments, группировка по шаблону URL</span></h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Источник</th><th class="num">Всего</th><th class="num">Загружено</th><th>Прогресс</th><th class="num">В очереди</th><th class="num">Недоступно</th><th>Статус</th></tr></thead>
      <tbody>{''.join(source_rows)}</tbody>
    </table></div>
  </section>

  <section>
    <h2>Последние 24 часа</h2>
    <div class="stat-grid">
      <div class="stat-card"><span class="stat-label">PDF загружено</span><span class="stat-value accent">{total_24h_downloads:,}</span><span class="stat-sub">по времени изменения файла на FTP</span></div>
      <div class="stat-card"><span class="stat-label">URL добавлено</span><span class="stat-value accent">{inserted_24h:,}</span><span class="stat-sub">по PdfDocuments.InsertedAt</span></div>
      <div class="stat-card"><span class="stat-label">Запусков DAG обнаружения</span><span class="stat-value">{total_dag_success + total_dag_failed:,}</span><span class="stat-sub">с ошибкой: {total_dag_failed:,}</span></div>
      <div class="stat-card"><span class="stat-label">Запусков pdf_downloading</span><span class="stat-value">{pdf_downloading_runs['success'] + pdf_downloading_runs['failed']:,}</span><span class="stat-sub">с ошибкой: {pdf_downloading_runs['failed']:,}</span></div>
    </div>
    <div class="table-wrap"><table>
      <thead><tr><th>Источник</th><th class="num">Запуски обнаружения — успешно</th><th class="num">Запуски обнаружения — с ошибкой</th></tr></thead>
      <tbody>{''.join(dag_rows)}</tbody>
    </table></div>
    <p style="font-size:0.82rem;color:var(--text-dim);max-width:70ch;">
      «URL добавлено» считает только строки с заполненным <code>InsertedAt</code> (столбец добавлен в database-v0.24.sql) — первые 24 часа после миграции это число будет заниженным, пока не накопится полное окно.
      «Запусков pdf_downloading» — это сам DAG загрузки PDF (не обнаружения); его сбои (истощение пула прокси, зависание FTP и т.п.) — то, что реально останавливает throughput, и раньше нигде в отчёте не отслеживалось.
    </p>
  </section>

  <section>
    <h2>Пропускная способность <span class="section-note">PdfDocuments.ClaimedAt, последние 4 часа по 15 мин</span></h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Время (UTC)</th><th>Загружено PDF</th></tr></thead>
      <tbody>{''.join(throughput_rows)}</tbody>
    </table></div>
    <p style="font-size:0.82rem;color:var(--text-dim);max-width:70ch;">
      Добавлено 2026-07-16: 24-часовые совокупные показатели выше не показывают
      кратковременный простой (10-30 минут почти не меняют суточную сумму).
      Пустой (красный) столбец здесь означает ноль загрузок за это окно.
      Последний столбец обычно ещё не заполнен полностью на момент формирования отчёта.
    </p>
  </section>

  <section>
    <h2>Хранилище на FTP</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Папка</th><th class="num">Файлов</th><th class="num">Размер</th><th class="num">Добавлено (24ч)</th></tr></thead>
      <tbody>{''.join(ftp_rows)}</tbody>
    </table></div>
  </section>

  <section>
    <h2>Ресурсы сервера <span class="section-note">172.21.128.103 — {host['nproc']} ядер</span></h2>
    <div class="meter-grid">
      {meter(f"Память ({host['mem_used_gb']:.0f} / {host['mem_total_gb']:.0f} ГиБ)", host['mem_used_gb'], host['mem_total_gb'], 'GiB')}
      {meter(f"Подкачка ({host['swap_used_gb']:.1f} / {host['swap_total_gb']:.0f} ГиБ)", host['swap_used_gb'], host['swap_total_gb'], 'GiB', warn_pct=60)}
    </div>
  </section>

  <section>
    <h2>Диски <span class="section-note">sda vs nvme0n1 — ёмкость и загрузка в реальном времени (iostat)</span></h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Диск</th><th class="num">Занято / Всего</th><th>Загрузка (util%)</th><th class="num">Задержка записи</th></tr></thead>
      <tbody>{''.join(disk_rows)}</tbody>
    </table></div>
    <p style="font-size:0.82rem;color:var(--text-dim);max-width:70ch;">
      2026-07-16: mssql-data/mssql-backups и Docker storage root перенесены с sda
      (был на уровне 80-99% загрузки, задержка записи 170-450 мс — узкое место
      throughput) на ранее неиспользуемый nvme0n1. Обе секции отслеживаются здесь,
      чтобы регресс обратно к насыщению sda был виден сразу, а не находился вручную.
    </p>
  </section>

  <section>
    <h2>Использование ресурсов контейнерами <span class="section-note">docker stats, текущий снимок</span></h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Контейнер</th><th class="num">CPU</th><th class="num">Память</th><th class="num">Память %</th></tr></thead>
      <tbody>{''.join(container_rows)}</tbody>
    </table></div>
  </section>

  <section>
    <h2>Требует внимания</h2>
    <div class="issue-list">{issue_html}</div>
  </section>

  <footer>
    <span>Конвейер обработки текстовых корпусов — внутренний отчёт о работе системы, не для внешнего распространения.</span>
    <span>Формируется автоматически каждые 15 минут скриптом generate_ops_report.py. Данные приведены на момент формирования отчёта.</span>
  </footer>
</div>
"""


def main():
    generated_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    sources = get_source_breakdown()
    grand_total = get_grand_total()
    dag_runs = get_24h_dag_runs()
    paused_states = get_dag_paused_states()
    ftp_stats = get_ftp_stats()
    host = get_host_resources()
    disks = get_disk_stats()
    containers = get_container_stats()
    shodhganga_up = check_shodhganga_reachable()
    inserted_24h = get_24h_inserted()
    pdf_downloading_runs = get_pdf_downloading_runs()
    recent_throughput = get_recent_throughput()

    output = render(sources, grand_total, dag_runs, ftp_stats, host, containers,
                     shodhganga_up, paused_states, generated_at, inserted_24h, disks,
                     pdf_downloading_runs, recent_throughput)

    with open(REPORT_OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(output)
    print(f'{generated_at} UTC: report written to {REPORT_OUTPUT_PATH}')


if __name__ == '__main__':
    main()
