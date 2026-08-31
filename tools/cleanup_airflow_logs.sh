#!/bin/bash
# Deletes Airflow task logs older than 1 day.
#
# Log volume grows unbounded under @continuous scheduling across ~30 DAGs -
# some accumulate tens of thousands of run_id directories within days (e.g.
# download_arxiv_scientific hit ~19,700 in under a week), which slows down
# container restarts (the airflow-init chown step walks this whole tree)
# and eats disk. Airflow has no built-in retention for local file logging,
# so this runs nightly via cron on the deployment host.
#
# Intended to run on the deployment host, not inside a container - it
# operates on the host-side bind mount (${AIRFLOW_PROJ_DIR:-.}/logs in
# docker-compose.yaml), not /opt/airflow/logs inside a container.
set -euo pipefail

LOG_DIR="${1:-/home/s939/apache-airflow/logs}"

# Found 2026-08-30/31: on a bad night (huge backlog, slow disk contention
# from other things -- an airflow-init chown walking the same tree, in this
# case), a single run can take longer than the 24h until the next cron
# fire, and without a lock the next run starts anyway -- both `find -delete`
# instances then race on the same files (harmless "No such file or
# directory" errors, since both operations are idempotent, but doubles disk
# I/O on an already-contended slow spinning disk for no benefit). flock
# ensures only one instance ever runs; a second cron fire while the first
# is still going just exits immediately instead of piling on.
exec 200>"/tmp/cleanup_airflow_logs.lock"
flock -n 200 || { echo "Another cleanup run is still in progress, skipping."; exit 0; }

find "$LOG_DIR" -type f -mtime +1 -delete
find "$LOG_DIR" -depth -mindepth 1 -type d -empty -delete
