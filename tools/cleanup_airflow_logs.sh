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

find "$LOG_DIR" -type f -mtime +1 -delete
find "$LOG_DIR" -depth -mindepth 1 -type d -empty -delete
