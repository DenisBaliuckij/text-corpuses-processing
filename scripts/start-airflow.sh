#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

if ! command -v docker &>/dev/null; then
    echo "Error: Docker is not installed or not in PATH." >&2
    exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "Error: Docker daemon is not running." >&2
    exit 1
fi

echo "Building Airflow image (first run takes 10-20 minutes due to PyTorch and spaCy models)..."
docker compose build

echo "Starting all services..."
docker compose up -d

echo ""
echo "Airflow UI:  http://localhost:8080"
echo "Login:       admin / admin"
echo ""
echo "Useful commands:"
echo "  docker compose logs -f            # stream logs from all services"
echo "  docker compose logs -f airflow-scheduler  # scheduler logs only"
echo "  docker compose down               # stop and remove containers"
echo "  docker compose down -v            # stop and also delete the postgres volume"
