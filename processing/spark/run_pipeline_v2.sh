#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose --env-file .env -f compose.yml exec -T airflow-scheduler /opt/python-runtime/bin/python /opt/project/processing/spark/pipeline.py "$@"
