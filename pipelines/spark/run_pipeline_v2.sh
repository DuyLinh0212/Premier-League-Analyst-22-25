#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose --env-file portable/.env -f portable/compose.yaml exec -T airflow-scheduler /opt/python-runtime/bin/python /opt/project/pipelines/spark/pipeline.py "$@"
