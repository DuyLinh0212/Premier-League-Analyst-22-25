#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
test -f .env || { echo 'Copy .env.example to .env and configure existing services.' >&2; exit 1; }
docker compose --env-file .env config --quiet
if [[ "${1:-}" == "--build" ]]; then
  docker compose --env-file .env --profile tools build runtime api web
fi
docker compose --env-file .env --profile tools run --rm --no-deps --pull never publish
docker compose --env-file .env up -d --no-build --pull never --wait api web
