#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "${PL_PYTHON_BIN:-python3}" "$script_dir/run.py" all "$@"
