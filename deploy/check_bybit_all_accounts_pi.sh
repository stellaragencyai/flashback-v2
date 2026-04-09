#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$REPO_ROOT/.venv/bin/python"
ENV_FILE="$REPO_ROOT/deploy/flashback_pi.env"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Missing repo venv python at $VENV_PY" >&2
  exit 2
fi

cd "$REPO_ROOT"
exec PYTHONPATH="$REPO_ROOT" "$VENV_PY" "$REPO_ROOT/app/tools/bybit_connectivity_check.py" \
  --env-file "$ENV_FILE" \
  --labels flashback01 flashback02 flashback03 flashback04 flashback05 flashback06 flashback07 flashback08 flashback09 flashback10 \
  --include-main \
  --include-disabled \
  --strict-creds \
  --strict-connectivity
