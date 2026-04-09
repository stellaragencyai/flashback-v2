#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$REPO_ROOT/.venv/bin/python"
ENV_FILE="$REPO_ROOT/deploy/flashback_pi.env"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Missing repo venv python at $VENV_PY" >&2
  exit 2
fi

if [[ -f "$ENV_FILE" ]]; then
  CLEAN_ENV_FILE="$(mktemp)"
  trap 'rm -f "$CLEAN_ENV_FILE"' EXIT
  tr -d '\r' < "$ENV_FILE" > "$CLEAN_ENV_FILE"
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$CLEAN_ENV_FILE"
  set +a
  set -u
fi

cd "$REPO_ROOT"
"$VENV_PY" "$REPO_ROOT/app/tools/bybit_connectivity_check.py" \
  --env-file "$ENV_FILE" \
  --labels main \
  --include-main \
  --strict-creds \
  --strict-connectivity

exec "$VENV_PY" -u -m app.trading.main_account.supervisor
