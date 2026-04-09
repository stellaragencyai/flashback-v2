#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$REPO_ROOT/.venv/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Missing repo venv python at $VENV_PY" >&2
  exit 2
fi

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$VENV_PY" "$REPO_ROOT/app/tools/reset_lane_runtime_state.py" \
  --labels flashback01 flashback02 flashback03 flashback04 flashback05 flashback06 flashback07 flashback08 flashback09 \
  --apply

PYTHONPATH="$REPO_ROOT" "$VENV_PY" "$REPO_ROOT/app/tools/fleet_truth_guard.py" --window all --write
PYTHONPATH="$REPO_ROOT" "$VENV_PY" "$REPO_ROOT/app/tools/fleet_meta_brain.py" --window all --write
exec PYTHONPATH="$REPO_ROOT" "$VENV_PY" "$REPO_ROOT/app/tools/dashboard_truth_audit.py" --window all
