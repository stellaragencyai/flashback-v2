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

"$VENV_PY" "$REPO_ROOT/app/tools/build_fleet_manifest.py" >/dev/null
"$VENV_PY" "$REPO_ROOT/app/tools/validate_config.py"

LABELS="$("$VENV_PY" - <<'PY'
from pathlib import Path
import yaml

repo_root = Path.cwd()
cfg_path = repo_root / "config" / "subaccounts.yaml"
cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
labels = []

accounts = cfg.get("accounts")
if isinstance(accounts, list):
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        label = str(acc.get("account_label") or "").strip()
        if not label:
            continue
        if bool(acc.get("enabled", True)) and bool(acc.get("enable_ai_stack", False)):
            labels.append(label)
else:
    for key, value in cfg.items():
        if key in ("version", "notes", "legacy"):
            continue
        if not isinstance(value, dict):
            continue
        if bool(value.get("enabled", True)) and bool(value.get("enable_ai_stack", False)):
            labels.append(str(key).strip())

print(",".join([x for x in labels if x]))
PY
)"

if [[ -z "$LABELS" ]]; then
  echo "No enabled AI-stack labels found in config/subaccounts.yaml" >&2
  exit 3
fi

if [[ "${FLASHBACK_SKIP_BYBIT_CHECK:-0}" != "1" ]]; then
  "$VENV_PY" "$REPO_ROOT/app/tools/bybit_connectivity_check.py" \
    --env-file "$ENV_FILE" \
    --labels ${LABELS//,/ } \
    --strict-creds \
    --strict-connectivity
fi

exec "$VENV_PY" -u "$REPO_ROOT/app/ops/orchestrator_v2.py" \
  --labels "$LABELS" \
  --restart \
  --health \
  --health-every 30 \
  --status-every 5
