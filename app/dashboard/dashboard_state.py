from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any
from time import time

from app.dashboard.schema_dashboard_v1 import empty_account_row

STATE_DIR = Path("state")
ORCH_STATE = STATE_DIR / "orchestrator_state.json"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"_error": str(e)}


def build_rows_from_orchestrator() -> Dict[str, Dict[str, Any]]:
    raw = load_json(ORCH_STATE)
    rows: Dict[str, Dict[str, Any]] = {}

    procs = raw.get("procs", {}) or {}

    now_ms = int(time() * 1000)

    for account_label, p in procs.items():
        row = empty_account_row(account_label)

        alive = bool(p.get("alive"))
        started_ts = p.get("started_ts_ms")

        row["enabled"] = True
        row["online"] = alive
        row["phase"] = "running" if alive else "stopped"
        row["last_heartbeat_ms"] = started_ts
        row["last_updated_ms"] = now_ms

        rows[account_label] = row

    return rows


def get_dashboard_state() -> Dict[str, Any]:
    rows = build_rows_from_orchestrator()

    return {
        "schema_version": 1,
        "rows": rows,
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(get_dashboard_state())
