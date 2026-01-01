from __future__ import annotations
import json, os, threading, time
from pathlib import Path
from typing import Dict, Any
_LOCK = threading.Lock()
def _now_ms() -> int:
    return int(time.time() * 1000)
def emit_supervisor_event(*, event: str, account_label: str, payload: Dict[str, Any]) -> None:
    try:
        root = Path(os.getenv("FLASHBACK_ROOT", Path(__file__).resolve().parents[2]))
        state = root / "state"
        state.mkdir(parents=True, exist_ok=True)
        path = state / "supervisor_events.v1.jsonl"
        rec = {
            "ts_ms": _now_ms(),
            "event": event,
            "account_label": account_label,
            "mode": os.getenv("FLASHBACK_MODE","UNKNOWN"),
            "payload": payload,
        }
        with _LOCK:
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass
