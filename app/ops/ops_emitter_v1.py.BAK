import json
import time
from pathlib import Path
from threading import Lock

OPS_PATH = Path("state/ops_snapshot.json")
OPS_PATH.parent.mkdir(parents=True, exist_ok=True)

_lock = Lock()

def emit_ops(
    subaccount_id: str,
    status: str = "idle",
    pnl: float = 0.0,
    trades_open: int = 0,
    trades_closed: int = 0,
    risk_level: str = "normal",
    ai_confidence: float = 0.0,
    last_event: str = ""
):
    payload = {
        "subaccount_id": subaccount_id,
        "status": status,
        "pnl": pnl,
        "trades_open": trades_open,
        "trades_closed": trades_closed,
        "risk_level": risk_level,
        "ai_confidence": ai_confidence,
        "last_event": last_event,
        "ts_ms": int(time.time() * 1000)
    }

    with _lock:
        if OPS_PATH.exists():
            try:
                data = json.loads(OPS_PATH.read_text(encoding="utf-8"))
            except Exception:
                data = {}
        else:
            data = {}

        data[subaccount_id] = payload
        OPS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
