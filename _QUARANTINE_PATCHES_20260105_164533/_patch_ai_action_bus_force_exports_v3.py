from pathlib import Path

p = Path(r"app\core\ai_action_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "FORCE_EXPORTS_V3" in s:
    print("OK: FORCE_EXPORTS_V3 already present")
    raise SystemExit(0)

append = """
# --- FORCE_EXPORTS_V3 ---
# Guarantee expected bus API for ai_action_router + legacy callers:
#   ensure_bus(), publish_action(dict)->bool, read_actions(since_ts_ms=0, limit=500)->list[dict]
# If older module provides different names, alias to them. Otherwise provide a minimal JSONL bus.

import json as _json
from typing import Any as _Any, Dict as _Dict, List as _List

def ensure_bus() -> None:
    try:
        # Prefer any existing implementation
        fn = globals().get("ensure_bus")
        if callable(fn) and fn is not ensure_bus:
            return fn()
    except Exception:
        pass
    try:
        # Minimal: ensure file exists
        path = globals().get("ACTION_BUS_PATH") or globals().get("ACTION_LOG_PATH") or globals().get("AI_ACTION_LOG_PATH")
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("", encoding="utf-8")
    except Exception:
        return

def publish_action(action: _Dict[str, _Any]) -> bool:
    # Prefer any existing “append/publish” style function if present
    candidates = [
        "publish_action",
        "append_action",
        "write_action",
        "log_action",
        "emit_action",
        "push_action",
    ]
    for name in candidates:
        fn = globals().get(name)
        if callable(fn) and fn is not publish_action:
            try:
                out = fn(action)
                return bool(out) if out is not None else True
            except Exception:
                pass

    # Minimal fallback: append JSONL
    try:
        ensure_bus()
        path = globals().get("ACTION_BUS_PATH") or globals().get("ACTION_LOG_PATH") or globals().get("AI_ACTION_LOG_PATH")
        if path is None:
            return False
        row = dict(action or {})
        row.setdefault("schema_version", "action.v1")
        # ts_ms only if we can compute it safely
        try:
            now_fn = globals().get("_now_ms")
            row.setdefault("ts_ms", int(now_fn()) if callable(now_fn) else None)
            if row.get("ts_ms") is None:
                row.pop("ts_ms", None)
        except Exception:
            row.pop("ts_ms", None)

        with path.open("a", encoding="utf-8") as f:
            f.write(_json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\\n")
        return True
    except Exception:
        return False

def read_actions(since_ts_ms: int = 0, limit: int = 500) -> _List[_Dict[str, _Any]]:
    # Prefer any existing read function
    candidates = [
        "read_actions",
        "read_action_bus",
        "read_bus",
        "load_actions",
        "get_actions",
    ]
    for name in candidates:
        fn = globals().get(name)
        if callable(fn) and fn is not read_actions:
            try:
                return fn(since_ts_ms=since_ts_ms, limit=limit)  # preferred signature
            except TypeError:
                try:
                    return fn(since_ts_ms, limit)
                except Exception:
                    pass
            except Exception:
                pass

    # Minimal fallback: read JSONL
    out: _List[_Dict[str, _Any]] = []
    try:
        path = globals().get("ACTION_BUS_PATH") or globals().get("ACTION_LOG_PATH") or globals().get("AI_ACTION_LOG_PATH")
        if path is None or (not path.exists()):
            return out
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = _json.loads(line)
                except Exception:
                    continue
                if not isinstance(row, dict):
                    continue
                ts = row.get("ts_ms", 0)
                try:
                    ts_i = int(ts)
                except Exception:
                    ts_i = 0
                if ts_i < int(since_ts_ms):
                    continue
                out.append(row)
                if len(out) >= int(limit):
                    break
        return out
    except Exception:
        return out

# Export contract
try:
    __all__ = list(set((globals().get("__all__", []) or []) + [
        "ACTION_BUS_PATH","ACTION_LOG_PATH","AI_ACTION_LOG_PATH",
        "ensure_bus","publish_action","read_actions"
    ]))
except Exception:
    pass
# --- END FORCE_EXPORTS_V3 ---
"""

p.write_text(s.rstrip() + "\n\n" + append + "\n", encoding="utf-8")
print("OK: appended FORCE_EXPORTS_V3 to ai_action_bus.py")
