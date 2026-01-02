from pathlib import Path
import re

p = Path(r"app\core\ai_action_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "FORCE_EXPORTS_V2" in s:
    print("OK: FORCE_EXPORTS_V2 already present")
    raise SystemExit(0)

append = """
# --- FORCE_EXPORTS_V2 ---
# Ensure legacy callers can import these names reliably (bus/router compatibility).
# This must be import-safe (no network, no side effects).

try:
    from pathlib import Path as _Path
except Exception:
    _Path = None  # type: ignore

# Prefer whatever the module already defines.
# Older versions may define ACTION_LOG_PATH or AI_ACTION_LOG_PATH only.
if "ACTION_BUS_PATH" not in globals():
    if "ACTION_LOG_PATH" in globals():
        ACTION_BUS_PATH = ACTION_LOG_PATH  # type: ignore
    elif "AI_ACTION_LOG_PATH" in globals():
        ACTION_BUS_PATH = AI_ACTION_LOG_PATH  # type: ignore
    else:
        try:
            # Try common ROOT/STATE patterns if present
            if "STATE_DIR" in globals():
                ACTION_BUS_PATH = STATE_DIR / "ai_action_bus.jsonl"  # type: ignore
            else:
                ACTION_BUS_PATH = _Path(__file__).resolve().parents[2] / "state" / "ai_action_bus.jsonl"  # type: ignore
        except Exception:
            # Last-ditch: just a relative path
            ACTION_BUS_PATH = _Path("state") / "ai_action_bus.jsonl"  # type: ignore

# Provide aliases expected by various callers
ACTION_LOG_PATH = ACTION_BUS_PATH  # type: ignore
AI_ACTION_LOG_PATH = ACTION_BUS_PATH  # type: ignore

# Optionally publish __all__ so `from x import y` behaves predictably.
try:
    __all__ = list(set((globals().get("__all__", []) or []) + [
        "ACTION_BUS_PATH","ACTION_LOG_PATH","AI_ACTION_LOG_PATH",
        "publish_action","read_actions","ensure_bus"
    ]))
except Exception:
    pass
# --- END FORCE_EXPORTS_V2 ---
"""

p.write_text(s.rstrip() + "\n\n" + append + "\n", encoding="utf-8")
print("OK: appended FORCE_EXPORTS_V2 to ai_action_bus.py")
