from __future__ import annotations
from pathlib import Path
import re

P = Path(r"app\ai\ai_events_spine.py")

s = P.read_text(encoding="utf-8", errors="ignore")
s = s.lstrip("\ufeff")  # strip BOM if present

# 1) Ensure argparse is imported
if "import argparse" not in s:
    s = s.replace("import hashlib", "import argparse\nimport hashlib", 1)

# 2) If run_once_tick not present, inject helpers before main()
if "def run_once_tick" not in s:
    needle = "def main() -> None:"
    if needle not in s:
        raise SystemExit('PATCH_FAIL: could not find "def main() -> None:" anchor')

    inject = r'''def _env_bool(name: str, default: str = "true") -> bool:
    raw = str(os.getenv(name, default)).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def run_once_tick() -> None:
    """Phase 8: deterministic single tick for health refresh + orchestration."""
    try:
        record_heartbeat("ai_events_spine")
    except Exception:
        pass

    # Refresh memory snapshot mtime safely (schema stays dict-of-records)
    try:
        if _env_bool("AI_EVENTS_SPINE_TICK_MEMORY_SNAPSHOT", "true"):
            snap = _load_memory_snapshot()
            _save_memory_snapshot(snap)
    except Exception as e:
        try:
            log.warning("[ai_events_spine] tick memory_snapshot failed: %r", e)
        except Exception:
            pass

    # Prune pending registry (keeps it bounded + refreshes file mtime)
    try:
        if _env_bool("AI_EVENTS_SPINE_TICK_PENDING", "true"):
            reg = _load_pending()
            _save_pending(reg)
    except Exception as e:
        try:
            log.warning("[ai_events_spine] tick pending failed: %r", e)
        except Exception:
            pass


def loop(interval_sec: float = 10.0) -> None:
    log.info("AI Events Spine loop started (disk logger + heartbeat, v2.8.2).")
    while True:
        run_once_tick()
        time.sleep(max(0.5, float(interval_sec)))


def main() -> None:
'''
    s = s.replace(needle, inject, 1)

# 3) Replace old main-loop footer with argparse-driven CLI
cli_block = r'''def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run one ops tick (heartbeat + snapshot refresh) and exit")
    ap.add_argument("--interval", type=float, default=float(os.getenv("AI_EVENTS_SPINE_INTERVAL", "10") or "10"), help="Loop interval seconds")
    args = ap.parse_args()

    if args.once:
        run_once_tick()
        return

    loop(interval_sec=float(args.interval))


if __name__ == "__main__":
    main()
'''

# Replace everything from the last def main() to EOF (safe + deterministic)
idx = s.rfind("def main()")
if idx == -1:
    raise SystemExit("PATCH_FAIL: could not find def main() for bottom replacement")

s = s[:idx] + cli_block

P.write_text(s, encoding="utf-8")
print("PATCH_OK:", str(P))
