from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\bots\signal_engine.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # --- Patch A: replace _ensure_debug() with merge-preserving version ---
    # We preserve existing keys (especially 'regime') while guaranteeing last_close/prev_close/ma exist.
    new_ensure = r'''
# FORCE_DEBUG_CALLSITE_V1: ensure observed.jsonl debug always has real numbers when candles are available
def _ensure_debug(candles, dbg):
    """
    Ensure we always have:
      - last_close, prev_close, ma
    BUT do NOT drop existing debug keys like:
      - regime (dict with adx/atr_pct/vol_z)
      - setup, signal_origin, etc.
    """
    base = dbg if isinstance(dbg, dict) else {}
    try:
        lc = base.get("last_close")
        pc = base.get("prev_close")
        ma = base.get("ma")

        # If core MA fields already exist, return as-is.
        if (lc is not None) and (pc is not None) and (ma is not None):
            return base

        # Otherwise compute MA debug and merge into existing dict.
        _side, computed = compute_simple_signal(candles)
        if isinstance(computed, dict) and computed:
            merged = dict(base)
            for k in ("last_close", "prev_close", "ma"):
                if merged.get(k) is None and computed.get(k) is not None:
                    merged[k] = computed.get(k)
            # Preserve existing "reason" if present; otherwise take computed.
            if not merged.get("reason") and computed.get("reason"):
                merged["reason"] = computed.get("reason")
            return merged
    except Exception:
        pass

    return base
'''.strip("\n")

    # Find existing _ensure_debug def block and replace it
    pat_ensure = re.compile(r"(?ms)^# FORCE_DEBUG_CALLSITE_V1:.*?^def _ensure_debug\(.*?\n(?:.*\n)*?^    return .*?\n", re.MULTILINE)
    if pat_ensure.search(s):
        s = pat_ensure.sub(new_ensure + "\n\n", s, count=1)
        print("OK: patched _ensure_debug() to preserve regime keys")
    else:
        # Fallback: replace by locating def _ensure_debug(...)
        pat_alt = re.compile(r"(?ms)^def _ensure_debug\(.*?\n(?:.*\n)*?^    return .*?\n", re.MULTILINE)
        if pat_alt.search(s):
            s = pat_alt.sub(new_ensure + "\n\n", s, count=1)
            print("OK: patched _ensure_debug() (alt matcher)")
        else:
            print("WARN: could not locate _ensure_debug() block to patch")

    # --- Patch B: ensure append_signal_jsonl() includes debug['regime'] in payload ---
    # Insert "regime": debug.get("regime") into the payload debug dict if missing.
    if '"regime": debug.get("regime")' not in s:
        # Find the payload debug dict inside append_signal_jsonl and inject right after raw_reason
        s2 = s.replace(
            '"raw_reason": reason,',
            '"raw_reason": reason,\n            "regime": debug.get("regime"),',
            1
        )
        if s2 != s:
            s = s2
            print('OK: patched append_signal_jsonl() to include debug.regime')
        else:
            print("WARN: could not patch append_signal_jsonl() debug dict (raw_reason marker not found)")
    else:
        print("OK: append_signal_jsonl() already includes debug.regime")

    TARGET.write_text(s, encoding="utf-8", newline="\n")

    # Compile check
    try:
        compile(TARGET.read_text(encoding="utf-8", errors="ignore"), str(TARGET), "exec")
        print("PASS: signal_engine.py compiles after patch")
        return 0
    except Exception as e:
        print("FAIL: signal_engine.py does not compile after patch:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
