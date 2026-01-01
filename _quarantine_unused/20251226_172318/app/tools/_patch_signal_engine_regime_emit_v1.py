from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\bots\signal_engine.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    changed = 0

    # ------------------------------------------------------------
    # FIX 1: Ensure _ensure_debug merges computed MA fields but keeps existing keys (esp. regime)
    # ------------------------------------------------------------
    # Replace the whole function body with a safe merge version.
    pat_ensure = re.compile(r"(?ms)^def _ensure_debug\(candles, dbg\):\n(.*?)\n(?=^# ------------------------------------------------------------|^def |^SETUP_LOGIC|^if __name__)",
                            re.MULTILINE)

    m = pat_ensure.search(s)
    if not m:
        print("FAIL: could not find _ensure_debug() to patch")
        return 1

    new_ensure = """def _ensure_debug(candles, dbg):
    \"""
    Ensure observed.jsonl debug always has MA fields, while preserving any extra keys
    already present (especially dbg['regime'] computed earlier).
    \"""
    base = dbg if isinstance(dbg, dict) else {}
    try:
        # If MA fields already present, keep as-is
        if isinstance(base, dict):
            lc = base.get("last_close")
            pc = base.get("prev_close")
            ma = base.get("ma")
            if (lc is not None) and (pc is not None) and (ma is not None):
                return base

        # Otherwise compute MA fields from candles
        _side, computed = compute_simple_signal(candles)
        if isinstance(computed, dict) and computed:
            merged = dict(base) if isinstance(base, dict) else {}
            merged.update(computed)  # adds last_close/prev_close/ma/reason
            return merged
    except Exception:
        pass
    return base if isinstance(base, dict) else {}
"""

    s2 = pat_ensure.sub(new_ensure + "\n", s, count=1)
    if s2 != s:
        s = s2
        changed += 1

    # ------------------------------------------------------------
    # FIX 2: Ensure append_signal_jsonl writes debug.regime when present
    # ------------------------------------------------------------
    # Insert `"regime": debug.get("regime"),` into payload["debug"] dict if missing.
    if '"raw_reason": reason,' in s and '"regime": debug.get("regime"),' not in s:
        s = s.replace(
            '"raw_reason": reason,',
            '"raw_reason": reason,\n            "regime": debug.get("regime"),',
            1
        )
        changed += 1

    # ------------------------------------------------------------
    # FIX 3: Repair any accidental unindented "regime_tags=regime_tags," line in log_signal_from_engine call
    # ------------------------------------------------------------
    # If it appears at column 0, indent it to match neighboring kwargs.
    s2 = re.sub(r"(?m)^\s*regime_tags=regime_tags,\s*$", "                        regime_tags=regime_tags,", s)
    if s2 != s:
        s = s2
        changed += 1

    TARGET.write_text(s, encoding="utf-8", newline="\n")
    print(f"OK: patched signal_engine.py changed_blocks={changed}")

    # compile check
    try:
        compile(s, str(TARGET), "exec")
        print("PASS: signal_engine.py compiles")
        return 0
    except Exception as e:
        print("FAIL: signal_engine.py still does not compile:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
