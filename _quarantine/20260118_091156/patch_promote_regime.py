from pathlib import Path
import re, time, shutil

p = Path("app/ai/ai_events_spine.py")
s = p.read_text(encoding="utf-8")

bak = p.with_suffix(".py.bak_" + str(int(time.time())))
shutil.copy2(p, bak)

# Guard: don't double-inject
if "PROMOTE_DEBUG_REGIME_TO_CANONICAL" in s:
    print("SKIP: already patched")
    print("BACKUP=", bak)
    raise SystemExit(0)

needle = r"feats\s*=\s*features\s*if\s*isinstance\(features,\s*dict\)\s*else\s*\{\s*\}"
m = re.search(needle, s)
if not m:
    print("FAIL: could not find feats= line to anchor patch")
    print("BACKUP=", bak)
    raise SystemExit(2)

inject = """
    # -----------------------------------------------------------------------
    # PROMOTE_DEBUG_REGIME_TO_CANONICAL
    # If signal engine computed regime metrics in signal.debug.regime but the
    # canonical feature fields are still 0/None, promote them so learning sees them.
    # -----------------------------------------------------------------------
    try:
        if isinstance(feats, dict):
            sig = feats.get("signal") if isinstance(feats.get("signal"), dict) else None
            dbg = sig.get("debug") if isinstance(sig, dict) else None
            reg = dbg.get("regime") if isinstance(dbg, dict) else None

            if isinstance(reg, dict):
                # atr_pct
                if feats.get("atr_pct") in (None, 0, 0.0) and reg.get("atr_pct") is not None:
                    try: feats["atr_pct"] = float(reg.get("atr_pct"))
                    except Exception: pass

                # volume_zscore (debug uses vol_z)
                if feats.get("volume_zscore") in (None, 0, 0.0) and reg.get("vol_z") is not None:
                    try: feats["volume_zscore"] = float(reg.get("vol_z"))
                    except Exception: pass

                # adx (optional but useful)
                if feats.get("adx") in (None, 0, 0.0) and reg.get("adx") is not None:
                    try: feats["adx"] = float(reg.get("adx"))
                    except Exception: pass
    except Exception:
        pass
"""

# Insert right after the feats= line (with a newline)
pos = m.end()
new = s[:pos] + inject + s[pos:]

p.write_text(new, encoding="utf-8")
print("PATCH_OK")
print("BACKUP=", bak)
print("FILE=", p)
