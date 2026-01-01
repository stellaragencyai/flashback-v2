from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# We patch right after universe is built, before all_symbols is printed.
# Anchor: the line that defines all_symbols
anchor = "    all_symbols = sorted({k[0] for k in universe.keys()})"
if anchor not in s:
    raise SystemExit("PATCH FAILED: could not find universe all_symbols anchor")

if "PRUNE_DEAD_SYMBOLS" in s:
    raise SystemExit("REFUSE: patch already applied (PRUNE_DEAD_SYMBOLS found)")

patch = r'''
    # PRUNE_DEAD_SYMBOLS (startup sanity): drop symbols that return empty klines / invalid market
    # This prevents silent 'not enough candles' loops on delisted/mismatched instruments.
    dead_syms = set()
    try:
        probe_tf = "5"
        for sym in sorted({k[0] for k in universe.keys()}):
            try:
                _ = fetch_recent_klines(sym, probe_tf, limit=10)
            except Exception:
                dead_syms.add(sym)
        if dead_syms:
            print(f"[WARN] Pruning dead symbols (no klines): {sorted(dead_syms)}")
            universe = {k:v for k,v in universe.items() if k[0] not in dead_syms}
    except Exception as _e:
        print(f"[WARN] Dead-symbol prune failed (non-fatal): {type(_e).__name__}: {_e}")

'''

s2 = s.replace(anchor, patch + "\n" + anchor, 1)
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (startup prune dead symbols)")
