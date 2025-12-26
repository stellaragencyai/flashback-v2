from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "PRUNE_DEAD_SYMBOLS" not in s:
    raise SystemExit("PATCH FAILED: could not find PRUNE_DEAD_SYMBOLS block")

# Replace the whole prune block with a safer version.
pattern = re.compile(
    r"\n\s*# PRUNE_DEAD_SYMBOLS \(startup sanity\):.*?\n\s*except Exception as _e:\n\s*.*?\n",
    re.S
)

m = pattern.search(s)
if not m:
    raise SystemExit("PATCH FAILED: could not locate full PRUNE_DEAD_SYMBOLS block region")

replacement = r'''
    # PRUNE_DEAD_SYMBOLS (startup sanity): drop symbols that return *consistently empty* klines.
    # IMPORTANT:
    # - Only prune on our explicit "Empty kline list ..." error
    # - Do NOT prune on transient HTTP/network/timeouts/rate-limits
    dead_syms = set()
    try:
        probe_tf = "5"
        probe_syms = sorted({k[0] for k in universe.keys()})
        for sym in probe_syms:
            empty_hits = 0
            for _attempt in range(2):
                try:
                    _ = fetch_recent_klines(sym, probe_tf, limit=10)
                    break
                except Exception as e:
                    msg = str(e)
                    if "Empty kline list" in msg:
                        empty_hits += 1
                        continue
                    # Non-empty exceptions are treated as transient: keep symbol
                    print(f"[WARN] Dead-symbol probe transient error (keeping {sym}): {type(e).__name__}: {e}")
                    break
            if empty_hits >= 2:
                dead_syms.add(sym)

        if dead_syms:
            print(f"[WARN] Pruning dead symbols (empty klines x2): {sorted(dead_syms)}")
            universe = {k: v for k, v in universe.items() if k[0] not in dead_syms}
    except Exception as _e:
        print(f"[WARN] Dead-symbol prune failed (non-fatal): {type(_e).__name__}: {_e}")

'''

s2 = s[:m.start()] + "\n" + replacement + "\n" + s[m.end():]
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: patched PRUNE_DEAD_SYMBOLS block (v3 safe prune)")
