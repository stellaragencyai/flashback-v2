from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Refuse if already patched
if any("STRAT_DEBUG_FIX_V1" in l for l in lines):
    raise SystemExit("REFUSE: STRAT_DEBUG_FIX_V1 already applied")

needle = "s_side, s_reason = logic_fn(candles)"
hit = None
for i, l in enumerate(lines):
    if needle in l:
        hit = i
        break

if hit is None:
    raise SystemExit("PATCH FAILED: could not find logic_fn(candles) call line")

indent = lines[hit][:len(lines[hit]) - len(lines[hit].lstrip())]

inject = [
    indent + "# STRAT_DEBUG_FIX_V1: populate debug from compute_simple_signal so observed.jsonl has real numbers",
    indent + "try:",
    indent + "    _tmp_side, _tmp_dbg = compute_simple_signal(candles)",
    indent + "    if isinstance(_tmp_dbg, dict) and _tmp_dbg:",
    indent + "        debug = _tmp_dbg",
    indent + "except Exception:",
    indent + "    pass",
]

# Insert immediately after the logic_fn call
out = lines[:hit+1] + inject + lines[hit+1:]

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (STRAT_DEBUG_FIX_V1)")
