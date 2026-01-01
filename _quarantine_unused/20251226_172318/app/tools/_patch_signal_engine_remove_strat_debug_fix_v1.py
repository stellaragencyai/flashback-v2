from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

if not any("STRAT_DEBUG_FIX_V1" in l for l in lines):
    raise SystemExit("REFUSE: STRAT_DEBUG_FIX_V1 not found (nothing to remove)")

out = []
skip = 0
removed = 0

for i, l in enumerate(lines):
    if skip > 0:
        skip -= 1
        continue

    if "STRAT_DEBUG_FIX_V1" in l:
        # Remove the injected 7-line block (comment + try..except + pass)
        # This matches the exact injection we applied earlier.
        removed += 1
        skip = 6
        continue

    out.append(l)

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print(f"OK: removed STRAT_DEBUG_FIX_V1 blocks removed={removed}")
