from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Refuse if already patched
if any("GATE_PRUNE_DEAD_SYMBOLS_V1" in l for l in lines):
    raise SystemExit("REFUSE: patch already applied (GATE_PRUNE_DEAD_SYMBOLS_V1 found)")

needle = "probe_syms = sorted({k[0] for k in universe.keys()})"
hit = None
for i,l in enumerate(lines):
    if l.strip() == needle.strip():
        hit = i
        break

if hit is None:
    raise SystemExit("PATCH FAILED: could not find probe_syms line in PRUNE_DEAD_SYMBOLS block")

indent = lines[hit][:len(lines[hit]) - len(lines[hit].lstrip())]
lines[hit] = indent + "# GATE_PRUNE_DEAD_SYMBOLS_V1"
lines.insert(hit+1, indent + "probe_syms = ([] if os.getenv('SIG_PRUNE_DEAD_SYMBOLS','0') != '1' else sorted({k[0] for k in universe.keys()}))")

p.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
print("OK: gated PRUNE_DEAD_SYMBOLS behind SIG_PRUNE_DEAD_SYMBOLS=1 (default OFF)")
