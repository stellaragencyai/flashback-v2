from __future__ import annotations

from pathlib import Path

P = Path(r"app\ops\orchestrator_watchdog.py")
lines = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# Find indentation used for restarted.append(label) as the "correct" reference
ref_indent = None
for ln in lines:
    if ln.lstrip().startswith("restarted.append("):
        ref_indent = ln[: len(ln) - len(ln.lstrip())]
        break

if ref_indent is None:
    # Reasonable default: inside for-loop within main() usually 8 spaces
    ref_indent = " " * 8

fixed = 0
out = []
for ln in lines:
    if ln.lstrip().startswith("blocked.append("):
        out.append(ref_indent + "blocked.append(label)\n")
        fixed += 1
    else:
        out.append(ln)

if fixed == 0:
    raise SystemExit("FATAL: could not find any 'blocked.append(' lines to fix")

P.write_text("".join(out), encoding="utf-8")
print(f"OK: fixed blocked.append(label) indentation edits={fixed} ref_indent_len={len(ref_indent)}")
