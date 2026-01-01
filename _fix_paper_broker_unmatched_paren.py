from pathlib import Path
import re

p = Path("app/sim/paper_broker.py")
L = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

def die(msg: str):
    raise SystemExit("FAIL: " + msg)

# 1) Find the exact bad line: a top-level ") -> None:" (no indentation)
start = None
for i, ln in enumerate(L):
    if ln.startswith(") -> None:"):
        start = i
        break
if start is None:
    die("could not find the stray top-level ') -> None:' line")

# 2) Find where to stop deleting:
#    stop at the next obvious top-level boundary AFTER start.
stop = None
boundary_patterns = [
    r"^class\s+\w+",
    r"^def\s+\w+",
    r"^#\s*-{5,}",
    r"^if\s+__name__\s*==\s*[\"']__main__[\"']\s*:"
]

for j in range(start + 1, len(L)):
    s = L[j]
    if any(re.search(pat, s) for pat in boundary_patterns):
        stop = j
        break

if stop is None:
    # If we can't find a clean boundary, just chop to EOF (better than broken syntax)
    stop = len(L)

# 3) Delete the zombie block
before = "".join(L[:start])
after = "".join(L[stop:])
new_text = before + after

p.write_text(new_text, encoding="utf-8")
print(f"OK: removed zombie block lines {start+1}..{stop} from {p}")
