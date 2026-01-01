from pathlib import Path
import re

p = Path("app/sim/paper_broker.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

ERR_LINE = 211
pre = "".join(lines[:ERR_LINE])

# Count triple-quote tokens before the error line
tokens = []
for m in re.finditer(r"'''|\"\"\"", pre):
    tokens.append((m.start(), m.group(0)))

if len(tokens) % 2 == 0:
    raise SystemExit("FAIL: triple-quote token count before line 211 is even; not a simple unclosed triple-quote case")

# Remove the line containing the last unmatched opener
last_pos, last_tok = tokens[-1]

# Find which line contains last_pos
running = 0
kill_idx = None
for i, ln in enumerate(lines[:ERR_LINE]):
    running += len(ln)
    if running > last_pos:
        kill_idx = i
        break

if kill_idx is None:
    raise SystemExit("FAIL: could not map last triple-quote to a line")

old = lines[kill_idx]
# Remove ONLY the triple quote token from that line (don’t nuke the whole line if it has code)
new = old.replace(last_tok, "", 1)
lines[kill_idx] = new

p.write_text("".join(lines), encoding="utf-8")
print(f"OK: removed one unmatched {last_tok} token on line {kill_idx+1}")
