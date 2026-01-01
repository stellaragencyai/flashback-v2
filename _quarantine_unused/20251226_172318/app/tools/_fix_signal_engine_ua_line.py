from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

hits = []
for i, l in enumerate(lines, start=1):
    if "requests.get(" in l and ("params=paramsheaders" in l or "headers={User-Agent:Mozilla/5.0}" in l):
        hits.append(i)

if not hits:
    raise SystemExit("REFUSE: no broken UA line found (expected params=paramsheaders or headers={User-Agent:Mozilla/5.0}).")

hits = sorted(set(hits))

for i in hits:
    old = lines[i-1]
    indent = old[:len(old) - len(old.lstrip())]
    m = re.search(r"timeout\s*=\s*(\d+)", old)
    timeout = m.group(1) if m else "10"
    lines[i-1] = indent + f"resp = requests.get(url, params=params, headers={{'User-Agent':'Mozilla/5.0'}}, timeout={timeout})"

p.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
print("OK: fixed UA line(s):", hits)
print("NEW:", lines[hits[0]-1])
