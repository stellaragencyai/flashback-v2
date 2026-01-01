from pathlib import Path
import re

p = Path("app/sim/paper_broker.py")
text = p.read_text(encoding="utf-8", errors="ignore")

# Find the injected helper start
m = re.search(r"(?m)^def _maybe_publish_outcome_record\(", text)
if not m:
    raise SystemExit("FAIL: could not find def _maybe_publish_outcome_record to remove")

start = m.start()

# Heuristic: if we are inside an unclosed triple-quote right before this,
# nuke back to the nearest preceding triple-quote opener on its own line.
pre = text[:start]
q = list(re.finditer(r"(?m)^\\s*(['\\\"]{3})", pre))
if q:
    # only roll back if there's an odd count of triple quotes (likely unclosed)
    # simplistic but effective for this kind of mess
    if len(q) % 2 == 1:
        start = q[-1].start()

# Find end of helper block: next top-level def/class after the helper
m2 = re.search(r"(?m)^(def |class )", text[m.end():])
end = len(text)
if m2:
    end = m.end() + m2.start()

new_text = text[:start] + "\n" + text[end:]
p.write_text(new_text, encoding="utf-8")
print("OK: removed broken _maybe_publish_outcome_record injection (compile-check next)")
