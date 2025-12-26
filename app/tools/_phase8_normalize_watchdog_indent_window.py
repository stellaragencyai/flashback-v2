from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\orchestrator_watchdog.py")
lines = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

start_ln = 175
end_ln = 210
start_i = start_ln - 1
end_i = min(end_ln, len(lines)) - 1

def indent_of(s: str) -> str:
    return s[: len(s) - len(s.lstrip(" \t"))].replace("\t", "    ")

def is_meaningful(s: str) -> bool:
    t = s.strip()
    return bool(t) and not t.startswith("#")

def is_block_opener(s: str) -> bool:
    t = s.rstrip()
    # crude but effective: treat a colon at end as block opener (ignore dict literals most of the time)
    return t.endswith(":") and not t.strip().startswith(("{", "[", "(", "}","],", "),"))

# Normalize tabs to spaces globally (safe)
lines = [ln.replace("\t", "    ") for ln in lines]

# Find a baseline indent from the first meaningful line in the window
baseline = ""
for i in range(start_i, end_i + 1):
    if is_meaningful(lines[i]):
        baseline = indent_of(lines[i])
        break

fixed = 0
last_meaningful_indent = baseline
last_meaningful_was_block = False

for i in range(start_i, end_i + 1):
    raw = lines[i]
    stripped = raw.lstrip(" ")

    if not is_meaningful(raw):
        continue  # leave blank/comment lines alone

    desired = last_meaningful_indent + ("    " if last_meaningful_was_block else "")

    cur_indent = indent_of(raw)
    # If this line itself is a new block opener, its indent should be "desired" too (not deeper)
    # Example: else:/elif:/except:/finally: should align with previous block level, but we won't overthink it here.
    # We'll only enforce that it's not *more* indented than desired unless previous line opened a block.
    new_line = desired + raw.lstrip(" ")
    if new_line != raw:
        lines[i] = new_line
        fixed += 1

    # Update trackers based on this (post-fix) line
    last_meaningful_indent = indent_of(lines[i])
    last_meaningful_was_block = is_block_opener(lines[i].rstrip("\n"))

P.write_text("".join(lines), encoding="utf-8")
print(f"OK: normalized indentation in lines {start_ln}-{end_ln} edits={fixed}")
