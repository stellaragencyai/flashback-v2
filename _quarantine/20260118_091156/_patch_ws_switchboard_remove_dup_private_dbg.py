from pathlib import Path

p = Path(r"app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

hdr = "    # DEBUG (throttled): sample PRIVATE WS messages to confirm Bybit payload shapes/topics"
idx1 = s.find(hdr)
if idx1 < 0:
    raise SystemExit("CANNOT_FIND: private debug header (first)")

idx2 = s.find(hdr, idx1 + 1)
if idx2 < 0:
    print("NOOP: only one private debug block found")
    raise SystemExit(0)

# remove from second header up to the next 'if not topic:' that follows it
end = s.find("\n    if not topic:", idx2)
if end < 0:
    raise SystemExit("CANNOT_FIND: end marker '\\n    if not topic:' after second private debug header")

s2 = s[:idx2] + s[end+1:]  # +1 to drop the leading newline we matched
p.write_text(s2, encoding="utf-8")
print("PATCHED_OK: removed duplicate private debug block at offset", idx2)
