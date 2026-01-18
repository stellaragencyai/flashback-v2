from pathlib import Path

path = Path(r".\app\ai\ai_events_spine.py")
s = path.read_text(encoding="utf-8")

# 1) Inbox fallback reader: bytes BOM-safe strip
old1 = "raw = line.strip()"
new1 = 'raw = line.lstrip(b"\\xef\\xbb\\xbf").strip()'
if old1 not in s:
    raise SystemExit("ERROR: could not find expected 'raw = line.strip()' in fallback reader")
s = s.replace(old1, new1, 1)

# 2) Decisions tail scanner: currently treats bytes like str
old2 = 'raw = raw.lstrip("\\ufeff").strip()'
new2 = 'raw = raw.lstrip(b"\\xef\\xbb\\xbf").strip()'
if old2 not in s:
    raise SystemExit("ERROR: could not find expected decisions BOM strip line")
s = s.replace(old2, new2, 1)

path.write_text(s, encoding="utf-8")
print("OK: patched BOM handling in inbox fallback reader + decisions tail scanner")
