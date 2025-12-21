from pathlib import Path
import re

p = Path("app/core/ai_action_router.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Extract the whole _normalize_open_action function
pat = r"(def _normalize_open_action\(payload: Dict\[str, Any\], profile: Dict\[str, Any\]\) -> Dict\[str, Any\]:\n)([\s\S]*?)(?=\n\ndef |\Z)"
m = re.search(pat, s)
if not m:
    raise SystemExit("PATCH_FAIL: _normalize_open_action function not found")

header = m.group(1)
body = m.group(2)

# Normalize indentation: strip leading whitespace and re-indent uniformly
lines = body.splitlines()
new_lines = []
for line in lines:
    stripped = line.lstrip()
    if stripped:
        new_lines.append("    " + stripped)
    else:
        new_lines.append("")

fixed = header + "\n".join(new_lines)

s = s[:m.start()] + fixed + s[m.end():]
p.write_text(s, encoding="utf-8")

print("PATCH_OK: _normalize_open_action indentation normalized")
