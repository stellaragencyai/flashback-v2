from pathlib import Path
import re

path = Path(r".\app\ai\ai_events_spine.py")
s = path.read_text(encoding="utf-8")

# We patch the "bad_jsonl_line" handler so BOM-only lines are skipped quietly.
# Minimal blast radius: only affects the error path that logs bad jsonl lines.

pat = re.compile(
    r'(?ms)(^\s*#?.*bad_jsonl_line.*?$.*?$)',  # anchor near the bad_jsonl_line region
)

if "bad_jsonl_line" not in s:
    raise SystemExit("ERROR: could not find 'bad_jsonl_line' in ai_events_spine.py")

# Insert a BOM/empty-line guard right before the first time we write a bad_jsonl_line record.
# We look for the first occurrence of '"kind":"bad_jsonl_line"' OR "'kind': 'bad_jsonl_line'" in code.
m = re.search(r'(?m)^(?P<indent>\s*).*(kind\s*[:=]\s*[\'"]bad_jsonl_line[\'"]).*$', s)
if not m:
    raise SystemExit("ERROR: could not locate code line containing kind='bad_jsonl_line'")

indent = m.group("indent")

# Walk upward a bit to find a nearby variable name used for the raw line.
# Most of your code uses 'raw' or 'line'. We'll just guard both safely by using locals().
guard = (
    f"{indent}# BOM-only lines (Windows PowerShell UTF8 writes BOM even for empty files) are not real data\n"
    f"{indent}_tmp = locals().get('raw', locals().get('line', ''))\n"
    f"{indent}try:\n"
    f"{indent}    _tmp2 = _tmp.lstrip('\\ufeff').strip() if isinstance(_tmp, str) else _tmp\n"
    f"{indent}except Exception:\n"
    f"{indent}    _tmp2 = _tmp\n"
    f"{indent}if isinstance(_tmp2, str) and _tmp2 == '':\n"
    f"{indent}    continue\n\n"
)

# Insert guard ONLY once, right before the matched bad_jsonl_line line.
s2 = s[:m.start()] + guard + s[m.start():]
path.write_text(s2, encoding="utf-8")

print("OK: patched ai_events_spine.py to skip BOM-only/empty lines before bad_jsonl_line logging")
