from pathlib import Path
import re

p = Path(r"app\dashboard\dashboard_server.py")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# 1) Remove any improperly indented 'from flask import request'
s = re.sub(
    r'^[ \t]+from flask import request\s*$',
    '',
    s,
    flags=re.MULTILINE
)

# 2) Ensure it exists exactly once at top-level imports
if 'from flask import request' not in s:
    s = re.sub(
        r'(from flask import Flask[^\n]*\n)',
        r'\1from flask import request\n',
        s,
        count=1
    )

if s == orig:
    print("PATCH_FAIL: no changes applied")
    raise SystemExit(2)

p.write_text(s, encoding="utf-8")
print("OK: Fixed misplaced flask.request import")
