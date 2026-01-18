from pathlib import Path
import re

p = Path(r"app/dashboard/dashboard_server.py")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# Add settings after SECRET_KEY if missing
if "TEMPLATES_AUTO_RELOAD" not in s:
    s = re.sub(
        r'(app\.config\["SECRET_KEY"\]\s*=\s*"[^\"]+"\s*\n)',
        r'\1app.config["TEMPLATES_AUTO_RELOAD"] = True\napp.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0\n',
        s,
        count=1
    )

if s == orig:
    print("PATCH_FAIL: no changes applied")
    raise SystemExit(2)

p.write_text(s, encoding="utf-8")
print("OK: Enabled template auto-reload + no max-age for files")
