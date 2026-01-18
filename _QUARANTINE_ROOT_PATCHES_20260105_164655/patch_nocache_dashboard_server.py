from pathlib import Path
import re

p = Path(r"app/dashboard/dashboard_server.py")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

if "after_request" not in s:
    # Insert after Flask import
    s = s.replace(
        "from flask import Flask",
        "from flask import Flask\nfrom flask import request",
        1
    )

# Add after_request block if missing
if "def _no_cache_headers" not in s:
    inject = r'''
@app.after_request
def _no_cache_headers(resp):
    # Force browsers (and proxies) to always fetch fresh dashboard HTML/assets
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp
'''
    # Insert right after app = Flask(__name__) block
    s = re.sub(
        r"(app\s*=\s*Flask\(__name__\)\s*\napp\.config\[[^\]]+\]\s*=\s*[^\n]+\n)",
        r"\1" + inject + "\n",
        s,
        count=1,
        flags=re.DOTALL
    )

if s == orig:
    print("PATCH_FAIL: no changes applied")
    raise SystemExit(2)

p.write_text(s, encoding="utf-8")
print("OK: Added no-cache headers to dashboard_server.py")
