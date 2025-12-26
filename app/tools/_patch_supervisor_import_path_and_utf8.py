from __future__ import annotations
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
P = ROOT / "app" / "bots" / "supervisor_ai_stack.py"

if not P.exists():
    raise SystemExit(f"FATAL: missing {P}")

s = P.read_text(encoding="utf-8", errors="ignore")

orig = s
edits = 0

# 1) Ensure ROOT (repo root) is on sys.path very early
# Insert after the first block of imports (best-effort).
marker = "from __future__ import annotations"
if marker in s and "PHASE8_IMPORT_PATH_SHIM" not in s:
    # Find a safe insertion point: after last top-level import line near top.
    lines = s.splitlines(True)
    insert_at = None
    for i in range(min(120, len(lines))):
        if lines[i].startswith("import ") or lines[i].startswith("from "):
            insert_at = i
    if insert_at is None:
        insert_at = 0
    shim = "\n# --- PHASE8_IMPORT_PATH_SHIM ---\n" \
           "import os as _os\n" \
           "import sys as _sys\n" \
           "from pathlib import Path as _Path\n" \
           "_ROOT = _Path(__file__).resolve().parents[2]\n" \
           "if str(_ROOT) not in _sys.path:\n" \
           "    _sys.path.insert(0, str(_ROOT))\n" \
           "# Force UTF-8 so logs/emoji do not crash on Windows cp1252\n" \
           "_os.environ.setdefault('PYTHONUTF8','1')\n" \
           "_os.environ.setdefault('PYTHONIOENCODING','utf-8')\n" \
           "# --- END PHASE8_IMPORT_PATH_SHIM ---\n\n"
    lines.insert(insert_at + 1, shim)
    s = "".join(lines)
    edits += 1

# 2) Remove common bad sys.path insertions that point to ROOT/app (breaks `app.tools`)
# We comment them out instead of deleting.
patterns = [
    r"^\s*_?sys\.path\.insert\(\s*0\s*,\s*str\([^)]*[/\\]app\)\s*\)\s*$",
    r"^\s*_?sys\.path\.append\(\s*str\([^)]*[/\\]app\)\s*\)\s*$",
]
lines = s.splitlines(True)
new_lines = []
for ln in lines:
    stripped = ln.rstrip("\r\n")
    if any(re.match(p, stripped) for p in patterns) and "PHASE8_DISABLED" not in ln:
        new_lines.append("# PHASE8_DISABLED: " + ln)
        edits += 1
    else:
        new_lines.append(ln)
s = "".join(new_lines)

# 3) Stop emoji from hard-gate error strings (Windows logging was dying on 🛑)
# Replace the specific emoji with plain text.
if "🛑" in s:
    s2 = s.replace("🛑", "STOP")
    if s2 != s:
        s = s2
        edits += 1

if s == orig:
    print("OK: no changes needed (already patched)")
else:
    P.write_text(s, encoding="utf-8")
    print(f"OK: patched supervisor_ai_stack.py edits={edits}")
