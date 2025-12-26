from __future__ import annotations
from pathlib import Path
import sys, os

ROOT = Path(__file__).resolve().parents[2]
SUP = ROOT / "app" / "bots" / "supervisor_ai_stack.py"
TOOLS_DIR = ROOT / "app" / "tools"

print("ROOT=", ROOT)
print("SUP_EXISTS=", SUP.exists())
print("TOOLS_DIR_EXISTS=", TOOLS_DIR.exists())
print("CWD=", Path.cwd())

if SUP.exists():
    s = SUP.read_text(encoding="utf-8", errors="ignore").splitlines()
    hits = []
    for i, line in enumerate(s, 1):
        if "sys.path" in line or "PYTHONUTF8" in line or "PYTHONIOENCODING" in line:
            hits.append((i, line))
    print("\n=== supervisor_ai_stack.py sys.path / encoding hits ===")
    for i, line in hits[:120]:
        print(f"{i:04d}: {line}")

print("\n=== Import probe (current process) ===")
print("sys.path[0:5]=", sys.path[0:5])
try:
    import app
    print("import app OK", "app=", app, "file=", getattr(app, "__file__", None), "path=", getattr(app, "__path__", None))
except Exception as e:
    print("import app FAIL", repr(e))

try:
    import app.tools
    print("import app.tools OK", "path=", getattr(app.tools, "__path__", None))
except Exception as e:
    print("import app.tools FAIL", repr(e))

vc = ROOT / "app" / "tools" / "validate_config.py"
print("validate_config.py exists=", vc.exists(), "path=", vc)

