from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# Guard: don't double patch
if "_atomic_write_text(" in s and "PermissionError" in s and "os.replace" in s:
    print("OK: ops_snapshot_tick.py already patched for atomic write + retry")
    raise SystemExit(0)

# Ensure imports include os/time
if "import os" not in s:
    s = s.replace("import json", "import json\nimport os", 1)
if "import time" not in s:
    # Insert after os import if present, else after json
    if "import os" in s:
        s = s.replace("import os", "import os\nimport time", 1)
    else:
        s = s.replace("import json", "import json\nimport time", 1)

helper = r'''
def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8", retries: int = 12, sleep_ms: int = 50) -> None:
    """
    Windows-safe atomic-ish write with retries:
    - Write to temp file in same directory
    - os.replace to target (atomic on Windows when possible)
    - Retry on PermissionError (transient lock by AV/editor/other reader)
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    last_err: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            tmp.write_text(text, encoding=encoding)
            os.replace(str(tmp), str(path))
            return
        except PermissionError as e:
            last_err = e
            try:
                time.sleep(max(0.0, sleep_ms / 1000.0))
            except Exception:
                pass
        except Exception as e:
            # cleanup tmp then re-raise
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    # final cleanup attempt
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    if last_err:
        raise last_err
'''.lstrip("\n")

# Insert helper before first def in file
m_def = re.search(r"^\s*def\s+", s, flags=re.M)
if not m_def:
    raise SystemExit("FATAL: could not find any function defs in ops_snapshot_tick.py to insert helper")
s = s[:m_def.start()] + helper + "\n" + s[m_def.start():]

# Replace OUT.write_text(json.dumps(out...)) with atomic writer
# We match the existing write_text call loosely.
pat = re.compile(r"OUT\.write_text\(\s*json\.dumps\(out,\s*indent=2\)\s*,\s*encoding\s*=\s*\"utf-8\"\s*\)")
if not pat.search(s):
    # fallback: any OUT.write_text(json.dumps(out,...), encoding="utf-8")
    pat = re.compile(r"OUT\.write_text\(\s*json\.dumps\(out,\s*indent=2\)\s*,\s*encoding\s*=\s*'utf-8'\s*\)")
if not pat.search(s):
    raise SystemExit("FATAL: could not find OUT.write_text(json.dumps(out, indent=2), encoding='utf-8') to patch")

s, n = pat.subn("_atomic_write_text(OUT, json.dumps(out, indent=2), encoding=\"utf-8\")", s, count=1)
if n != 1:
    raise SystemExit(f"FATAL: unexpected substitution count for write_text patch: {n}")

P.write_text(s, encoding="utf-8")
print("OK: patched ops_snapshot_tick.py to atomic write + retry on PermissionError")
