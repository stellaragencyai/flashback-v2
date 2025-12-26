from __future__ import annotations
from pathlib import Path

P = Path(r"app\tools\ws_health_check.py")

lines = P.read_text(encoding="utf-8", errors="ignore").splitlines()

# We expect the broken sequence:
#   print("
#   PASS")
# at lines 527-528 (1-based), indices 526-527 (0-based)
i = 526
if len(lines) <= i + 1:
    raise SystemExit(f"PATCH_FAIL: file too short, lines={len(lines)}")

print("BEFORE:", repr(lines[i]), repr(lines[i+1]))

# Fix it
lines[i] = '    print("\\nPASS")'
del lines[i+1]

P.write_text("\n".join(lines) + "\n", encoding="utf-8")

lines2 = P.read_text(encoding="utf-8", errors="ignore").splitlines()
print("AFTER :", repr(lines2[i]))
print("PATCH_OK")
