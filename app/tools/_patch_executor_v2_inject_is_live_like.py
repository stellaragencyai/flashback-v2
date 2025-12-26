from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app/bots/executor_v2.py")
t = p.read_text(encoding="utf-8", errors="ignore")

# If already present, do nothing
if re.search(r"^def\s+_is_live_like\s*\(", t, flags=re.MULTILINE):
    print("OK: _is_live_like already exists (no changes).")
    raise SystemExit(0)

# Insert right after _normalize_setup_type block (best locality)
m = re.search(
    r"^def\s+_normalize_setup_type\s*\(.*?\):.*?\n(?=^def\s|\Z)",
    t,
    flags=re.DOTALL | re.MULTILINE,
)
if not m:
    # Fallback: insert near top after imports
    mi = re.search(r"^(from __future__.*?\n\n)", t, flags=re.DOTALL | re.MULTILINE)
    insert_at = mi.end(1) if mi else 0
else:
    insert_at = m.end(0)

insert = r"""

def _is_live_like(trade_mode: str) -> bool:
    m = str(trade_mode or "").upper().strip()
    return m in ("LIVE_CANARY", "LIVE_FULL")

"""

t2 = t[:insert_at] + insert + t[insert_at:]
p.write_text(t2, encoding="utf-8")
print("OK: injected _is_live_like into executor_v2.py")
