from __future__ import annotations
from pathlib import Path
import re

p = Path(r"C:\Flashback\app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

# Insert after the first occurrence of ACCOUNT_LABEL assignment.
pat = re.compile(r"(ACCOUNT_LABEL\s*=\s*.*?\n)", re.DOTALL)
m = pat.search(s)
if not m:
    raise SystemExit("Could not find ACCOUNT_LABEL assignment to patch.")

inject = m.group(1) + r"""
# ---------------------------------------------------------------------------
# Bind per-account bus paths now that ACCOUNT_LABEL is known
# ---------------------------------------------------------------------------
if _is_main(ACCOUNT_LABEL):
    POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", "positions_bus.json")
    ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", "orderbook_bus.json")
    TRADES_BUS_PATH    = _env_path("TRADES_BUS_PATH",    "trades_bus.json")
else:
    POSITIONS_BUS_PATH = _env_path("POSITIONS_BUS_PATH", f"positions_bus_{ACCOUNT_LABEL}.json")
    ORDERBOOK_BUS_PATH = _env_path("ORDERBOOK_BUS_PATH", f"orderbook_bus_{ACCOUNT_LABEL}.json")
    TRADES_BUS_PATH    = _env_path("TRADES_BUS_PATH",    f"trades_bus_{ACCOUNT_LABEL}.json")

PUBLIC_TRADES_PATH = _env_path("PUBLIC_TRADES_PATH", f"public_trades_{ACCOUNT_LABEL}.jsonl")
EXECUTIONS_PATH    = _env_path("EXECUTIONS_PATH",    f"ws_executions_{ACCOUNT_LABEL}.jsonl")
"""

s2 = s[:m.start()] + inject + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: bound per-account bus paths after ACCOUNT_LABEL.")
