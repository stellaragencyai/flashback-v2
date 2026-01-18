from __future__ import annotations
from pathlib import Path
import re

p = Path(r"C:\Flashback\app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

pattern = re.compile(
    r"STATE_DIR:\s*Path\s*=\s*ROOT\s*/\s*\"state\"\s*\n"
    r"STATE_DIR\.mkdir\(parents=True,\s*exist_ok=True\)\s*\n\s*\n"
    r"POSITIONS_BUS_PATH:.*?\n"
    r"ORDERBOOK_BUS_PATH:.*?\n"
    r"TRADES_BUS_PATH:.*?\n"
    r"PUBLIC_TRADES_PATH:.*?\n"
    r"EXECUTIONS_PATH:.*?\n",
    re.DOTALL
)

m = pattern.search(s)
if not m:
    raise SystemExit("Could not find WS path constants block to patch (pattern mismatch).")

replacement = """STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Bus paths (env-overridable for per-account isolation)
# Defaults:
#   - main -> legacy shared filenames (back-compat)
#   - non-main -> labeled filenames (per-account truth)
# ---------------------------------------------------------------------------
def _env_path(name: str, default: str) -> Path:
    v = os.getenv(name)
    if v:
        return Path(v)
    return STATE_DIR / default

def _is_main(label: str) -> bool:
    return (label or "").lower() in ("main", "primary")

# NOTE: ACCOUNT_LABEL is already loaded below in this module; we reference it after it's set.
# We set placeholders here; later we re-bind these after ACCOUNT_LABEL is resolved.
POSITIONS_BUS_PATH: Path = STATE_DIR / "positions_bus.json"
ORDERBOOK_BUS_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_BUS_PATH: Path = STATE_DIR / "trades_bus.json"
PUBLIC_TRADES_PATH: Path = STATE_DIR / "public_trades.jsonl"
EXECUTIONS_PATH: Path = STATE_DIR / "ws_executions.jsonl"
"""

s2 = s[:m.start()] + replacement + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched ws_switchboard path constants block (env-overridable scaffolding).")
