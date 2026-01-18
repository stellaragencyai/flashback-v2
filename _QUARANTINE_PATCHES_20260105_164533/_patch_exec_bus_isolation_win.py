from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(r"C:\Flashback")
ws_path = ROOT / "app" / "core" / "ws_switchboard.py"
rec_path = ROOT / "app" / "bots" / "trade_outcome_recorder.py"

def die(msg: str) -> None:
    raise SystemExit(msg)

# -----------------------------
# Patch ws_switchboard.py
# -----------------------------
ws = ws_path.read_text(encoding="utf-8")

needle = 'EXECUTIONS_PATH    = _env_path("EXECUTIONS_PATH",    f"ws_executions_{ACCOUNT_LABEL}.jsonl")'
if needle not in ws:
    die("ws_switchboard.py: expected EXECUTIONS_PATH bind line not found. Refusing to patch.")

replacement = """# EXECUTIONS path precedence:
# 1) EXEC_BUS_PATH (systemd / per-instance)
# 2) EXECUTIONS_BUS_PATH (alias)
# 3) EXECUTIONS_PATH (legacy override)
# 4) default per-account name
_default_exec = _env_path("EXECUTIONS_PATH", f"ws_executions_{ACCOUNT_LABEL}.jsonl")
EXECUTIONS_PATH = _env_path("EXECUTIONS_BUS_PATH", str(_default_exec))
EXECUTIONS_PATH = _env_path("EXEC_BUS_PATH", str(EXECUTIONS_PATH))"""

ws2 = ws.replace(needle, replacement)
ws_path.write_text(ws2, encoding="utf-8")

# -----------------------------
# Patch trade_outcome_recorder.py
# -----------------------------
rec = rec_path.read_text(encoding="utf-8")

# Insert _env_path helper right after STATE_DIR.mkdir(...)
anchor = 'STATE_DIR.mkdir(parents=True, exist_ok=True)\n'
if anchor not in rec:
    die("trade_outcome_recorder.py: STATE_DIR mkdir anchor not found. Refusing to patch.")

helper = anchor + """
def _env_path(name: str, default: str) -> Path:
    v = os.getenv(name)
    s = (v or "").strip()
    p = Path(s) if s else Path(default)
    if not p.is_absolute():
        p = STATE_DIR / p
    return p

"""

if "_env_path(name: str, default: str)" not in rec:
    rec = rec.replace(anchor, helper)

# Replace hardcoded path constants block
pattern = re.compile(
    r'EXEC_BUS_PATH:\s*Path\s*=\s*STATE_DIR\s*/\s*"ws_executions\.jsonl"\s*\n'
    r'CURSOR_PATH:\s*Path\s*=\s*STATE_DIR\s*/\s*"trade_outcome_recorder\.cursor"\s*\n\s*\n'
    r'AI_EVENTS_DIR:\s*Path\s*=\s*STATE_DIR\s*/\s*"ai_events"\s*\n'
    r'AI_EVENTS_DIR\.mkdir\(parents=True,\s*exist_ok=True\)\s*\n\s*\n'
    r'PENDING_SETUPS_PATH:\s*Path\s*=\s*AI_EVENTS_DIR\s*/\s*"pending_setups\.json"\s*\n'
    r'PENDING_SETUPS_LOCK:\s*Path\s*=\s*PENDING_SETUPS_PATH\.with_suffix\(PENDING_SETUPS_PATH\.suffix\s*\+\s*"\.lock"\)\s*\n',
    re.MULTILINE
)

m = pattern.search(rec)
if not m:
    die("trade_outcome_recorder.py: expected hardcoded path block not found. Refusing to patch.")

new_block = """# Paths (env-overridable for per-account isolation)
# EXEC path precedence:
# 1) EXEC_BUS_PATH
# 2) EXECUTIONS_BUS_PATH
# 3) EXECUTIONS_PATH
# 4) default state/ws_executions.jsonl
_default_exec = _env_path("EXECUTIONS_PATH", "ws_executions.jsonl")
_default_exec = _env_path("EXECUTIONS_BUS_PATH", str(_default_exec))
EXEC_BUS_PATH: Path = _env_path("EXEC_BUS_PATH", str(_default_exec))

CURSOR_PATH: Path = _env_path("TRADE_OUTCOME_CURSOR_PATH", "trade_outcome_recorder.cursor")

AI_EVENTS_DIR: Path = _env_path("AI_EVENTS_DIR", "ai_events")
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

PENDING_SETUPS_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"
PENDING_SETUPS_LOCK: Path = PENDING_SETUPS_PATH.with_suffix(PENDING_SETUPS_PATH.suffix + ".lock")
"""

rec2 = rec[:m.start()] + new_block + rec[m.end():]
rec_path.write_text(rec2, encoding="utf-8")

print("OK: patched ws_switchboard.py + trade_outcome_recorder.py for EXEC_BUS_PATH isolation compatibility")
