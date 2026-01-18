from __future__ import annotations
from pathlib import Path
import re

p = Path(r"C:\Flashback\app\core\ws_switchboard.py")
s = p.read_text(encoding="utf-8")

# 1) Remove any prior injected bind block (safe delete by markers)
bind_pat = re.compile(
    r"\n# -{10,}\n# Bind per-account bus paths now that ACCOUNT_LABEL is known\n# -{10,}\n.*?"
    r"\nEXECUTIONS_PATH\s*=\s*_env_path\(\"EXECUTIONS_PATH\".*?\)\n",
    re.DOTALL
)

s2, n = bind_pat.subn("\n", s)

# 2) Find a SAFE anchor to re-insert (prefer top-level ACCOUNT_LABEL assignment)
anchor = re.search(r"(?m)^(ACCOUNT_LABEL\s*=\s*.*\n)", s2)
if anchor:
    insert_at = anchor.end()
    indent = ""
else:
    # Fallback: first occurrence anywhere, capture indent
    anchor = re.search(r"(?m)^(?P<indent>\s*)ACCOUNT_LABEL\s*=\s*.*\n", s2)
    if not anchor:
        raise SystemExit("Could not find ACCOUNT_LABEL assignment to anchor insertion.")
    insert_at = anchor.end()
    indent = anchor.group("indent")

# 3) Build binding block with correct indentation.
# If we're inside a function (indent not empty), we MUST declare globals.
global_line = ""
if indent:
    global_line = indent + "global POSITIONS_BUS_PATH, ORDERBOOK_BUS_PATH, TRADES_BUS_PATH, PUBLIC_TRADES_PATH, EXECUTIONS_PATH\n"

block = (
    "\n"
    + indent + "# ---------------------------------------------------------------------------\n"
    + indent + "# Bind per-account bus paths now that ACCOUNT_LABEL is known\n"
    + indent + "# ---------------------------------------------------------------------------\n"
    + global_line
    + indent + "if _is_main(ACCOUNT_LABEL):\n"
    + indent + "    POSITIONS_BUS_PATH = _env_path(\"POSITIONS_BUS_PATH\", \"positions_bus.json\")\n"
    + indent + "    ORDERBOOK_BUS_PATH = _env_path(\"ORDERBOOK_BUS_PATH\", \"orderbook_bus.json\")\n"
    + indent + "    TRADES_BUS_PATH    = _env_path(\"TRADES_BUS_PATH\",    \"trades_bus.json\")\n"
    + indent + "else:\n"
    + indent + "    POSITIONS_BUS_PATH = _env_path(\"POSITIONS_BUS_PATH\", f\"positions_bus_{ACCOUNT_LABEL}.json\")\n"
    + indent + "    ORDERBOOK_BUS_PATH = _env_path(\"ORDERBOOK_BUS_PATH\", f\"orderbook_bus_{ACCOUNT_LABEL}.json\")\n"
    + indent + "    TRADES_BUS_PATH    = _env_path(\"TRADES_BUS_PATH\",    f\"trades_bus_{ACCOUNT_LABEL}.json\")\n"
    + "\n"
    + indent + "PUBLIC_TRADES_PATH = _env_path(\"PUBLIC_TRADES_PATH\", f\"public_trades_{ACCOUNT_LABEL}.jsonl\")\n"
    + indent + "EXECUTIONS_PATH    = _env_path(\"EXECUTIONS_PATH\",    f\"ws_executions_{ACCOUNT_LABEL}.jsonl\")\n"
)

s3 = s2[:insert_at] + block + s2[insert_at:]
p.write_text(s3, encoding="utf-8")

print(f\"OK: removed prior bind blocks={n} and re-inserted safely (indent_len={len(indent)}).\")
