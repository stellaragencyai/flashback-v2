from __future__ import annotations

from pathlib import Path
import re

p = Path("app/ai/outcome_writer.py")
s = p.read_text(encoding="utf-8")

# Ensure json import exists (your file may already import it inside the function; we need it at module scope now)
if re.search(r"^\s*import\s+json\s*$", s, flags=re.M) is None:
    # Put json import near other imports, after typing import
    s = s.replace(
        "from typing import Any, Dict",
        "from typing import Any, Dict\n\nimport json"
    )

# Ensure _now_ms exists
if "_now_ms" not in s:
    # Insert after outcome_contract import if present
    marker = "from app.ai.outcome_contract import OUTCOME_SCHEMA_VERSION, validate_outcome_v1"
    if marker in s:
        s = s.replace(
            marker,
            marker
            + "\n\n\ndef _now_ms() -> int:\n"
              "    import time\n"
              "    return int(time.time() * 1000)\n"
        )
    else:
        # Fallback: append near top
        s = s + "\n\n\ndef _now_ms() -> int:\n    import time\n    return int(time.time() * 1000)\n"

# Replace append_outcome_v1 implementation robustly
m = re.search(r"^def append_outcome_v1\(outcome: Dict\[str, Any\]\) -> None:\n", s, flags=re.M)
if not m:
    raise SystemExit("FAIL: append_outcome_v1 signature not found")

start = m.start()

# Find the next top-level def after append_outcome_v1 to determine end
m2 = re.search(r"^def\s+\w+\(.*?\):\n", s[m.end():], flags=re.M)
if m2:
    end = m.end() + m2.start()
else:
    end = len(s)

new_block = """def append_outcome_v1(outcome: Dict[str, Any]) -> None:
    \"""
    Append a validated outcome.v1 row to the canonical outcomes.v1 bus.

    Writer is the canonical normalizer:
    - sets schema_version
    - ensures event_type + ts_ms exist
    - ensures exit_side exists (derived if possible)
    \"""

    # Canonical schema version
    outcome["schema_version"] = OUTCOME_SCHEMA_VERSION

    # Canonical event type (required by contract)
    outcome.setdefault("event_type", "trade_outcome")

    # Canonical timestamp (required by contract in practice)
    outcome.setdefault("ts_ms", _now_ms())

    # If exit_side is missing, derive from entry_side when possible
    if "exit_side" not in outcome:
        es = str(outcome.get("entry_side") or "")
        if es.lower() == "buy":
            outcome["exit_side"] = "Sell"
        elif es.lower() == "sell":
            outcome["exit_side"] = "Buy"

    # Hard schema validation (fail fast)
    validate_outcome_v1(outcome)

    out_path = _resolve_out_path()

    # Append as JSONL (utf-8, one line)
    line = json.dumps(outcome, ensure_ascii=False)
    with out_path.open("a", encoding="utf-8", newline="\\n") as f:
        f.write(line + "\\n")
"""

s2 = s[:start] + new_block + s[end:]
p.write_text(s2, encoding="utf-8")

print("OK: patched outcome_writer append_outcome_v1 normalization (event_type/ts_ms/exit_side) + ensured json import + _now_ms")
