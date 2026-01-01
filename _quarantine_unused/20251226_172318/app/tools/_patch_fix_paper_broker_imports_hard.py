from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app\sim\paper_broker.py")
s = p.read_text(encoding="utf-8", errors="ignore")

GOOD = """try:
    from app.ai.ai_events_spine import (  # type: ignore
        build_outcome_record,
        publish_ai_event,
    )
except Exception:  # pragma: no cover
    def build_outcome_record(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {}
    def publish_ai_event(*args: Any, **kwargs: Any) -> None:  # type: ignore
        pass

# --- Outcome v1 writer (fail-soft) ---
try:
    from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
except Exception:
    write_outcome_from_paper_close = None  # type: ignore
"""

# This regex matches the exact corrupted shape you're showing:
# - starts at "try:" line importing ai_events_spine
# - contains an injected Outcome writer try/except before the import list closes
# - ends after the ai_events_spine except stub
pat = re.compile(
    r"(?ms)^try:\s*\n"
    r"\s+from app\.ai\.ai_events_spine import\s*\(\s*# type: ignore\s*\n"
    r".*?"
    r"^except Exception:\s*# pragma: no cover\s*\n"
    r"(?:\s+def build_outcome_record.*?\n\s+return \{\}\s*\n)?"
    r"(?:\s+def publish_ai_event.*?\n\s+pass\s*\n)?"
)

m = pat.search(s)
if not m:
    raise SystemExit("FAIL: could not find the corrupted ai_events_spine import block to replace (pattern mismatch).")

start, end = m.span()
s2 = s[:start] + GOOD + "\n\n" + s[end:]

p.write_text(s2, encoding="utf-8")
print("OK: repaired app/sim/paper_broker.py import block (ai_events_spine + outcome writer)")
