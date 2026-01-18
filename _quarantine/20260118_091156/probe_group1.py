from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

checks = {
  "has_dead_outcomes_if": bool(re.search(r"\{%\s*if\s+dead_outcomes\s*%\}", s)),
  "has_stale_elif": bool(re.search(r"\{%\s*elif\s+stale\s*%\}", s)),
  "has_outcomes_dead_text": ("Outcomes DEAD" in s),
  "has_outcomes_stale_text": ("Outcomes STALE" in s),
  "has_topbar_subset_red": ("truthPill red\">Subset:" in s),
  "has_lastOutcome_block": ("js-lastOutcome" in s),
  "has_updatedAgo": ("updatedAgo" in s),
  "has_freshHuman": ("freshHuman" in s),
  "has_format_fresh_secs": ("format(fresh) }}s" in s),
}

print("PROBE:", checks)

# Show a tiny snippet around Outcomes DEAD if present
idx = s.find("Outcomes DEAD")
if idx != -1:
    print("\\nSNIP Outcomes DEAD:")
    print(s[max(0, idx-160): idx+220])

idx2 = s.find("truthPill red\">Subset:")
if idx2 != -1:
    print("\\nSNIP Subset chip:")
    print(s[max(0, idx2-80): idx2+120])
