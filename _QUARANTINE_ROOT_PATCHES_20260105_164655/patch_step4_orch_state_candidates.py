from pathlib import Path
import re

p = Path(r"app\dashboard\data_hydrator_v1.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Insert ORCH_STATE_CANDIDATES right after ORCH_STATE definition
if "ORCH_STATE_CANDIDATES" not in s:
    # Find the ORCH_STATE line
    m = re.search(r"^ORCH_STATE\s*=\s*.*$", s, flags=re.MULTILINE)
    if not m:
        raise SystemExit("ERROR: Could not find ORCH_STATE in data_hydrator_v1.py")

    insert = (
        m.group(0)
        + "\n\n# Orchestrator state can live in multiple places depending on phase/patches\n"
        + "ORCH_STATE_CANDIDATES = [\n"
        + "    STATE_ROOT / \"orchestrator_state.json\",\n"
        + "    REPO_ROOT / \"app\" / \"ops\" / \"orchestrator_state.json\",\n"
        + "    REPO_ROOT / \"app\" / \"ops\" / \"orchestrator_state.json\".replace(\"/\", \"\\\\\"),\n"
        + "]\n"
    )
    # The replace line above is harmless even if ugly; keeps Windows path behavior stable.

    s = s[:m.start()] + insert + s[m.end():]

# 2) Replace _load_orchestrator_state() to pick first existing candidate
pat = r"def _load_orchestrator_state\(\)\s*->\s*Dict\[str,\s*Any\]:\s*\n\s*return _safe_read_json\(ORCH_STATE\)\s*"
repl = (
    "def _load_orchestrator_state() -> Dict[str, Any]:\n"
    "    # Prefer whichever orchestrator_state.json is actually present.\n"
    "    for cand in ORCH_STATE_CANDIDATES:\n"
    "        try:\n"
    "            cp = Path(str(cand))\n"
    "            if cp.exists():\n"
    "                return _safe_read_json(cp)\n"
    "        except Exception:\n"
    "            continue\n"
    "    # Fall back to original path (fail-soft)\n"
    "    return _safe_read_json(ORCH_STATE)\n"
)

if re.search(pat, s, flags=re.MULTILINE) is None:
    print("WARN: _load_orchestrator_state() pattern not matched (maybe already updated).")
else:
    s = re.sub(pat, repl, s, flags=re.MULTILINE)

p.write_text(s, encoding="utf-8")
print("OK: Patched data_hydrator_v1.py to load orchestrator_state.json from multiple locations")
