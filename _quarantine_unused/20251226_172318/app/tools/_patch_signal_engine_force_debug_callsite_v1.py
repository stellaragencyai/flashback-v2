from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "FORCE_DEBUG_CALLSITE_V1" in s:
    raise SystemExit("REFUSE: FORCE_DEBUG_CALLSITE_V1 already applied")

# 1) Inject helper right after compute_simple_signal definition (after its return)
# We'll insert after the line: "return side, debug"
needle = "return side, debug"
idx = s.find(needle)
if idx == -1:
    raise SystemExit("PATCH FAILED: could not find compute_simple_signal return line")

# Find end-of-line after needle
eol = s.find("\n", idx)
if eol == -1:
    raise SystemExit("PATCH FAILED: could not find EOL after compute_simple_signal return")

inject = r'''

# FORCE_DEBUG_CALLSITE_V1: ensure observed.jsonl debug always has real numbers when candles are available
def _ensure_debug(candles, dbg):
    try:
        if isinstance(dbg, dict):
            lc = dbg.get("last_close")
            pc = dbg.get("prev_close")
            ma = dbg.get("ma")
            if (lc is not None) and (pc is not None) and (ma is not None):
                return dbg
        _side, _dbg = compute_simple_signal(candles)
        if isinstance(_dbg, dict) and _dbg:
            return _dbg
    except Exception:
        pass
    return dbg if isinstance(dbg, dict) else {}
'''

s2 = s[:eol+1] + inject + s[eol+1:]

# 2) Replace call-site "debug=debug," with "debug=_ensure_debug(candles, debug),"
# only where candles is in scope (we assume existing code uses variable name "candles")
# Do a conservative replace on lines that exactly contain "debug=debug,"
lines = s2.splitlines()
repl_count = 0
for i, line in enumerate(lines):
    if "debug=debug," in line:
        # mark the first replacement with a comment once
        if repl_count == 0:
            lines[i] = line.replace("debug=debug,", "debug=_ensure_debug(candles, debug),  # FORCE_DEBUG_CALLSITE_V1")
        else:
            lines[i] = line.replace("debug=debug,", "debug=_ensure_debug(candles, debug),")
        repl_count += 1

if repl_count == 0:
    raise SystemExit("PATCH FAILED: did not find any 'debug=debug,' call-sites to patch")

p.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
print(f"OK: patched signal_engine.py (FORCE_DEBUG_CALLSITE_V1). callsites_fixed={repl_count}")
