import re
from pathlib import Path
from datetime import datetime

p = Path(r"C:\Flashback\app\ops\orchestrator_v2.py")
s = p.read_text(encoding="utf-8")

bak = p.with_suffix(p.suffix + ".bak_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
bak.write_text(s, encoding="utf-8")

# We want ws_switchboard/trade_outcome_recorder compatibility.
# Some components look for EXEC_BUS_PATH, others look for EXECUTIONS_PATH.
# Ensure both are set to the same per-label file.

if 'env["EXECUTIONS_PATH"]' in s:
    print("NOOP: orchestrator_v2.py already sets EXECUTIONS_PATH")
else:
    pat = r'(env\["EXEC_BUS_PATH"\]\s*=\s*str\(state\s*/\s*f"ws_executions_\{label\}\.jsonl"\)\s*)'
    m = re.search(pat, s)
    if not m:
        raise SystemExit("FAILED: couldn't find EXEC_BUS_PATH assignment to patch.")

    ins = (
        m.group(1)
        + '\n\n'
        + '    # Legacy compatibility: some workers prefer EXECUTIONS_PATH\\n'
        + '    env["EXECUTIONS_PATH"] = str(state / f"ws_executions_{label}.jsonl")\\n'
    )
    s = re.sub(pat, lambda _m: ins, s, count=1)

    p.write_text(s, encoding="utf-8")
    print("PATCHED: added EXECUTIONS_PATH")
    print("BACKUP:", bak)
