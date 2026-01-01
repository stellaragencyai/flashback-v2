from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
lines = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# -------------------------
# 1) REMOVE WRONG INJECTION (inside _extract_supervisor_from_heartbeats)
# -------------------------
start_pat = re.compile(r'^\s*#\s*Phase8:\s*Merge watchdog restart/backoff/blocked state into accounts\s*$')
end_pat = re.compile(r'^\s*if\s+isinstance\(accounts,\s*dict\)\s*:\s*$')

start = None
end = None
for i, ln in enumerate(lines):
    if start is None and start_pat.search(ln):
        start = i
        continue
    if start is not None and end is None and end_pat.search(ln):
        end = i  # do not remove this end line
        break

removed_bad = 0
if start is not None and end is not None and end > start:
    removed_bad = (end - start)
    del lines[start:end]

# -------------------------
# 2) FIND def main() and inject merge into main
# -------------------------
text = "".join(lines)

m_main = re.search(r'^\s*def\s+main\s*\(\)\s*->\s*int\s*:\s*$', text, flags=re.M)
if not m_main:
    raise SystemExit("FATAL: could not find 'def main() -> int:' in ops_snapshot_tick.py")

# Find the main() line index
idx_main_line = text[:m_main.start()].count("\n")  # 0-based line number in splitlines(True)

# Work with mutable list again
lines2 = text.splitlines(True)

# Guard: don't double-inject
if "Phase8: Merge watchdog into ops_snapshot accounts (main)" in text:
    raise SystemExit("FATAL: main() already contains watchdog merge block marker. Refusing to double-inject.")

inject = r'''
    # Phase8: Merge watchdog into ops_snapshot accounts (main)
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}

    # Prefer: merge into local 'accounts' variable if it exists
    try:
        _acc = accounts  # type: ignore[name-defined]
    except Exception:
        _acc = None

    if isinstance(_acc, dict) and isinstance(wd_labels, dict):
        for lbl, w in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(w, dict):
                continue
            if lbl not in _acc or not isinstance(_acc.get(lbl), dict):
                _acc[lbl] = {}
            try:
                rc = int(w.get("restart_count") or 0)
            except Exception:
                rc = 0
            try:
                bo = float(w.get("backoff_sec") or 0.0)
            except Exception:
                bo = 0.0
            _acc[lbl]["watchdog"] = {
                "alive": bool(w.get("alive")) if "alive" in w else None,
                "pid": w.get("pid"),
                "restart_count": rc,
                "backoff_sec": bo,
                "next_restart_allowed_ts_ms": w.get("next_restart_allowed_ts_ms"),
                "blocked": bool(w.get("blocked")) if "blocked" in w else False,
                "blocked_reason": w.get("blocked_reason"),
                "last_checked_ts_ms": w.get("last_checked_ts_ms"),
                "last_restart_ts_ms": w.get("last_restart_ts_ms"),
                "source": "orchestrator_watchdog",
            }

    # Fallback: merge into out["accounts"] if accounts var doesn't exist
    if _acc is None and "out" in locals() and isinstance(locals().get("out"), dict) and isinstance(wd_labels, dict):
        out_dict = locals().get("out")  # type: ignore[assignment]
        if isinstance(out_dict, dict):
            out_accounts = out_dict.get("accounts")
            if not isinstance(out_accounts, dict):
                out_accounts = {}
                out_dict["accounts"] = out_accounts
            for lbl, w in wd_labels.items():
                if not isinstance(lbl, str) or not isinstance(w, dict):
                    continue
                if lbl not in out_accounts or not isinstance(out_accounts.get(lbl), dict):
                    out_accounts[lbl] = {}
                try:
                    rc = int(w.get("restart_count") or 0)
                except Exception:
                    rc = 0
                try:
                    bo = float(w.get("backoff_sec") or 0.0)
                except Exception:
                    bo = 0.0
                out_accounts[lbl]["watchdog"] = {
                    "alive": bool(w.get("alive")) if "alive" in w else None,
                    "pid": w.get("pid"),
                    "restart_count": rc,
                    "backoff_sec": bo,
                    "next_restart_allowed_ts_ms": w.get("next_restart_allowed_ts_ms"),
                    "blocked": bool(w.get("blocked")) if "blocked" in w else False,
                    "blocked_reason": w.get("blocked_reason"),
                    "last_checked_ts_ms": w.get("last_checked_ts_ms"),
                    "last_restart_ts_ms": w.get("last_restart_ts_ms"),
                    "source": "orchestrator_watchdog",
                }
'''.lstrip("\n")

# Strategy to insert:
# Prefer: right AFTER a line in main that creates 'out = {'
# Else: right BEFORE the OUT write_text(...) call
main_indent = 4

def is_in_main(i: int) -> bool:
    # crude: lines after def main that start with 4+ spaces and before next top-level def/if
    return i > idx_main_line

out_assign_idx = None
write_idx = None

for i in range(idx_main_line + 1, len(lines2)):
    ln = lines2[i]
    # stop if we hit a new top-level def (indent 0)
    if re.match(r'^\S', ln) and re.match(r'^(def|class)\s+', ln):
        break
    if out_assign_idx is None and re.match(r'^\s{4}out\s*=\s*{', ln):
        out_assign_idx = i
    if write_idx is None and ("ops_snapshot.json" in ln or "OUT_PATH" in ln or "write_text" in ln) and "write_text" in ln:
        write_idx = i

if out_assign_idx is not None:
    insert_at = out_assign_idx + 1
elif write_idx is not None:
    insert_at = write_idx
else:
    raise SystemExit("FATAL: could not find either 'out = {' or a write_text(...) line in main() to anchor injection")

lines2.insert(insert_at, inject)

P.write_text("".join(lines2), encoding="utf-8")
print(f"OK: repaired ops_snapshot_tick.py (removed_bad_block_lines={removed_bad}, injected_at_line={insert_at+1})")
