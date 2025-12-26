from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
lines = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# -------------------------
# 1) REMOVE WRONG INJECTION inside _extract_supervisor_from_heartbeats()
# -------------------------
start_pat = re.compile(r'^\s*#\s*Phase8:\s*Merge watchdog restart/backoff/blocked state into accounts\s*$')
# End anchor: first line that starts the normal supervisor extraction block
end_pat = re.compile(r'^\s*if\s+isinstance\(accounts,\s*dict\)\s*:\s*$')

start = None
end = None
for i, ln in enumerate(lines):
    if start is None and start_pat.search(ln):
        start = i
        continue
    if start is not None and end is None and end_pat.search(ln):
        end = i  # do not remove the end line
        break

removed_bad = 0
if start is not None and end is not None and end > start:
    removed_bad = (end - start)
    del lines[start:end]

text = "".join(lines)

# -------------------------
# 2) FIND main() and inject watchdog merge BEFORE the final base-indent return
# -------------------------
m_main = re.search(r'^\s*def\s+main\s*\(\)\s*->\s*int\s*:\s*$', text, flags=re.M)
if not m_main:
    raise SystemExit("FATAL: could not find 'def main() -> int:' in ops_snapshot_tick.py")

# Re-split after removal
lines = text.splitlines(True)

# Find main() start line index (0-based)
main_start = text[:m_main.start()].count("\n")

# Find end of main(): next top-level def/class/if __name__ OR EOF
main_end = None
for i in range(main_start + 1, len(lines)):
    ln = lines[i]
    if re.match(r'^\S', ln) and re.match(r'^(def|class)\s+', ln):
        main_end = i
        break
    if re.match(r'^\s*if\s+__name__\s*==\s*["\']__main__["\']\s*:', ln):
        main_end = i
        break
if main_end is None:
    main_end = len(lines)

main_block = lines[main_start:main_end]

# Guard: don't double-inject
if any("Phase8: Merge orchestrator_watchdog into ops snapshot (main)" in ln for ln in main_block):
    raise SystemExit("FATAL: main() already contains watchdog merge marker. Refusing to double-inject.")

# Find last base-indent return (indent == 4 spaces) inside main
return_idx = None
for i in range(main_end - 1, main_start, -1):
    ln = lines[i]
    if re.match(r'^\s{4}return\b', ln):
        return_idx = i
        break

if return_idx is None:
    raise SystemExit("FATAL: could not find a base-indent 'return' inside main() to anchor injection")

inject = r'''
    # Phase8: Merge orchestrator_watchdog into ops snapshot (main)
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}

    # Try to attach into local 'accounts' if it exists
    try:
        _acc = accounts  # type: ignore[name-defined]
    except Exception:
        _acc = None

    # If accounts doesn't exist, try to locate an output dict that contains accounts
    if _acc is None:
        # common patterns: out, data, snapshot, result
        for _name in ("out", "data", "snapshot", "result"):
            obj = locals().get(_name)
            if isinstance(obj, dict) and isinstance(obj.get("accounts"), dict):
                _acc = obj.get("accounts")
                break

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
'''.lstrip("\n")

# Insert injection right BEFORE the return line
lines.insert(return_idx, inject)

P.write_text("".join(lines), encoding="utf-8")
print(f"OK: ops_snapshot_tick.py repaired (removed_bad_block_lines={removed_bad}, injected_before_return_line={return_idx+1})")
