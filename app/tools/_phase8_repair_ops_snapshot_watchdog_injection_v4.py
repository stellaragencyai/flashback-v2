from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
lines = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# -------------------------
# 1) REMOVE WRONG INJECTION inside _extract_supervisor_from_heartbeats()
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
        end = i
        break

removed_bad = 0
if start is not None and end is not None and end > start:
    removed_bad = end - start
    del lines[start:end]

text = "".join(lines)

# -------------------------
# 2) FIND main() bounds
# -------------------------
m_main = re.search(r'^\s*def\s+main\s*\(\)\s*->\s*int\s*:\s*$', text, flags=re.M)
if not m_main:
    raise SystemExit("FATAL: could not find 'def main() -> int:' in ops_snapshot_tick.py")

lines = text.splitlines(True)

main_start = text[:m_main.start()].count("\n")

# main_end = next top-level def/class/if __name__ OR EOF
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

# Guard: don't double-inject in main
if any("Phase8: Merge orchestrator_watchdog into ops snapshot (end-of-main)" in ln for ln in main_block):
    raise SystemExit("FATAL: main() already contains end-of-main watchdog merge marker. Refusing to double-inject.")

# Find insertion point: last non-empty line in main() block
insert_at = None
for i in range(main_end - 1, main_start, -1):
    if lines[i].strip():
        insert_at = i + 1  # insert AFTER this line (still inside main block)
        break
if insert_at is None:
    raise SystemExit("FATAL: main() block appears empty; cannot inject")

inject = r'''

    # Phase8: Merge orchestrator_watchdog into ops snapshot (end-of-main)
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}

    # Find the accounts dict to attach to:
    # 1) direct local 'accounts'
    # 2) any local dict that has key "accounts" and that value is a dict
    _acc = None
    if "accounts" in locals() and isinstance(locals().get("accounts"), dict):
        _acc = locals().get("accounts")

    if _acc is None:
        for _name, obj in list(locals().items()):
            if isinstance(obj, dict) and isinstance(obj.get("accounts"), dict):
                _acc = obj.get("accounts")
                break

    if isinstance(_acc, dict) and isinstance(wd_labels, dict):
        for lbl, w in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(w, dict):
                continue
            if lbl not in _acc or not isinstance(_acc.get(lbl), dict):
                _acc[lbl] = {}
            # Normalize fields
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

lines.insert(insert_at, inject)

P.write_text("".join(lines), encoding="utf-8")
print(f"OK: ops_snapshot_tick.py repaired v4 (removed_bad_block_lines={removed_bad}, injected_at_line={insert_at+1})")
