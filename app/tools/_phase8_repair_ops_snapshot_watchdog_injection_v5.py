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
# 2) FIND A REAL WRITER ANCHOR (write_text call for ops snapshot)
# -------------------------
lines = text.splitlines(True)

writer_idx = None
for i, ln in enumerate(lines):
    if "write_text" in ln and ("ops_snapshot" in ln or "OPS_SNAPSHOT" in ln or "ops_snapshot.json" in ln):
        writer_idx = i
        break

if writer_idx is None:
    # fallback: any mention of ops_snapshot.json, then search nearby for write_text
    for i, ln in enumerate(lines):
        if "ops_snapshot.json" in ln:
            for j in range(max(0, i-30), min(len(lines), i+30)):
                if "write_text" in lines[j]:
                    writer_idx = j
                    break
            if writer_idx is not None:
                break

if writer_idx is None:
    raise SystemExit("FATAL: could not find an ops_snapshot writer line (write_text) to anchor injection")

# Determine indentation of the writer line
indent = re.match(r'^(\s*)', lines[writer_idx]).group(1)

# Guard against double patch
if "Phase8: Merge watchdog into accounts (writer-anchor)" in text:
    raise SystemExit("FATAL: ops_snapshot_tick.py already contains writer-anchored watchdog merge. Refusing to double-patch.")

inject_raw = r'''
# Phase8: Merge watchdog into accounts (writer-anchor)
wd = _load_watchdog_state()
wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}

# locate the accounts dict without assuming variable names
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
'''.strip("\n").splitlines()

inject = "".join(indent + ln + "\n" for ln in inject_raw) + "\n"

# Insert right BEFORE the writer line
lines.insert(writer_idx, inject)

P.write_text("".join(lines), encoding="utf-8")
print(f"OK: ops_snapshot_tick.py repaired v5 (removed_bad_block_lines={removed_bad}, injected_before_line={writer_idx+1})")
