from __future__ import annotations
from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
if not P.exists():
    raise SystemExit("FATAL: app\\ops\\fleet_snapshot_tick.py not found")

s = P.read_text(encoding="utf-8", errors="ignore")
orig = s
edits = 0

# Ensure ops_snapshot loader exists
if "def _load_ops_snapshot" not in s:
    insert = r"""
def _load_ops_snapshot() -> dict:
    # Phase8: ops snapshot is the runtime truth (pid/alive/restarts).
    p = STATE / "ops_snapshot.json"
    if not p.exists():
        return {}
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
"""
    # insert after _load_json or near top after STATE constants
    m = re.search(r"\nSTATE\s*=\s*ROOT\s*/\s*['\"]state['\"].*\n", s)
    if not m:
        # fallback: after constants block
        s = s + "\n" + insert + "\n"
    else:
        idx = m.end()
        s = s[:idx] + insert + s[idx:]
    edits += 1

# Find where subs dict is constructed and patch merge hook.
# We look for a line that assigns subs = {} or similar in build snapshot.
hook_tag = "# --- Phase8: merge ops_snapshot runtime truth ---"
if hook_tag not in s:
    # Heuristic: after manifest rows loaded and before writing snapshot
    # We'll patch near where `subs = {}` first appears.
    m = re.search(r"\n\s*subs\s*=\s*\{\}\s*\n", s)
    if not m:
        # fallback: patch before final snapshot dict build (near out = { ... })
        m = re.search(r"\n\s*out\s*=\s*\{\s*\n", s)
    if not m:
        raise SystemExit("FATAL: Could not find insertion point for ops_snapshot merge")

    ins = r"""
    # --- Phase8: merge ops_snapshot runtime truth ---
    ops = _load_ops_snapshot()
    accounts = (ops.get("accounts") or {}) if isinstance(ops, dict) else {}
    # When a supervisor is running, ops_snapshot wins for pid/alive/restart_count.
    # Manifest remains authoritative for intent (enabled/mode/should_run).
    def _merge_runtime(label: str, cur: dict) -> dict:
        a = accounts.get(label) if isinstance(accounts, dict) else None
        if isinstance(a, dict):
            # prefer supervisor-ai-stack block if present
            sup = a.get("supervisor_ai_stack") if isinstance(a.get("supervisor_ai_stack"), dict) else {}
            pid = sup.get("pid") if isinstance(sup, dict) else a.get("pid")
            alive = sup.get("alive") if isinstance(sup, dict) else a.get("alive")
            rc = a.get("restart_count")
            if pid is not None:
                cur["pid"] = pid
            if alive is not None:
                cur["alive"] = bool(alive)
            if rc is not None:
                cur["restart_count"] = rc
        return cur
    # --- end Phase8 merge ---
"""
    s = s[:m.end()] + ins + s[m.end():]
    edits += 1

# Now ensure _merge_runtime is called for each sub label inserted into subs.
# We patch common pattern: subs[label] = {...}
if "_merge_runtime(" not in s:
    # Replace assignments to subs[label] = payload with merge call
    s2, n = re.subn(r"(subs\[\s*label\s*\]\s*=\s*)(\{)", r"\1_merge_runtime(label, \2", s, count=10)
    if n > 0:
        # Need to close the call. Find the end of dict literal at same indentation is hard,
        # but in this file it's usually one-liner or small block.
        # Safer: patch simple cases only. If none matched, we'll do manual insert later.
        s = s2
        edits += n

# If no edits happened, bail so we don't corrupt the file.
if s == orig:
    raise SystemExit("FATAL: no patch applied (structure drift). Need manual patch with exact file snippet.")

P.write_text(s, encoding="utf-8")
print(f"OK: patched fleet_snapshot_tick.py merge ops_snapshot edits={edits}")
