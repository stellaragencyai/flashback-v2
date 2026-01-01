from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

start = "# --- Phase8: ignore flags (fleet-level overrides) ---"
end = "# --- end ignore flags ---"

if start not in s or end not in s:
    raise SystemExit("FATAL: ignore block tags not found")

a = s.find(start)
b = s.find(end, a)
if b == -1:
    raise SystemExit("FATAL: ignore block end tag not found")

b2 = b + len(end)

new_block = """# --- Phase8: ignore flags (fleet-level overrides) ---
    if ignore_ws_heartbeat:
        drop = {"WS_HEARTBEAT_MISSING", "WS_HEARTBEAT_STALE"}
        faults_fail = [f for f in faults_fail if getattr(f, "value", None) not in drop]
        faults_warn = [f for f in faults_warn if getattr(f, "value", None) not in drop]
    if ignore_memory:
        drop = {
            "MEMORY_SNAPSHOT_MISSING",
            "MEMORY_SNAPSHOT_STALE",
            "MEMORY_SNAPSHOT_PARSE_FAILED",
            "MEMORY_COUNT_MISSING",
        }
        faults_fail = [f for f in faults_fail if getattr(f, "value", None) not in drop]
        faults_warn = [f for f in faults_warn if getattr(f, "value", None) not in drop]
    if ignore_decisions:
        drop = {
            "DECISIONS_MISSING",
            "DECISIONS_STALE",
            "DECISIONS_TAIL_PARSE_FAILED",
            "DECISIONS_SCHEMA_INVALID",
        }
        faults_fail = [f for f in faults_fail if getattr(f, "value", None) not in drop]
        faults_warn = [f for f in faults_warn if getattr(f, "value", None) not in drop]
    # --- end ignore flags ---"""

# Keep indentation correct by replacing the whole tagged region
pre = s[:a]
post = s[b2:]
s2 = pre + new_block + post

p.write_text(s2, encoding="utf-8")
print("OK: fixed ignore flag drop sets to match real FaultCodes")
