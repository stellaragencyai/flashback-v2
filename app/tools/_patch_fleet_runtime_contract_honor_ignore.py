from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Must have the new args already
if "ignore_ws_heartbeat" not in s:
    raise SystemExit("FATAL: ignore flags not in file; patch signature first")

# We patch by inserting a small block near the end of validate_runtime_contract,
# right before returning ContractResult (or the object that holds faults_fail/faults_warn).
# We'll locate the last 'return ContractResult' inside validate_runtime_contract.
needle = "return ContractResult"
idx = s.rfind(needle)
if idx == -1:
    raise SystemExit("FATAL: could not find return ContractResult")

# Insert a filter block just before that return.
insert_block = """
        # --- Phase8: ignore flags (fleet-level overrides) ---
        if ignore_ws_heartbeat:
            faults_fail = [f for f in faults_fail if getattr(f, "value", None) != "WS_HEARTBEAT_MISSING"]
            faults_warn = [f for f in faults_warn if getattr(f, "value", None) != "WS_HEARTBEAT_MISSING"]
        if ignore_memory:
            # Memory-related faults (best-effort)
            drop = {"MEMORY_MISSING", "MEMORY_STALE", "MEMORY_PARSE_FAIL", "MEMORY_COUNT_EXCEEDED"}
            faults_fail = [f for f in faults_fail if getattr(f, "value", None) not in drop]
            faults_warn = [f for f in faults_warn if getattr(f, "value", None) not in drop]
        if ignore_decisions:
            # Decision-log-related faults (best-effort)
            drop = {"DECISIONS_MISSING", "DECISIONS_STALE", "DECISIONS_TAIL_PARSE_FAIL", "DECISIONS_SCHEMA_INVALID"}
            faults_fail = [f for f in faults_fail if getattr(f, "value", None) not in drop]
            faults_warn = [f for f in faults_warn if getattr(f, "value", None) not in drop]
        # --- end ignore flags ---
"""

# We need to inject inside validate_runtime_contract scope. We'll do a conservative replace:
# Find a spot where faults_fail and faults_warn exist. We'll inject before the return block.
# We'll insert just before the last occurrence of 'return ContractResult'.
s2 = s[:idx] + insert_block + s[idx:]

p.write_text(s2, encoding="utf-8")
print("OK: patched validate_runtime_contract to honor ignore flags")
