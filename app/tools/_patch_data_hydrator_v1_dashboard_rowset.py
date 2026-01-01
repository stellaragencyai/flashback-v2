from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "app" / "dashboard" / "data_hydrator_v1.py"

def main() -> int:
    s = TARGET.read_text(encoding="utf-8", errors="replace")

    # --- Patch A: harden ops_snapshot normalization (skip meta keys) ---
    # Replace the body of _normalize_ops_accounts with a guarded version.
    pat_func = r"def _normalize_ops_accounts\(ops_raw: Dict\[str, Any\]\) -> Dict\[str, Dict\[str, Any\]\]:\n(?:.|\n)*?\n\n"
    m = re.search(pat_func, s)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find _normalize_ops_accounts function block")

    new_func = """def _normalize_ops_accounts(ops_raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    \"""
    ops_snapshot.json can be shaped as:
      A) {"accounts": { ... }}  (future)
      B) { "flashback_01": {...}, ... , "version": 1, "updated_ms": ..., "components": {...} }  (current)

    Return map indexed by normalized account label.
    IMPORTANT: Never treat metadata keys as account labels.
    \"""
    if not isinstance(ops_raw, dict):
        return {}

    # Preferred future shape
    acc = ops_raw.get("accounts", None)
    if isinstance(acc, dict):
        src = acc
    else:
        src = ops_raw

    reserved = {
        "version", "updated_ms", "updatedms", "updated", "components", "component",
        "schema_version", "schema", "global", "source"
    }

    out: Dict[str, Dict[str, Any]] = {}
    for k, v in (src.items() if isinstance(src, dict) else []):
        kk = str(k or "").strip()
        if not kk:
            continue
        if kk.lower().replace("_", "") in reserved:
            continue
        # extra guard: if value is not a dict, it's not an account record
        if not isinstance(v, dict):
            continue
        nk = _norm_account_label(kk)
        if not nk:
            continue
        out[nk] = v
    return out


"""
    s2 = re.sub(pat_func, new_func, s, count=1)

    # --- Patch B: Only build rows from expected accounts + manifest accounts ---
    pat_allids = r"\s*# Union of accounts: manifest \+ orch \+ ops \+ integrity maps\n\s*all_ids = set\(\)\n(?:\s*all_ids\.update\([^)]+\)\n)+"
    m2 = re.search(pat_allids, s2)
    if not m2:
        raise SystemExit("PATCH_FAIL: could not find all_ids union block")

    repl_allids = """
    # Union of accounts (DISPLAY SET):
    # We ONLY display expected accounts + any explicitly declared in fleet_manifest.
    # All other sources (ops/orch/outcomes) are overlays only.
    all_ids = set()

    # Expected canonical labels (prevents junk keys like 'version'/'components' from becoming rows)
    def _expected_accounts() -> set[str]:
        out = {"main"}
        for i in range(1, 11):
            out.add(f"flashback{i:02d}")
        return out

    all_ids.update(_expected_accounts())
    all_ids.update(manifest_idx.keys())
"""
    s3 = re.sub(pat_allids, repl_allids, s2, count=1)

    if s3 == s:
        raise SystemExit("PATCH_FAIL: no changes applied (unexpected)")

    TARGET.write_text(s3, encoding="utf-8")
    print(f"OK: patched {TARGET}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
