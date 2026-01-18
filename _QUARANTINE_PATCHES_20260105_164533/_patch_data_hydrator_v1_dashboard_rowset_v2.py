from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "app" / "dashboard" / "data_hydrator_v1.py"

def patch_normalize_ops_accounts(lines: list[str]) -> tuple[list[str], bool]:
    """
    Replace _normalize_ops_accounts with a hardened version that never treats metadata keys
    (version/updated_ms/components/etc) as account labels.
    """
    out: list[str] = []
    i = 0
    changed = False

    while i < len(lines):
        line = lines[i]
        if line.startswith("def _normalize_ops_accounts("):
            changed = True
            # Consume until next top-level def (same indent 0) AFTER this function
            out.append(
                "def _normalize_ops_accounts(ops_raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:\n"
            )
            out.append(
                "    \"\"\"\n"
                "    ops_snapshot.json can be shaped as:\n"
                "      A) {\"accounts\": { ... }}  (future)\n"
                "      B) { \"flashback_01\": {...}, ... , \"version\": 1, \"updated_ms\": ..., \"components\": {...} }  (current)\n"
                "\n"
                "    Return map indexed by normalized account label.\n"
                "    IMPORTANT: Never treat metadata keys as account labels.\n"
                "    \"\"\"\n"
            )
            out.append("    if not isinstance(ops_raw, dict):\n")
            out.append("        return {}\n\n")
            out.append("    acc = ops_raw.get(\"accounts\", None)\n")
            out.append("    if isinstance(acc, dict):\n")
            out.append("        src = acc\n")
            out.append("    else:\n")
            out.append("        src = ops_raw\n\n")
            out.append(
                "    reserved = {\n"
                "        \"version\", \"updated_ms\", \"updatedms\", \"updated\", \"components\", \"component\",\n"
                "        \"schema_version\", \"schema\", \"global\", \"source\"\n"
                "    }\n\n"
            )
            out.append("    out: Dict[str, Dict[str, Any]] = {}\n")
            out.append("    for k, v in (src.items() if isinstance(src, dict) else []):\n")
            out.append("        kk = str(k or \"\").strip()\n")
            out.append("        if not kk:\n")
            out.append("            continue\n")
            out.append("        if kk.lower().replace(\"_\", \"\") in reserved:\n")
            out.append("            continue\n")
            out.append("        if not isinstance(v, dict):\n")
            out.append("            continue\n")
            out.append("        nk = _norm_account_label(kk)\n")
            out.append("        if not nk:\n")
            out.append("            continue\n")
            out.append("        out[nk] = v\n")
            out.append("    return out\n\n")

            # skip original function body
            i += 1
            while i < len(lines):
                if lines[i].startswith("def ") and not lines[i].startswith("def _normalize_ops_accounts("):
                    break
                i += 1
            continue

        out.append(line)
        i += 1

    return out, changed


def patch_all_ids_union(lines: list[str]) -> tuple[list[str], bool]:
    """
    Replace the whole all_ids union/update region with a safe display-set:
      - expected accounts (main + flashback01..flashback10)
      - plus whatever is explicitly in fleet_manifest
    Everything else becomes overlays, not row creators.
    """
    out: list[str] = []
    i = 0
    changed = False

    while i < len(lines):
        line = lines[i]

        # Find the first occurrence of all_ids = set()
        if line.strip() == "all_ids = set()":
            changed = True

            # Back up: include the comment line just above if it's the union comment
            # but we don't need to manipulate previous lines; just replace forward block.
            out.append(
                "    # Union of accounts (DISPLAY SET):\n"
                "    # We ONLY display expected accounts + any explicitly declared in fleet_manifest.\n"
                "    # All other sources (ops/orch/outcomes/integrity) are overlays only.\n"
                "    all_ids = set()\n\n"
                "    # Expected canonical labels (prevents junk keys like 'version'/'components' from becoming rows)\n"
                "    def _expected_accounts() -> set[str]:\n"
                "        out = {\"main\"}\n"
                "        for n in range(1, 11):\n"
                "            out.add(f\"flashback{n:02d}\")\n"
                "        return out\n\n"
                "    all_ids.update(_expected_accounts())\n"
                "    all_ids.update(manifest_idx.keys())\n"
            )

            # Now skip lines until we hit something that clearly starts the next section.
            # We stop skipping when we see "rows:" or "rows =" or "rows: List" or "rows: List["
            i += 1
            while i < len(lines):
                s = lines[i].lstrip()
                if s.startswith("rows") and (":" in s or "=" in s):
                    break
                # Also break if we reach the for-loop that uses all_ids (rare formatting)
                if s.startswith("for ") and " in sorted(all_ids" in s:
                    break
                i += 1
            continue

        out.append(line)
        i += 1

    return out, changed


def main() -> int:
    raw = TARGET.read_text(encoding="utf-8", errors="replace")
    lines = [l if l.endswith("\n") else l + "\n" for l in raw.splitlines()]

    # Patch A: normalize ops accounts
    lines, c1 = patch_normalize_ops_accounts(lines)

    # Patch B: all_ids union rowset hardening
    lines, c2 = patch_all_ids_union(lines)

    if not c1:
        print("WARN: did not patch _normalize_ops_accounts (function not found as expected)")
    if not c2:
        raise SystemExit("PATCH_FAIL: could not find 'all_ids = set()' to patch rowset union")

    TARGET.write_text("".join(lines), encoding="utf-8")
    print(f"OK: patched {TARGET} (normalize_ops={c1}, rowset_union={c2})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
