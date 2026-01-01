import re
from pathlib import Path

p = Path(r"app\ai\ai_decision_outcome_linker.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# ---------- helper insertion ----------
if "_extract_snapshot_linkage" not in s:
    marker = "# -------------------------\n# decisions index\n# -------------------------\n"
    if marker not in s:
        raise SystemExit("PATCH_FAIL: marker for decisions index not found")

    helper = (
        "\n"
        "def _extract_snapshot_linkage(d: Dict[str, Any]) -> Dict[str, Any]:\n"
        "    \"\"\"\\\n"
        "    Phase 7: decisions may carry snapshot linkage fields stamped by ai_decision_logger.\n"
        "    Tolerant: if fields are missing or invalid, return None values.\n"
        "    \"\"\"\n"
        "    try:\n"
        "        fp = d.get(\"snapshot_fp\")\n"
        "        mode = d.get(\"snapshot_mode\")\n"
        "        sv = d.get(\"snapshot_schema_version\")\n"
        "        return {\n"
        "            \"snapshot_fp\": _safe_str(fp) or None,\n"
        "            \"snapshot_mode\": _safe_str(mode) or None,\n"
        "            \"snapshot_schema_version\": _safe_int(sv, 0) if sv is not None else None,\n"
        "        }\n"
        "    except Exception:\n"
        "        return {\"snapshot_fp\": None, \"snapshot_mode\": None, \"snapshot_schema_version\": None}\n"
        "\n"
    )
    s = s.replace(marker, helper + marker)

# ---------- patch summarize_decision ----------
pat_sum = re.compile(
    r"def _summarize_decision\(d: Dict\[str, Any\]\) -> Dict\[str, Any\]:.*?return \{.*?\n\s*\}",
    re.S
)
m = pat_sum.search(s)
if not m:
    raise SystemExit("PATCH_FAIL: _summarize_decision not found")

block = m.group(0)
if "snapshot_fp" not in block:
    # inject snap extraction after policy_hash block (most stable anchor)
    if "policy_hash" not in block:
        raise SystemExit("PATCH_FAIL: summarize structure unexpected (no policy_hash)")

    if "snap = _extract_snapshot_linkage(d)" not in block:
        block = re.sub(
            r"(policy_hash = .*?\n.*?if not policy_hash.*?\n.*?policy_hash = .*?\n)",
            r"\1\n    snap = _extract_snapshot_linkage(d)\n",
            block,
            flags=re.S
        )

    # add fields into return dict (anchor on event_type line)
    if "\"event_type\": d.get(\"event_type\")," in block:
        block = block.replace(
            "\"event_type\": d.get(\"event_type\"),",
            "\"event_type\": d.get(\"event_type\"),\n\n"
            "        # ✅ Phase 7 linkage\n"
            "        \"snapshot_fp\": snap.get(\"snapshot_fp\"),\n"
            "        \"snapshot_mode\": snap.get(\"snapshot_mode\"),\n"
            "        \"snapshot_schema_version\": snap.get(\"snapshot_schema_version\"),"
        )
    else:
        raise SystemExit("PATCH_FAIL: summarize return anchor not found (event_type)")

    s = s[:m.start()] + block + s[m.end():]

# ---------- patch join top-level promotion ----------
join_marker = "decision_summary = _summarize_decision(decision) if decision else None"
if join_marker not in s:
    raise SystemExit("PATCH_FAIL: join_marker not found")

promo = (
    "\n"
    "    # ✅ Promote snapshot linkage to top-level for auditability\n"
    "    snap_fp = None\n"
    "    snap_mode = None\n"
    "    snap_sv = None\n"
    "    if decision_summary:\n"
    "        snap_fp = decision_summary.get(\"snapshot_fp\")\n"
    "        snap_mode = decision_summary.get(\"snapshot_mode\")\n"
    "        snap_sv = decision_summary.get(\"snapshot_schema_version\")\n"
)
if "snap_fp = None" not in s:
    s = s.replace(join_marker, join_marker + promo)

# inject into joined dict (anchor on account_label line)
s = s.replace(
    "\"account_label\": account_label,",
    "\"account_label\": account_label,\n\n"
    "        # Phase 7 linkage (top-level)\n"
    "        \"snapshot_fp\": snap_fp,\n"
    "        \"snapshot_mode\": snap_mode,\n"
    "        \"snapshot_schema_version\": snap_sv,"
)

# echo into integrity (anchor on linked_at_ms)
s = s.replace(
    "\"linked_at_ms\": _now_ms(),",
    "\"linked_at_ms\": _now_ms(),\n\n"
    "            # helpful integrity echoes\n"
    "            \"snapshot_fp\": snap_fp,\n"
    "            \"snapshot_mode\": snap_mode,\n"
    "            \"snapshot_schema_version\": snap_sv,"
)

p.write_text(s, encoding="utf-8", newline="\n")
print("PATCH_OK", str(p.resolve()))
