from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_phase5_backfill_patch")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Fix outcome_record enrichment: timeframe fallback via raw/setup payload
old_enrich = '        "timeframe": raw.get("timeframe", setup.get("timeframe")),\n'
new_enrich = (
'        "timeframe": (\n'
'            raw.get("timeframe")\n'
'            or setup.get("timeframe")\n'
'            or (raw.get("payload") if isinstance(raw.get("payload"), dict) else {}).get("timeframe")\n'
'            or (setup.get("payload") if isinstance(setup.get("payload"), dict) else {}).get("timeframe")\n'
'        ),\n'
)

n1 = s.count(old_enrich)
if n1 != 1:
    print("PATCH_FAIL: expected 1 match for timeframe enrichment, found", n1)
    print("Backup:", bak)
    sys.exit(1)

s = s.replace(old_enrich, new_enrich)

# 2) Add explicit backfill mode env toggle that disables skip-old filter in ingest mode
needle_since = (
"    # In ingest mode: auto since_ts_ms from DB max(ts_ms) + 1 if not provided\n"
"    if mode.lower() != \"rebuild\":\n"
"        if since_ts_ms is None:\n"
"            latest = _get_latest_ts_ms(conn)\n"
"            since_ts_ms = (latest + 1) if latest is not None else None\n"
)
if needle_since not in s:
    print("PATCH_FAIL: could not find since_ts_ms auto-derive block")
    print("Backup:", bak)
    sys.exit(1)

inject_backfill = needle_since + (
"\n"
"    # ✅ Phase 5 backfill mode (one-time history build)\n"
"    # If AI_MEMORY_BACKFILL=true, do NOT skip historical outcomes by ts_ms.\n"
"    # Optional cap: AI_MEMORY_BACKFILL_MAX_OUTCOMES (only applied when max_outcome_lines is not provided)\n"
"    try:\n"
"        backfill = os.getenv(\"AI_MEMORY_BACKFILL\", \"false\").strip().lower() in (\"1\",\"true\",\"yes\",\"y\",\"on\")\n"
"    except Exception:\n"
"        backfill = False\n"
"\n"
"    if mode.lower() != \"rebuild\" and backfill:\n"
"        since_ts_ms = None\n"
"        try:\n"
"            cap = int(os.getenv(\"AI_MEMORY_BACKFILL_MAX_OUTCOMES\", \"0\").strip() or \"0\")\n"
"        except Exception:\n"
"            cap = 0\n"
"        if (max_outcome_lines is None) and cap > 0:\n"
"            max_outcome_lines = cap\n"
)

s = s.replace(needle_since, inject_backfill)

# 3) Fix stats counters: inserted and skipped_existing should be meaningful
# Find the exact block around INSERT OR IGNORE result handling.
old_stats_block = (
"        before = conn.total_changes\n"
"        _insert_entry(conn, entry)\n"
"        after = conn.total_changes\n"
"\n"
"        if after == before:\n"
"            skipped_existing += 1\n"
"        else:\n"
"            append_jsonl(local_paths.memory_entries_path, entry)\n"
)

if old_stats_block not in s:
    print("PATCH_FAIL: could not find insert stats block to patch")
    print("Backup:", bak)
    sys.exit(1)

new_stats_block = (
"        before = conn.total_changes\n"
"        _insert_entry(conn, entry)\n"
"        after = conn.total_changes\n"
"\n"
"        if after == before:\n"
"            skipped_existing += 1\n"
"        else:\n"
"            inserted += 1\n"
"            append_jsonl(local_paths.memory_entries_path, entry)\n"
)

s = s.replace(old_stats_block, new_stats_block)

# Sanity: ensure inserted variable exists and is used in stats dict already
if "inserted = 0" not in s:
    print("PATCH_FAIL: inserted counter variable not found (unexpected file shape)")
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s, encoding="utf-8")
print("PATCH_OK: Phase 5 builder patched (timeframe fallback + backfill mode + inserted stats).")
print("Backup:", bak)
