from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_fix_double_inserted_increment")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Remove the SECOND inserted += 1 that happens right after append_jsonl
# We look for the exact sequence:
#     append_jsonl(...)
#     inserted += 1
removed = 0
out = []
i = 0
while i < len(s):
    line = s[i]
    if ("append_jsonl(local_paths.memory_entries_path, entry)" in line):
        out.append(line)
        # If next non-empty line is exactly inserted += 1 with same indent, remove it once
        j = i + 1
        if j < len(s) and s[j].strip() == "inserted += 1":
            removed += 1
            i = j + 1
            continue
    out.append(line)
    i += 1

if removed != 1:
    print("PATCH_FAIL: expected to remove 1 extra inserted increment, removed", removed)
    print("Backup:", bak)
    sys.exit(1)

p.write_text("\n".join(out) + "\n", encoding="utf-8")
print("PATCH_OK: Removed double inserted += 1 bug.")
print("Backup:", bak)
