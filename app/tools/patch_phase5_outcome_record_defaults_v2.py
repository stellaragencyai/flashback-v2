from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_outcome_record_defaults_v2")
if not bak.exists():
    shutil.copy2(p, bak)

lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# Find function start
fn = "def _try_enrich_outcome_record"
start = None
for i, line in enumerate(lines):
    if line.lstrip().startswith(fn):
        start = i
        break

if start is None:
    print("PATCH_FAIL: could not find", fn)
    print("Backup:", bak)
    sys.exit(1)

# Find function end (next top-level def)
end = None
for j in range(start + 1, len(lines)):
    if lines[j].startswith("def ") and not lines[j].startswith("def _try_enrich_outcome_record"):
        end = j
        break
if end is None:
    end = len(lines)

block = lines[start:end]

# Avoid double-patching
if any("LEGACY_OUTCOME_RECORD_DEFAULTS" in l for l in block):
    print("PATCH_SKIP: defaults already present in _try_enrich_outcome_record")
    print("Backup:", bak)
    sys.exit(0)

# Find first payload assignment line inside function
inject_at = None
payload_indent = None

for k, line in enumerate(block):
    stripped = line.lstrip()
    if stripped.startswith("payload") and "=" in stripped:
        inject_at = k + 1
        payload_indent = line[:len(line) - len(stripped)]
        break

if inject_at is None:
    print("PATCH_FAIL: could not find a payload assignment line inside _try_enrich_outcome_record")
    print("Backup:", bak)
    sys.exit(1)

ins = payload_indent + "# LEGACY_OUTCOME_RECORD_DEFAULTS\n"
ins += payload_indent + "if isinstance(raw, dict):\n"
ins += payload_indent + "    if not raw.get('symbol'):\n"
ins += payload_indent + "        raw['symbol'] = (payload.get('symbol') if isinstance(payload, dict) else None) or 'UNKNOWN'\n"
ins += payload_indent + "    if not raw.get('timeframe'):\n"
ins += payload_indent + "        raw['timeframe'] = (payload.get('timeframe') if isinstance(payload, dict) else None) or 'unknown'\n"

block2 = block[:inject_at] + [ins] + block[inject_at:]

lines2 = lines[:start] + block2 + lines[end:]
p.write_text("".join(lines2), encoding="utf-8")

print("PATCH_OK: inserted legacy outcome_record symbol/timeframe defaults.")
print("Backup:", bak)
