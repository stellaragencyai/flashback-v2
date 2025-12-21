from pathlib import Path
import sys

p = Path("app/bots/executor_v2.py")
src = p.read_text(encoding="utf-8")

patch_path = Path("_emit_patch.py")
patch = patch_path.read_text(encoding="utf-8").strip() + "\n\n"

start = src.find("def emit_pilot_input_decision(")
if start == -1:
    sys.exit("❌ start marker not found: def emit_pilot_input_decision(")

end = src.find("\n\ndef emit_ai_decision(", start)
if end == -1:
    end = src.find("\n\n# ---------------------------------------------------------------------------\n# ✅ Decision emitter", start)
if end == -1:
    sys.exit("❌ end marker not found: expected next function after emit_pilot_input_decision")

new_src = src[:start] + patch + src[end:]
p.write_text(new_src, encoding="utf-8")
print("✅ patched emit_pilot_input_decision (marker-based)")
