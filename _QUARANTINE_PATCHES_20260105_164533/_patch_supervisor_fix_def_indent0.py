from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

patched = False
for i, line in enumerate(lines):
    if line.lstrip().startswith("def _telemetry_emit_fleet("):
        # force top-level def (indent = 0)
        new_line = line.lstrip()
        if new_line != line:
            lines[i] = new_line
        patched = True
        print("FOUND_DEF_LINE", i+1)
        print("OLD:", repr(line))
        print("NEW:", repr(lines[i]))
        break

p.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("PATCHED", patched, "FILE", str(p))
