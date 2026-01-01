from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

patched = 0
for i, line in enumerate(lines):
    if "_telemetry_emit_fleet(" in line and "def _telemetry_emit_fleet" not in line:
        content = line.lstrip(" \t")
        # force it to 4 spaces indentation (inside main loop / function body)
        new_line = "    " + content
        if new_line != line:
            lines[i] = new_line
            patched += 1
            print("PATCHED_CALL_LINE", i+1, "OLD", repr(line[:120]), "NEW", repr(new_line[:120]))

p.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("PATCHED_CALLS", patched, "FILE", str(p))
