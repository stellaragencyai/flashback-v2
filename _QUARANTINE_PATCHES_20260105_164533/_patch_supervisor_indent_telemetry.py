from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

def leading_ws(s: str) -> str:
    i = 0
    while i < len(s) and s[i] in (" ", "\t"):
        i += 1
    return s[:i]

def prev_nonempty_idx(idx: int) -> int:
    j = idx - 1
    while j >= 0:
        if lines[j].strip() != "":
            return j
        j -= 1
    return -1

patched = False
for idx, line in enumerate(lines):
    if "_telemetry_emit_fleet(" in line and not patched:
        j = prev_nonempty_idx(idx)
        if j == -1:
            raise SystemExit("No previous non-empty line found; refusing to patch.")

        prev = lines[j]
        prev_ws = leading_ws(prev).replace("\t", "    ")
        prev_indent = len(prev_ws)

        desired_indent = prev_indent + 4 if prev.rstrip().endswith(":") else prev_indent

        # Keep the code content but normalize to spaces
        content = line.lstrip(" \t")
        lines[idx] = (" " * desired_indent) + content
        patched = True
        print("PATCHED_LINE", idx + 1, "desired_indent", desired_indent)
        print("PREV_LINE", j + 1, repr(prev[:120]))
        print("NEW_LINE ", idx + 1, repr(lines[idx][:120]))

p.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("PATCHED", patched, "FILE", str(p))
