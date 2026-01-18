from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

def indent_len(s: str) -> int:
    return len(s) - len(s.lstrip(" \t"))

def strip_ws(s: str) -> str:
    return s.lstrip(" \t")

patched_fn = False
patched_calls = 0

# 1) Fix function body indentation for def _telemetry_emit_fleet(...)
for i, line in enumerate(lines):
    if line.lstrip().startswith("def _telemetry_emit_fleet("):
        def_i = i
        def_indent = indent_len(line)
        body_indent = def_indent + 4

        # walk forward and indent anything that should be in the body but isn't
        j = def_i + 1
        while j < len(lines):
            l = lines[j]
            if l.strip() == "":
                j += 1
                continue

            cur_indent = indent_len(l)

            # stop when we hit the next top-level (or same-level) statement
            # (same or less indent than def) that isn't a continuation of the body
            if cur_indent <= def_indent and not l.lstrip().startswith((")", "]", "}", "except", "finally")):
                break

            # if this line isn't indented enough, bump it into the function body
            if cur_indent <= def_indent:
                lines[j] = (" " * body_indent) + strip_ws(l)
                patched_fn = True

            j += 1
        break

# 2) Comment out any CALLS to _telemetry_emit_fleet(...) (not the def)
for i, line in enumerate(lines):
    s = line.lstrip()
    if "_telemetry_emit_fleet(" in s and not s.startswith(("def _telemetry_emit_fleet", "#")):
        # comment it out, preserving original indentation
        pre = line[: len(line) - len(s)]
        lines[i] = pre + "# DISABLED: " + s
        patched_calls += 1

p.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("PATCHED_FN_BODY", patched_fn)
print("PATCHED_CALLS", patched_calls)
print("FILE", str(p))
