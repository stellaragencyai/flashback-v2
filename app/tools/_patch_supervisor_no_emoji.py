from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
s = p.read_text(encoding="utf-8", errors="ignore")

before = s.count("✅")
s = s.replace("✅", "OK")

p.write_text(s, encoding="utf-8")
print("PATCHED_CHECKMARKS", before, "->", s.count("OK"), "FILE", str(p))
