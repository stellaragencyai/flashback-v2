from pathlib import Path

p = Path(r"app\core\ai_profile.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Fix the exact corruption shown in the file: \"\"\" -> """
s2 = s.replace(r'\"\"\"', '"""')

if s2 == s:
    raise SystemExit("STOP: No \\\"\\\"\\\" patterns found to replace. File may have changed.")

p.write_text(s2, encoding="utf-8")
print("OK: replaced \\\\\\\"\\\\\\\"\\\\\\\" with triple quotes in ai_profile.py")
