from pathlib import Path

p = Path(r"app\core\ai_profile.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Normalize broken escape sequences introduced by a bad patch.
# Replace \"\"\" with """ and \" with " (and same for single quotes).
s = s.replace(r'\"\"\"', '"""')
s = s.replace(r"\'\'\'", "'''")
s = s.replace(r"\\\"", '"')
s = s.replace(r"\\'", "'")
s = s.replace(r"\"", '"')
s = s.replace(r"\'", "'")

p.write_text(s, encoding="utf-8")
print("OK: normalized escaping in ai_profile.py")
