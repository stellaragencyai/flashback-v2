from pathlib import Path

p = Path(r"app\tools\ai_outcome_scoreboard_v1.py")
text = p.read_text(encoding="utf-8", errors="replace")

# Remove the bad trailing literal "\n" if present
text = text.replace("main()\\n", "main()")

# Ensure clean newline at EOF
if not text.endswith("\n"):
    text += "\n"

p.write_text(text, encoding="utf-8")
print("OK: fixed ai_outcome_scoreboard_v1.py trailer")
