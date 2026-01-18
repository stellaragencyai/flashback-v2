from pathlib import Path
p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
print("HAS sysPanel=", 'class="sysPanel"' in s)
print("HAS subPill=", 'class="subPill"' in s)
print("HAS PREMIUM OVERRIDES=", "PREMIUM UI OVERRIDES v1" in s)
