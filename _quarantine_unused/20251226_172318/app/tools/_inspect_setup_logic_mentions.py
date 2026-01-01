from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

hits = [i for i,l in enumerate(lines, start=1) if "SETUP_LOGIC" in l]
print("HITS=", hits)

for h in hits[:50]:
    a = max(1, h-3)
    b = min(len(lines), h+3)
    print("\n--- context around", h, "---")
    for i in range(a, b+1):
        print(f"{i:04d}: {lines[i-1]}")
