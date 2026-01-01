from pathlib import Path
import time
import sys

# --- FLASHBACK SUPERVISOR (WINDOWS SNAPSHOT MODE) ---

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from app.ops.ops_state import read_ops_snapshot
except Exception as e:
    print("❌ Failed to import ops_state:", e)
    sys.exit(1)

REFRESH = 5

def main():
    print("FLASHBACK — Supervisor Snapshot Mode (Windows-safe)")
    print("=" * 80)

    while True:
        try:
            snap = read_ops_snapshot()
            print(snap)
            print("-" * 80)
            time.sleep(REFRESH)
        except KeyboardInterrupt:
            print("Exiting supervisor")
            return
        except Exception as e:
            print("Supervisor error:", e)
            time.sleep(REFRESH)

if __name__ == "__main__":
    main()
