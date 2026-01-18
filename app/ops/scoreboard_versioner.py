from pathlib import Path
import json, hashlib, datetime, sys

# Compute project root robustly (no hardcoded C:\flashback)
ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"

# Canonical location of the actual scoreboard content
SB_DIR = STATE / "scoreboard"
VERSIONS = SB_DIR / "versions"

SB_DIR.mkdir(parents=True, exist_ok=True)
VERSIONS.mkdir(parents=True, exist_ok=True)

# IMPORTANT:
# - SCOREBOARD_SRC is the real scoreboard content file
# - POINTER is a separate pointer file (so we never clobber the content)
SCOREBOARD_SRC = SB_DIR / "scoreboard.v1.json"
POINTER = SB_DIR / "scoreboard.pointer.v1.json"
AUDIT = SB_DIR / "audit.log.jsonl"

# Inputs used to bind the version snapshot to outcomes
SANITIZED = STATE / "ai_events" / "outcomes.v1.jsonl"

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

def now_utc():
    # keep it simple; no need for timezone-aware refactor right now
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def main() -> int:
    if not SCOREBOARD_SRC.exists():
        print("❌ Missing scoreboard.v1.json")
        print(f"   Expected: {SCOREBOARD_SRC}")
        return 2

    if not SANITIZED.exists():
        print("❌ Missing sanitized outcomes")
        print(f"   Expected: {SANITIZED}")
        return 2

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    sb_id = f"sb_{ts}"

    SB_SNAPSHOT = VERSIONS / f"{sb_id}.json"
    META = VERSIONS / f"{sb_id}.meta.json"
    HASHFILE = VERSIONS / f"{sb_id}.hash"

    # --- Copy immutable snapshot ---
    data = json.loads(SCOREBOARD_SRC.read_text(encoding="utf-8"))
    data["generated_at"] = now_utc()
    data["schema"] = "scoreboard.v1"

    SB_SNAPSHOT.write_text(json.dumps(data, indent=2), encoding="utf-8")

    # --- Hashes ---
    sb_hash = sha256(SB_SNAPSHOT)
    outcomes_hash = sha256(SANITIZED)

    # --- Metadata ---
    meta = {
        "scoreboard_id": sb_id,
        "created_at": now_utc(),
        "inputs": {
            "outcomes_file": str(SANITIZED),
            "outcomes_hash": outcomes_hash,
            "sanitizer_version": "outcome_sanitizer@v1.0"
        },
        "code": {
            "scoreboard_script": "app/ops/scoreboard_versioner.py"
        },
        "hashes": {
            "scoreboard": sb_hash
        }
    }

    META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    HASHFILE.write_text(sb_hash, encoding="utf-8")

    # --- Pointer update (separate file; DO NOT overwrite scoreboard.v1.json) ---
    POINTER.write_text(json.dumps({
        "current": sb_id,
        "hash": sb_hash,
        "generated_at": data["generated_at"]
    }, indent=2), encoding="utf-8")

    # --- Audit log ---
    AUDIT.open("a", encoding="utf-8").write(json.dumps({
        "ts": now_utc(),
        "event": "SCOREBOARD_VERSIONED",
        "scoreboard_id": sb_id,
        "hash": sb_hash,
        "reason": "manual_orchestrated"
    }) + "\n")

    print(f"✅ Scoreboard versioned: {sb_id}")
    print(f"   Hash: {sb_hash}")
    print(f"   Snapshot: {SB_SNAPSHOT}")
    print(f"   Pointer:  {POINTER}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
