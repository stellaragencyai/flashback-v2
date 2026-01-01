from pathlib import Path
import json, hashlib, datetime, sys

ROOT = Path(r"C:\flashback")
STATE = ROOT / "state"
SB_DIR = STATE / "scoreboard"
VERSIONS = SB_DIR / "versions"

SB_DIR.mkdir(parents=True, exist_ok=True)
VERSIONS.mkdir(parents=True, exist_ok=True)

POINTER = SB_DIR / "scoreboard.v1.json"
AUDIT = SB_DIR / "audit.log.jsonl"

SANITIZED = STATE / "ai_events" / "outcomes.v1.jsonl"
SCOREBOARD_SRC = SB_DIR / "scoreboard.v1.json"

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

def now_utc():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

if not SCOREBOARD_SRC.exists():
    sys.exit("❌ Missing scoreboard.v1.json")

if not SANITIZED.exists():
    sys.exit("❌ Missing sanitized outcomes")

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
        "scoreboard_script": "app/ops/scoreboard.py"
    },
    "hashes": {
        "scoreboard": sb_hash
    }
}

META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
HASHFILE.write_text(sb_hash, encoding="utf-8")

# --- Pointer update (only mutable file) ---
POINTER.write_text(json.dumps({
    "current": sb_id,
    "hash": sb_hash,
    "generated_at": data["generated_at"]
}, indent=2), encoding="utf-8")

# --- Audit log ---
AUDIT.open("a", encoding="utf-8").write(json.dumps({
    "ts": now_utc(),
    "event": "SCOREBOARD_GENERATED",
    "scoreboard_id": sb_id,
    "hash": sb_hash,
    "reason": "manual_orchestrated"
}) + "\n")

print(f"✅ Scoreboard versioned: {sb_id}")
print(f"   Hash: {sb_hash}")
