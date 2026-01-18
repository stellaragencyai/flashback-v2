from pathlib import Path
import re

p = Path(r"app\bots\ai_action_router.py")
s = p.read_text(encoding="utf-8", errors="ignore")

marker = "# --- DEBUG_PRINTS_V1 ---"
if marker in s:
    print("OK: DEBUG_PRINTS_V1 already present")
    raise SystemExit(0)

# Insert debug prints right after we read new objects
pat = r"offset,\s*objs\s*=\s*_iter_new_objects\(ACTION_LOG_PATH,\s*offset\)\s*"
m = re.search(pat, s)
if not m:
    raise SystemExit("STOP: could not find _iter_new_objects(ACTION_LOG_PATH, offset) line")

insert = (
    "offset, objs = _iter_new_objects(ACTION_LOG_PATH, offset)\n"
    f"{marker}\n"
    "print(f\"[DBG] poll: offset={offset} objs={len(objs)} path={ACTION_LOG_PATH}\")\n"
)

s2 = s[:m.start()] + insert + s[m.end():]

# Also print each envelope label + fingerprint if present, right after env normalization
pat2 = r"env\s*=\s*_normalize_to_envelope\(obj\)\s*"
m2 = re.search(pat2, s2)
if not m2:
    p.write_text(s2, encoding="utf-8")
    print("OK: added poll debug prints only (env hook not found)")
    raise SystemExit(0)

insert2 = (
    "env = _normalize_to_envelope(obj)\n"
    "try:\n"
    "    act = env.get('action') if isinstance(env, dict) else None\n"
    "    fp = act.get('setup_fingerprint') if isinstance(act, dict) else None\n"
    "    print(f\"[DBG] obj: label={env.get('label') if isinstance(env, dict) else None} fp={fp}\")\n"
    "except Exception:\n"
    "    pass\n"
)

s3 = s2[:m2.start()] + insert2 + s2[m2.end():]
p.write_text(s3, encoding="utf-8")
print("OK: inserted DEBUG_PRINTS_V1 into ai_action_router.py")
