import os, sys, shutil
from datetime import datetime

PATH = r"C:\Flashback\config\exit_profiles.yaml"

def die(msg: str, code: int = 1):
    print("ERROR:", msg)
    sys.exit(code)

try:
    import yaml  # PyYAML
except Exception as e:
    die(f"PyYAML not available: {e}")

if not os.path.exists(PATH):
    die(f"Missing file: {PATH}")

ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
backup = PATH + f".bak_auto_{ts}"
shutil.copy2(PATH, backup)
print("OK: backup ->", backup)

with open(PATH, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

if not isinstance(data, dict):
    die("exit_profiles.yaml did not parse to a dict at top level.")

# Heuristic: profiles may live under a key like "profiles"
profiles_container = data
if "profiles" in data and isinstance(data["profiles"], dict):
    profiles_container = data["profiles"]

if not isinstance(profiles_container, dict):
    die("Could not find profiles dict (top-level or under 'profiles').")

PROFILE_NAME = "standard_5"
if PROFILE_NAME not in profiles_container:
    # If standard_5 doesn't exist, try to derive it from standard_10
    if "standard_10" in profiles_container and isinstance(profiles_container["standard_10"], dict):
        profiles_container[PROFILE_NAME] = dict(profiles_container["standard_10"])
        print("WARN: standard_5 not found; cloned from standard_10")
    else:
        die("Neither 'standard_5' nor 'standard_10' profile exists to patch/clone.")

profile = profiles_container[PROFILE_NAME]
if not isinstance(profile, dict):
    die(f"Profile '{PROFILE_NAME}' is not a dict.")

LIST_KEYS = ["targets", "tps", "take_profits", "takeProfitTargets", "tp_targets"]
NUM_KEYS  = ["tp_count", "num_tps", "take_profit_count", "takeProfitCount", "tpCount"]

changed = False

# Truncate list-based TP targets
for k in LIST_KEYS:
    v = profile.get(k)
    if isinstance(v, list):
        if len(v) != 5:
            profile[k] = v[:5]
            print(f"OK: {PROFILE_NAME}.{k}: {len(v)} -> {len(profile[k])}")
            changed = True

# Force numeric tp count keys to 5 if present
for k in NUM_KEYS:
    v = profile.get(k)
    if isinstance(v, int) and v != 5:
        profile[k] = 5
        print(f"OK: {PROFILE_NAME}.{k}: {v} -> 5")
        changed = True

# If no list keys existed, we can't invent targets safely
found_any_list = any(isinstance(profile.get(k), list) for k in LIST_KEYS)
if not found_any_list:
    print("WARN: No TP list key found in profile (targets/tps/take_profits/etc).")
    print("      I did NOT invent TP targets. You likely define targets elsewhere or with a different schema.")
    print("      Paste the profile block and I’ll patch it precisely.")
else:
    profiles_container[PROFILE_NAME] = profile
    if profiles_container is not data:
        data["profiles"] = profiles_container

if changed:
    with open(PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    print("OK: wrote updated exit_profiles.yaml")
else:
    print("OK: no changes needed (already 5 TPs or no matching keys found).")
