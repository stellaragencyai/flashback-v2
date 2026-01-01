import os, sys
from pathlib import Path

# Force repo root onto sys.path (C:\Flashback)
ROOT = Path(__file__).resolve()
# ...\app\tools\file.py -> ...\app\tools -> ...\app -> ...\ (repo root)
ROOT = ROOT.parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

print("sys.executable =", sys.executable)
print("cwd =", os.getcwd())
print("repo_root =", ROOT)
print("sys.path[0] =", sys.path[0])

from app.bots import signal_engine as se  # now should work

print("signal_engine_file =", getattr(se, "__file__", None))

# Try to obtain universe from module (various possibilities)
universe = None
for name in ("universe", "UNIVERSE", "_universe"):
    if hasattr(se, name):
        universe = getattr(se, name)
        break

# If signal_engine builds universe via a function, try common names
if universe is None:
    for fn in ("build_universe", "build_universe_from_strategies", "get_universe"):
        if hasattr(se, fn):
            try:
                universe = getattr(se, fn)()
                break
            except Exception as e:
                print(f"[WARN] calling {fn} failed: {type(e).__name__}: {e}")

print("universe_type =", type(universe).__name__)
if not universe:
    raise SystemExit("Universe not accessible from module. Next step: inspect loader function directly.")

first_key = next(iter(universe.keys()))
strat_list = universe[first_key]
first_strat = strat_list[0]
raw_obj = first_strat.get("raw")

print("first_key =", first_key)
print("strat_list_len =", len(strat_list))
print("first_strat_keys =", sorted(list(first_strat.keys())))
print("raw_type =", type(raw_obj).__name__)

# Field presence checks
for k in ("setup_types", "setups", "setup_type", "symbols", "timeframes", "name"):
    print(f"hasattr({k}) =", hasattr(raw_obj, k))

# Dump keys depending on object type
d = None
if hasattr(raw_obj, "model_dump"):
    d = raw_obj.model_dump()
    print("model_dump_keys_sample =", sorted(list(d.keys()))[:120])
elif hasattr(raw_obj, "dict"):
    d = raw_obj.dict()
    print("dict_keys_sample =", sorted(list(d.keys()))[:120])
else:
    d = getattr(raw_obj, "__dict__", {}) or {}
    print("__dict___keys_sample =", sorted(list(d.keys()))[:120])

print("setup_types_value =", (d.get("setup_types") if isinstance(d, dict) else None))
print("setups_value =", (d.get("setups") if isinstance(d, dict) else None))
print("repr =", raw_obj)
