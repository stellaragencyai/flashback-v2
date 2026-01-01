import re
from pathlib import Path

import yaml

y_path = Path(r"C:\Flashback\config\strategies.yaml")
py_path = Path(r"app\bots\signal_engine.py")

d = yaml.safe_load(y_path.read_text(encoding="utf-8", errors="ignore")) or {}

types = set()
def grab(obj):
    if isinstance(obj, dict):
        st = obj.get("setup_types") or obj.get("setups") or []
        if isinstance(st, list):
            for x in st:
                if x is not None:
                    types.add(str(x))
        for v in obj.values():
            grab(v)
    elif isinstance(obj, list):
        for v in obj:
            grab(v)

grab(d)

s = py_path.read_text(encoding="utf-8", errors="ignore")
m = re.search(r"SETUP_LOGIC\s*=\s*\{(.*?)\n\}", s, re.S)

keys = set()
if m:
    block = m.group(1)
    keys = set(re.findall(r'"([^"]+)"\s*:', block))

print("found_SETUP_LOGIC=", bool(m))
print("yaml_types=", sorted(types))
print("logic_keys=", sorted(keys))
print("intersection=", sorted(types & keys))
print("yaml_only=", sorted(types - keys))
print("logic_only=", sorted(keys - types))
