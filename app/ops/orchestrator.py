import subprocess
import time
import yaml
import json
import os
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
CFG_PATH = os.path.join(ROOT, "app", "ops", "orchestrator.yaml")
STATE_PATH = os.path.join(ROOT, "app", "ops", "orchestrator_state.json")

print("=== Flashback Orchestrator v2 ===")
print(f"Config: {CFG_PATH}")

with open(CFG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

processes = cfg.get("processes", {})
procs = {}

print(f"Loaded {len(processes)} processes")

for name, spec in processes.items():
    spec.setdefault("mode", "daemon")
    spec.setdefault("restart", True)

    cmd = spec["cmd"]
    print(f"[STARTING] {name}: {cmd}")
    p = subprocess.Popen(
        cmd,
        cwd=ROOT,
        shell=True
    )
    procs[name] = {
        "process": p,
        "cmd": cmd,
        "start_ts": time.time(),
        "restart_count": 0
    }

while True:
    state = {
        "timestamp": datetime.utcnow().isoformat(),
        "processes": {}
    }

    for name, meta in procs.items():
        p = meta["process"]
        alive = p.poll() is None

        state["processes"][name] = {
            "cmd": meta["cmd"],
            "alive": alive,
            "pid": p.pid,
            "uptime_sec": int(time.time() - meta["start_ts"]),
            "restart_count": meta["restart_count"]
        }

        if not alive and spec.get("restart") and spec.get("mode") == "daemon":
            print(f"[RESTART] {name}")
            p = subprocess.Popen(
                meta["cmd"],
                cwd=ROOT,
                shell=True
            )
            meta["process"] = p
            meta["start_ts"] = time.time()
            meta["restart_count"] += 1

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    time.sleep(5)

