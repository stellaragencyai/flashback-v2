from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
CFG = ROOT / "config"
OUTDIR = STATE / "snapshot_packs"

FILES = [
    CFG / "fleet_manifest.yaml",
    STATE / "orchestrator_state.json",
    STATE / "orchestrator_watchdog.json",
    STATE / "ops_snapshot.json",
    STATE / "fleet_snapshot.json",
    STATE / "fleet_degraded.json",
]

def _now_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S")

def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    pack_dir = OUTDIR / _now_tag()
    pack_dir.mkdir(parents=True, exist_ok=True)

    copied: List[str] = []
    missing: List[str] = []

    for fp in FILES:
        try:
            if fp.exists():
                shutil.copy2(fp, pack_dir / fp.name)
                copied.append(fp.name)
            else:
                missing.append(fp.name)
        except Exception:
            missing.append(fp.name)

    # python version
    (pack_dir / "python_version.txt").write_text(sys.version, encoding="utf-8")

    # minimal metadata
    meta = {
        "ts_ms": int(time.time() * 1000),
        "root": str(ROOT),
        "pack_dir": str(pack_dir),
        "copied": copied,
        "missing": missing,
    }
    (pack_dir / "pack_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"OK: snapshot pack created dir={pack_dir}")
    print(f"OK: copied={len(copied)} missing={len(missing)}")
    if missing:
        print("WARN missing:", ", ".join(missing))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
