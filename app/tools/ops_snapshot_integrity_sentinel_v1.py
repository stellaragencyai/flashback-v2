#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ops_snapshot_integrity_sentinel_v1.py

Validates ops_snapshot.json stability for N seconds:
- JSON always parses
- key component exists
- freshness stays under threshold (with warmup + consecutive-stale tolerance)
- detects PermissionError / lock issues

Exit code:
  0 = PASS
  2 = FAIL (parse/lock/stale/missing)
"""

from __future__ import annotations

import json
import os
import sys
import time
import subprocess
from pathlib import Path
from typing import Dict, Any, Tuple


def _now_ms() -> float:
    return time.time() * 1000.0


def _proc_dump() -> str:
    """
    Best-effort python process listing with command lines.
    Uses PowerShell CIM query. If it fails, returns the exception text.
    """
    try:
        cmd = [
            "powershell",
            "-NoProfile",
            "-Command",
            "Get-CimInstance Win32_Process | "
            "Where-Object {$_.Name -match 'python'} | "
            "Select-Object ProcessId,CommandLine | Format-List"
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        out = (out or "").strip()
        return out if out else "<no python processes found>"
    except Exception as e:
        return f"<proc_dump_failed: {e!r}>"


def _read_json(path: Path) -> Tuple[bool, str, Dict[str, Any]]:
    try:
        s = path.read_text(encoding="utf-8")
        d = json.loads(s)
        return True, "ok", d
    except PermissionError as e:
        return False, f"PermissionError: {e}", {}
    except json.JSONDecodeError as e:
        return False, f"JSONDecodeError: {e}", {}
    except FileNotFoundError as e:
        return False, f"FileNotFoundError: {e}", {}
    except Exception as e:
        return False, f"OtherError: {e}", {}


def main() -> int:
    root = Path.cwd()
    snap = root / "state" / "ops_snapshot.json"

    duration_s = float(os.getenv("SENTINEL_DURATION_S", "600"))
    interval_s = float(os.getenv("SENTINEL_INTERVAL_S", "1.0"))

    # Freshness threshold in seconds for key component heartbeat
    fresh_max_s = float(os.getenv("SENTINEL_FRESH_MAX_S", "5.0"))

    # Warmup grace period where we do NOT fail freshness (helps startup jitter)
    warmup_s = float(os.getenv("SENTINEL_WARMUP_S", "20.0"))

    # Require consecutive stale hits before failing (filters single jitter spikes)
    stale_consecutive_fail = int(os.getenv("SENTINEL_STALE_CONSEC_FAIL", "3"))

    key = os.getenv("SENTINEL_KEY", "supervisor_ai_stack:flashback07")

    t0 = time.time()
    ok = 0
    perm = 0
    decode = 0
    other = 0
    stale = 0
    missing = 0

    stale_streak = 0
    last_age = None

    while (time.time() - t0) < duration_s:
        elapsed = time.time() - t0

        good, why, d = _read_json(snap)
        if not good:
            if why.startswith("PermissionError"):
                perm += 1
            elif why.startswith("JSONDecodeError"):
                decode += 1
            else:
                other += 1

            print("FAIL:", why)
            print("PROC_DUMP:\n" + _proc_dump())
            print(f"STATS ok={ok} perm={perm} decode={decode} other={other} stale={stale} missing={missing}")
            return 2

        comps = d.get("components") or {}
        if key not in comps:
            missing += 1
            print(f"FAIL: missing key={key}")
            print("PROC_DUMP:\n" + _proc_dump())
            print(f"STATS ok={ok} perm={perm} decode={decode} other={other} stale={stale} missing={missing}")
            return 2

        ts_ms = float((comps.get(key) or {}).get("ts_ms", 0.0))
        age_s = (_now_ms() - ts_ms) / 1000.0 if ts_ms > 0 else 9999.0
        last_age = age_s

        # Freshness evaluation with warmup + consecutive stale streak
        if age_s > fresh_max_s:
            stale += 1

            if elapsed < warmup_s:
                # Ignore staleness during warmup
                stale_streak = 0
            else:
                stale_streak += 1
                if stale_streak >= stale_consecutive_fail:
                    print(f"FAIL: stale key={key} age_sec={age_s:.3f} > {fresh_max_s:.3f} (stale_streak={stale_streak})")
                    print("PROC_DUMP:\n" + _proc_dump())
                    print(f"STATS ok={ok} perm={perm} decode={decode} other={other} stale={stale} missing={missing}")
                    return 2
        else:
            stale_streak = 0

        ok += 1
        time.sleep(interval_s)

    print("PASS")
    if last_age is not None:
        print(f"LAST age_sec={last_age:.3f} threshold={fresh_max_s:.3f} warmup_s={warmup_s:.1f} stale_consec_fail={stale_consecutive_fail}")
    print(f"STATS ok={ok} perm={perm} decode={decode} other={other} stale={stale} missing={missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
