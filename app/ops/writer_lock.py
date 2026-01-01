from pathlib import Path
import time
import os
import sys

LOCK_PATH = Path("state/orchestrator.lock")
LOCK_TTL_SECONDS = 120

def acquire_lock():
    now = time.time()

    if LOCK_PATH.exists():
        age = now - LOCK_PATH.stat().st_mtime
        if age < LOCK_TTL_SECONDS:
            print("FATAL: Writer lock already exists — another machine is writing state")
            sys.exit(1)
        else:
            LOCK_PATH.unlink()

    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCK_PATH.write_text(str(os.getpid()))

def release_lock():
    if LOCK_PATH.exists():
        LOCK_PATH.unlink()
