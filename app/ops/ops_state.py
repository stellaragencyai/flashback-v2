#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Ops State (single source of truth)

Writes/reads a single JSON snapshot:
  state/ops_snapshot.json

All workers/tools should write here so you have ONE truth surface.

HARDENING:
- Uses atomic replace (tmp -> os.replace) for writes.
- Uses a lock file to serialize read-modify-write across processes.
- read_ops_snapshot self-heals if file is corrupted (e.g. appended JSON / partial writes).
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from app.core.config import settings
    ROOT: Path = Path(settings.ROOT)  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

OPS_PATH: Path = ROOT / "state" / "ops_snapshot.json"
OPS_LOCK_PATH: Path = ROOT / "state" / "ops_snapshot.lock"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _acquire_lock(timeout_sec: float = 2.0, poll_sec: float = 0.02) -> Optional[int]:
    """
    Best-effort cross-process lock using an exclusive lock file.
    Returns a file descriptor if acquired, else None.
    """
    deadline = time.time() + max(0.1, float(timeout_sec))
    OPS_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)

    while time.time() < deadline:
        try:
            fd = os.open(str(OPS_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            try:
                os.write(fd, f"pid={os.getpid()} ts_ms={_now_ms()}\n".encode("utf-8", errors="ignore"))
            except Exception:
                pass
            return fd
        except FileExistsError:
            time.sleep(poll_sec)
        except Exception:
            # If locking fails for weird reasons, don't brick the system.
            return None

    return None


def _release_lock(fd: Optional[int]) -> None:
    try:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if OPS_LOCK_PATH.exists():
            OPS_LOCK_PATH.unlink()
    except Exception:
        pass


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Atomic write via tmp + os.replace.
    Avoids partially-written JSON.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{time.time_ns()}")
    # Write tmp
    tmp.write_text(data, encoding="utf-8")

    # Replace with retries
    last_err: Optional[Exception] = None
    for _ in range(10):
        try:
            os.replace(str(tmp), str(path))
            return
        except Exception as e:
            last_err = e
            time.sleep(0.03)

    # If replace keeps failing, last resort direct overwrite (still valid JSON, just non-atomic)
    try:
        path.write_text(data, encoding="utf-8")
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        if last_err:
            # Nothing to do here; caller remains best-effort.
            pass


def _default_snapshot() -> Dict[str, Any]:
    return {"version": 1, "updated_ms": 0, "components": {}}


def _normalize_snapshot(d: Any) -> Dict[str, Any]:
    if not isinstance(d, dict):
        d = {}
    d.setdefault("version", 1)
    d.setdefault("updated_ms", 0)
    d.setdefault("components", {})
    if not isinstance(d["components"], dict):
        d["components"] = {}
    return d


def _try_salvage_any_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    If the file contains extra garbage/appended content, try to locate ANY valid JSON object.
    Returns the last successfully decoded object (if dict), else None.
    """
    dec = json.JSONDecoder()
    i = 0
    n = len(text)
    last_obj: Optional[Dict[str, Any]] = None

    while i < n:
        j = text.find("{", i)
        if j < 0:
            break
        try:
            obj, end = dec.raw_decode(text, j)
            if isinstance(obj, dict):
                last_obj = obj
            i = end
        except Exception:
            i = j + 1

    return last_obj


def read_ops_snapshot() -> Dict[str, Any]:
    """
    Read snapshot safely.
    If corrupted, attempts salvage. If salvage succeeds, rewrites file cleanly.
    """
    try:
        if not OPS_PATH.exists():
            return _default_snapshot()

        raw = OPS_PATH.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw:
            return _default_snapshot()

        try:
            data = json.loads(raw)
            return _normalize_snapshot(data)
        except json.JSONDecodeError:
            salvaged = _try_salvage_any_json_object(raw)
            if salvaged is not None:
                fixed = _normalize_snapshot(salvaged)
                fixed["updated_ms"] = _now_ms()
                _atomic_write_json(OPS_PATH, fixed)
                return fixed
            return _default_snapshot()

    except Exception:
        return _default_snapshot()


def write_ops_snapshot(snapshot: Dict[str, Any]) -> None:
    if not isinstance(snapshot, dict):
        snapshot = _default_snapshot()
    snapshot = _normalize_snapshot(snapshot)
    snapshot["updated_ms"] = _now_ms()
    _atomic_write_json(OPS_PATH, snapshot)


def write_component_status(
    component: str,
    account_label: str,
    ok: bool,
    details: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Standard status writer used by all workers and health checks.
    Uses a lock to serialize read-modify-write across processes.
    """
    fd = _acquire_lock()
    try:
        snapshot = read_ops_snapshot()
        comps = snapshot.get("components")
        if not isinstance(comps, dict):
            comps = {}
            snapshot["components"] = comps

        key = f"{component}:{account_label}".strip(":")
        comps[key] = {
            "component": component,
            "account_label": account_label,
            "ok": bool(ok),
            "ts_ms": int(ts_ms or _now_ms()),
            "details": details or {},
        }

        write_ops_snapshot(snapshot)
    finally:
        _release_lock(fd)
