#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback - Spine API (v1)

Purpose:
- One canonical place for:
  - Paths (state/ai_events, ai_decisions.jsonl, ai_memory snapshot)
  - Fast JSONL tail reading (Windows-friendly)
  - Safe append + atomic writes
  - Lightweight helpers used by: decision_enforcer, outcome_linker, ws_health_check, ai_events_spine

Design rules:
- NEVER crash caller (fail-soft, return empty / None).
- Pure stdlib + optional orjson.
- No business logic. Just plumbing + helpers.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover
    orjson = None  # type: ignore

# -------------------------
# paths
# -------------------------
ROOT: Path = Path(__file__).resolve().parents[2]
STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"
AI_MEMORY_DIR: Path = STATE_DIR / "ai_memory"

AI_DECISIONS_PATH: Path = Path(os.getenv("AI_DECISIONS_PATH", str(STATE_DIR / "ai_decisions.jsonl"))).resolve()
AI_SNAPSHOTS_PATH: Path = Path(os.getenv("AI_SNAPSHOTS_PATH", str(STATE_DIR / "ai_snapshots.jsonl"))).resolve()

MEMORY_SNAPSHOT_PATH: Path = Path(os.getenv("AI_MEMORY_SNAPSHOT_PATH", str(AI_MEMORY_DIR / "memory_snapshot.json"))).resolve()

# Ensure parents exist (best-effort)
for _p in (STATE_DIR, AI_EVENTS_DIR, AI_MEMORY_DIR, AI_DECISIONS_PATH.parent, AI_SNAPSHOTS_PATH.parent, MEMORY_SNAPSHOT_PATH.parent):
    try:
        _p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def now_ms() -> int:
    return int(time.time() * 1000)


def file_age_sec(path: Path) -> Optional[float]:
    try:
        st = path.stat()
        return max(0.0, time.time() - float(st.st_mtime))
    except Exception:
        return None


def _dumps(obj: Any) -> bytes:
    if orjson is not None:
        try:
            return orjson.dumps(obj)
        except Exception:
            pass
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """Best-effort: append one JSON object as one line."""
    try:
        b = _dumps(payload) + b"\n"
        with path.open("ab") as f:
            f.write(b)
    except Exception:
        return


def atomic_write_text(path: Path, text: str) -> None:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    except Exception:
        return


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    try:
        atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    except Exception:
        return


# -------------------------
# fast tail reader
# -------------------------
def read_jsonl_tail(path: Path, *, tail_bytes: int = 1_048_576, max_lines: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    """
    Reads the last N bytes of a JSONL file and returns decoded dict rows.
    Returns: (rows, bad_lines_count)
    """
    rows: List[Dict[str, Any]] = []
    bad = 0
    try:
        if not path.exists():
            return [], 0

        size = path.stat().st_size
        if size <= 0:
            return [], 0

        start = max(0, size - int(max(1024, tail_bytes)))
        with path.open("rb") as f:
            f.seek(start)
            blob = f.read()

        # If we started mid-line, drop the first partial line.
        if start > 0:
            nl = blob.find(b"\n")
            if nl >= 0:
                blob = blob[nl + 1 :]

        lines = blob.splitlines()[-max_lines:]
        for raw in lines:
            r = raw.strip()
            if not r:
                continue
            try:
                if orjson is not None:
                    d = orjson.loads(r)
                else:
                    d = json.loads(r.decode("utf-8", errors="ignore"))
                if isinstance(d, dict):
                    rows.append(d)
            except Exception:
                bad += 1
        return rows, bad
    except Exception:
        return [], bad


def safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def safe_upper(x: Any) -> str:
    return safe_str(x).upper()


def normalize_timeframe(tf: Any) -> str:
    s = safe_str(tf).lower()
    if not s:
        return ""
    if s.endswith(("m", "h", "d", "w")):
        return s
    try:
        n = int(float(s))
        return f"{n}m" if n > 0 else ""
    except Exception:
        return ""


def decision_match_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Canonical key used for matching decisions."""
    tid = safe_str(row.get("trade_id"))
    acct = safe_str(row.get("account_label"))
    sym = safe_upper(row.get("symbol"))
    tf = normalize_timeframe(row.get("timeframe"))
    return tid, acct, sym, tf


def find_latest_decision(
    *,
    trade_id: str,
    account_label: str = "",
    symbol: str = "",
    timeframe: str = "",
    tail_bytes: int = 1_048_576,
) -> Optional[Dict[str, Any]]:
    """Return latest decision-like row matching (trade_id, optional acct/sym/tf)."""
    trade_id = safe_str(trade_id)
    if not trade_id:
        return None

    acct = safe_str(account_label)
    sym = safe_upper(symbol)
    tf = normalize_timeframe(timeframe)

    rows, _bad = read_jsonl_tail(AI_DECISIONS_PATH, tail_bytes=tail_bytes, max_lines=5000)
    best: Optional[Dict[str, Any]] = None
    best_ts = -1

    for r in rows:
        if safe_str(r.get("trade_id")) != trade_id:
            continue

        # if caller gave filters, enforce them
        if acct and safe_str(r.get("account_label")) != acct:
            continue
        if sym and safe_upper(r.get("symbol")) != sym:
            continue
        if tf and normalize_timeframe(r.get("timeframe")) != tf:
            continue

        ts = 0
        try:
            ts = int(r.get("ts_ms") or 0)
        except Exception:
            ts = 0

        if ts >= best_ts:
            best_ts = ts
            best = r

    return best


def sha256_hex(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()
