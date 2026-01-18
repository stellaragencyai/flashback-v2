#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback - Spine API (v2.1 lane-safe inbox defaults)

Purpose:
- One canonical place for:
  - Paths (state/ai_events, ai_decisions.jsonl, ai_memory snapshot)
  - AI event inbox (single-writer architecture)
  - Fast JSONL tail reading (Windows-friendly)
  - Safe append + atomic writes + best-effort file locks
  - Lightweight helpers used by: decision_enforcer, outcome_linker, ws_health_check, ai_events_spine,
    executor_v2, trade_outcome_recorder, etc.

Design rules:
- NEVER crash caller (fail-soft, return empty / None).
- Pure stdlib + optional orjson.
- No business logic. Just plumbing + helpers.

Key architectural rule (Phase 8+ hygiene):
- Producers emit into an inbox JSONL and the spine drains it.

Lane hygiene rule:
- MAIN defaults to:
    state/ai_events_inbox.jsonl
    state/ai_events_inbox.cursor
    state/ai_events_inbox.bad.jsonl
- SUBACCOUNT labels default to:
    state/ai_events_inbox_<account_slug>.jsonl
    state/ai_events_inbox_<account_slug>.cursor
    state/ai_events_inbox_<account_slug>.bad.jsonl

Explicit env overrides are always honored:
- AI_EVENTS_INBOX_PATH
- AI_EVENTS_INBOX_CURSOR_PATH
- AI_EVENTS_INBOX_BADLINES_PATH
(If override is relative, it is resolved under STATE_DIR.)
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import re

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

# Account label (used only for lane-safe DEFAULTS; overrides still win)
def _account_label() -> str:
    try:
        return (os.getenv("ACCOUNT_LABEL") or "").strip() or "main"
    except Exception:
        return "main"


ACCOUNT_LABEL: str = _account_label()
ACCOUNT_SLUG: str = (ACCOUNT_LABEL or "main").strip().lower() or "main"
IS_MAIN: bool = ACCOUNT_SLUG in ("main", "", "manual")

AI_DECISIONS_PATH: Path = Path(os.getenv("AI_DECISIONS_PATH", str(STATE_DIR / "ai_decisions.jsonl"))).resolve()
AI_SNAPSHOTS_PATH: Path = Path(os.getenv("AI_SNAPSHOTS_PATH", str(STATE_DIR / "ai_snapshots.jsonl"))).resolve()
MEMORY_SNAPSHOT_PATH: Path = Path(
    os.getenv("AI_MEMORY_SNAPSHOT_PATH", str(AI_MEMORY_DIR / "memory_snapshot.json"))
).resolve()


def _resolve_under_state(env_name: str, default_path: Path) -> Path:
    """
    If env var is set:
      - absolute -> use it
      - relative -> resolve under STATE_DIR
    Else use default_path.
    """
    try:
        raw = (os.getenv(env_name) or "").strip()
    except Exception:
        raw = ""

    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = STATE_DIR / p
        return p.resolve()

    return default_path.resolve()


# Lane-safe DEFAULTS (only used when env override is absent)
_default_inbox = (STATE_DIR / "ai_events_inbox.jsonl") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_SLUG}.jsonl")
_default_cursor = (STATE_DIR / "ai_events_inbox.cursor") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_SLUG}.cursor")
_default_bad = (STATE_DIR / "ai_events_inbox.bad.jsonl") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_SLUG}.bad.jsonl")

# Single-writer inbox for AI events (producers append here, spine drains)
AI_EVENTS_INBOX_PATH: Path = _resolve_under_state("AI_EVENTS_INBOX_PATH", _default_inbox)
AI_EVENTS_INBOX_CURSOR_PATH: Path = _resolve_under_state("AI_EVENTS_INBOX_CURSOR_PATH", _default_cursor)
AI_EVENTS_INBOX_BADLINES_PATH: Path = _resolve_under_state("AI_EVENTS_INBOX_BADLINES_PATH", _default_bad)

# Ensure parents exist (best-effort)
for _p in (
    STATE_DIR,
    AI_EVENTS_DIR,
    AI_MEMORY_DIR,
    AI_DECISIONS_PATH.parent,
    AI_SNAPSHOTS_PATH.parent,
    MEMORY_SNAPSHOT_PATH.parent,
    AI_EVENTS_INBOX_PATH.parent,
    AI_EVENTS_INBOX_CURSOR_PATH.parent,
    AI_EVENTS_INBOX_BADLINES_PATH.parent,
):
    try:
        _p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


# -------------------------
# time helpers
# -------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def file_age_sec(path: Path) -> Optional[float]:
    try:
        st = path.stat()
        return max(0.0, time.time() - float(st.st_mtime))
    except Exception:
        return None


# -------------------------
# json helpers
# -------------------------
def _dumps(obj: Any) -> bytes:
    if orjson is not None:
        try:
            return orjson.dumps(obj)
        except Exception:
            pass
    return json.dumps(
        obj,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")


def _loads(b: bytes) -> Any:
    if orjson is not None:
        try:
            return orjson.loads(b)
        except Exception:
            return None
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return None


# -------------------------
# file locking (best effort)
# -------------------------
def _lock_file(f) -> None:
    """
    Best-effort exclusive lock on a file handle.
    - Windows: msvcrt.locking
    - POSIX: fcntl.flock
    Fail-soft: if locks aren't available, do nothing.
    """
    try:
        if os.name == "nt":
            import msvcrt  # type: ignore

            try:
                # Lock 1 byte (advisory). Keep it simple and fast.
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            except Exception:
                return
        else:
            import fcntl  # type: ignore

            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                return
    except Exception:
        return


def _unlock_file(f) -> None:
    try:
        if os.name == "nt":
            import msvcrt  # type: ignore

            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                return
        else:
            import fcntl  # type: ignore

            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                return
    except Exception:
        return


# -------------------------
# append helpers
# -------------------------
def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """Best-effort: append one JSON object as one line. No locking."""
    try:
        b = _dumps(payload) + b"\n"
        with path.open("ab") as f:
            f.write(b)
    except Exception:
        return


def append_jsonl_locked(path: Path, payload: Dict[str, Any]) -> None:
    """
    Best-effort: append one JSON object as one line with a best-effort lock.
    Prevents some multi-writer interleaving on Windows.
    """
    try:
        b = _dumps(payload) + b"\n"
        with path.open("ab") as f:
            _lock_file(f)
            try:
                f.write(b)
            finally:
                _unlock_file(f)
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
# inbox emitter (producers use this)
# -------------------------
# SPINE_API_ROUTE_BY_EVENT_LABEL_PATCH_v1
# Route inbox writes by event["account_label"] when present.
# Prevents cross-account contamination when one process emits multiple labels.
_LABEL_SAFE_RE = re.compile(r"^[a-z0-9_]+$")

def _normalize_label(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("", "main", "global"):
        return ""
    return v

def _inbox_path_for_label(label: str) -> Path:
    lab = _normalize_label(label)
    if not lab:
        return (STATE_DIR / "ai_events_inbox.jsonl")
    return (STATE_DIR / f"ai_events_inbox_{lab}.jsonl")

def emit_ai_event(event: Dict[str, Any]) -> None:
    """
    Producers call this instead of touching canonical ai_events files.
    This appends to the AI inbox (single-writer pattern).

    Lane-safe default behavior:
    - MAIN: writes to state/ai_events_inbox.jsonl
    - SUB:  writes to state/ai_events_inbox_<account_slug>.jsonl

    Env overrides still win:
    - AI_EVENTS_INBOX_PATH
    """
    try:
        if not isinstance(event, dict):
            return
        if "event_type" not in event:
            return
        if "ts" not in event:
            event["ts"] = now_ms()
        # Route by event label when present (lane-safe)
        _evt_label = _normalize_label(str(event.get("account_label") or event.get("label") or ""))
        if _evt_label and _LABEL_SAFE_RE.match(_evt_label):
            _target = _inbox_path_for_label(_evt_label)
        else:
            _target = AI_EVENTS_INBOX_PATH
        append_jsonl_locked(_target, event)
    except Exception:
        return


# -------------------------
# cursor helpers
# -------------------------
def read_cursor(path: Path) -> int:
    """Return cursor byte offset (0 if missing/invalid)."""
    try:
        if not path.exists():
            return 0
        s = path.read_text(encoding="utf-8", errors="ignore").strip()
        n = int(s or "0")
        return max(0, n)
    except Exception:
        return 0


def write_cursor(path: Path, pos: int) -> None:
    """Atomic cursor write."""
    try:
        atomic_write_text(path, str(int(max(0, pos))))
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
                d = _loads(r)
                if isinstance(d, dict):
                    rows.append(d)
                else:
                    bad += 1
            except Exception:
                bad += 1
        return rows, bad
    except Exception:
        return [], bad


# -------------------------
# streaming reader (used by spine to drain inbox)
# -------------------------
def read_jsonl_from_offset(
    path: Path,
    *,
    offset: int,
    max_bytes: int = 8_388_608,  # 8MB per drain batch by default
    max_lines: int = 10_000,
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """
    Read JSONL starting at a byte offset.
    Returns: (rows, next_offset, bad_lines, bytes_read)

    Notes:
    - Fail-soft: bad lines are skipped.
    - If offset is beyond EOF, returns empty and clamps.
    - If file was truncated (EOF < offset), caller should reset offset=0.
    """
    rows: List[Dict[str, Any]] = []
    bad = 0
    bytes_read = 0
    try:
        if not path.exists():
            return [], 0, 0, 0

        size = path.stat().st_size
        if size <= 0:
            return [], 0, 0, 0

        off = int(max(0, offset))
        if off > size:
            off = size

        with path.open("rb") as f:
            f.seek(off)
            blob = f.read(int(max(1024, max_bytes)))
            bytes_read = len(blob)

        if not blob:
            return [], off, 0, 0

        # If we started mid-line (offset not 0), drop partial first line.
        if off > 0:
            nl = blob.find(b"\n")
            if nl >= 0:
                blob = blob[nl + 1 :]
                off = off + nl + 1

        # Split lines and parse
        lines = blob.splitlines()
        if max_lines and len(lines) > max_lines:
            lines = lines[:max_lines]

        consumed = 0
        for raw in lines:
            consumed += len(raw) + 1  # + newline
            r = raw.strip()
            if not r:
                continue
            try:
                d = _loads(r)
                if isinstance(d, dict):
                    rows.append(d)
                else:
                    bad += 1
                    _quarantine_bad_line(r)
            except Exception:
                bad += 1
                _quarantine_bad_line(r)

        next_offset = off + consumed
        return rows, next_offset, bad, bytes_read
    except Exception:
        return [], int(max(0, offset)), bad, bytes_read


def _quarantine_bad_line(raw: bytes) -> None:
    """Best-effort: write bad JSONL line to a quarantine file for later inspection."""
    try:
        if not raw:
            return
        payload = {
            "ts": now_ms(),
            "kind": "bad_jsonl_line",
            "path": str(AI_EVENTS_INBOX_PATH),
            "raw": raw[:2000].decode("utf-8", errors="replace"),
        }
        append_jsonl_locked(AI_EVENTS_INBOX_BADLINES_PATH, payload)
    except Exception:
        return


# -------------------------
# safe string helpers
# -------------------------
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


# -------------------------
# decisions helpers
# -------------------------
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
            ts = int(r.get("ts_ms") or r.get("ts") or 0)
        except Exception:
            ts = 0

        if ts >= best_ts:
            best_ts = ts
            best = r

    return best


# -------------------------
# hashing helper
# -------------------------
def sha256_hex(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()
