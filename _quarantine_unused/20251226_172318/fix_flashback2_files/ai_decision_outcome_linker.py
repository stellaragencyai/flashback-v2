#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision ↔ Outcome Linker (Phase 4/5) v1.4

Non-breaking wrapper update:
- Uses centralized spine_api paths when available.
- Keeps existing scan/append behavior.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, Tuple, List

import orjson

# -------------------------
# ROOT + paths (tolerant)
# -------------------------
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

try:
    from app.core.spine_api import STATE_DIR as _STATE_DIR  # type: ignore
    from app.core.spine_api import AI_EVENTS_DIR as _AI_EVENTS_DIR  # type: ignore
    from app.core.spine_api import AI_DECISIONS_PATH as _AI_DECISIONS_PATH  # type: ignore
except Exception:  # pragma: no cover
    _STATE_DIR = None  # type: ignore
    _AI_EVENTS_DIR = None  # type: ignore
    _AI_DECISIONS_PATH = None  # type: ignore

STATE_DIR: Path = Path(_STATE_DIR) if _STATE_DIR is not None else (ROOT / "state")
AI_EVENTS_DIR: Path = Path(_AI_EVENTS_DIR) if _AI_EVENTS_DIR is not None else (STATE_DIR / "ai_events")
DECISIONS_PATH: Path = Path(_AI_DECISIONS_PATH) if _AI_DECISIONS_PATH is not None else (STATE_DIR / "ai_decisions.jsonl")

OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"
OUT_PATH: Path = STATE_DIR / "ai_decision_outcomes.jsonl"
CURSOR_PATH: Path = STATE_DIR / "ai_decision_outcome_cursor.json"

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return dict(default)
        data = orjson.loads(path.read_bytes())
        return data if isinstance(data, dict) else dict(default)
    except Exception:
        return dict(default)


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2))
    except Exception:
        pass


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = orjson.dumps(obj) + b"\n"
        with open(str(path), "ab") as f:
            f.write(line)
    except Exception:
        pass


def _is_decision_row(d: Dict[str, Any]) -> bool:
    try:
        et = str(d.get("event_type") or "").strip().lower()
        if et in ("ai_decision", "pilot_decision"):
            return True
        # legacy heuristic
        return ("allow" in d) and ("snapshot_fp" in d) and (("trade_id" in d) or ("client_trade_id" in d))
    except Exception:
        return False


def _decision_key(d: Dict[str, Any]) -> Tuple[str, str]:
    trade_id = str(d.get("trade_id") or d.get("client_trade_id") or "").strip()
    snapshot_fp = str(d.get("snapshot_fp") or "").strip()
    return trade_id, snapshot_fp


def _load_decision_index(decisions_path: Path) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not decisions_path.exists():
        return idx
    try:
        for raw in decisions_path.read_bytes().splitlines():
            r = raw.strip()
            if not r:
                continue
            try:
                d = orjson.loads(r)
                if not isinstance(d, dict):
                    continue
            except Exception:
                continue
            if not _is_decision_row(d):
                continue
            k = _decision_key(d)
            if not k[0] or not k[1]:
                continue
            prev = idx.get(k)
            if prev is None:
                idx[k] = d
            else:
                try:
                    if int(d.get("ts_ms") or 0) >= int(prev.get("ts_ms") or 0):
                        idx[k] = d
                except Exception:
                    idx[k] = d
        return idx
    except Exception:
        return idx


def _iter_jsonl_from_line(path: Path, start_line: int) -> Tuple[int, List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return start_line, rows
    try:
        lines = path.read_bytes().splitlines()
        start = max(0, int(start_line))
        for raw in lines[start:]:
            r = raw.strip()
            if not r:
                continue
            try:
                d = orjson.loads(r)
                if isinstance(d, dict):
                    rows.append(d)
            except Exception:
                continue
        return len(lines), rows
    except Exception:
        return start_line, rows


def _extract_outcome_join_fields(out: Dict[str, Any]) -> Tuple[str, str]:
    trade_id = str(out.get("trade_id") or "").strip()
    snapshot_fp = str(out.get("snapshot_fp") or "").strip()

    payload = out.get("payload") if isinstance(out.get("payload"), dict) else {}
    if isinstance(payload, dict):
        if not trade_id:
            trade_id = str(payload.get("trade_id") or payload.get("client_trade_id") or "").strip()
        if not snapshot_fp:
            snapshot_fp = str(payload.get("snapshot_fp") or "").strip()

        outcome_record = payload.get("outcome_record") if isinstance(payload.get("outcome_record"), dict) else {}
        if isinstance(outcome_record, dict):
            if not trade_id:
                trade_id = str(outcome_record.get("trade_id") or outcome_record.get("client_trade_id") or "").strip()
            if not snapshot_fp:
                snapshot_fp = str(outcome_record.get("snapshot_fp") or "").strip()

    return trade_id, snapshot_fp


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--sleep", type=float, default=2.0)
    args = ap.parse_args()

    cursor = _read_json(CURSOR_PATH, {"line": 0})
    line = int(cursor.get("line") or 0)

    decision_idx = _load_decision_index(DECISIONS_PATH)

    while True:
        new_line, outcomes = _iter_jsonl_from_line(OUTCOMES_PATH, start_line=line)
        if outcomes:
            for out in outcomes:
                trade_id, snapshot_fp = _extract_outcome_join_fields(out)
                if not trade_id or not snapshot_fp:
                    continue
                d = decision_idx.get((trade_id, snapshot_fp))
                if not d:
                    continue
                record = {
                    "ts_ms": _now_ms(),
                    "trade_id": trade_id,
                    "snapshot_fp": snapshot_fp,
                    "decision_ts_ms": d.get("ts_ms"),
                    "decision_allow": d.get("allow"),
                    "decision_reason": d.get("reason"),
                    "outcome": out,
                }
                _append_jsonl(OUT_PATH, record)

        line = new_line
        _write_json(CURSOR_PATH, {"line": line, "ts_ms": _now_ms()})

        if args.once:
            return 0
        time.sleep(max(0.1, float(args.sleep)))


if __name__ == "__main__":
    raise SystemExit(main())
