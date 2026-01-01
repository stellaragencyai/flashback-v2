#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Paths (prefer centralized spine_api if present)
# ---------------------------------------------------------------------------
try:
    from app.core.spine_api import AI_DECISIONS_PATH as _AI_DECISIONS_PATH  # type: ignore
    from app.core.spine_api import read_jsonl_tail as _read_jsonl_tail  # type: ignore
except Exception:  # pragma: no cover
    _AI_DECISIONS_PATH = None  # type: ignore
    _read_jsonl_tail = None  # type: ignore

DECISIONS_PATH = (
    Path(_AI_DECISIONS_PATH) if _AI_DECISIONS_PATH is not None else Path("state/ai_decisions.jsonl")
)

# Coverage modes:
# - strict (default): missing decision => BLOCK, semantic conflicts => BLOCK
# - warn: missing => BLOCK, conflicts => prefer newest but annotate reason
DECISION_COVERAGE_MODE = os.getenv("DECISION_COVERAGE_MODE", "strict").strip().lower()

# Avoid loading huge decisions files into memory
AI_DECISION_ENFORCER_TAIL_BYTES: int = int(
    os.getenv("AI_DECISION_ENFORCER_TAIL_BYTES", "2097152") or "2097152"
)  # 2MB

# Float comparison tolerance (signature rounding)
_SIG_ROUND_DP = int(os.getenv("AI_DECISION_ENFORCER_SIG_ROUND_DP", "6") or "6")

# Require decision to pre-exist this executor run (optional)
EXEC_REQUIRE_PREEXISTING_DECISION: bool = (
    os.getenv("EXEC_REQUIRE_PREEXISTING_DECISION", "false").strip().lower()
    in ("1", "true", "yes", "y", "on")
)
EXEC_PREEXISTING_MIN_AGE_MS: int = int(os.getenv("EXEC_PREEXISTING_MIN_AGE_MS", "5000") or "5000")


def _matches_trade_id(d: Dict[str, Any], trade_id: str) -> bool:
    """
    Join rules:
    - If caller provides account-prefixed trade_id (contains ":"), match ONLY trade_id/client_trade_id.
      Do NOT match source_trade_id (shared across subs).
    - If caller provides raw trade_id, allow matching by trade_id/client_trade_id/source_trade_id.
    """
    try:
        tid = str(trade_id or "")
        d_trade = str(d.get("trade_id") or "")
        d_client = str(d.get("client_trade_id") or "")
        d_source = str(d.get("source_trade_id") or "")

        if not tid:
            return False

        if ":" in tid:
            return tid == d_trade or tid == d_client

        return tid in {d_trade, d_client, d_source}
    except Exception:
        return False


def _tail_read_text(path: Path, tail_bytes: int) -> str:
    try:
        if not path.exists():
            return ""
        size = path.stat().st_size
        read_n = min(max(0, int(tail_bytes)), size)
        with path.open("rb") as f:
            if read_n < size:
                f.seek(size - read_n)
            b = f.read(read_n)
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _tail_recent_rows(path: Path, tail_bytes: int, max_lines: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    """
    Return recent JSONL rows from the end of a file (best effort).
    Prefer app.core.spine_api.read_jsonl_tail if available.
    """
    # Prefer centralized fast tail if present
    try:
        if callable(_read_jsonl_tail):  # type: ignore[arg-type]
            rows, bad = _read_jsonl_tail(path, tail_bytes=tail_bytes, max_lines=max_lines)  # type: ignore[misc]
            return rows, bad
    except Exception:
        pass

    # Fallback local tail parsing
    text = _tail_read_text(path, tail_bytes=tail_bytes)
    bad = 0
    rows: List[Dict[str, Any]] = []
    for raw in text.splitlines()[-max_lines:]:
        r = raw.strip()
        if not r:
            continue
        try:
            d = json.loads(r)
            if isinstance(d, dict):
                rows.append(d)
        except Exception:
            bad += 1
    return rows, bad


def _sig(d: Dict[str, Any]) -> Tuple[Any, ...]:
    """Signature used to detect semantic differences between decision rows."""
    try:
        allow = bool(d.get("allow"))
        reason = str(d.get("reason") or "").strip()

        def _rf(x: Any) -> Any:
            try:
                if x is None:
                    return None
                return round(float(x), _SIG_ROUND_DP)
            except Exception:
                return x

        return (
            allow,
            reason,
            _rf(d.get("size_multiplier")),
            _rf(d.get("tp_multiplier")),
            _rf(d.get("sl_multiplier")),
            _rf(d.get("risk_multiplier")),
        )
    except Exception:
        return ()


def _age_ms(d: Dict[str, Any]) -> int:
    try:
        ts = d.get("ts_ms") or d.get("ts") or d.get("timestamp_ms") or 0
        return int(time.time() * 1000) - int(ts)
    except Exception:
        return 0


def _is_executor_output(d: Dict[str, Any]) -> bool:
    """Exclude executor outputs so we don't treat executor logs as decisions."""
    try:
        et = str(d.get("event_type") or "")
        src = str(d.get("source") or "")
        return ("executor" in et.lower()) or ("executor" in src.lower())
    except Exception:
        return False


def enforce_decision(
    *,
    trade_id: str,
    snapshot_fp: str,
    account_label: str,
    symbol: str,
    timeframe: str,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Enforce that an AI decision exists and is consistent for this trade_id + snapshot_fp.
    Returns:
      - allow (bool)
      - reason (str)
      - decision (optional dict)
      - coverage (FOUND|MISSING|CONFLICT)
    """
    _ = int(now_ms if now_ms is not None else time.time() * 1000)

    rows, _bad = _tail_recent_rows(DECISIONS_PATH, AI_DECISION_ENFORCER_TAIL_BYTES, max_lines=5000)

    candidates: List[Dict[str, Any]] = []
    for d in rows:
        if not isinstance(d, dict):
            continue
        if _is_executor_output(d):
            continue
        if str(d.get("snapshot_fp") or "") != str(snapshot_fp or ""):
            continue
        if not _matches_trade_id(d, trade_id):
            continue

        # Optional narrowing if fields exist in row
        if account_label and str(d.get("account_label") or "") and str(d.get("account_label") or "") != account_label:
            continue
        if symbol and str(d.get("symbol") or "") and str(d.get("symbol") or "") != symbol:
            continue
        if timeframe and str(d.get("timeframe") or "") and str(d.get("timeframe") or "") != timeframe:
            continue

        # Preexisting gate
        if EXEC_REQUIRE_PREEXISTING_DECISION and _age_ms(d) < EXEC_PREEXISTING_MIN_AGE_MS:
            continue

        candidates.append(d)

    if not candidates:
        return {
            "allow": False,
            "reason": f"DECISION_MISSING trade_id={trade_id} snapshot_fp={snapshot_fp}",
            "coverage": "MISSING",
        }

    def _ts(x: Dict[str, Any]) -> int:
        try:
            return int(x.get("ts_ms") or x.get("ts") or x.get("timestamp_ms") or 0)
        except Exception:
            return 0

    candidates.sort(key=_ts, reverse=True)
    chosen = candidates[0]

    sig0 = _sig(chosen)
    for other in candidates[1:]:
        if _sig(other) != sig0:
            if DECISION_COVERAGE_MODE == "warn":
                return {
                    "allow": bool(chosen.get("allow")),
                    "reason": f"DECISION_CONFLICT_WARN picked_newest trade_id={trade_id} snapshot_fp={snapshot_fp}",
                    "decision": chosen,
                    "coverage": "CONFLICT",
                }
            return {
                "allow": False,
                "reason": f"DECISION_CONFLICT trade_id={trade_id} snapshot_fp={snapshot_fp}",
                "decision": chosen,
                "coverage": "CONFLICT",
            }

    return {
        "allow": bool(chosen.get("allow")),
        "reason": str(chosen.get("reason") or ""),
        "decision": chosen,
        "coverage": "FOUND",
    }
