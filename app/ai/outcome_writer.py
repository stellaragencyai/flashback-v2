"""
Flashback — Outcome Writer (v1)

Writes ONLY canonical v1 outcomes.

Routing (critical for per-account isolation):
- If AI_EVENTS_DIR is set (env), write to: <AI_EVENTS_DIR>/outcomes.v1.jsonl
- Else (default), write to: state/ai_events/outcomes.v1.jsonl

Optional strict safety:
- If LANE_REQUIRED=1, the writer will refuse to write unless AI_EVENTS_DIR is set.

Stable API contract (do NOT break):
- append_outcome_v1(payload: dict) -> None
- write_outcome_from_paper_close(*, payload: Optional[dict]=None, **fields) -> dict

This is intentionally boring. Boring is reliable.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from app.ai.outcome_contract import OUTCOME_SCHEMA_VERSION, validate_outcome_v1


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _resolve_root() -> Path:
    """
    Resolve repo root WITHOUT importing app.core.* (which may not exist).
    File: <ROOT>/app/ai/outcome_writer.py
    parents[0]=<ROOT>/app/ai
    parents[1]=<ROOT>/app
    parents[2]=<ROOT>
    """
    return Path(__file__).resolve().parents[2]


ROOT = _resolve_root()


def _truthy_env(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _resolve_out_path() -> Path:
    """
    Outcome path resolution:
      - Prefer AI_EVENTS_DIR env when present (per-account lane).
      - Fallback to legacy global state/ai_events.
      - If LANE_REQUIRED=1, AI_EVENTS_DIR MUST be set.
    """
    lane_required = _truthy_env("LANE_REQUIRED")
    ai_events_dir = (os.environ.get("AI_EVENTS_DIR") or "").strip()

    if lane_required and not ai_events_dir:
        raise RuntimeError("LANE_REQUIRED=1 but AI_EVENTS_DIR is not set (refusing to write outcomes)")

    if ai_events_dir:
        base = Path(ai_events_dir)
        if not base.is_absolute():
            base = (ROOT / base).resolve()
        out_path = base / "outcomes.v1.jsonl"
    else:
        out_path = ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path


def _json_sanitize(x: Any) -> Any:
    """
    Convert common non-JSON-safe types into safe values.
    Keep it conservative: if it's not obviously JSON-safe, stringify it.
    """
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, bytes):
        return x.hex()
    if isinstance(x, dict):
        return {str(k): _json_sanitize(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [_json_sanitize(v) for v in x]
    return str(x)


def _normalize_outcome_v1(outcome: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonical normalizer responsibilities:
    - sets schema_version
    - ensures event_type + ts_ms exist
    - ensures exit_side exists (derived if possible)
    - ensures client_trade_id + source_trade_id default to trade_id when absent
    - sanitizes non-JSON-safe values
    - validates schema (hard fail)
    """
    if not isinstance(outcome, dict):
        raise TypeError("_normalize_outcome_v1(outcome) expects a dict")

    o: Dict[str, Any] = dict(outcome)

    # Canonical schema version
    o["schema_version"] = OUTCOME_SCHEMA_VERSION

    # Canonical event type + timestamp
    o.setdefault("event_type", "trade_outcome")
    o.setdefault("ts_ms", _now_ms())

    # Trade id must exist
    if not (o.get("trade_id") or ""):
        raise ValueError("outcome.v1: missing required field trade_id")

    # Ensure client/source trade ids default to trade_id
    o.setdefault("client_trade_id", o["trade_id"])
    o.setdefault("source_trade_id", o["trade_id"])

    # If exit_side is missing, derive from entry_side when possible
    if "exit_side" not in o:
        es = str(o.get("entry_side") or "")
        if es.lower() == "buy":
            o["exit_side"] = "Sell"
        elif es.lower() == "sell":
            o["exit_side"] = "Buy"

    # Fees default
    o.setdefault("fees_usd", 0.0)

    # Sanitize to JSON-safe primitives
    o = _json_sanitize(o)  # type: ignore[assignment]

    # Hard schema validation (fail fast)
    validate_outcome_v1(o)

    return o


def append_outcome_v1(outcome: Dict[str, Any]) -> None:
    """
    Append a validated outcome.v1 row to the outcomes.v1 bus.
    """
    o = _normalize_outcome_v1(outcome)
    out_path = _resolve_out_path()

    # Append as JSONL (utf-8, one line)
    line = json.dumps(o, separators=(",", ":"), ensure_ascii=False)
    with out_path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(line + "\n")
        f.flush()
        if _truthy_env("OUTCOME_FSYNC"):
            try:
                os.fsync(f.fileno())
            except Exception:
                pass


def write_outcome_from_paper_close(*, payload: Optional[Dict[str, Any]] = None, **fields: Any) -> Dict[str, Any]:
    """
    Stable adapter used by PaperBroker close path.

    Accepts either:
      - payload=dict (preferred)
      - or **fields, or both (fields override payload)

    Returns the normalized dict that was written.
    """
    data: Dict[str, Any] = {}
    if payload is not None:
        if not isinstance(payload, dict):
            raise TypeError("write_outcome_from_paper_close(payload=...) must be a dict when provided")
        data.update(payload)

    # Fields override payload
    data.update(fields)

    # Normalize + validate once, write once, return the exact normalized row
    normalized = _normalize_outcome_v1(data)
    append_outcome_v1(normalized)
    return normalized
