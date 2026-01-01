from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

import orjson


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


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
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2))
    except Exception:
        pass


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = orjson.dumps(obj) + b"\n"
        import os as _os
        fd = _os.open(str(path), _os.O_APPEND | _os.O_CREAT | _os.O_WRONLY, 0o666)
        try:
            _os.write(fd, line)
        finally:
            _os.close(fd)
    except Exception:
        pass


# Tolerant ROOT + state paths
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_DECISIONS_PATH = STATE_DIR / "ai_decisions.jsonl"
STUB_INDEX_PATH = STATE_DIR / "ai_decision_stub_index.json"


def ensure_default_ai_decision(
    *,
    trade_id: str,
    account_label: str,
    symbol: str,
    mode: Optional[str] = None,
    snapshot_fp: Optional[str] = None,
    snapshot_mode: Optional[str] = None,
    snapshot_schema_version: Optional[int] = None,
    size_multiplier: Optional[float] = 0.25,
    allow: Optional[bool] = True,
    reason: str = "no_prior_decision",
) -> bool:
    """
    Emits a minimal ai_decision row for a trade_id if we haven't emitted one before.
    This is an integrity backfill so outcomes always have a decision partner.

    Returns True if emitted, False if skipped (already emitted / invalid).
    """
    tid = _safe_str(trade_id)
    if not tid:
        return False

    acct = _safe_str(account_label)
    sym = _safe_str(symbol).upper()
    md = _safe_str(mode)

    idx = _read_json(STUB_INDEX_PATH, {"version": 1, "updated_ms": 0, "emitted": {}})
    emitted = idx.get("emitted")
    if not isinstance(emitted, dict):
        emitted = {}
        idx["emitted"] = emitted

    if tid in emitted:
        return False

    # If the canonical decisions file is missing, don't emit into the void.
    # (But in your system it should exist once restored.)
    if not AI_DECISIONS_PATH.parent.exists():
        return False

    row: Dict[str, Any] = {
        "schema_version": 2,
        "event_type": "ai_decision",
        "ts_ms": _now_ms(),
        "trade_id": tid,
        "account_label": acct or None,
        "symbol": sym or None,
        "decision": "DEFAULT_EMITTED",
        "allow": bool(allow) if allow is not None else True,
        "size_multiplier": _safe_float(size_multiplier, 0.25) if size_multiplier is not None else 0.25,
        "gates": {"reason": "DEFAULT_EMITTED", "note": _safe_str(reason)},
        "meta": {
            "source": "default_decision_emitter",
            "reason": _safe_str(reason),
            "mode": md or None,
        },
    }

    # Phase 7 snapshot linkage (optional)
    if snapshot_fp is not None:
        row["snapshot_fp"] = _safe_str(snapshot_fp) or None
    if snapshot_mode is not None:
        row["snapshot_mode"] = _safe_str(snapshot_mode) or None
    if snapshot_schema_version is not None:
        try:
            row["snapshot_schema_version"] = int(snapshot_schema_version)
        except Exception:
            row["snapshot_schema_version"] = None

    _append_jsonl(AI_DECISIONS_PATH, row)

    emitted[tid] = _now_ms()
    idx["updated_ms"] = _now_ms()

    # Hard cap to avoid infinite growth
    MAX = 100000
    if len(emitted) > MAX:
        # Drop oldest by insertion order
        drop = len(emitted) - MAX
        for k in list(emitted.keys())[:drop]:
            emitted.pop(k, None)

    _write_json(STUB_INDEX_PATH, idx)
    return True
