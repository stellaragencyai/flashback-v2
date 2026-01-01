from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCOREBOARD_PATH = str(ROOT / "state" / "ai_memory" / "scoreboard.v1.json")


def _as_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _load_scoreboard(path: str) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None


def _bucket_key(setup_type: str, timeframe: str, symbol: str) -> Tuple[str, str, str]:
    return (
        str(setup_type or "unknown"),
        str(timeframe or "unknown"),
        str(symbol or "unknown"),
    )


def scoreboard_gate_decide(
    setup_type: str,
    timeframe: str,
    symbol: str,
    account_label: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    path = os.getenv("SCOREBOARD_PATH", DEFAULT_SCOREBOARD_PATH)
    sb = _load_scoreboard(path)
    if not sb or sb.get("schema_version") != "scoreboard.v1":
        return None

    min_n = int(os.getenv("SCOREBOARD_GATE_MIN_N", str(sb.get("min_n") or 10)))
    min_conf = float(os.getenv("SCOREBOARD_GATE_MIN_CONF", "0.60"))
    hard_block_exp = float(os.getenv("SCOREBOARD_GATE_BLOCK_EXPECTANCY_LTE", "-0.05"))
    soft_cut_exp = float(os.getenv("SCOREBOARD_GATE_SOFT_EXPECTANCY_LTE", "0.00"))

    soft_size = float(os.getenv("SCOREBOARD_GATE_SOFT_SIZE_MULT", "0.25"))
    boost_size = float(os.getenv("SCOREBOARD_GATE_BOOST_SIZE_MULT", "1.25"))
    max_size = float(os.getenv("SCOREBOARD_GATE_MAX_SIZE_MULT", "2.00"))

    key = _bucket_key(setup_type, timeframe, symbol)
    buckets = sb.get("buckets") or []

    match = None
    for b in buckets:
        bk = b.get("bucket_key") or {}
        k2 = _bucket_key(
            bk.get("setup_type"),
            bk.get("timeframe"),
            bk.get("symbol"),
        )
        if k2 == key:
            match = b
            break

    if not match:
        return None

    n = int(match.get("n") or 0)
    conf = _as_float(match.get("confidence"))
    exp = _as_float(match.get("expectancy"))

    if n <= 0 or conf is None or exp is None:
        return None

    if n < min_n or conf < min_conf:
        return {
            "allow": True,
            "size_multiplier": None,
            "decision_code": "SCOREBOARD_INSUFFICIENT_DATA",
            "reason": (
                f"insufficient_data n={n} conf={conf:.2f} "
                f"(min_n={min_n} min_conf={min_conf})"
            ),
            "bucket_key": match.get("bucket_key"),
            "bucket_stats": match,
            "scoreboard_path": path,
        }

    if exp <= hard_block_exp:
        return {
            "allow": False,
            "size_multiplier": 0.0,
            "decision_code": "SCOREBOARD_BLOCK_NEG_EXPECTANCY",
            "reason": f"block exp={exp:.4f} <= {hard_block_exp}",
            "bucket_key": match.get("bucket_key"),
            "bucket_stats": match,
            "scoreboard_path": path,
        }

    if exp <= soft_cut_exp:
        sm = _clamp(soft_size, 0.0, max_size)
        return {
            "allow": True,
            "size_multiplier": sm,
            "decision_code": "SCOREBOARD_SOFT_CUT",
            "reason": f"soft_cut exp={exp:.4f} <= {soft_cut_exp} -> sm={sm}",
            "bucket_key": match.get("bucket_key"),
            "bucket_stats": match,
            "scoreboard_path": path,
        }

    sm = _clamp(boost_size, 0.0, max_size)
    return {
        "allow": True,
        "size_multiplier": sm,
        "decision_code": "SCOREBOARD_ALLOW_BOOST",
        "reason": f"allow_boost exp={exp:.4f} -> sm={sm}",
        "bucket_key": match.get("bucket_key"),
        "bucket_stats": match,
        "scoreboard_path": path,
    }
