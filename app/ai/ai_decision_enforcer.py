#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set

# Optional faster JSON
try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover
    orjson = None  # type: ignore


# -----------------------------------------------------------------------------
# Imports from project (preferred), with safe fallbacks
# -----------------------------------------------------------------------------
AI_DECISIONS_PATH: Optional[str] = None
read_jsonl_tail = None  # type: ignore

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

try:
    from app.core.spine_api import (  # type: ignore
        AI_DECISIONS_PATH as _AI_DECISIONS_PATH,
        read_jsonl_tail as _read_jsonl_tail,
        safe_str as _safe_str,
        safe_upper as _safe_upper,
        normalize_timeframe as _normalize_timeframe,
    )
    AI_DECISIONS_PATH = _AI_DECISIONS_PATH
    read_jsonl_tail = _read_jsonl_tail  # type: ignore

    # Prefer project-provided utilities
    safe_str = _safe_str  # type: ignore
    safe_upper = _safe_upper  # type: ignore
    normalize_timeframe = _normalize_timeframe  # type: ignore
except Exception:
    # Keep local fallbacks
    pass


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DECISIONS_PATH = (
    Path(AI_DECISIONS_PATH)
    if AI_DECISIONS_PATH
    else Path("state/ai_decisions.jsonl")
)

# Coverage modes:
# - strict (default): semantic conflicts => block
# - warn: conflicts => prefer newest but annotate
DECISION_COVERAGE_MODE = os.getenv("DECISION_COVERAGE_MODE", "strict").strip().lower()

# Avoid loading huge decisions files into memory
AI_DECISION_ENFORCER_TAIL_BYTES: int = int(os.getenv("AI_DECISION_ENFORCER_TAIL_BYTES", "2097152") or "2097152")  # 2MB
AI_DECISION_ENFORCER_MAX_LINES: int = int(os.getenv("AI_DECISION_ENFORCER_MAX_LINES", "20000") or "20000")

# Full scan fallback limit (MB)
AI_DECISION_ENFORCER_FULLSCAN_MAX_MB: int = int(os.getenv("AI_DECISION_ENFORCER_FULLSCAN_MAX_MB", "200") or "200")

# Float comparison tolerance (signature rounding)
_SIG_ROUND_DP = int(os.getenv("AI_DECISION_ENFORCER_SIG_ROUND_DP", "6") or "6")

# Require decision to PRE-EXIST this executor run (age gate)
EXEC_REQUIRE_PREEXISTING_DECISION: bool = (
    os.getenv("EXEC_REQUIRE_PREEXISTING_DECISION", "false").strip().lower()
    in ("1", "true", "yes", "y", "on")
)
EXEC_PREEXISTING_MIN_AGE_MS: int = int(os.getenv("EXEC_PREEXISTING_MIN_AGE_MS", "5000") or "5000")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _json_loads_line(raw: bytes) -> Optional[Dict[str, Any]]:
    """Parse a JSONL line robustly and quickly when possible."""
    r = raw.strip()
    if not r:
        return None
    try:
        if orjson is not None:
            d = orjson.loads(r)  # type: ignore[arg-type]
        else:
            d = json.loads(r.decode("utf-8", errors="ignore"))
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _tail_recent_rows_fast(path: Path, tail_bytes: int, max_lines: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Prefer centralized tail reader if available; fallback to local best-effort.
    Returns (rows, bad_count).
    """
    # Prefer centralized reader (if provided by project)
    try:
        if callable(read_jsonl_tail):  # type: ignore[arg-type]
            return read_jsonl_tail(path, tail_bytes=tail_bytes, max_lines=max_lines)  # type: ignore[misc]
    except Exception:
        pass

    rows: List[Dict[str, Any]] = []
    bad = 0
    try:
        if not path.exists():
            return [], 0

        size = path.stat().st_size
        start = max(0, size - int(max(1024, tail_bytes)))

        with path.open("rb") as f:
            f.seek(start)
            blob = f.read()

        # If we started mid-line, drop partial first line
        if start > 0:
            nl = blob.find(b"\n")
            if nl >= 0:
                blob = blob[nl + 1 :]

        for raw in blob.splitlines()[-max_lines:]:
            d = _json_loads_line(raw)
            if d is None:
                bad += 1
                continue
            rows.append(d)

        return rows, bad
    except Exception:
        return [], bad


def _tail_read_text(path: Path, tail_bytes: int) -> str:
    """Fallback helper for tail-reading text; used when iterating reverse."""
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


def _normalize_trade_id_input(tid: str) -> str:
    """
    Normalize alternate trade_id formats to canonical if possible:
      - flashback05-HBARUSDT-2580... -> flashback05:2580...
      - flashback05-SOLUSDT-a65e...  -> flashback05:a65e...
      - flashback05-anything-a65e... -> flashback05:a65e...
      - flashback05-a65e...         -> flashback05:a65e...
    If it can't parse, returns original trimmed string.
    """
    try:
        import re as _re

        t = str(tid or "").strip()
        if not t:
            return t
        if ":" in t:
            return t  # already canonical

        # Most common: <label>-<symbol>-<hex> (symbol can be anything without '-')
        m = _re.match(r"^([A-Za-z0-9_]+)-([^-]+)-([0-9a-f]{8,})$", t)
        if m:
            lab = m.group(1)
            hx = m.group(3)
            return f"{lab}:{hx}"

        # Just in case: <label>-<hex>
        m2 = _re.match(r"^([A-Za-z0-9_]+)-([0-9a-f]{8,})$", t)
        if m2:
            return f"{m2.group(1)}:{m2.group(2)}"

        return t
    except Exception:
        return str(tid or "").strip()


def _matches_trade_id(d: Dict[str, Any], trade_id: str) -> bool:
    """
    Phase-4 lifecycle join:
    Allow enforcement by canonical trade identifier.

    RULE:
    - If caller provides account-prefixed trade_id (contains ":"),
      do NOT match by source_trade_id (source may be shared/collide across accounts).
    - If caller provides a raw trade_id (no ":"), allow matching by source_trade_id.
    """
    try:
        tid = str(trade_id or "")
        if not tid:
            return False

        d_trade = str(d.get("trade_id") or "")
        d_client = str(d.get("client_trade_id") or "")
        d_source = str(d.get("source_trade_id") or "")

        if ":" in tid:
            return tid == d_trade or tid == d_client

        return tid in {d_trade, d_client, d_source}
    except Exception:
        return False


def _read_lines_reverse() -> Iterable[Dict[str, Any]]:
    """
    Read decisions from the tail of the file and iterate newest-first.
    Fail-soft: parse errors are skipped.
    """
    if not DECISIONS_PATH.exists():
        return []

    txt = _tail_read_text(DECISIONS_PATH, AI_DECISION_ENFORCER_TAIL_BYTES)
    if not txt:
        return []

    out: List[Dict[str, Any]] = []
    for line in reversed(txt.splitlines()):
        try:
            d = json.loads(line)
            if isinstance(d, dict):
                out.append(d)
        except Exception:
            continue
    return out


def _scan_file_for_trade_id(path: Path, trade_id: str, *, max_mb: int) -> Optional[Dict[str, Any]]:
    """
    FALLBACK_FULL_SCAN: If tail scan misses an older decision, scan file once (bounded).
    Looks for exact match on trade_id or client_trade_id.
    """
    try:
        if not path.exists():
            return None
        if path.stat().st_size > max_mb * 1024 * 1024:
            return None

        with path.open("rb") as f:
            for raw in f:
                d = _json_loads_line(raw)
                if not isinstance(d, dict):
                    continue
                if str(d.get("trade_id") or "") == trade_id:
                    return d
                if str(d.get("client_trade_id") or "") == trade_id:
                    return d
        return None
    except Exception:
        return None


def _is_manual_override_row(d: Dict[str, Any]) -> bool:
    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    stage = str(extra.get("stage") or "")
    if stage in ("manual_seed", "manual_override", "manual_block"):
        return True
    if str(d.get("account_label") or "") == "manual":
        return True
    if str(d.get("strategy_id") or "") == "manual":
        return True
    return False


def _is_executor_post_enforce_row(d: Dict[str, Any]) -> bool:
    """
    Executor post-enforce rows are audit outputs and must NOT be treated as input decisions.
    """
    meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
    if str(meta.get("source") or "") == "executor_post_enforce":
        return True
    if str(meta.get("stage") or "") == "post_enforce":
        return True

    for k in ("enforced_code", "enforced_reason", "enforced_size_multiplier"):
        if k in meta:
            return True

    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    if gates.get("enforced") is True:
        return True

    return False


def _is_executor_output_row(d: Dict[str, Any]) -> bool:
    """
    Executor outputs / audit rows (NOT enforcement inputs).
    """
    if _is_manual_override_row(d):
        return False

    if _is_executor_post_enforce_row(d):
        return True

    if str(d.get("event_type") or "") != "ai_decision":
        return False

    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    stage = str(extra.get("stage") or "")
    if stage.startswith("decision_enforced"):
        return True
    if stage in ("pre_entry", "post_entry", "ai_gate", "corr_gate", "portfolio_guard"):
        return True

    return True


def _safe_float(x: Any, default: float) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _clean_code(x: Any) -> str:
    try:
        return str(x or "").strip()
    except Exception:
        return ""


def _canonical_pilot_decision_code(d: Dict[str, Any]) -> str:
    dec = _clean_code(d.get("decision"))
    return dec or _clean_code(d.get("decision_code"))


def _normalize_manual_or_pilot(d: Dict[str, Any]) -> Dict[str, Any]:
    # Manual override rows are already semantically “decision”
    if _is_manual_override_row(d):
        allow = bool(d.get("allow", False))
        code = _clean_code(d.get("decision_code") or ("ALLOW_TRADE" if allow else "BLOCK_TRADE"))
        reason = _clean_code(d.get("reason") or ("passed" if allow else "blocked"))
        sm = _safe_float(d.get("size_multiplier"), 1.0 if allow else 0.0)
        if sm < 0:
            sm = 0.0
        if allow and sm <= 0:
            sm = 1.0
        return {"allow": allow, "size_multiplier": sm, "decision_code": code, "reason": reason}

    code = _canonical_pilot_decision_code(d)

    # If the decision already has allow/size_multiplier, honor it (with guardrails)
    if d.get("allow") is not None or d.get("size_multiplier") is not None:
        allow = bool(d.get("allow", False))
        sm = _safe_float(d.get("size_multiplier"), 1.0 if allow else 0.0)

        # COLD_START defaults
        if code == "COLD_START":
            if d.get("size_multiplier") is None:
                sm = 0.25
            allow = True if d.get("allow") is None else bool(d.get("allow"))

        gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
        reason = _clean_code(d.get("reason") or gates.get("reason") or ("passed" if allow else "blocked"))

        if sm < 0:
            sm = 0.0
        if allow and sm <= 0:
            sm = 1.0

        out_code = code or ("ALLOW_TRADE" if allow else "BLOCK_TRADE")
        return {"allow": allow, "size_multiplier": sm, "decision_code": out_code, "reason": reason}

    # Interpret older pilot schema forms
    if code == "ALLOW_TRADE":
        pa = d.get("proposed_action") if isinstance(d.get("proposed_action"), dict) else {}
        sm = _safe_float(pa.get("size_multiplier"), 1.0)
        gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
        reason = _clean_code(gates.get("reason") or "passed")
        if sm < 0:
            sm = 0.0
        if sm <= 0:
            sm = 1.0
        return {"allow": True, "size_multiplier": sm, "decision_code": "ALLOW_TRADE", "reason": reason}

    if code == "COLD_START":
        return {"allow": True, "size_multiplier": 0.25, "decision_code": "COLD_START", "reason": "cold_start_reduced"}

    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    reason = _clean_code(gates.get("reason") or "blocked")
    return {"allow": False, "size_multiplier": 0.0, "decision_code": (code or "BLOCK_TRADE"), "reason": reason}


def _decision_signature_semantic(norm: Dict[str, Any]) -> Tuple[bool, float]:
    try:
        sm = float(norm.get("size_multiplier", 1.0))
    except Exception:
        sm = 1.0
    try:
        sm = round(sm, _SIG_ROUND_DP)
    except Exception:
        pass
    return (bool(norm.get("allow", False)), sm)


def _ts_ms(d: Dict[str, Any]) -> int:
    """
    Normalize timestamps to milliseconds.
    Accepts:
      - ts_ms (preferred)
      - ts (could be ms OR seconds depending on writer)
    """
    try:
        v = int(d.get("ts_ms") or d.get("ts") or 0)
    except Exception:
        return 0

    if v <= 0:
        return 0

    # If it looks like seconds, convert to ms.
    if v < 1_000_000_000_000:
        v *= 1000
    return v


def _is_valid_pilot_input_row(d: Dict[str, Any]) -> bool:
    if d.get("schema_version") != 1:
        return False
    if "decision" not in d:
        return False
    dec = _clean_code(d.get("decision"))
    return bool(dec)


def _is_preexisting_ok(d: Dict[str, Any], now_ms: int) -> bool:
    if not EXEC_REQUIRE_PREEXISTING_DECISION:
        return True
    if _is_manual_override_row(d):
        return True

    ts = _ts_ms(d)
    cutoff = now_ms - max(0, int(EXEC_PREEXISTING_MIN_AGE_MS))
    return ts > 0 and ts < cutoff


def _load_effective_input_decision(
    trade_id: str,
    *,
    account_label: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    now_ms = int(time.time() * 1000)

    manual_rows: List[Dict[str, Any]] = []
    pilot_rows: List[Dict[str, Any]] = []
    filtered_too_new = 0

    for d in _read_lines_reverse():
        if not _matches_trade_id(d, trade_id):
            continue

        if account_label:
            if str(d.get("account_label") or "") != str(account_label):
                continue

        if not _is_preexisting_ok(d, now_ms):
            filtered_too_new += 1
            continue

        if _is_manual_override_row(d):
            manual_rows.append(d)
            continue

        if _is_executor_output_row(d):
            continue

        if _is_valid_pilot_input_row(d):
            pilot_rows.append(d)
            continue

    if manual_rows:
        chosen = max(manual_rows, key=_ts_ms)
        return chosen, {
            "source": "manual",
            "count": len(manual_rows),
            "conflict": False,
            "require_preexisting": EXEC_REQUIRE_PREEXISTING_DECISION,
            "filtered_too_new": filtered_too_new,
            "min_age_ms": EXEC_PREEXISTING_MIN_AGE_MS,
        }

    if not pilot_rows:
        return None, {
            "source": "none",
            "count": 0,
            "conflict": False,
            "require_preexisting": EXEC_REQUIRE_PREEXISTING_DECISION,
            "filtered_too_new": filtered_too_new,
            "min_age_ms": EXEC_PREEXISTING_MIN_AGE_MS,
        }

    norms = [_normalize_manual_or_pilot(r) for r in pilot_rows]
    sigs: Set[Tuple[bool, float]] = {_decision_signature_semantic(n) for n in norms}
    conflict = (len(sigs) > 1)

    codes = {str(n.get("decision_code") or "") for n in norms if str(n.get("decision_code") or "").strip()}
    code_disagreement = (len(codes) > 1)

    if conflict and DECISION_COVERAGE_MODE == "strict":
        newest = max(pilot_rows, key=_ts_ms)
        return newest, {
            "source": "pilot",
            "count": len(pilot_rows),
            "conflict": True,
            "mode": "strict_block",
            "unique_signatures": len(sigs),
            "codes": sorted(codes),
            "code_disagreement": code_disagreement,
            "require_preexisting": EXEC_REQUIRE_PREEXISTING_DECISION,
            "filtered_too_new": filtered_too_new,
            "min_age_ms": EXEC_PREEXISTING_MIN_AGE_MS,
        }

    chosen = max(pilot_rows, key=_ts_ms)
    return chosen, {
        "source": "pilot",
        "count": len(pilot_rows),
        "conflict": conflict,
        "mode": ("warn_prefer_newest" if conflict else "ok"),
        "unique_signatures": len(sigs),
        "codes": sorted(codes),
        "code_disagreement": code_disagreement,
        "require_preexisting": EXEC_REQUIRE_PREEXISTING_DECISION,
        "filtered_too_new": filtered_too_new,
        "min_age_ms": EXEC_PREEXISTING_MIN_AGE_MS,
    }


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def enforce_decision(trade_id: str, *, account_label: Optional[str] = None) -> Dict[str, Any]:
    """
    Enforce the decision for this trade_id:
      - Find effective input decision (manual overrides first, then pilot inputs)
      - Block on missing decision
      - Block on semantic conflicts in strict mode
      - Provide deterministic reasons + meta for diagnostics
    """
    raw_tid = safe_str(trade_id)
    norm_tid = _normalize_trade_id_input(raw_tid)

    d, meta = _load_effective_input_decision(norm_tid, account_label=account_label)

    if not d:
        r = "no_decision_found"
        if meta.get("require_preexisting"):
            r = (
                f"{r}|require_preexisting=true|min_age_ms={meta.get('min_age_ms')}"
                f"|filtered_too_new={meta.get('filtered_too_new')}"
            )

        # Full scan fallback (bounded) trying BOTH normalized and raw
        d2 = _scan_file_for_trade_id(DECISIONS_PATH, norm_tid, max_mb=AI_DECISION_ENFORCER_FULLSCAN_MAX_MB)
        if not isinstance(d2, dict) and raw_tid and raw_tid != norm_tid:
            d2 = _scan_file_for_trade_id(DECISIONS_PATH, raw_tid, max_mb=AI_DECISION_ENFORCER_FULLSCAN_MAX_MB)

        if isinstance(d2, dict):
            # Normalize found row through the same interpretation pipeline
            norm2 = _normalize_manual_or_pilot(d2)
            return {
                "allow": bool(norm2.get("allow", False)),
                "size_multiplier": float(norm2.get("size_multiplier", 0.0) or 0.0),
                "decision_code": str(norm2.get("decision_code") or "FOUND_BY_FULL_SCAN"),
                "reason": str(norm2.get("reason") or "full_scan_fallback"),
                "meta": {
                    **meta,
                    "fallback": "full_scan",
                    "fallback_max_mb": AI_DECISION_ENFORCER_FULLSCAN_MAX_MB,
                    "trade_id_in": raw_tid,
                    "trade_id_norm": norm_tid,
                },
            }

        return {
            "allow": False,
            "size_multiplier": 0.0,
            "decision_code": "NO_DECISION",
            "reason": r,
            "meta": {
                **meta,
                "trade_id_in": raw_tid,
                "trade_id_norm": norm_tid,
            },
        }

    norm = _normalize_manual_or_pilot(d)

    if meta.get("conflict") and DECISION_COVERAGE_MODE == "strict":
        return {
            "allow": False,
            "size_multiplier": 0.0,
            "decision_code": "AMBIGUOUS_DECISION",
            "reason": f"conflicting_input_decisions(count={meta.get('count')}, unique={meta.get('unique_signatures')})",
            "meta": {
                **meta,
                "trade_id_in": raw_tid,
                "trade_id_norm": norm_tid,
            },
        }

    reason = str(norm.get("reason") or "ok")

    if meta.get("conflict") and DECISION_COVERAGE_MODE != "strict":
        reason = f"{reason} | ambiguous_inputs_prefer_newest(count={meta.get('count')}, unique={meta.get('unique_signatures')})"

    if meta.get("code_disagreement"):
        reason = f"{reason} | decision_code_disagreement(codes={meta.get('codes')})"

    if meta.get("require_preexisting"):
        reason = (
            f"{reason} | require_preexisting=true|min_age_ms={meta.get('min_age_ms')}"
            f"|filtered_too_new={meta.get('filtered_too_new')}"
        )

    return {
        "allow": bool(norm["allow"]),
        "size_multiplier": float(norm["size_multiplier"]),
        "decision_code": str(norm["decision_code"]),
        "reason": reason,
        "meta": {
            **meta,
            "trade_id_in": raw_tid,
            "trade_id_norm": norm_tid,
        },
    }
