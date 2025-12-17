#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, List, Tuple


DECISIONS_PATH = Path("state/ai_decisions.jsonl")

# Coverage modes:
# - strict (default): missing decision => BLOCK, conflicting decisions => BLOCK
# - warn: missing => BLOCK, conflicting => prefer newest but annotate reason
DECISION_COVERAGE_MODE = os.getenv("DECISION_COVERAGE_MODE", "strict").strip().lower()

def _matches_trade_id(d: Dict[str, Any], trade_id: str) -> bool:
    """
    Phase-4 lifecycle join:
    Allow enforcement by any canonical trade identifier.
    """
    try:
        return trade_id in {
            str(d.get("trade_id") or ""),
            str(d.get("client_trade_id") or ""),
            str(d.get("source_trade_id") or ""),
        }
    except Exception:
        return False



def _read_lines_reverse() -> Iterable[Dict[str, Any]]:
    if not DECISIONS_PATH.exists():
        return []
    try:
        lines = DECISIONS_PATH.read_text("utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for line in reversed(lines):
        try:
            d = json.loads(line)
            if isinstance(d, dict):
                out.append(d)
        except Exception:
            continue
    return out


def _is_executor_output_row(d: Dict[str, Any]) -> bool:
    """
    Executor outputs:
      - event_type == "ai_decision"
      - extra.stage in {"pre_entry","post_entry","ai_gate","corr_gate","portfolio_guard","decision_enforced_*",...}

    These are NOT enforcement inputs. They exist for auditing/joining.
    """
    if str(d.get("event_type") or "") != "ai_decision":
        return False

    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    stage = str(extra.get("stage") or "")
    if stage.startswith("decision_enforced"):
        return True
    if stage in (
        "pre_entry",
        "post_entry",
        "ai_gate",
        "corr_gate",
        "portfolio_guard",
    ):
        return True

    # If it's ai_decision with no stage, treat as output unless explicitly marked manual
    return True


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


def _normalize_manual_or_pilot(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return canonical:
      { allow, size_multiplier, decision_code, reason }
    Supports:
      - Manual override rows (executor-style but marked manual)
      - Pilot legacy schema rows
    """
    # Manual override rows (can be executor-style)
    if _is_manual_override_row(d):
        allow = bool(d.get("allow", False))
        code = str(d.get("decision_code") or ("ALLOW_TRADE" if allow else "BLOCK_TRADE"))
        reason = str(d.get("reason") or ("passed" if allow else "blocked"))
        sm_raw = d.get("size_multiplier", 1.0 if allow else 0.0)
        try:
            sm = float(sm_raw) if sm_raw is not None else (1.0 if allow else 0.0)
        except Exception:
            sm = 1.0 if allow else 0.0
        if sm < 0:
            sm = 0.0
        if allow and sm <= 0:
            sm = 1.0
        return {"allow": allow, "size_multiplier": sm, "decision_code": code, "reason": reason}

    # Pilot legacy schema
    code = d.get("decision")
    code = str(code) if code is not None else ""

    if code == "ALLOW_TRADE":
        pa = d.get("proposed_action") if isinstance(d.get("proposed_action"), dict) else {}
        sm_raw = pa.get("size_multiplier", 1.0)
        try:
            sm = float(sm_raw) if sm_raw is not None else 1.0
        except Exception:
            sm = 1.0
        gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
        reason = str(gates.get("reason") or "passed")
        if sm < 0:
            sm = 0.0
        if sm <= 0:
            sm = 1.0
        return {"allow": True, "size_multiplier": sm, "decision_code": "ALLOW_TRADE", "reason": reason}

    if code == "COLD_START":
        return {"allow": True, "size_multiplier": 0.25, "decision_code": "COLD_START", "reason": "cold_start_reduced"}

    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    reason = str(gates.get("reason") or "blocked")
    return {"allow": False, "size_multiplier": 0.0, "decision_code": (code or "BLOCK_TRADE"), "reason": reason}


def _decision_signature(norm: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    A compact signature to detect conflicting decisions.
    """
    try:
        sm = float(norm.get("size_multiplier", 1.0))
    except Exception:
        sm = 1.0
    return (bool(norm.get("allow", False)), sm, str(norm.get("decision_code") or ""))


def _ts(d: Dict[str, Any]) -> int:
    try:
        return int(d.get("ts_ms") or d.get("ts") or 0)
    except Exception:
        return 0



def _load_effective_input_decision(
    trade_id: str,
    *,
    account_label: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Select best INPUT decision for trade_id.

    Priority:
      1) Newest manual override row (optionally account_label-filtered)
      2) Newest pilot legacy row (schema_version==1 and "decision" in payload)
      3) None

    Coverage guard:
      - If multiple conflicting input decisions exist for the same trade_id => BLOCK (strict)
    """
    manual_rows: List[Dict[str, Any]] = []
    pilot_rows: List[Dict[str, Any]] = []

    for d in _read_lines_reverse():
        if not _matches_trade_id(d, trade_id):
            continue


        # ignore executor audit outputs
        if _is_executor_output_row(d):
            continue

        # optional scoping
        if account_label:
            if str(d.get("account_label") or "") != str(account_label):
                continue

        if _is_manual_override_row(d):
            manual_rows.append(d)
            continue

        if d.get("schema_version") == 1 and "decision" in d:
            pilot_rows.append(d)
            continue

    # Pick newest manual if any
    if manual_rows:
        chosen = max(manual_rows, key=_ts)
        return chosen, {"source": "manual", "count": len(manual_rows), "conflict": False}

    if not pilot_rows:
        return None, {"source": "none", "count": 0, "conflict": False}

    # Coverage guard: detect conflicts across pilot decisions
    norms = [_normalize_manual_or_pilot(r) for r in pilot_rows]
    sigs = {_decision_signature(n) for n in norms}

    conflict = (len(sigs) > 1)

    if conflict and DECISION_COVERAGE_MODE == "strict":
        # In strict mode, ambiguity is a hard block.
        newest = max(pilot_rows, key=_ts)
        return newest, {
            "source": "pilot",
            "count": len(pilot_rows),
            "conflict": True,
            "mode": "strict_block",
            "unique_signatures": len(sigs),
        }

    # warn mode (or no conflict): choose newest pilot row
    chosen = max(pilot_rows, key=_ts)
    return chosen, {
        "source": "pilot",
        "count": len(pilot_rows),
        "conflict": conflict,
        "mode": ("warn_prefer_newest" if conflict else "ok"),
        "unique_signatures": len(sigs),
    }


def enforce_decision(trade_id: str, *, account_label: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns canonical enforcement dict:
      { allow, size_multiplier, decision_code, reason }

    Coverage behavior:
      - Missing decision => BLOCK (NO_DECISION)
      - Conflicting decisions:
          strict => BLOCK (AMBIGUOUS_DECISION)
          warn   => choose newest but annotate reason
    """
    d, meta = _load_effective_input_decision(trade_id, account_label=account_label)

    if not d:
        return {"allow": False, "size_multiplier": 0.0, "decision_code": "NO_DECISION", "reason": "no_decision_found"}

    norm = _normalize_manual_or_pilot(d)

    if meta.get("conflict") and DECISION_COVERAGE_MODE == "strict":
        return {
            "allow": False,
            "size_multiplier": 0.0,
            "decision_code": "AMBIGUOUS_DECISION",
            "reason": f"conflicting_input_decisions(count={meta.get('count')}, unique={meta.get('unique_signatures')})",
        }

    reason = str(norm.get("reason") or "ok")
    if meta.get("conflict") and DECISION_COVERAGE_MODE != "strict":
        reason = f"{reason} | ambiguous_inputs_prefer_newest(count={meta.get('count')}, unique={meta.get('unique_signatures')})"

    return {
    "allow": bool(norm["allow"]),
    "size_multiplier": float(norm["size_multiplier"]),
    "decision_code": str(norm["decision_code"]),
    "reason": reason,
    "meta": meta,  # Phase-4 introspection (ignored by executor)
}
