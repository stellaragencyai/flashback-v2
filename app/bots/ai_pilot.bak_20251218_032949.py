#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Pilot v2.9 ✅ (Decision writer unified via ai_decision_logger)

PATCH v2.9.1:
- Marker only (verification).

PATCH v2.9.2:
- Cold start sizing semantics made consistent with ai_decision_enforcer:
  - When decision == COLD_START, pilot writes size_multiplier default 0.25
    (configurable via AI_PILOT_COLD_START_SIZE_MULT).
  - If memory-based size_multiplier exists, cold-start still caps it.

PATCH v2.9.3 (THIS PATCH):
- Stop writing directly to ai_actions.jsonl.
- Route all action writes through app.core.ai_action_bus (dedupe + normalization).
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import orjson

from app.core.ai_action_builder import build_trade_action_from_sample
from app.core.ai_state_bus import build_ai_snapshot, validate_snapshot_v2
from app.core.flashback_common import send_tg, record_heartbeat, alert_bot_error

try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        return logger_

logger = get_logger("ai_pilot")

try:
    from app.ai.ai_policy_sample import evaluate_state as sample_evaluate_state
except Exception:  # pragma: no cover
    sample_evaluate_state = None  # type: ignore[assignment]

try:
    from app.ai.ai_policy_core import evaluate_state as core_evaluate_state
except Exception:  # pragma: no cover
    core_evaluate_state = None  # type: ignore[assignment]

try:
    from app.ai.ai_memory_store import query_memories_tiered, QueryOptions
except Exception:  # pragma: no cover
    query_memories_tiered = None  # type: ignore[assignment]
    QueryOptions = None  # type: ignore[assignment]

try:
    from app.ai.ai_gatekeeper import evaluate_memory_gates
except Exception:  # pragma: no cover
    evaluate_memory_gates = None  # type: ignore[assignment]

try:
    from app.core.ai_decision_types import PilotDecision, DECISION_SCHEMA_VERSION
except Exception:  # pragma: no cover
    PilotDecision = Dict[str, Any]  # type: ignore[misc,assignment]
    DECISION_SCHEMA_VERSION = 1

# Canary controls live in memory contract (single source of truth)
try:
    from app.ai.ai_memory_contract import canary_enabled, is_canary_account
except Exception:  # pragma: no cover
    def canary_enabled() -> bool:  # type: ignore
        return False
    def is_canary_account(_: Optional[str]) -> bool:  # type: ignore
        return False

# ✅ hardened decision writer
try:
    from app.core.ai_decision_logger import append_decision
except Exception:  # pragma: no cover
    append_decision = None  # type: ignore[assignment]

# ✅ action bus (dedupe + normalization)
try:
    from app.core.ai_action_bus import append_actions as bus_append_actions
except Exception:  # pragma: no cover
    bus_append_actions = None  # type: ignore[assignment]


def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default).strip())
    except Exception:
        return float(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

AI_PILOT_ENABLED: bool = _env_bool("AI_PILOT_ENABLED", "true")
POLL_SECONDS: int = _env_int("AI_PILOT_POLL_SECONDS", "3")
DRY_RUN: bool = _env_bool("AI_PILOT_DRY_RUN", "true")

USE_SAMPLE_POLICY: bool = _env_bool("AI_PILOT_SAMPLE_POLICY", "false")
USE_CORE_POLICY: bool = _env_bool("AI_PILOT_CORE_POLICY", "false")

WRITE_ACTIONS: bool = _env_bool("AI_PILOT_WRITE_ACTIONS", "false")

AI_PILOT_USE_MEMORY_GATES: bool = _env_bool("AI_PILOT_USE_MEMORY_GATES", "true")
AI_PILOT_ALLOW_COLD_START: bool = _env_bool("AI_PILOT_ALLOW_COLD_START", "true")
AI_PILOT_BLOCK_COLD_START_IN_LIVE: bool = _env_bool("AI_PILOT_BLOCK_COLD_START_IN_LIVE", "true")

AI_MEM_MIN_N_ANY: int = _env_int("AI_MEM_MIN_N_ANY", "3")
AI_MEM_MIN_N_SYMBOL: int = _env_int("AI_MEM_MIN_N_SYMBOL", "2")
AI_MEM_MIN_R_MEAN: float = _env_float("AI_MEM_MIN_R_MEAN", "0.10")
AI_MEM_MAX_LOSS_RATE: float = _env_float("AI_MEM_MAX_LOSS_RATE", "0.60")
AI_MEM_MIN_ABS_R_SUM: float = _env_float("AI_MEM_MIN_ABS_R_SUM", "0.0")
AI_PILOT_BLOCK_ON_BAD_MEMORY: bool = _env_bool("AI_PILOT_BLOCK_ON_BAD_MEMORY", "true")

AI_PILOT_MEMORY_CANARY_ONLY: bool = _env_bool("AI_PILOT_MEMORY_CANARY_ONLY", "true")

# ✅ cold start sizing default (matches enforcer behavior)
AI_PILOT_COLD_START_SIZE_MULT: float = _env_float("AI_PILOT_COLD_START_SIZE_MULT", "0.25")


def _write_decision(payload: Dict[str, Any]) -> None:
    """
    Single writer gateway for AI Pilot.
    Uses hardened logger when available, else best-effort append.
    """
    try:
        if append_decision is not None:
            append_decision(payload)  # lock + dedupe + normalize
            return
    except Exception:
        pass

    # fallback (should almost never happen)
    try:
        from pathlib import Path
        path = Path(os.getenv("AI_DECISIONS_PATH", "state/ai_decisions.jsonl")).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception:
        return


def _build_ai_state() -> Dict[str, Any]:
    snap = build_ai_snapshot(
        focus_symbols=None,
        include_trades=False,
        trades_limit=0,
        include_orderbook=True,
    )
    ok, errors = validate_snapshot_v2(snap)
    if not ok:
        raise RuntimeError(f"snapshot_v2_invalid: {errors}")

    account = snap.get("account") or {}
    pos_block = snap.get("positions") or {}
    positions_by_symbol = pos_block.get("by_symbol") or {}
    positions_list: List[Dict[str, Any]] = list(positions_by_symbol.values())

    freshness = snap.get("freshness") or {}
    safety = snap.get("safety") or {}

    return {
        "label": ACCOUNT_LABEL,
        "dry_run": DRY_RUN,
        "account": {
            "equity_usdt": account.get("equity_usdt"),
            "mmr_pct": account.get("mmr_pct"),
            "open_positions": len(positions_list),
        },
        "positions": positions_list,
        "buses": freshness,
        "safety": safety,
        "snapshot_v2": snap,
    }


def _safe_first_match(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    matched = r.get("matched") or []
    if isinstance(matched, list) and matched:
        return matched[0] if isinstance(matched[0], dict) else None
    return None


def _decision_base(ts: int) -> PilotDecision:
    return {
        "schema_version": int(DECISION_SCHEMA_VERSION),
        "ts": ts,
        "decision": "COLD_START",
        "tier_used": "NONE",
        "memory": None,
        "gates": {},
        "proposed_action": None,
        "allow": False,
        "size_multiplier": 1.0,
    }


def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _extract_decision_join_keys(setup_event: Dict[str, Any]) -> Dict[str, Any]:
    trade_id = _safe_str(setup_event.get("trade_id") or "")
    symbol = _safe_str(setup_event.get("symbol") or "").upper()
    account_label = _safe_str(setup_event.get("account_label") or setup_event.get("label") or "") or ACCOUNT_LABEL
    timeframe = _safe_str(setup_event.get("timeframe") or "")

    client_trade_id = _safe_str(setup_event.get("client_trade_id") or setup_event.get("clientTradeId") or "")

    policy_hash = ""
    if isinstance(setup_event.get("policy"), dict):
        policy_hash = _safe_str(setup_event["policy"].get("policy_hash") or "")

    memory_fingerprint = ""
    payload = setup_event.get("payload")
    if isinstance(payload, dict):
        feats = payload.get("features")
        if isinstance(feats, dict):
            memory_fingerprint = _safe_str(feats.get("memory_fingerprint") or "")

    out = {
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "timeframe": timeframe,
        "policy_hash": policy_hash,
        "memory_fingerprint": memory_fingerprint,
    }
    return {k: v for k, v in out.items() if v != "" or k == "trade_id"}


def _set_allow_and_size(out: PilotDecision) -> None:
    dec = str(out.get("decision") or "").upper().strip()

    if dec == "ALLOW_TRADE":
        allow = True
    elif dec == "COLD_START":
        allow = bool(AI_PILOT_ALLOW_COLD_START)
    else:
        allow = False

    out["allow"] = bool(allow)

    # default multiplier
    mult = 1.0

    # memory-derived multiplier (if any)
    mem = out.get("memory")
    if isinstance(mem, dict):
        for k in ("size_multiplier", "sizeMult", "multiplier"):
            if k in mem:
                try:
                    mult = float(mem.get(k))  # type: ignore[arg-type]
                    break
                except Exception:
                    pass

    # ✅ cold-start semantics: cap/force to configured multiplier
    if dec == "COLD_START":
        try:
            cold = float(AI_PILOT_COLD_START_SIZE_MULT)
        except Exception:
            cold = 0.25
        try:
            mult = min(float(mult), cold)
        except Exception:
            mult = cold
        g = out.get("gates")
        if isinstance(g, dict):
            g.setdefault("cold_start_size_mult", cold)
            out["gates"] = g

    try:
        out["size_multiplier"] = float(mult)
    except Exception:
        out["size_multiplier"] = 1.0


def _memory_gating_active_for_account(account_label: str) -> bool:
    if not AI_PILOT_USE_MEMORY_GATES:
        return False
    if not AI_PILOT_MEMORY_CANARY_ONLY:
        return True
    if not canary_enabled():
        return False
    return is_canary_account(account_label)


def pilot_decide(setup_event: Dict[str, Any]) -> PilotDecision:
    ts = _now_ms()
    out: PilotDecision = _decision_base(ts)

    try:
        out.update(_extract_decision_join_keys(setup_event))
    except Exception:
        pass

    acct = str(out.get("account_label") or ACCOUNT_LABEL)

    if not _memory_gating_active_for_account(acct):
        out["decision"] = "COLD_START"
        out["tier_used"] = "NONE"
        out["memory"] = None
        out["gates"] = {"reason": "memory_gates_disabled_or_not_canary"}
        _set_allow_and_size(out)
        _write_decision(dict(out))
        return out

    if query_memories_tiered is None or QueryOptions is None:
        out["decision"] = "BLOCKED_BY_GATES"
        out["gates"] = {"reason": "ai_memory_store_missing"}
        _set_allow_and_size(out)
        _write_decision(dict(out))
        return out

    if evaluate_memory_gates is None:
        out["decision"] = "BLOCKED_BY_GATES"
        out["gates"] = {"reason": "ai_gatekeeper_missing"}
        _set_allow_and_size(out)
        _write_decision(dict(out))
        return out

    try:
        opts = QueryOptions(
            k=5,
            min_n=int(AI_MEM_MIN_N_ANY),
            min_n_any=int(AI_MEM_MIN_N_ANY),
            min_n_symbol=int(AI_MEM_MIN_N_SYMBOL),
        )
        r = query_memories_tiered(setup_event, opts)
        tier = str(r.get("tier_used") or "NONE")
        best = _safe_first_match(r)

        out["tier_used"] = tier
        out["memory"] = best

        if not best:
            live_mode = not DRY_RUN
            if live_mode and AI_PILOT_BLOCK_COLD_START_IN_LIVE and (not AI_PILOT_ALLOW_COLD_START):
                out["decision"] = "BLOCKED_BY_GATES"
                out["gates"] = {"reason": "cold_start_blocked_in_live"}
            else:
                out["decision"] = "COLD_START" if AI_PILOT_ALLOW_COLD_START else "BLOCKED_BY_GATES"
                out["gates"] = {"reason": "no_matches"}
            _set_allow_and_size(out)
            _write_decision(dict(out))
            return out

        min_n_eff = int(AI_MEM_MIN_N_ANY)
        if tier == "A" and int(AI_MEM_MIN_N_SYMBOL) > 0:
            min_n_eff = int(AI_MEM_MIN_N_SYMBOL)

        ok, info = evaluate_memory_gates(
            best,
            min_n_effective=min_n_eff,
            min_r_mean=float(AI_MEM_MIN_R_MEAN),
            max_loss_rate=float(AI_MEM_MAX_LOSS_RATE),
            min_abs_r_sum=float(AI_MEM_MIN_ABS_R_SUM),
        )

        gates = {
            "min_n_effective": min_n_eff,
            "min_r_mean": float(AI_MEM_MIN_R_MEAN),
            "max_loss_rate": float(AI_MEM_MAX_LOSS_RATE),
            "min_abs_r_sum": float(AI_MEM_MIN_ABS_R_SUM),
            **(info if isinstance(info, dict) else {"reason": "unknown"}),
        }
        out["gates"] = gates
        out["decision"] = "ALLOW_TRADE" if ok else ("BLOCKED_BY_GATES" if AI_PILOT_BLOCK_ON_BAD_MEMORY else "COLD_START")

        _set_allow_and_size(out)
        _write_decision(dict(out))
        return out

    except Exception as e:
        out["decision"] = "BLOCKED_BY_GATES"
        out["tier_used"] = "NONE"
        out["memory"] = None
        out["gates"] = {"reason": "error", "error": str(e)}
        _set_allow_and_size(out)
        _write_decision(dict(out))
        return out


def _extract_setup_from_action(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(action, dict):
        return None
    if isinstance(action.get("setup_context"), dict):
        return action["setup_context"]  # type: ignore[return-value]
    extra = action.get("extra")
    if isinstance(extra, dict) and isinstance(extra.get("setup_context"), dict):
        return extra["setup_context"]  # type: ignore[return-value]
    ctx = action.get("context")
    if isinstance(ctx, dict) and isinstance(ctx.get("setup_context"), dict):
        return ctx["setup_context"]  # type: ignore[return-value]
    return None


def _apply_memory_gates(actions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta = {
        "enabled": AI_PILOT_USE_MEMORY_GATES,
        "canary_only": AI_PILOT_MEMORY_CANARY_ONLY,
        "canary_enabled": bool(canary_enabled()),
        "account_label": ACCOUNT_LABEL,
        "is_canary_account": bool(is_canary_account(ACCOUNT_LABEL)),
        "active_for_account": bool(_memory_gating_active_for_account(ACCOUNT_LABEL)),
        "total_in": len(actions),
        "total_out": 0,
        "blocked": 0,
        "cold_start": 0,
        "allowed": 0,
        "no_setup_context": 0,
    }

    if not AI_PILOT_USE_MEMORY_GATES:
        meta["total_out"] = len(actions)
        return actions, meta

    out: List[Dict[str, Any]] = []
    for a in actions:
        if not isinstance(a, dict):
            continue

        setup_ctx = _extract_setup_from_action(a)
        if not setup_ctx:
            meta["no_setup_context"] += 1
            aa = dict(a)
            aa.setdefault("meta", {})
            if isinstance(aa["meta"], dict):
                aa["meta"]["memory_gate"] = {"decision": "SKIP", "reason": "no_setup_context"}
            out.append(aa)
            continue

        decision = pilot_decide(setup_ctx)
        dec = str(decision.get("decision") or "BLOCKED_BY_GATES")

        aa = dict(a)
        aa.setdefault("meta", {})
        if isinstance(aa["meta"], dict):
            aa["meta"]["memory_gate"] = decision

        if dec == "ALLOW_TRADE":
            meta["allowed"] += 1
            out.append(aa)
        elif dec == "COLD_START":
            meta["cold_start"] += 1
            if AI_PILOT_ALLOW_COLD_START:
                out.append(aa)
            else:
                meta["blocked"] += 1
        else:
            meta["blocked"] += 1
            if (not AI_PILOT_BLOCK_ON_BAD_MEMORY) and DRY_RUN:
                out.append(aa)

    meta["total_out"] = len(out)
    return out, meta


def _run_sample_policy(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not USE_SAMPLE_POLICY or sample_evaluate_state is None:
        return []
    try:
        raw_actions = sample_evaluate_state(ai_state)  # type: ignore[misc]
        if not isinstance(raw_actions, list):
            return []

        tf_default = os.getenv("AI_PILOT_DEFAULT_TIMEFRAME", "5").strip() or "5"

        ai_actions: List[Dict[str, Any]] = []
        for raw in raw_actions:
            if not isinstance(raw, dict):
                continue
            symbol = raw.get("symbol")
            side = raw.get("side")
            if not symbol or not side:
                continue

            sym = str(symbol).strip().upper()
            side_s = str(side).strip().lower()

            ai_action = build_trade_action_from_sample(
                account_label=ACCOUNT_LABEL,
                symbol=sym,
                side=side_s,
                reason=str(raw.get("reason") or "sample_policy"),
                risk_R=1.0,
                expected_R=2.0,
                size_fraction=1.0,
                confidence=float(raw.get("confidence", 0.6)),
                tags=["sample_policy"],
                model_id="SAMPLE_POLICY_V1",
                extra={"legacy_action": raw},
            )

            trade_id = str(raw.get("trade_id") or raw.get("client_trade_id") or raw.get("clientTradeId") or "").strip()
            if not trade_id:
                trade_id = f"SAMPLE_{_now_ms()}_{sym}_{side_s}".replace(" ", "")

            setup_ctx = {
                "trade_id": trade_id,
                "client_trade_id": trade_id,
                "symbol": sym,
                "account_label": ACCOUNT_LABEL,
                "timeframe": str(raw.get("timeframe") or tf_default),
                "side": "buy" if side_s in ("buy", "long") else ("sell" if side_s in ("sell", "short") else side_s),
                "policy": {"policy_hash": "SAMPLE_POLICY_V1"},
                "payload": {"features": {"memory_fingerprint": str(raw.get("memory_fingerprint") or ""), "source": "sample_policy"}},
            }

            ai_action["setup_context"] = setup_ctx
            ai_actions.append(ai_action)

        return ai_actions
    except Exception as e:
        alert_bot_error("ai_pilot", f"sample_policy error: {e}", "ERROR")
        return []


def _run_core_policy(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not USE_CORE_POLICY or core_evaluate_state is None:
        return []
    try:
        actions = core_evaluate_state(ai_state)  # type: ignore[misc]
        if not isinstance(actions, list):
            return []
        return [a for a in actions if isinstance(a, dict)]
    except Exception as e:
        alert_bot_error("ai_pilot", f"core_policy error: {e}", "ERROR")
        return []


def _dispatch_actions(actions: List[Dict[str, Any]], *, label: str) -> int:
    """
    Write actions to the action bus (NOT direct file append).
    Bus handles:
      - stamping ts_ms/source/dry_run/account_label defaults
      - dedupe (account_label|type|trade_id)
    """
    if not actions or not WRITE_ACTIONS:
        return 0

    if bus_append_actions is None:
        alert_bot_error("ai_pilot", "ai_action_bus missing; cannot write actions", "ERROR")
        return 0

    try:
        # label becomes default account_label if missing.
        return int(bus_append_actions(actions, source="ai_pilot", label=label, dry_run=DRY_RUN))
    except Exception as e:
        alert_bot_error("ai_pilot", f"dispatch_actions(bus) error: {e}", "ERROR")
        return 0


def run_once() -> None:
    record_heartbeat("ai_pilot")
    ai_state = _build_ai_state()
    safety = ai_state.get("safety") or {}
    if safety.get("is_safe") is False:
        logger.warning("🚫 Snapshot unsafe, skipping policy eval: %s", safety.get("reasons"))
        return

    sample_actions = _run_sample_policy(ai_state)
    core_actions = _run_core_policy(ai_state)

    if AI_PILOT_USE_MEMORY_GATES:
        sample_actions, meta_s = _apply_memory_gates(sample_actions)
        core_actions, meta_c = _apply_memory_gates(core_actions)
        logger.info("🧠 Memory gates meta: sample=%s core=%s", meta_s, meta_c)

    _dispatch_actions(sample_actions, label=ACCOUNT_LABEL)
    _dispatch_actions(core_actions, label=ACCOUNT_LABEL)


def loop() -> None:
    if not AI_PILOT_ENABLED:
        logger.warning("AI Pilot disabled (AI_PILOT_ENABLED=false). Exiting.")
        return

    mode_bits = ["DRY-RUN" if DRY_RUN else "LIVE?"]
    if USE_SAMPLE_POLICY:
        mode_bits.append("sample_policy")
    if USE_CORE_POLICY:
        mode_bits.append("core_policy")
    if WRITE_ACTIONS:
        mode_bits.append("write_actions")
    if AI_PILOT_USE_MEMORY_GATES:
        mode_bits.append("memory_gates")
    if AI_PILOT_MEMORY_CANARY_ONLY:
        mode_bits.append("canary_only")
    mode_str = ", ".join(mode_bits)

    try:
        send_tg(f"🧠 AI Pilot started for label={ACCOUNT_LABEL} ({mode_str}, poll={POLL_SECONDS}s)")
    except Exception:
        logger.info("AI Pilot started for label=%s (%s, poll=%ss)", ACCOUNT_LABEL, mode_str, POLL_SECONDS)

    while True:
        t0 = time.time()
        try:
            run_once()
        except Exception as e:
            alert_bot_error("ai_pilot", f"loop error: {e}", "ERROR")

        elapsed = time.time() - t0
        time.sleep(max(0.5, POLL_SECONDS - elapsed))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run one evaluation cycle and exit")
    args = ap.parse_args()
    if args.once:
        run_once()
        return 0
    loop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
