#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated, policy-aware)

Purpose
-------
- Consume signals from an append-only JSONL file (signals/observed.jsonl).
- For EACH strategy defined in config/strategies.yaml:
    • Check symbol + timeframe match via strategy_gate.
    • Check enabled + automation mode.
    • Run AI gating (classifier + min-score policy).
    • Run correlation gate.
    • Size entries (bayesian + risk_capped, policy-adjusted risk).
    • Log feature snapshot for setup memory (feature_logger v3, trade_id-based).
    • Place live or paper entries depending on automation_mode.

Automation modes
----------------
- OFF         : ignore strategy.
- LEARN_DRY   : run AI + logging, NO live orders (paper / learning only).
- LIVE_CANARY : live orders with small risk (as per strategies.yaml + policy).
- LIVE_FULL   : normal live trading (once proven).

Exits:
- TP/SL handled by a separate bot (tp_sl_manager) for LIVE positions.
- For PAPER positions, TP/SL are handled by PaperBroker.update_price(...).

Notes
-----
- Stateless across runs except for the cursor file.
- Strategy definitions live in config/strategies.yaml.
"""

from __future__ import annotations

import os
import json
import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List, Any, Iterable, Tuple

from app.core.config import settings

# ---------- Robust logger import ---------- #
try:
    # Preferred: dedicated logger module
    from app.core.logger import get_logger, bind_context  # type: ignore
except Exception:
    try:
        # Fallback: older / alternate logging module
        from app.core.log import get_logger as _get_logger  # type: ignore

        import logging

        def bind_context(logger: "logging.Logger", **ctx):
            """
            Minimal bind_context fallback:
            just returns the same logger, ignoring context.
            """
            return logger

        get_logger = _get_logger  # type: ignore
    except Exception:
        # Last resort: plain stdlib logging
        import logging

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler()
                fmt = logging.Formatter(
                    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
                )
                handler.setFormatter(fmt)
            logger_.addHandler(handler)
            logger_.setLevel(logging.INFO)
            return logger_

        def bind_context(logger: "logging.Logger", **ctx):  # type: ignore
            return logger

from app.core.bybit_client import Bybit
from app.core.notifier_bot import tg_send
from app.core.trade_classifier import classify as classify_trade
from app.core.corr_gate_v2 import allow as corr_allow
from app.core.sizing import bayesian_size, risk_capped_qty
from app.core.strategy_gate import (
    get_strategies_for_signal,
    strategy_label,
    strategy_risk_pct,
)
from app.core.portfolio_guard import can_open_trade
from app.core.flashback_common import get_equity_usdt, record_heartbeat, GLOBAL_BREAKER
from app.core.session_guard import should_block_trading
from app.ai.setup_memory_policy import get_risk_multiplier, get_min_ai_score

# NEW: orders bus for AI/journal spine
from app.core.orders_bus import record_order_event

# NEW: trade_id-based feature snapshot logger
from app.ai.feature_logger import log_features_at_open

# NEW: AI events (setup/outcome logs → state/ai_events)
from app.ai.ai_events_spine import build_setup_context, publish_ai_event

# NEW: Policy decision logger (audit)
try:
    from app.ai.policy_log import record_policy_decision  # type: ignore
except Exception:  # pragma: no cover
    def record_policy_decision(*args, **kwargs):
        return None

# NEW: Position snapshot for feature context
from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot

# NEW: Paper Broker for LEARN_DRY / EXEC_DRY_RUN
from app.sim.paper_broker import PaperBroker  # type: ignore

log = get_logger("executor_v2")

# Global dry-run override (env EXEC_DRY_RUN=true/false)
EXEC_DRY_RUN: bool = os.getenv("EXEC_DRY_RUN", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

# Paths
ROOT: Path = settings.ROOT
SIGNAL_FILE: Path = ROOT / "signals" / "observed.jsonl"
CURSOR_FILE: Path = ROOT / "state" / "observed.cursor"

SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

# Latency logging
LATENCY_LOG_PATH: Path = ROOT / "state" / "latency_exec.jsonl"
LATENCY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

try:
    LATENCY_WARN_MS = int(os.getenv("EXECUTOR_LATENCY_WARN_MS", "1500"))
except Exception:
    LATENCY_WARN_MS = 1500

# Lock created by execution_recovery_daemon when accounting <-> reality diverge
SUSPECT_LOCK_PATH: Path = ROOT / "state" / "execution_suspect.lock"


# ---------------------------------------------------------------------------
# BYBIT CLIENTS — ONE PER SUBUID (OR MAIN) + PAPER BROKERS
# ---------------------------------------------------------------------------

_TRADE_CLIENTS: Dict[str, Bybit] = {}
_PAPER_BROKER_CACHE: Dict[str, PaperBroker] = {}


def get_trade_client(sub_uid: Optional[str]) -> Bybit:
    """
    Return a Bybit trade client for the given sub_uid.

    - sub_uid is a string UID (e.g. "524649709") for unified_sub accounts.
    - If sub_uid is None/empty, we use the main unified account client.
    """
    key = str(sub_uid) if sub_uid else "main"

    client = _TRADE_CLIENTS.get(key)
    if client is not None:
        return client

    if sub_uid:
        client = Bybit("trade", sub_uid=sub_uid)
    else:
        client = Bybit("trade")

    _TRADE_CLIENTS[key] = client
    return client


def get_paper_broker(
    account_label: str,
    starting_equity: float,
) -> PaperBroker:
    """
    Return a PaperBroker for the given account_label, creating it if needed.

    starting_equity is only used on first creation; after that the on-disk
    ledger decides.
    """
    broker = _PAPER_BROKER_CACHE.get(account_label)
    if broker is not None:
        return broker

    broker = PaperBroker.load_or_create(
        account_label=account_label,
        starting_equity=starting_equity,
    )
    _PAPER_BROKER_CACHE[account_label] = broker
    return broker


# ---------- CURSOR HELPERS ---------- #

def load_cursor() -> int:
    if not CURSOR_FILE.exists():
        return 0
    try:
        return int(CURSOR_FILE.read_text().strip() or "0")
    except Exception:
        return 0


def save_cursor(pos: int) -> None:
    try:
        CURSOR_FILE.write_text(str(pos))
    except Exception as e:
        log.warning("failed to save cursor %s: %r", pos, e)


# ---------- LATENCY HELPERS ---------- #

def record_latency(
    event: str,
    symbol: str,
    strat: str,
    mode: str,
    duration_ms: int,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Append a latency record to state/latency_exec.jsonl.

    event: short label, e.g. "entry_order" or "decision_pipeline"
    duration_ms: measured latency in milliseconds
    extra: any additional context (sub_uid, account_label, qty, etc.)
    """
    row: Dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "event": event,
        "symbol": symbol,
        "strategy": strat,
        "mode": mode,
        "duration_ms": int(duration_ms),
    }
    if extra:
        row["extra"] = extra

    try:
        with LATENCY_LOG_PATH.open("ab") as f:
            f.write(json.dumps(row).encode("utf-8") + b"\n")
    except Exception as e:
        log.warning("failed to write latency log: %r", e)


# ---------- AI GATE WRAPPER ---------- #

def run_ai_gate(signal: Dict[str, Any], strat_id: str, bound_log) -> Dict[str, Any]:
    """
    Unified wrapper around trade_classifier + policy threshold.

    Returns a dict with at least:
      {
        "allow": bool,
        "score": float | None,
        "reason": str,
        "features": dict
      }

    Every decision is also logged to state/ai_policy_decisions.jsonl.
    """
    # Default decision in case classifier explodes
    decision: Dict[str, Any] = {
        "allow": True,
        "score": None,
        "reason": "default_allow_fallback",
        "features": {},
    }

    # 1) Classifier itself
    try:
        clf = classify_trade(signal, strat_id)
    except Exception as e:
        bound_log.warning(
            "AI classifier crashed or misbehaved for [%s]: %r — bypassing gate (allow=True).",
            strat_id,
            e,
        )
        decision = {
            "allow": True,
            "score": None,
            "reason": f"classifier_error: {e}",
            "features": {},
        }
        record_policy_decision(
            strategy_id=strat_id,
            allow=decision["allow"],
            score=decision["score"],
            reason=decision["reason"],
            signal=signal,
        )
        return decision

    if not isinstance(clf, dict):
        bound_log.warning(
            "AI classifier returned non-dict for [%s]: %r — treating as allow=True.",
            strat_id,
            clf,
        )
        decision = {
            "allow": True,
            "score": None,
            "reason": "classifier_non_dict",
            "features": {},
        }
        record_policy_decision(
            strategy_id=strat_id,
            allow=decision["allow"],
            score=decision["score"],
            reason=decision["reason"],
            signal=signal,
        )
        return decision

    allow = bool(clf.get("allow", True))
    score = clf.get("score")
    reason = clf.get("reason") or clf.get("why") or "ok"
    features = clf.get("features") or {}

    # 2) Policy-based min score per strategy
    min_score = get_min_ai_score(strat_id)
    score_f: Optional[float]
    try:
        score_f = float(score) if score is not None else None
    except Exception:
        score_f = None

    if min_score > 0 and score_f is not None and score_f < min_score:
        bound_log.info(
            "AI score %.3f < min threshold %.3f for [%s]; rejecting trade.",
            score_f,
            min_score,
            strat_id,
        )
        decision = {
            "allow": False,
            "score": score_f,
            "reason": f"score_below_min ({score_f:.3f} < {min_score:.3f})",
            "features": features,
        }
        record_policy_decision(
            strategy_id=strat_id,
            allow=decision["allow"],
            score=decision["score"],
            reason=decision["reason"],
            signal=signal,
        )
        return decision

    if not allow:
        bound_log.info("AI gate rejected [%s]: %s", strat_id, reason)

    decision = {
        "allow": allow,
        "score": score_f,
        "reason": reason,
        "features": features,
    }

    record_policy_decision(
        strategy_id=strat_id,
        allow=decision["allow"],
        score=decision["score"],
        reason=decision["reason"],
        signal=signal,
    )
    return decision


# ---------- SIGNAL PROCESSOR ---------- #

def _normalize_strategies_for_signal(
    strategies: Any,
) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """
    Normalize whatever get_strategies_for_signal(...) returns into
    an iterable of (strat_name, strat_cfg) pairs.
    """
    if not strategies:
        return []

    # Case 1: dict[name] -> cfg
    if isinstance(strategies, dict):
        return strategies.items()

    # Case 2: list-like
    if isinstance(strategies, (list, tuple)):
        if not strategies:
            return []

        first = strategies[0]

        # 2a: list of (name, cfg) tuples
        if isinstance(first, (list, tuple)) and len(first) == 2:
            return strategies

        # 2b: list of dicts → extract name from each
        if isinstance(first, dict):
            normalized: List[Tuple[str, Dict[str, Any]]] = []
            for cfg in strategies:
                if not isinstance(cfg, dict):
                    continue
                name = (
                    cfg.get("name")
                    or cfg.get("id")
                    or cfg.get("label")
                    or cfg.get("strategy_name")
                    or "unnamed_strategy"
                )
                normalized.append((str(name), cfg))
            return normalized

        return []

    return []


async def process_signal_line(line: str) -> None:
    """
    Process one raw signal line from signals/observed.jsonl.
    """
    try:
        sig = json.loads(line)
    except Exception:
        log.warning("Invalid JSON in observed.jsonl: %r", line[:200])
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
    if not symbol or not tf:
        return

    strategies = get_strategies_for_signal(symbol, tf)
    strat_items = _normalize_strategies_for_signal(strategies)

    if not strat_items:
        return

    for strat_name, strat_cfg in strat_items:
        try:
            await handle_strategy_signal(strat_name, strat_cfg, sig)
        except Exception as e:
            log.exception("Strategy error (%s): %r", strat_name, e)


# ---------- STRATEGY PROCESSOR ---------- #

def _automation_mode_from_cfg(cfg: Dict[str, Any]) -> str:
    mode = str(cfg.get("automation_mode", "OFF")).upper().strip()
    if mode not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
        mode = "OFF"
    return mode


def _normalize_paper_side(signal_side: str) -> str:
    """
    Normalize signal side into PaperBroker side ("long"/"short").
    Accepts: "buy"/"sell"/"long"/"short" (case-insensitive).
    """
    s = str(signal_side or "").strip().lower()
    if s in ("buy", "long"):
        return "long"
    if s in ("sell", "short"):
        return "short"
    raise ValueError(f"Unsupported side value for paper entry: {signal_side!r}")


async def handle_strategy_signal(
    strat_name: str,
    strat_cfg: Dict[str, Any],
    sig: Dict[str, Any],
) -> None:
    strat_id = strategy_label(strat_cfg)  # e.g. "Sub1_Trend (sub 524630315)"
    bound = bind_context(log, strat=strat_id)

    enabled = bool(strat_cfg.get("enabled", False))
    mode_raw = _automation_mode_from_cfg(strat_cfg)

    if not enabled or mode_raw == "OFF":
        bound.debug("strategy disabled or automation_mode=OFF")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
    side = sig.get("side")

    # Price can come from top-level or from debug block
    price = sig.get("price") or sig.get("last") or sig.get("close")
    debug = sig.get("debug") or {}
    if price is None and isinstance(debug, dict):
        for key in ("last_close", "last", "close"):
            v = debug.get(key)
            if v is not None:
                price = v
                break

    if not symbol or not tf or not side or price is None:
        bound.warning("missing required fields in signal: %r", sig)
        return

    try:
        price_f = float(price)
    except Exception:
        bound.warning("invalid price in signal: %r", sig)
        return

    # Normalize mode for feature logging
    if EXEC_DRY_RUN:
        trade_mode = "PAPER"
    else:
        if mode_raw == "LEARN_DRY":
            trade_mode = "PAPER"
        elif mode_raw == "LIVE_CANARY":
            trade_mode = "LIVE_CANARY"
        elif mode_raw == "LIVE_FULL":
            trade_mode = "LIVE_FULL"
        else:
            trade_mode = mode_raw

    # Sub UID for logging + guards
    sub_uid = str(
        strat_cfg.get("sub_uid")
        or strat_cfg.get("subAccountId")
        or strat_cfg.get("accountId")
        or strat_cfg.get("subId")
        or ""
    )

    # Account label for orders_bus / state joins (e.g. "main", "flashback10")
    account_label = str(
        strat_cfg.get("account_label")
        or strat_cfg.get("label")
        or strat_cfg.get("account_label_slug")
        or "main"
    )

    # Whether execution_recovery_daemon has raised a hard lock
    lock_active = SUSPECT_LOCK_PATH.exists()

    # Global breaker flag snapshot
    try:
        breaker_on = bool(GLOBAL_BREAKER.get("on", False))
    except Exception:
        breaker_on = False

    # Training vs live profile
    is_training_mode = EXEC_DRY_RUN or mode_raw == "LEARN_DRY"

    # Timestamp for latency measurement (signal → decision → order)
    started_ms = int(time.time() * 1000)

    # ---------- Session Guard (loss streak / daily limits) ---------- #
    try:
        if should_block_trading():
            bound.info("Session Guard blocking new trades (limits reached).")
            return
    except Exception as e:
        bound.warning("Session Guard error; bypassing: %r", e)

    # ---------- AI Classifier + Policy Gate ---------- #
    ai = run_ai_gate(sig, strat_id, bound)
    if not ai["allow"]:
        return

    # ---------- Correlation gate (unpack (allowed, reason)) ---------- #
    try:
        allowed_corr, corr_reason = corr_allow(symbol)
    except Exception as e:
        bound.warning("Correlation gate error for %s: %r; bypassing.", symbol, e)
        allowed_corr, corr_reason = True, "corr_gate_v2 exception, bypassed"

    if not allowed_corr:
        bound.info("Correlation gate rejected for %s: %s", symbol, corr_reason)
        return

    # ---------- Sizing + Risk Policy ---------- #
    try:
        base_risk_pct = Decimal(str(strategy_risk_pct(strat_cfg)))
    except Exception:
        base_risk_pct = Decimal("0")

    risk_mult = Decimal(str(get_risk_multiplier(strat_id)))
    eff_risk_pct = base_risk_pct * risk_mult

    if eff_risk_pct <= 0:
        bound.info("effective risk_pct <= 0 for %s; skipping.", strat_id)
        return

    try:
        equity_val = Decimal(str(get_equity_usdt()))
    except Exception as e:
        bound.warning("get_equity_usdt failed: %r; assuming equity=0.", e)
        equity_val = Decimal("0")

    # Stop distance used for risk sizing (keep conservative)
    stop_pct_for_size = 0.005  # 0.5% dummy for sizing
    stop_distance = Decimal(str(price_f * stop_pct_for_size))

    qty_suggested, risk_usd = bayesian_size(
        symbol=symbol,
        equity_usd=equity_val,
        risk_pct=float(eff_risk_pct),
        stop_distance=stop_distance,
    )

    if qty_suggested <= 0 or risk_usd <= 0:
        bound.info(
            "bayesian_size returned non-positive sizing for %s; equity=%s risk_pct=%s",
            strat_id,
            equity_val,
            eff_risk_pct,
        )
        return

    qty_capped, risk_capped = risk_capped_qty(
        symbol=symbol,
        qty=qty_suggested,
        equity_usd=equity_val,
        max_risk_pct=float(eff_risk_pct),
        stop_distance=stop_distance,
    )

    if qty_capped <= 0 or risk_capped <= 0:
        bound.info(
            "qty <= 0 after risk_capped_qty; skipping entry. "
            "qty_suggested=%s risk_usd=%s equity=%s risk_pct=%s",
            qty_suggested,
            risk_usd,
            equity_val,
            eff_risk_pct,
        )
        return

    # ---------- Portfolio Guard (global / per-trade caps) ---------- #
    try:
        guard_ok, guard_reason = can_open_trade(
            sub_uid=sub_uid or None,
            strategy_name=strat_cfg.get("name", strat_name),
            risk_usd=risk_capped,
            equity_now_usd=equity_val,
        )
    except TypeError:
        try:
            guard_ok = bool(can_open_trade(symbol, float(risk_capped)))
            guard_reason = "legacy_bool_guard"
        except Exception as e:
            bound.warning("Portfolio guard failed for %s: %r; bypassing.", symbol, e)
            guard_ok, guard_reason = True, "guard_exception_bypass"
    except Exception as e:
        bound.warning("Portfolio guard failed for %s: %r; bypassing.", symbol, e)
        guard_ok, guard_reason = True, "guard_exception_bypass"

    if not guard_ok:
        bound.info("Portfolio guard blocked trade for %s: %s", symbol, guard_reason)
        return

    # ---------- Decision-pipeline latency ---------- #
    decision_done_ms = int(time.time() * 1000)
    try:
        record_latency(
            event="decision_pipeline",
            symbol=symbol,
            strat=strat_id,
            mode=trade_mode,
            duration_ms=decision_done_ms - started_ms,
            extra={
                "sub_uid": sub_uid or None,
                "account_label": account_label,
                "equity_usd": float(equity_val),
                "eff_risk_pct": float(eff_risk_pct),
                "lock_active": lock_active,
                "breaker_on": breaker_on,
            },
        )
    except Exception as e:
        bound.warning("latency logging (decision_pipeline) failed: %r", e)

    # ---------- Unified trade_id (orderLinkId) + feature snapshot + setup event ---------- #
    ts_open_ms = int(time.time() * 1000)
    strat_safe = strat_id.replace(" ", "_").replace("(", "").replace(")", "")
    trade_id = f"{strat_safe}-{ts_open_ms}"

    try:
        ai_score = ai.get("score")
        ai_reason = ai.get("reason", "")
        ai_features = ai.get("features") or {}

        # Pull a compact snapshot of current positions for context
        positions_view: Optional[Dict[str, Any]] = None
        try:
            pos_rows = bus_get_positions_snapshot(
                label=None,
                category="linear",
                max_age_seconds=10,
                allow_rest_fallback=True,
            )
            if isinstance(pos_rows, list):
                canon: List[Dict[str, Any]] = []
                for p in pos_rows:
                    if not isinstance(p, dict):
                        continue
                    try:
                        canon.append(
                            {
                                "symbol": p.get("symbol"),
                                "side": p.get("side"),
                                "size": p.get("size"),
                                "avgPrice": p.get("avgPrice")
                                or p.get("entryPrice")
                                or p.get("avg_price"),
                                "unrealisedPnl": p.get("unrealisedPnl")
                                or p.get("unrealised_pnl"),
                                "markPrice": p.get("markPrice") or p.get("mark_price"),
                                "stopLoss": p.get("stopLoss")
                                or p.get("stopLossPrice")
                                or p.get("slPrice"),
                                "takeProfit": p.get("takeProfit") or p.get("tpPrice"),
                                "sub_uid": p.get("sub_uid")
                                or p.get("subAccountId")
                                or p.get("accountId")
                                or p.get("subId"),
                                "account_label": p.get("account_label")
                                or p.get("label")
                                or None,
                            }
                        )
                    except Exception:
                        continue
                positions_view = {
                    "ts_ms": int(time.time() * 1000),
                    "positions": canon,
                }
        except Exception:
            positions_view = None

        # Regime tag (hook) if the signal provides one
        regime_tag = sig.get("regime") or sig.get("market_regime")

        features_payload: Dict[str, Any] = {
            **ai_features,
            "schema_version": "setup_features_v1",
            "signal": sig,
            "symbol": symbol,
            "timeframe": str(tf),
            "side": side,
            "account_label": account_label,
            "sub_uid": sub_uid or None,
            "strategy_name": strat_cfg.get("name", strat_name),
            "automation_mode": mode_raw,
            "trade_mode": trade_mode,
            "equity_usd": float(equity_val),
            "risk_usd": float(risk_capped),
            "risk_pct": float(eff_risk_pct),
            "ai_score": float(ai_score) if ai_score is not None else None,
            "ai_reason": str(ai_reason),
            "execution_lock_active": lock_active,
            "execution_global_breaker_on": breaker_on,
            "positions_snapshot": positions_view,
            "size": float(qty_capped),
            "qty": float(qty_capped),
            "stop_pct_for_size": float(stop_pct_for_size),
            "train_mode": "DRY_RUN_V1" if is_training_mode else "LIVE_OR_CANARY",
        }

        if regime_tag is not None:
            features_payload["regime_tag"] = regime_tag

        # Feature logger (local setup memory / feature store)
        log_features_at_open(
            trade_id=trade_id,
            ts_open_ms=ts_open_ms,
            symbol=symbol,
            sub_uid=sub_uid,
            strategy_name=strat_cfg.get("name", strat_name),
            setup_type=str(sig.get("setup_type") or "unknown"),
            mode=trade_mode,
            features=features_payload,
        )

        # AI events: setup_context → state/ai_events/setups.jsonl
        try:
            setup_event = build_setup_context(
                trade_id=trade_id,
                symbol=symbol,
                account_label=account_label,
                strategy=strat_cfg.get("name", strat_name),
                features=features_payload,
                setup_type=str(sig.get("setup_type") or "unknown"),
                timeframe=str(tf),
                ai_profile=strat_cfg.get("ai_profile") or None,
                extra={
                    "mode": trade_mode,
                    "sub_uid": sub_uid or None,
                },
            )
            publish_ai_event(setup_event)
        except Exception as e:
            bound.warning("AI setup_context logging failed: %r", e)

    except Exception as e:
        bound.warning("feature logging / setup event failed: %r", e)

    # ---------- Execute or paper log (lock-aware + EXEC_DRY_RUN + breaker) ---------- #
    live_mode_requested = mode_raw in ("LIVE_CANARY", "LIVE_FULL")
    live_allowed = (
        live_mode_requested
        and not lock_active
        and not EXEC_DRY_RUN
        and not breaker_on
    )

    if live_mode_requested and lock_active:
        bound.warning(
            "execution_suspect.lock present; forcing PAPER-only for [%s] %s",
            strat_id,
            symbol,
        )
    elif live_mode_requested and EXEC_DRY_RUN:
        bound.info(
            "EXEC_DRY_RUN=true; forcing PAPER-only for [%s] %s (would be %s).",
            strat_id,
            symbol,
            mode_raw,
        )
    elif live_mode_requested and breaker_on:
        bound.info(
            "GLOBAL_BREAKER.on=true; forcing PAPER-only for [%s] %s (mode=%s).",
            strat_id,
            symbol,
            mode_raw,
        )

    if live_allowed:
        await execute_entry(
            symbol=symbol,
            signal_side=str(side),
            qty=float(qty_capped),
            price=price_f,
            strat=strat_id,
            mode=trade_mode,
            sub_uid=sub_uid,
            account_label=account_label,
            trade_id=trade_id,
            bound_log=bound,
            started_ms=started_ms,
        )
    else:
        # PAPER execution path: use PaperBroker to simulate entry and later TP/SL.

        # Training-mode stop/TP profile:
        # - In DRY modes we compress stops/TP to get more completed trades / hour.
        # - Live / non-dry keeps the wider profile.
        if is_training_mode:
            paper_stop_pct = 0.0015  # ~0.15%
            paper_tp_mult = 1.5      # 1.5R
        else:
            paper_stop_pct = 0.005   # 0.5%
            paper_tp_mult = 2.0      # 2R

        try:
            paper_side = _normalize_paper_side(str(side))
        except ValueError as e:
            bound.warning("cannot normalize paper side %r for %s: %r", side, symbol, e)
            return

        stop_distance_f = float(price_f * paper_stop_pct)
        if stop_distance_f <= 0:
            bound.warning("stop_distance <= 0 for %s; skipping PAPER entry.", symbol)
            return

        if paper_side == "long":
            stop_price = max(0.0001, price_f - stop_distance_f)
            take_profit_price = price_f + (paper_tp_mult * stop_distance_f)
        else:  # short
            stop_price = price_f + stop_distance_f
            take_profit_price = max(0.0001, price_f - (paper_tp_mult * stop_distance_f))

        # Extend features with exit info for the paper engine
        features_for_paper = {
            **features_payload,
            "paper_stop_pct": float(paper_stop_pct),
            "paper_tp_mult": float(paper_tp_mult),
            "stop_price": float(stop_price),
            "take_profit_price": float(take_profit_price),
        }

        try:
            broker = get_paper_broker(
                account_label=account_label,
                starting_equity=float(equity_val) if equity_val > 0 else 1000.0,
            )
            broker.open_position(
                symbol=symbol,
                side=paper_side,  # "long"/"short"
                entry_price=price_f,
                stop_price=stop_price,
                take_profit_price=take_profit_price,
                setup_type=str(sig.get("setup_type") or "unknown"),
                timeframe=str(tf),
                features=features_for_paper,
                extra={
                    "mode": trade_mode,
                    "sub_uid": sub_uid or None,
                    "strategy_name": strat_cfg.get("name", strat_name),
                },
                trade_id=trade_id,
                log_setup=False,  # executor already logged setup_context
            )

            bound.info(
                "PAPER entry [%s]: %s %s qty=%s @ ~%s (risk_pct=%s stop=%s tp=%s trade_id=%s)",
                strat_id,
                symbol,
                paper_side,
                qty_capped,
                price_f,
                eff_risk_pct,
                stop_price,
                take_profit_price,
                trade_id,
            )
        except Exception as e:
            bound.warning("PaperBroker entry failed for %s %s: %r", strat_id, symbol, e)


# ---------- EXECUTOR ---------- #

def _normalize_order_side(signal_side: str) -> str:
    """
    Normalize signal side into Bybit API side ("Buy"/"Sell").
    Accepts: "buy"/"sell"/"long"/"short" (case-insensitive).
    """
    s = str(signal_side or "").strip().lower()
    if s in ("buy", "long"):
        return "Buy"
    if s in ("sell", "short"):
        return "Sell"
    raise ValueError(f"Unsupported side value for order: {signal_side!r}")


async def execute_entry(
    symbol: str,
    signal_side: str,
    qty: float,
    price: float,
    strat: str,
    mode: str,
    sub_uid: str,
    account_label: str,
    trade_id: str,
    bound_log,
    started_ms: Optional[int] = None,
) -> None:
    """
    Execute a live entry order and record latency metrics.

    started_ms: timestamp in ms from the moment the strategy pipeline
                started handling this signal. If None, we measure from
                just before placing the order.
    """
    client = get_trade_client(sub_uid or None)
    try:
        order_side = _normalize_order_side(signal_side)
    except ValueError as e:
        bound_log.error("cannot normalize side %r for %s: %r", signal_side, symbol, e)
        return

    order_link_id = trade_id
    success = False
    start_ms = started_ms or int(time.time() * 1000)

    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=order_side,
            qty=qty,
            orderType="Market",
            orderLinkId=order_link_id,
        )
        bound_log.info(
            "LIVE entry executed [%s %s]: %s %s qty=%s trade_id=%s resp=%r",
            mode,
            strat,
            symbol,
            order_side,
            qty,
            trade_id,
            resp,
        )
        success = True

        # --- Orders bus logging (NEW event) --- #
        try:
            result = resp.get("result") if isinstance(resp, dict) else None
            r = result or {}

            order_id = (
                r.get("orderId")
                or r.get("order_id")
                or r.get("orderID")
                or order_link_id
            )
            status = (
                r.get("orderStatus")
                or r.get("order_status")
                or "New"
            )
            order_type = r.get("orderType") or r.get("order_type") or "Market"

            price_str = str(r.get("price") or price)
            qty_str = str(r.get("qty") or qty)
            cum_exec_qty = str(r.get("cumExecQty") or r.get("cum_exec_qty") or 0)
            cum_exec_value = str(r.get("cumExecValue") or r.get("cum_exec_value") or 0)
            cum_exec_fee = str(r.get("cumExecFee") or r.get("cum_exec_fee") or 0)

            record_order_event(
                account_label=account_label,
                symbol=symbol,
                order_id=str(order_id),
                side=order_side,
                order_type=str(order_type),
                status=str(status),
                event_type="NEW",
                price=price_str,
                qty=qty_str,
                cum_exec_qty=cum_exec_qty,
                cum_exec_value=cum_exec_value,
                cum_exec_fee=cum_exec_fee,
                position_side=None,
                reduce_only=r.get("reduceOnly"),
                client_order_id=order_link_id,
                raw={
                    "api_response": r,
                    "sub_uid": sub_uid,
                    "mode": mode,
                    "strategy": strat,
                },
            )
        except Exception as e:
            bound_log.warning("orders_bus logging failed for %s %s: %r", symbol, order_link_id, e)

        try:
            tg_send(
                f"🚀 Entry placed [{mode}/{strat}] {symbol} {order_side} qty={qty} "
                f"trade_id={trade_id}"
            )
        except Exception as e:
            bound_log.warning("telegram send failed: %r", e)
    except Exception as e:
        bound_log.error(
            "order failed for %s %s qty=%s (strat=%s trade_id=%s): %r",
            symbol,
            order_side,
            qty,
            strat,
            trade_id,
            e,
        )
    finally:
        end_ms = int(time.time() * 1000)
        duration = end_ms - start_ms
        try:
            record_latency(
                event="entry_order",
                symbol=symbol,
                strat=strat,
                mode=mode,
                duration_ms=duration,
                extra={
                    "sub_uid": sub_uid or None,
                    "account_label": account_label,
                    "qty": qty,
                    "price": price,
                    "success": success,
                },
            )
            if duration > LATENCY_WARN_MS:
                bound_log.warning(
                    "High executor latency for %s (%s): %d ms (threshold=%d ms)",
                    symbol,
                    strat,
                    duration,
                    LATENCY_WARN_MS,
                )
                try:
                    tg_send(
                        f"⚠️ High executor latency [{mode}/{strat}] {symbol} "
                        f"{duration} ms (threshold={LATENCY_WARN_MS} ms)"
                    )
                except Exception:
                    pass
        except Exception as e:
            bound_log.warning("latency logging failed for %s %s: %r", symbol, trade_id, e)


# ---------- MAIN LOOP ---------- #

async def executor_loop() -> None:
    pos = load_cursor()
    log.info("executor_v2 starting at cursor=%s (EXEC_DRY_RUN=%s)", pos, EXEC_DRY_RUN)

    while True:
        try:
            record_heartbeat("executor_v2")

            if not SIGNAL_FILE.exists():
                await asyncio.sleep(0.5)
                continue

            file_size = SIGNAL_FILE.stat().st_size
            if pos > file_size:
                log.info(
                    "Signal file truncated (size=%s, cursor=%s). Resetting cursor to 0.",
                    file_size,
                    pos,
                )
                pos = 0
                save_cursor(pos)

            with SIGNAL_FILE.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()

                    try:
                        line = raw.decode("utf-8").strip()
                    except Exception as e:
                        log.warning(
                            "executor_v2: failed to decode line at pos=%s: %r",
                            pos,
                            e,
                        )
                        continue

                    if not line:
                        continue

                    await asyncio.sleep(0)  # yield between signals
                    await process_signal_line(line)
                    save_cursor(pos)

            await asyncio.sleep(0.25)

        except Exception as e:
            log.exception("executor loop error: %r; backing off 1s", e)
            await asyncio.sleep(1.0)


# ---------- ENTRYPOINT ---------- #

def main() -> None:
    try:
        asyncio.run(executor_loop())
    except KeyboardInterrupt:
        log.info("executor_v2 stopped by user")


if __name__ == "__main__":
    main()
