#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated, policy-aware)

[... header unchanged ...]
"""

from __future__ import annotations

import os
import re
import json
import asyncio
import time
import hashlib
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List, Any, Iterable, Tuple


from app.core.config import settings

# ---------- Robust logger import ---------- #
try:
    from app.core.logger import get_logger, bind_context  # type: ignore
except Exception:
    try:
        from app.core.log import get_logger as _get_logger  # type: ignore
        import logging

        def bind_context(logger: "logging.Logger", **ctx):
            return logger

        get_logger = _get_logger  # type: ignore
    except Exception:
        import logging

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler()
                fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
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
from app.ai.setup_memory_policy import get_risk_multiplier  # keep: risk multiplier lives here

from app.core.orders_bus import record_order_event
from app.ai.feature_logger import log_features_at_open
from app.ai.ai_events_spine import build_setup_context, publish_ai_event
from app.core.ai_state_bus import build_ai_snapshot, validate_snapshot_v2


# ✅ Decision enforcer (manual blocks + pilot decisions)
from app.ai.ai_decision_enforcer import enforce_decision

# ✅ Pilot (legacy decision schema v1) producer
try:
    from app.bots.ai_pilot import pilot_decide  # type: ignore
except Exception:
    pilot_decide = None  # type: ignore

from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot
from app.sim.paper_broker import PaperBroker  # type: ignore

# ✅ NEW: canonical policy gate + audit log
from app.ai.ai_scoreboard_gatekeeper_v1 import scoreboard_gate_decide
from app.ai.ai_executor_gate import ai_gate_decide, load_setup_policy, resolve_policy_cfg_for_strategy

log = get_logger("executor_v2")

EXEC_DRY_RUN: bool = os.getenv("EXEC_DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y", "on")

# ✅ Decision enforcement toggles
EXEC_ENFORCE_DECISIONS: bool = os.getenv("EXEC_ENFORCE_DECISIONS", "true").strip().lower() in ("1", "true", "yes", "y", "on")
EXEC_FORCE_TRADE_ID: str = os.getenv("EXEC_FORCE_TRADE_ID", "").strip()

ROOT: Path = settings.ROOT
SIGNAL_FILE: Path = ROOT / "signals" / "observed.jsonl"
CURSOR_FILE: Path = ROOT / "state" / "observed.cursor"

SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

LATENCY_LOG_PATH: Path = ROOT / "state" / "latency_exec.jsonl"
LATENCY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

try:
    LATENCY_WARN_MS = int(os.getenv("EXECUTOR_LATENCY_WARN_MS", "1500"))
except Exception:
    LATENCY_WARN_MS = 1500

SUSPECT_LOCK_PATH: Path = ROOT / "state" / "execution_suspect.lock"

# ✅ Decisions store (for enforcer + joiner)
DECISIONS_PATH: Path = ROOT / "state" / "ai_decisions.jsonl"
DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# ✅ Hardened decision logger import (lock + tail-dedupe)
# ---------------------------------------------------------------------------
try:
    from app.core.ai_decision_logger import append_decision as _append_decision_hardened  # type: ignore
except Exception:
    _append_decision_hardened = None  # type: ignore


# ---------------------------------------------------------------------------
# Phase 5 label normalization + enforcement (2025-12-20a)
# ---------------------------------------------------------------------------

_CANON_TF = {
    "1m", "3m", "5m", "15m", "30m",
    '60m',
    "1h", "2h", "4h",
    "1d",
}

_SETUP_TYPE_ALIASES: Dict[str, str] = {

    # --- added by patch_extend_setup_type_aliases_v1 ---
    "mm_spread_capture": "scalp",
    "spread_capture": "scalp",
    "market_maker": "scalp",
    "pump_chase_momo": "breakout",
    "momo": "breakout",
    "momentum": "breakout",
    "trend_pullback": "pullback",
    "pullback_trend": "pullback",
    "swing_reversion_extreme": "mean_reversion",
    "reversion_extreme": "mean_reversion",
    # --- end patch_extend_setup_type_aliases_v1 ---

    "breakout": "breakout",
    "bo": "breakout",
    "break_out": "breakout",

    "pullback": "pullback",
    "pb": "pullback",

    "trend": "trend_continuation",
    "trend_continuation": "trend_continuation",
    "tc": "trend_continuation",

    "range": "range_fade",
    "range_fade": "range_fade",
    "rf": "range_fade",

    "mean_reversion": "mean_reversion",
    "mr": "mean_reversion",

    "scalp": "scalp",

    "breakout_pullback": "breakout_pullback",
    "breakout-pullback": "breakout_pullback",
    # --- added by patch_extend_setup_type_normalizer_v2 ---
    "ma_long_mm_spread_capture": "scalp",
    "ma_short_mm_spread_capture": "scalp",
    "ma_long_swing_reversion_extreme": "mean_reversion",
    "ma_short_swing_reversion_extreme": "mean_reversion",
    # --- end patch_extend_setup_type_normalizer_v2 ---
}

def _clean_token(x: Any) -> str:
    try:
        s = str(x or "").strip().lower()
    except Exception:
        return ""
    s = s.replace(" ", "_").replace("-", "_").replace(":", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def _normalize_timeframe(raw: Any) -> Tuple[str, str]:
    s = _clean_token(raw)
    if not s:
        return "unknown", "empty"

    if s in _CANON_TF:
        return s, "canonical"

    s = s.replace("mins", "m").replace("min", "m")
    s = s.replace("hrs", "h").replace("hr", "h").replace("hour", "h").replace("hours", "h")
    s = s.replace("days", "d").replace("day", "d")

    if len(s) >= 2 and s[-1] in ("m", "h", "d"):
        n_raw = s[:-1]
        try:
            n = int(n_raw)
        except Exception:
            return "unknown", f"bad_suffix_number:{s}"
        if n <= 0:
            return "unknown", f"nonpositive:{s}"
        if s[-1] == "m":
            tf = f"{n}m"
        if tf == "60m":
            tf = "1h"  # canonicalize 60m -> 1h
        elif s[-1] == "h":
            tf = f"{n}h"
        else:
            tf = f"{n}d"
        if tf in _CANON_TF:
            return tf, "suffix_norm"
        return "unknown", f"unsupported_tf:{tf}"

    if s.endswith("s"):
        try:
            sec = int(s[:-1])
        except Exception:
            return "unknown", f"bad_seconds:{s}"
        if sec <= 0:
            return "unknown", f"nonpositive_seconds:{s}"
        if (sec % 60) != 0:
            return "unknown", f"seconds_not_div60:{s}"
        mins = sec // 60
        if mins >= 60 and (mins % 60) == 0:
            tf = f"{mins // 60}h"
        else:
            tf = f"{mins}m"
        if tf in _CANON_TF:
            return tf, "seconds_norm"
        return "unknown", f"unsupported_tf:{tf}"

    try:
        n = int(s)
        if n <= 0:
            return "unknown", f"nonpositive_numeric:{s}"
        if n >= 60 and (n % 60) == 0:
            tf = f"{n // 60}h"
        else:
            tf = f"{n}m"
        if tf in _CANON_TF:
            return tf, "numeric_norm"
        return "unknown", f"unsupported_tf:{tf}"
    except Exception:
        return "unknown", f"unparsed:{s}"

def _normalize_setup_type(raw: Any) -> Tuple[str, str]:
    """
    Accept both:
      - coarse canonical types (breakout/pullback/etc.)
      - rich signal labels like 'ma_long_trend_pullback:close_up_above_ma'
    Returns:
      (setup_type_family, reason)
    """
    s0 = _clean_token(raw)
    s = str(raw or '').lower()
    if not s0:
        return "unknown", "empty"

    # Heuristics for verbose setup strings produced by Signal Engine
    # NOTE: _clean_token normalizes ':' -> '_' so substring checks work.
    if "trend_pullback" in s0:
        return "pullback", "substring:trend_pullback"

    if "scalp" in s0 or "liquidity_sweep" in s0:
        return "scalp", "substring:scalp_or_liquidity_sweep"

    if "pump_chase" in s0 or "momo" in s0 or "momentum" in s0:
        return "momentum", "substring:pump_chase_momo"

    if "range_fade" in s0 or "intraday_range_fade" in s0:
        return "range_fade", "substring:range_fade"

    if "mean_reversion" in s0:
        return "mean_reversion", "substring:mean_reversion"

    if "breakout_pullback" in s0:
        return "breakout_pullback", "substring:breakout_pullback"

    if "breakout" in s0:
        return "breakout", "substring:breakout"

    # --- added by patch_extend_setup_type_normalizer_v2 ---
    if "mm_spread_capture" in s or "spread_capture" in s:
        return "market_make", "substring:mm_spread_capture"

    # swing reversion extreme is a mean-reversion family setup
    if "swing_reversion_extreme" in s or "reversion_extreme" in s or "swing_reversion" in s:
        return "mean_reversion", "substring:swing_reversion_extreme"
    # --- end patch_extend_setup_type_normalizer_v2 ---

    if "mm_spread_capture" in s:
        return "market_make", "substring:mm_spread_capture"
    if "reversion" in s:
        return "mean_reversion", "substring:reversion"
    # Exact aliases
    if s0 in _SETUP_TYPE_ALIASES:
        return _SETUP_TYPE_ALIASES[s0], "alias"

    # Prefix fallback (covers 'ma_long_breakout_*' etc)
    for k, v in _SETUP_TYPE_ALIASES.items():
        if s0.startswith(k):
            return v, "prefix"

    return "unknown", "unrecognized"



def _is_live_like(trade_mode: str) -> bool:
    m = str(trade_mode or "").upper().strip()
    return m in ("LIVE_CANARY", "LIVE_FULL")

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

def _env_bool(name: str, default: str = "true") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")

EXEC_CURSOR_SELF_HEAL: bool = _env_bool("EXEC_CURSOR_SELF_HEAL", "true")
EXEC_IDLE_HEARTBEAT_SEC: float = _env_float("EXEC_IDLE_HEARTBEAT_SEC", "10")
EXEC_CURSOR_HEAL_READBACK_BYTES: int = _env_int("EXEC_CURSOR_HEAL_READBACK_BYTES", "4096")
EXEC_CURSOR_BADLINE_RESET: bool = _env_bool("EXEC_CURSOR_BADLINE_RESET", "true")


# ---------------------------------------------------------------------------
# BYBIT CLIENTS — ONE PER SUBUID (OR MAIN) + PAPER BROKERS
# ---------------------------------------------------------------------------

_TRADE_CLIENTS: Dict[str, Bybit] = {}
_PAPER_BROKER_CACHE: Dict[str, PaperBroker] = {}

def get_trade_client(sub_uid: Optional[str]) -> Bybit:
    key = str(sub_uid) if sub_uid else "main"
    client = _TRADE_CLIENTS.get(key)
    if client is not None:
        return client
    client = Bybit("trade", sub_uid=sub_uid) if sub_uid else Bybit("trade")
    _TRADE_CLIENTS[key] = client
    return client

def get_paper_broker(account_label: str, starting_equity: float) -> PaperBroker:
    broker = _PAPER_BROKER_CACHE.get(account_label)
    if broker is not None:
        return broker
    broker = PaperBroker.load_or_create(account_label=account_label, starting_equity=starting_equity)
    _PAPER_BROKER_CACHE[account_label] = broker
    return broker

def _paper_equity_usd(account_label: str, fallback: float = 1000.0) -> float:
    try:
        b = get_paper_broker(account_label=account_label, starting_equity=float(fallback))
        for attr in ("equity", "equity_usd", "equity_now", "equity_now_usd"):
            v = getattr(b, attr, None)
            if v is None:
                continue
            try:
                return float(v)
            except Exception:
                pass
        return float(fallback)
    except Exception:
        return float(fallback)


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
        pass  # auto-fix: empty except block
    except Exception as e:

        pass  # auto-fix: empty except block

def _cursor_heal_to_line_boundary(pos: int) -> int:
    try:
        if not SIGNAL_FILE.exists():
            return 0

        size = SIGNAL_FILE.stat().st_size
        if size <= 0:
            return 0

        if pos < 0:
            pos = 0

        if pos == size:
            return pos

        if pos > size:
            return 0

        if pos == 0:
            return 0

        with SIGNAL_FILE.open("rb") as f:
            try:
                f.seek(pos - 1)
                prev = f.read(1)
                if prev == b"\n":
                    return pos
            except Exception:
                pass

            f.seek(pos)
            raw = f.readline()
            if raw:
                s = raw.lstrip()
                if s.startswith(b"{"):
                    return pos

            back = min(EXEC_CURSOR_HEAL_READBACK_BYTES, pos)
            f.seek(pos - back)
            chunk = f.read(back)
            if not chunk:
                return 0

            idx = chunk.rfind(b"\n")
            if idx == -1:
                return 0 if EXEC_CURSOR_BADLINE_RESET else pos

            healed = (pos - back) + idx + 1
            if healed < 0:
                healed = 0
            if healed > size:
                healed = 0

            if healed == size:
                return healed

            f.seek(healed)
            raw2 = f.readline()
            if raw2 and raw2.lstrip().startswith(b"{"):
                return healed

            return 0 if EXEC_CURSOR_BADLINE_RESET else healed
    except Exception:
        return 0


# ---------- LATENCY HELPERS ---------- #

def record_latency(event: str, symbol: str, strat: str, mode: str, duration_ms: int, extra: Optional[Dict[str, Any]] = None) -> None:
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
        pass  # auto-fix: empty except block
    except Exception as e:


# ---------------------------------------------------------------------------
# Raw JSONL append helper (fallback only)
# ---------------------------------------------------------------------------

        pass  # auto-fix: empty except block


# ---------------------------------------------------------------------------
# Raw JSONL append helper (fallback only)
# ---------------------------------------------------------------------------

def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    try:
        with path.open("ab") as f:
            f.write(json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n")
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:


# ---------------------------------------------------------------------------
# ✅ Decision-store append wrapper (prefer hardened logger)
# ---------------------------------------------------------------------------

        pass  # auto-fix: empty except block


# ---------------------------------------------------------------------------
# ✅ Decision-store append wrapper (prefer hardened logger)
# ---------------------------------------------------------------------------

def _append_decision(payload: Dict[str, Any]) -> None:
    if _append_decision_hardened is not None:
        try:
            _append_decision_hardened(payload)  # type: ignore[arg-type]
            return
        except TypeError:
            try:
                _append_decision_hardened(payload, path=DECISIONS_PATH)  # type: ignore[arg-type]
                return
            except Exception as e:
                pass  # auto-fix: empty except block
            except Exception as e:
                pass  # auto-fix: empty except block
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

    _append_jsonl(DECISIONS_PATH, payload)


# ---------------------------------------------------------------------------
# ✅ Trade-ID namespacing helpers (2025-12-14a)
# ---------------------------------------------------------------------------

def _safe_str(x: Any) -> str:
    try:
        return str(x) if x is not None else ""
    except Exception:
        return ""

def _make_effective_trade_id(source_trade_id: str, account_label: str, sub_uid: str, strategy_id: str) -> str:
    src = _safe_str(source_trade_id).strip() or "NO_SRC"
    acct = _safe_str(account_label).strip() or (_safe_str(sub_uid).strip() or "main")
    candidate = f"{acct}:{src}"

    if len(candidate) <= 36:
        return candidate

    h = hashlib.sha1(src.encode("utf-8", errors="ignore")).hexdigest()[:16]
    candidate2 = f"{acct}:{h}"
    if len(candidate2) <= 36:
        return candidate2

    h2 = hashlib.sha1(acct.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"{h2}:{h}"


# ---------------------------------------------------------------------------
# ✅ Pilot decision emission (INPUT row that enforcer consumes)
# ---------------------------------------------------------------------------

def emit_pilot_input_decision(setup_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if pilot_decide is None:
        return None

    try:
        raw = pilot_decide(setup_event)
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        raw = None

    if not isinstance(setup_event, dict):
        return None

    trade_id = str(setup_event.get("trade_id") or "").strip()
    if not trade_id:
        return None

    client_trade_id = str(setup_event.get("client_trade_id") or trade_id).strip()
    source_trade_id = setup_event.get("source_trade_id")

    symbol = str(setup_event.get("symbol") or "").strip().upper()
    account_label = str(setup_event.get("account_label") or "").strip()
    timeframe = str(setup_event.get("timeframe") or "").strip()

    d = raw if isinstance(raw, dict) else {}

    decision = str(
        d.get("decision")
        or d.get("decision_code")
        or d.get("code")
        or d.get("action")
        or "COLD_START"
    ).strip()

    allow = d.get("allow")
    sm = d.get("size_multiplier")

    if sm is None:
        pa = d.get("proposed_action") if isinstance(d.get("proposed_action"), dict) else {}
        sm = pa.get("size_multiplier")

    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    reason = str(d.get("reason") or gates.get("reason") or "pilot_input").strip()

    if allow is None:
        allow = decision in ("ALLOW_TRADE", "COLD_START")

    try:
        sm_f = float(sm) if sm is not None else (0.25 if decision == "COLD_START" else (1.0 if allow else 0.0))
    except Exception:
        sm_f = 0.25 if decision == "COLD_START" else (1.0 if allow else 0.0)

    if sm_f < 0:
        sm_f = 0.0
    if allow and sm_f <= 0:
        sm_f = 1.0

    row = {
        "schema_version": 1,
        "ts": int(time.time() * 1000),
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "timeframe": str(tf),
        "decision": decision,
        "allow": bool(allow),
        "size_multiplier": float(sm_f),
        "gates": {"reason": reason},
        "meta": {"source": "pilot_input_normalized"},
    }

    _append_decision(row)
    return row


# ---------------------------------------------------------------------------
# ✅ Pilot enforced decision emission (the thing you were missing/breaking)
# ---------------------------------------------------------------------------

def emit_pilot_enforced_decision(
    setup_event: Dict[str, Any],
    enforced: Dict[str, Any],
    *,
    pilot_row: Optional[Dict[str, Any]] = None,
    size_multiplier_applied: float = 1.0,
    enforced_code: str = "",
    enforced_reason: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Emits a schema_version==1 decision row reflecting the FINAL enforced decision.
    Non-fatal, fail-soft.
    """
    try:
        if not isinstance(setup_event, dict) or not isinstance(enforced, dict):
            return None

        trade_id = str(setup_event.get("trade_id") or "").strip()
        if not trade_id:
            return None

        client_trade_id = str(setup_event.get("client_trade_id") or trade_id).strip()
        source_trade_id = setup_event.get("source_trade_id")

        symbol = str(setup_event.get("symbol") or "").strip().upper()
        account_label = str(setup_event.get("account_label") or "").strip()
        timeframe = str(setup_event.get("timeframe") or "").strip()

        allow = bool(enforced.get("allow", True))
        code = str(enforced_code or enforced.get("decision_code") or ("ALLOW_TRADE" if allow else "BLOCK_TRADE")).strip()
        reason = str(enforced_reason or enforced.get("reason") or "enforced").strip()

        sm = enforced.get("size_multiplier", size_multiplier_applied)
        try:
            sm_f = float(sm) if sm is not None else float(size_multiplier_applied)
        except Exception:
            sm_f = float(size_multiplier_applied)

        if sm_f < 0:
            sm_f = 0.0
        if allow and sm_f <= 0:
            sm_f = 1.0
        if not allow:
            sm_f = 0.0

        row = {
            "schema_version": 1,
            "ts": int(time.time() * 1000),
            "trade_id": trade_id,
            "client_trade_id": client_trade_id,
            "source_trade_id": source_trade_id,
            "symbol": symbol,
            "account_label": account_label,
            "timeframe": str(tf),
            "decision": code,
            "allow": bool(allow),
            "size_multiplier": float(sm_f),
            "gates": {"reason": reason},
            "meta": {
                "source": "pilot_enforced_normalized",
                "has_pilot_input": bool(isinstance(pilot_row, dict)),
                "pilot_input_decision": (str(pilot_row.get("decision")) if isinstance(pilot_row, dict) else None),
            },
        }

        _append_decision(row)
        return row
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        return None


def emit_ai_decision(
    *,
    trade_id: str,
    client_trade_id: str,
    source_trade_id: Optional[str],
    symbol: str,
    account_label: str,
    sub_uid: str,
    strategy_id: str,
    strategy_name: str,
    timeframe: str,
    side: str,
    mode: str,
    allow: bool,
    decision_code: str,
    reason: str,
    model_used: Optional[str] = None,
    regime: Optional[str] = None,
    confidence_source: Optional[str] = None,
    learning_tags: Optional[list[str]] = None,
    ai_score: Optional[float] = None,
    tier_used: Optional[str] = None,
    gates_reason: Optional[str] = None,
    memory_id: Optional[str] = None,
    memory_score: Optional[float] = None,
    size_multiplier: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    row: Dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "event_type": "ai_decision",
        "trade_id": str(trade_id),
        "client_trade_id": str(client_trade_id),
        "source_trade_id": (str(source_trade_id) if source_trade_id else None),
        "symbol": str(symbol),
        "account_label": str(account_label),
        "sub_uid": (str(sub_uid) if sub_uid else None),
        "strategy_id": str(strategy_id),
        "strategy_name": str(strategy_name),
        "timeframe": str(timeframe),
        "model_used": model_used,
        "regime": regime,
        "confidence_source": confidence_source,
        "learning_tags": learning_tags or [],
        "side": str(side),
        "mode": str(mode),
        "allow": bool(allow),
        "decision_code": str(decision_code),
        "reason": str(reason),
        "ai_score": (float(ai_score) if ai_score is not None else None),
        "tier_used": tier_used,
        "gates_reason": gates_reason,
        "memory_id": memory_id,
        "memory_score": (float(memory_score) if memory_score is not None else None),
        "size_multiplier": (float(size_multiplier) if size_multiplier is not None else None),
    }
    if extra:
        row["extra"] = extra
    _append_decision(row)


# ---------- AI GATE WRAPPER ---------- #

# Cache policy in-memory to avoid re-reading file for every signal
_POLICY_CACHE: Optional[Dict[str, Any]] = None
_POLICY_CACHE_TS: float = 0.0
_POLICY_CACHE_TTL_SEC: float = 5.0  # light refresh so Telegram/UI edits apply quickly

def _get_policy_cached() -> Dict[str, Any]:
    global _POLICY_CACHE, _POLICY_CACHE_TS
    now = time.time()
    if _POLICY_CACHE is None or (now - _POLICY_CACHE_TS) > _POLICY_CACHE_TTL_SEC:
        _POLICY_CACHE = load_setup_policy()
        _POLICY_CACHE_TS = now
    return _POLICY_CACHE


def run_ai_gate(signal: Dict[str, Any], strat_id: str, bound_log, *, account_label: str, mode: str, trade_id: str, symbol: str) -> Dict[str, Any]:
    """
    Canonical AI gate path:
      1) classifier produces score/features + optional hard allow/block
      2) ai_executor_gate enforces policy thresholds + logs decision
    """
    decision: Dict[str, Any] = {"allow": True, "score": None, "reason": "default_allow_fallback", "features": {}}

    try:
        clf = classify_trade(signal, strat_id)
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        clf = {"allow": True, "score": None, "reason": f"classifier_error:{e}", "features": {}}

    if not isinstance(clf, dict):
        log.warning("AI classifier returned non-dict for [%s]: %r — treating as allow=True.", strat_id, clf)
        clf = {"allow": True, "score": None, "reason": "classifier_non_dict", "features": {}}

    pre_allow = bool(clf.get("allow", True))
    raw_score = clf.get("score")
    pre_reason = str(clf.get("reason") or clf.get("why") or "ok")
    features = clf.get("features") or {}
    if not isinstance(features, dict):
        features = {}

    try:
        score_f = float(raw_score) if raw_score is not None else None
    except Exception:
        score_f = None

    policy = _get_policy_cached()
    policy_cfg = resolve_policy_cfg_for_strategy(policy, strat_id)

    # This logs to state/ai_policy_log.jsonl (canonical) and returns allow/block
    gate = ai_gate_decide(
        strategy_name=strat_id,
        symbol=str(symbol),
        account_label=str(account_label),
        mode=str(mode),
        features=features,
        raw_score=score_f,
        policy_cfg=policy_cfg,
        trade_id=str(trade_id),
        precheck_allow=pre_allow,
        precheck_reason=pre_reason,
    )

    decision = {
        "allow": bool(gate.get("allow", True)),
        "score": gate.get("score", score_f),
        "reason": str(gate.get("reason") or "ok"),
        "features": features,
    }
    return decision


# ---------- SIGNAL PROCESSOR ---------- #

def _normalize_strategies_for_signal(strategies: Any) -> Iterable[Tuple[str, Dict[str, Any]]]:
    if not strategies:
        return []
    if isinstance(strategies, dict):
        return strategies.items()
    if isinstance(strategies, (list, tuple)):
        if not strategies:
            return []
        first = strategies[0]
        if isinstance(first, (list, tuple)) and len(first) == 2:
            return strategies
        if isinstance(first, dict):
            normalized: List[Tuple[str, Dict[str, Any]]] = []
            for cfg in strategies:
                if not isinstance(cfg, dict):
                    continue
                name = cfg.get("name") or cfg.get("id") or cfg.get("label") or cfg.get("strategy_name") or "unnamed_strategy"
                normalized.append((str(name), cfg))
            return normalized
        return []
    return []


async def process_signal_line(line: str) -> None:
    try:
        sig = json.loads(line)
    except Exception:
        log.warning("Invalid JSON in observed.jsonl: %r", line[:200])
        return

    # --- HARD FILTER: drop test/junk signals permanently ---
    try:
        src = sig.get('source')
        if src == 'emit_test_signal':
            return
        st = sig.get('setup_type') or sig.get('setup_type_raw') or sig.get('reason')
        if isinstance(st, str) and st.strip().lower() == 'tick':
            return
    except Exception:
        # never crash ingestion over filtering
        pass

    symbol = sig.get("symbol")
    tf_raw = sig.get("timeframe") or sig.get("tf")
    if not symbol or not tf_raw:
        return

    tf_norm, tf_reason = _normalize_timeframe(tf_raw)
    if tf_norm == "unknown":
        log.warning("label_norm timeframe invalid: raw=%r reason=%s symbol=%s", tf_raw, tf_reason, symbol)
        return

    sig["timeframe"] = tf_norm
    sig["tf"] = tf_norm

    strategies = get_strategies_for_signal(symbol, tf_norm)
    strat_items = _normalize_strategies_for_signal(strategies)
    if not strat_items:
        return

    for strat_name, strat_cfg in strat_items:
        try:
            await handle_strategy_signal(strat_name, strat_cfg, sig)
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:


# ---------- STRATEGY PROCESSOR ---------- #

            pass  # auto-fix: empty except block


# ---------- STRATEGY PROCESSOR ---------- #

def _automation_mode_from_cfg(cfg: Dict[str, Any]) -> str:
    mode = str(cfg.get("automation_mode", "OFF")).upper().strip()
    if mode not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
        mode = "OFF"
    return mode

def _normalize_paper_side(signal_side: str) -> str:
    s = str(signal_side or "").strip().lower()
    if s in ("buy", "long"):
        return "long"
    if s in ("sell", "short"):
        return "short"
    raise ValueError(f"Unsupported side value for paper entry: {signal_side!r}")


async def handle_strategy_signal(strat_name: str, strat_cfg: Dict[str, Any], sig: Dict[str, Any]) -> None:
    strat_id = strategy_label(strat_cfg)
    bound = bind_context(log, strat=strat_id)

    enabled = bool(strat_cfg.get("enabled", False))
    mode_raw = _automation_mode_from_cfg(strat_cfg)

    if not enabled or mode_raw == "OFF":
        bound.debug("strategy disabled or automation_mode=OFF")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
    side = sig.get("side")

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

    tf_norm, tf_reason = _normalize_timeframe(tf)
    if tf_norm == "unknown":
        bound.warning("label_norm timeframe invalid: raw=%r reason=%s symbol=%s", tf, tf_reason, symbol)
        return

    setup_type_raw = sig.get("setup_type")
    setup_type_norm, st_reason = _normalize_setup_type(setup_type_raw)

    try:
        price_f = float(price)
    except Exception:
        bound.warning("invalid price in signal: %r", sig)
        return

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

    sub_uid = str(strat_cfg.get("sub_uid") or strat_cfg.get("subAccountId") or strat_cfg.get("accountId") or strat_cfg.get("subId") or "")
    account_label = str(strat_cfg.get("account_label") or strat_cfg.get("label") or strat_cfg.get("account_label_slug") or "main")

    lock_active = SUSPECT_LOCK_PATH.exists()
    try:
        breaker_on = bool(GLOBAL_BREAKER.get("on", False))
    except Exception:
        breaker_on = False

    is_training_mode = EXEC_DRY_RUN or mode_raw == "LEARN_DRY"
    started_ms = int(time.time() * 1000)

    bound.info(
        "label_norm symbol=%s tf_raw=%r tf=%s(tf_reason=%s) setup_type_raw=%r setup_type=%s(st_reason=%s) mode=%s",
        symbol, tf, tf_norm, tf_reason, setup_type_raw, setup_type_norm, st_reason, trade_mode
    )

    try:
        if should_block_trading():
            bound.info("Session Guard blocking new trades (limits reached).")
            return
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:

        pass  # auto-fix: empty except block

    ts_open_ms = int(time.time() * 1000)
    strat_safe = strat_id.replace(" ", "_").replace("(", "").replace(")", "")
    default_source_trade_id = f"{strat_safe}-{ts_open_ms}"

    sig_trade_id_raw = ""
    try:
        sig_trade_id_raw = str(sig.get("trade_id") or sig.get("client_trade_id") or "").strip()
    except Exception:
        sig_trade_id_raw = ""

    used_sig_trade_id = bool(sig_trade_id_raw)
    used_force_trade_id = False

    if used_sig_trade_id:
        source_trade_id = sig_trade_id_raw
        trade_id_source = "signal"
    elif EXEC_FORCE_TRADE_ID:
        source_trade_id = EXEC_FORCE_TRADE_ID
        used_force_trade_id = True
        trade_id_source = "exec_force"
    else:
        source_trade_id = default_source_trade_id
        trade_id_source = "generated"

    client_trade_id = _make_effective_trade_id(source_trade_id, account_label=account_label, sub_uid=sub_uid, strategy_id=strat_id)
    trade_id = client_trade_id

    bound.info(
        "trade_id_map source=%s -> effective=%s account=%s sub_uid=%s strat=%s",
        source_trade_id, client_trade_id, account_label, (sub_uid or None), strat_id
    )

    if _is_live_like(trade_mode):
        if setup_type_norm == "unknown":
            emit_ai_decision(
                trade_id=client_trade_id,
                client_trade_id=client_trade_id,
                source_trade_id=source_trade_id,
                symbol=symbol,
                account_label=account_label,
                sub_uid=sub_uid,
                strategy_id=strat_id,
                strategy_name=strat_cfg.get("name", strat_name),
                timeframe=tf_norm,
                side=str(side),
                mode=trade_mode,
                allow=False,
                decision_code="BAD_LABEL_SETUP_TYPE",
                reason=f"setup_type_missing_or_unknown (raw={setup_type_raw!r} reason={st_reason})",
                ai_score=None,
                size_multiplier=0.0,
                extra={
                    "stage": "label_gate",
                    "label_gate": "setup_type",
                    "setup_type_raw": setup_type_raw,
                    "setup_type_norm": setup_type_norm,
                    "setup_type_reason": st_reason,
                    "timeframe_norm": tf_norm,
                    "timeframe_reason": tf_reason,
                    "trade_id_source": trade_id_source,
                },
            )
            bound.info("⛔ BAD_LABEL setup_type (LIVE/CANARY) trade_id=%s raw=%r reason=%s", client_trade_id, setup_type_raw, st_reason)
            try:
                tg_send(f"⛔ BAD_LABEL (setup_type) blocked LIVE/CANARY trade: trade_id={client_trade_id} symbol={symbol} strat={strat_id} raw={setup_type_raw!r} reason={st_reason}")
            except Exception:
                pass
            return

        if tf_norm == "unknown":
            emit_ai_decision(
                trade_id=client_trade_id,
                client_trade_id=client_trade_id,
                source_trade_id=source_trade_id,
                symbol=symbol,
                account_label=account_label,
                sub_uid=sub_uid,
                strategy_id=strat_id,
                strategy_name=strat_cfg.get("name", strat_name),
                timeframe=tf_norm,
                side=str(side),
                mode=trade_mode,
                allow=False,
                decision_code="BAD_LABEL_TIMEFRAME",
                reason=f"timeframe_unknown (raw={tf!r} reason={tf_reason})",
                ai_score=None,
                size_multiplier=0.0,
                extra={
                    "stage": "label_gate",
                    "label_gate": "timeframe",
                    "timeframe_raw": tf,
                    "timeframe_norm": tf_norm,
                    "timeframe_reason": tf_reason,
                    "setup_type_norm": setup_type_norm,
                    "setup_type_reason": st_reason,
                    "trade_id_source": trade_id_source,
                },
            )
            bound.info("⛔ BAD_LABEL timeframe (LIVE/CANARY) trade_id=%s raw=%r reason=%s", client_trade_id, tf, tf_reason)
            return

    pilot_setup_event = {
        "trade_id": client_trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "symbol": symbol,
        "timeframe": tf_norm,
        "setup_type": setup_type_norm,
        "account_label": account_label,
        "policy": {"policy_hash": str(strat_cfg.get("policy_hash") or (strat_cfg.get("policy", {}) or {}).get("policy_hash") or "")},
        "payload": {
            "features": {
                "memory_fingerprint": (
                    (sig.get("payload", {}).get("features", {}).get("memory_fingerprint") if isinstance(sig.get("payload"), dict) else None)
                    or (sig.get("debug", {}).get("memory_fingerprint") if isinstance(sig.get("debug"), dict) else None)
                    or (sig.get("memory_fingerprint"))
                    or ""
                ),
            }
        },
    }

    ph_sig = None
    try:
        ph_sig = (sig.get("policy", {}).get("policy_hash") if isinstance(sig.get("policy"), dict) else None)
    except Exception:
        ph_sig = None
    if ph_sig:
        pilot_setup_event["policy"]["policy_hash"] = str(ph_sig)

    pilot_row = emit_pilot_input_decision(pilot_setup_event)

    size_multiplier_applied = 1.0
    enforced_reason = "not_enforced"
    enforced_code = None

    if EXEC_ENFORCE_DECISIONS:
        try:
            enforced = enforce_decision(client_trade_id, account_label=account_label)
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

        allow = bool(enforced.get("allow", True))
        enforced_code = enforced.get("decision_code")
        enforced_reason = str(enforced.get("reason") or "ok")

        sm_raw = enforced.get("size_multiplier", 1.0)
        try:
            sm = float(sm_raw) if sm_raw is not None else 1.0
        except Exception:
            sm = 1.0

        if not allow:
            emit_ai_decision(
                trade_id=client_trade_id,
                client_trade_id=client_trade_id,
                source_trade_id=source_trade_id,
                symbol=symbol,
                account_label=account_label,
                sub_uid=sub_uid,
                strategy_id=strat_id,
                strategy_name=strat_cfg.get("name", strat_name),
                timeframe=tf_norm,
                side=str(side),
                mode=trade_mode,
                allow=False,
                decision_code=str(enforced_code or "BLOCK_TRADE"),
                reason=enforced_reason,
                ai_score=None,
                size_multiplier=0.0,
                extra={
                    "stage": "decision_enforced_pre_sizing",
                    "trade_id_source": trade_id_source,
                    "sig_trade_id_present": bool(used_sig_trade_id),
                    "forced_trade_id": bool(used_force_trade_id),
                    "pilot_emitted": bool(pilot_row),
                    "enforced_code": str(enforced_code or ""),
                    "enforced_reason": str(enforced_reason or ""),
                    "enforced_size_multiplier": 0.0,
                    "label_tf": tf_norm,
                    "label_setup_type": setup_type_norm,
                },
            )
            bound.info("⛔ Decision enforcer BLOCKED (pre-sizing) trade_id=%s reason=%s", client_trade_id, enforced_reason)
            try:
                tg_send(f"⛔ Trade BLOCKED by decision enforcer (pre-sizing): trade_id={client_trade_id} source_trade_id={source_trade_id} symbol={symbol} strat={strat_id} reason={enforced_reason}")
            except Exception:
                pass
            return

        if sm > 0:
            size_multiplier_applied = sm

        emit_pilot_enforced_decision(
            pilot_setup_event,
            enforced,
            pilot_row=pilot_row,
            size_multiplier_applied=float(size_multiplier_applied),
            enforced_code=str(enforced_code or ""),
            enforced_reason=str(enforced_reason or "ok"),
        )

    effective_code = str(enforced_code) if enforced_code else "ALLOW_TRADE"
    effective_reason = str(enforced_reason) if (enforced_reason and enforced_reason != "not_enforced") else "passed"


    # ---------------------------------------------------------------------------
    # ✅ Phase 7: build one canonical AI snapshot for this candidate and enforce safety
    # ---------------------------------------------------------------------------
    try:
        snap = build_ai_snapshot(
            focus_symbols=[symbol],
            include_trades=False,
            trades_limit=50,
            include_orderbook=True,
        )
        snap_ok, snap_errs = validate_snapshot_v2(snap)
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        snap_ok, snap_errs = False, [f"snapshot_exception:{e}"]

    # In LIVE/LIVE_CANARY, snapshot must be valid
    if trade_mode in ("LIVE_CANARY", "LIVE_FULL") and not snap_ok:
        bound.info("⛔ SNAPSHOT_INVALID blocked trade_id=%s errs=%s", client_trade_id, snap_errs)

        emit_ai_decision(
            trade_id=client_trade_id,
            client_trade_id=client_trade_id,
            source_trade_id=source_trade_id,
            symbol=symbol,
            account_label=account_label,
            sub_uid=sub_uid,
            strategy_id=strat_id,
            strategy_name=strat_cfg.get("name", strat_name),
            timeframe=tf_norm,
            side=str(side),
            mode=trade_mode,
            allow=False,
            decision_code="SNAPSHOT_INVALID",
            reason="snapshot_invalid",
            ai_score=None,
            extra={
                "stage": "snapshot_gate",
                "trade_id_source": trade_id_source,
                "sig_trade_id_present": bool(used_sig_trade_id),
                "forced_trade_id": bool(used_force_trade_id),
                "pilot_emitted": bool(pilot_row),
                "enforced_code": effective_code,
                "enforced_reason": effective_reason,
                "enforced_size_multiplier": float(size_multiplier_applied),
                "snap_errs": snap_errs,
                "freshness": snap.get("freshness") if isinstance(snap, dict) else {},
                "thresholds": (
                    snap.get("safety", {}).get("thresholds_sec")
                    if isinstance(snap, dict) and isinstance(snap.get("safety"), dict)
                    else {}
                ),
            },
        )
        try:
            tg_send(f"⛔ SNAPSHOT_INVALID blocked LIVE/CANARY: trade_id={client_trade_id} symbol={symbol} errs={snap_errs}")
        except Exception:
            pass
        return

    # In LIVE/LIVE_CANARY, snapshot must be safe (fresh enough)
    if trade_mode in ("LIVE_CANARY", "LIVE_FULL"):
        safety = snap.get("safety") if isinstance(snap, dict) else {}
        is_safe = bool(safety.get("is_safe")) if isinstance(safety, dict) else False
        reasons = safety.get("reasons") if isinstance(safety, dict) else None
        if not is_safe:
            bound.info("⛔ SNAPSHOT_UNSAFE blocked trade_id=%s reasons=%s", client_trade_id, reasons)

            emit_ai_decision(
                trade_id=client_trade_id,
                client_trade_id=client_trade_id,
                source_trade_id=source_trade_id,
                symbol=symbol,
                account_label=account_label,
                sub_uid=sub_uid,
                strategy_id=strat_id,
                strategy_name=strat_cfg.get("name", strat_name),
                timeframe=tf_norm,
                side=str(side),
                mode=trade_mode,
                allow=False,
                decision_code="SNAPSHOT_UNSAFE",
                reason="snapshot_unsafe",
                ai_score=None,
                extra={
                    "stage": "snapshot_gate",
                    "trade_id_source": trade_id_source,
                    "sig_trade_id_present": bool(used_sig_trade_id),
                    "forced_trade_id": bool(used_force_trade_id),
                    "pilot_emitted": bool(pilot_row),
                    "enforced_code": effective_code,
                    "enforced_reason": effective_reason,
                    "enforced_size_multiplier": float(size_multiplier_applied),
                    "reasons": reasons,
                    "freshness": snap.get("freshness") if isinstance(snap, dict) else {},
                    "thresholds": (
                        snap.get("safety", {}).get("thresholds_sec")
                        if isinstance(snap, dict) and isinstance(snap.get("safety"), dict)
                        else {}
                    ),
                },
            )
            try:
                tg_send(f"⛔ SNAPSHOT_UNSAFE blocked LIVE/CANARY: trade_id={client_trade_id} symbol={symbol} reasons={reasons}")
            except Exception:
                pass
            return

    
    # ✅ Scoreboard evidence gate (optional)
    try:
        use_scoreboard_gate = os.getenv("EXEC_SCOREBOARD_GATE", "true").strip().lower() in ("1","true","yes","y")
    except Exception:
        use_scoreboard_gate = True

    scoreboard_gate = None
    if use_scoreboard_gate:
        try:
            scoreboard_gate = scoreboard_gate_decide(
                setup_type=str(setup_type),
                timeframe=str(tf),
                symbol=str(symbol),
                account_label=str(account_label) if account_label is not None else None,
            )
            try:
                _tid = locals().get('client_trade_id') or locals().get('trade_id') or '?'
                _st_norm = locals().get('setup_type')
                _st_raw = locals().get('setup_type_raw')
                _tf = locals().get('tf')
                _sym = locals().get('symbol')
                _code = None if scoreboard_gate is None else scoreboard_gate.get('decision_code')
                log.info("🧪 SCOREBOARD_GATE call trade_id=%s st_norm=%s st_raw=%s tf=%s sym=%s -> %s", _tid, _st_norm, _st_raw, _tf, _sym, _code)
                if scoreboard_gate is not None:
                    log.info("🧪 SCOREBOARD_GATE decision allow=%s sm=%s reason=%s bucket=%s", scoreboard_gate.get('allow'), scoreboard_gate.get('size_multiplier'), scoreboard_gate.get('reason'), scoreboard_gate.get('bucket_key'))
            except Exception as e:
                log.warning("Scoreboard gate debug logging failed (non-fatal): %r", e)
                if scoreboard_gate is not None:
                    log.info("🧪 SCOREBOARD_GATE decision allow=%s sm=%s reason=%s bucket=%s", scoreboard_gate.get('allow'), scoreboard_gate.get('size_multiplier'), scoreboard_gate.get('reason'), scoreboard_gate.get('bucket_key'))
            except Exception as e:
                log.warning("Scoreboard gate debug logging failed (non-fatal): %r", e)
                if scoreboard_gate is not None:
                    bound.info(
                        "✅ Scoreboard gate MATCH trade_id=%s bucket=%s code=%s sm=%s reason=%s",
                        client_trade_id,
                        scoreboard_gate.get("bucket_key"),
                        scoreboard_gate.get("decision_code"),
                        scoreboard_gate.get("size_multiplier"),
                        scoreboard_gate.get("reason"),
                    )
            except Exception:
                pass
        except Exception as e:
            log.warning("Scoreboard gate failed (non-fatal): %r", e)
            scoreboard_gate = None

    if scoreboard_gate is not None:
        sm_sb = scoreboard_gate.get("size_multiplier")
        try:
            if sm_sb is not None:
                size_multiplier_applied = float(size_multiplier_applied) * float(sm_sb)
        except Exception:
            pass

        if not bool(scoreboard_gate.get("allow", True)):
            emit_ai_decision(
                trade_id=client_trade_id,
                account_label=account_label,
                symbol=symbol,
                allow=False,
                decision_code=str(scoreboard_gate.get("decision_code") or "SCOREBOARD_BLOCK"),
                size_multiplier=0.0,
                reason=str(scoreboard_gate.get("reason") or "scoreboard_block"),
                extra={
                    "stage": "scoreboard_gate_pre_policy",
                    "bucket_key": scoreboard_gate.get("bucket_key"),
                    "bucket_stats": scoreboard_gate.get("bucket_stats"),
                    "scoreboard_path": scoreboard_gate.get("scoreboard_path"),
                    "enforced_size_multiplier": float(size_multiplier_applied),
                },
            )
            bound.info("⛔ Scoreboard gate BLOCKED trade_id=%s reason=%s", client_trade_id, scoreboard_gate.get("reason"))
            try:
                tg_send(f"⛔ Scoreboard gate blocked: trade_id={client_trade_id} symbol={symbol} reason={scoreboard_gate.get('reason')}")
            except Exception:
                pass
            return

# ✅ NEW canonical policy gate (logs to ai_policy_log.jsonl and returns real allow/block)
    ai = run_ai_gate(
        sig,
        strat_id,
        bound,
        account_label=account_label,
        mode=trade_mode,
        trade_id=client_trade_id,
        symbol=symbol,
    )
    if not ai["allow"]:
        emit_ai_decision(
            trade_id=client_trade_id,
            client_trade_id=client_trade_id,
            source_trade_id=source_trade_id,
            symbol=symbol,
            account_label=account_label,
            sub_uid=sub_uid,
            strategy_id=strat_id,
            strategy_name=strat_cfg.get("name", strat_name),
            timeframe=tf_norm,
            side=str(side),
            mode=trade_mode,
            allow=False,
            decision_code="REJECT_TRADE",
            reason=str(ai.get("reason") or "ai_reject"),
            ai_score=ai.get("score"),
            extra={
                "stage": "ai_gate",
                "trade_id_source": trade_id_source,
                "sig_trade_id_present": bool(used_sig_trade_id),
                "forced_trade_id": bool(used_force_trade_id),
                "pilot_emitted": bool(pilot_row),
                "enforced_code": effective_code,
                "enforced_reason": effective_reason,
                "enforced_size_multiplier": float(size_multiplier_applied),
                "label_tf": tf_norm,
                "label_setup_type": setup_type_norm,
            },
        )
        return

    # --- rest of file unchanged from your version ---
    # (I’m not repeating it here because it’s already in your pasted file and unchanged.)

    # NOTE:
    # The remainder of executor_v2.py should stay exactly as you pasted it after the AI gate call.
    # If you want, I can re-post the full remainder too, but it is byte-for-byte unchanged.

    # -----------------------------------------------------------------------
    # IMPORTANT:
    # Copy the rest of your existing executor_v2.py (everything after the old
    # ai gate call site) directly below this point.
    # -----------------------------------------------------------------------


    try:
        allowed_corr, corr_reason = corr_allow(symbol)
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        allowed_corr, corr_reason = True, "corr_gate_v2 exception, bypassed"

    if not allowed_corr:
        emit_ai_decision(
            trade_id=client_trade_id,
            client_trade_id=client_trade_id,
            source_trade_id=source_trade_id,
            symbol=symbol,
            account_label=account_label,
            sub_uid=sub_uid,
            strategy_id=strat_id,
            strategy_name=strat_cfg.get("name", strat_name),
            timeframe=tf_norm,
            side=str(side),
            mode=trade_mode,
            allow=False,
            decision_code="REJECT_TRADE",
            reason=f"corr_gate:{corr_reason}",
            ai_score=ai.get("score"),
            extra={
                "stage": "corr_gate",
                "trade_id_source": trade_id_source,
                "sig_trade_id_present": bool(used_sig_trade_id),
                "forced_trade_id": bool(used_force_trade_id),
                "pilot_emitted": bool(pilot_row),
                "enforced_code": effective_code,
                "enforced_reason": effective_reason,
                "enforced_size_multiplier": float(size_multiplier_applied),
                "label_tf": tf_norm,
                "label_setup_type": setup_type_norm,
            },
        )
        return

    try:
        base_risk_pct = Decimal(str(strategy_risk_pct(strat_cfg)))
    except Exception:
        base_risk_pct = Decimal("0")

    risk_mult = Decimal(str(get_risk_multiplier(strat_id)))
    eff_risk_pct = base_risk_pct * risk_mult

    try:
        eff_risk_pct = eff_risk_pct * Decimal(str(size_multiplier_applied))
    except Exception:
        pass

    if eff_risk_pct <= 0:
        bound.info("effective risk_pct <= 0 for %s; skipping.", strat_id)
        return

    try:
        if EXEC_DRY_RUN or mode_raw == "LEARN_DRY" or trade_mode == "PAPER":
            equity_val = Decimal(str(_paper_equity_usd(account_label=account_label, fallback=1000.0)))
        else:
            equity_val = Decimal(str(get_equity_usdt()))
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        equity_val = Decimal("1000")

    stop_pct_for_size = 0.005
    stop_distance = Decimal(str(price_f * stop_pct_for_size))

    qty_suggested, risk_usd = bayesian_size(
        symbol=symbol,
        equity_usd=equity_val,
        risk_pct=float(eff_risk_pct),
        stop_distance=stop_distance,
    )

    if qty_suggested <= 0 or risk_usd <= 0:
        bound.info("bayesian_size returned non-positive sizing for %s; equity=%s risk_pct=%s", strat_id, equity_val, eff_risk_pct)
        return

    qty_capped, risk_capped = risk_capped_qty(
        symbol=symbol,
        qty=qty_suggested,
        equity_usd=equity_val,
        max_risk_pct=float(eff_risk_pct),
        stop_distance=stop_distance,
    )

    if qty_capped <= 0 or risk_capped <= 0:
        bound.info("qty <= 0 after risk_capped_qty; skipping entry.")
        return

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
            pass  # auto-fix: empty except block
        except Exception as e:
            guard_ok, guard_reason = True, "guard_exception_bypass"
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        guard_ok, guard_reason = True, "guard_exception_bypass"

    if not guard_ok:
        emit_ai_decision(
            trade_id=client_trade_id,
            client_trade_id=client_trade_id,
            source_trade_id=source_trade_id,
            symbol=symbol,
            account_label=account_label,
            sub_uid=sub_uid,
            strategy_id=strat_id,
            strategy_name=strat_cfg.get("name", strat_name),
            timeframe=tf_norm,
            side=str(side),
            mode=trade_mode,
            allow=False,
            decision_code="REJECT_TRADE",
            reason=f"portfolio_guard:{guard_reason}",
            ai_score=ai.get("score"),
            extra={
                "stage": "portfolio_guard",
                "trade_id_source": trade_id_source,
                "sig_trade_id_present": bool(used_sig_trade_id),
                "forced_trade_id": bool(used_force_trade_id),
                "pilot_emitted": bool(pilot_row),
                "enforced_code": effective_code,
                "enforced_reason": effective_reason,
                "enforced_size_multiplier": float(size_multiplier_applied),
                "label_tf": tf_norm,
                "label_setup_type": setup_type_norm,
            },
        )
        bound.info("Portfolio guard blocked trade for %s: %s", symbol, guard_reason)
        return

    decision_done_ms = int(time.time() * 1000)
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
            "decision_enforced": bool(EXEC_ENFORCE_DECISIONS),
            "decision_size_multiplier": float(size_multiplier_applied),
            "decision_reason": effective_reason,
            "decision_code": effective_code,
            "pilot_emitted": bool(pilot_row),
            "trade_id_source": trade_id_source,
            "source_trade_id": source_trade_id,
            "label_tf": tf_norm,
            "label_setup_type": setup_type_norm,
        },
    )

    setup_type_val: str = setup_type_norm
    base_features: Dict[str, Any] = {
        "schema_version": "setup_features_v1",
        "signal": sig,
        "symbol": symbol,
        "timeframe": tf_norm,
        "side": side,
        "account_label": account_label,
        "sub_uid": sub_uid or None,
        "strategy_name": strat_cfg.get("name", strat_name),
        "automation_mode": mode_raw,
        "trade_mode": trade_mode,
        "equity_usd": float(equity_val),
        "risk_usd": float(risk_capped),
        "risk_pct": float(eff_risk_pct),
        "stop_pct_for_size": float(stop_pct_for_size),
        "train_mode": "DRY_RUN_V1" if is_training_mode else "LIVE_OR_CANARY",
        "setup_type": setup_type_val,
        "setup_label_raw": str(setup_type_raw) if setup_type_raw is not None else None,
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "ts_open_ms": ts_open_ms,
        "qty": float(qty_capped),
        "size": float(qty_capped),
        "execution_lock_active": lock_active,
        "execution_global_breaker_on": breaker_on,
        "decision_size_multiplier": float(size_multiplier_applied),
        "decision_enforced": bool(EXEC_ENFORCE_DECISIONS),
        "decision_reason": effective_reason,
        "decision_code": effective_code,
        "trade_id_source": trade_id_source,
        "forced_trade_id": bool(used_force_trade_id),
        "sig_trade_id_present": bool(used_sig_trade_id),
        "label_norm": {
            "timeframe_raw": tf,
            "timeframe_norm": tf_norm,
            "timeframe_reason": tf_reason,
            "setup_type_raw": setup_type_raw,
            "setup_type_norm": setup_type_norm,
            "setup_type_reason": st_reason,
        },
    }

    features_payload: Dict[str, Any] = dict(base_features)
    setup_logged: bool = False

    try:
        ai_score = ai.get("score")
        ai_reason = ai.get("reason", "")
        ai_features = ai.get("features") or {}
        features_payload.update(ai_features)
        features_payload["ai_score"] = float(ai_score) if ai_score is not None else None
        features_payload["ai_reason"] = str(ai_reason)
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:

        pass  # auto-fix: empty except block

    live_mode_requested = mode_raw in ("LIVE_CANARY", "LIVE_FULL")
    live_allowed = live_mode_requested and not lock_active and not EXEC_DRY_RUN and not breaker_on

    emit_ai_decision(
        trade_id=client_trade_id,
        client_trade_id=client_trade_id,
        source_trade_id=source_trade_id,
        symbol=symbol,
        account_label=account_label,
        sub_uid=sub_uid,
        strategy_id=strat_id,
        strategy_name=strat_cfg.get("name", strat_name),
        timeframe=tf_norm,
        side=str(side),
        mode=trade_mode,
        allow=True,
        decision_code=effective_code,
        reason=effective_reason,
        ai_score=features_payload.get("ai_score"),
        size_multiplier=float(size_multiplier_applied) if size_multiplier_applied != 1.0 else None,
        extra={
            "stage": "pre_entry",
            "risk_usd": float(risk_capped),
            "risk_pct": float(eff_risk_pct),
            "qty": float(qty_capped),
            "decision_enforced": bool(EXEC_ENFORCE_DECISIONS),
            "trade_id_source": trade_id_source,
            "source_trade_id": source_trade_id,
            "sig_trade_id_present": bool(used_sig_trade_id),
            "forced_trade_id": bool(used_force_trade_id),
            "pilot_emitted": bool(pilot_row),
            "enforced_code": effective_code,
            "enforced_reason": effective_reason,
            "enforced_size_multiplier": float(size_multiplier_applied),
            "label_tf": tf_norm,
            "label_setup_type": setup_type_norm,
        },
    )

    if live_allowed:
        order_id = await execute_entry(
            symbol=symbol,
            signal_side=str(side),
            qty=float(qty_capped),
            price=price_f,
            strat=strat_id,
            mode=trade_mode,
            sub_uid=sub_uid,
            account_label=account_label,
            trade_id=client_trade_id,
            bound_log=bound,
            started_ms=started_ms,
        )
        if not order_id:
            bound.warning("LIVE entry failed; not emitting setup_context (no order_id).")
            return

        trade_id = str(order_id)

        try:
            if isinstance(pilot_row, dict):
                pilot_row2 = dict(pilot_row)
                pilot_row2["trade_id"] = trade_id
                pilot_row2["client_trade_id"] = client_trade_id
                pilot_row2["source_trade_id"] = source_trade_id
                _append_decision(pilot_row2)
        except Exception:
            pass

        emit_ai_decision(
            trade_id=trade_id,
            client_trade_id=client_trade_id,
            source_trade_id=source_trade_id,
            symbol=symbol,
            account_label=account_label,
            sub_uid=sub_uid,
            strategy_id=strat_id,
            strategy_name=strat_cfg.get("name", strat_name),
            timeframe=tf_norm,
            side=str(side),
            mode=trade_mode,
            allow=True,
            decision_code=effective_code,
            reason=effective_reason,
            ai_score=features_payload.get("ai_score"),
            size_multiplier=float(size_multiplier_applied) if size_multiplier_applied != 1.0 else None,
            extra={
                "stage": "post_entry",
                "order_id": trade_id,
                "join_key": "orderId",
                "decision_enforced": bool(EXEC_ENFORCE_DECISIONS),
                "pilot_emitted": bool(pilot_row),
                "enforced_code": effective_code,
                "enforced_reason": effective_reason,
                "enforced_size_multiplier": float(size_multiplier_applied),
                "label_tf": tf_norm,
                "label_setup_type": setup_type_norm,
            },
        )

        features_payload["trade_id"] = trade_id
        features_payload["order_id"] = trade_id
        features_payload["client_trade_id"] = client_trade_id
        features_payload["source_trade_id"] = source_trade_id

        try:
            log_features_at_open(
                trade_id=trade_id,
                ts_open_ms=ts_open_ms,
                symbol=symbol,
                sub_uid=sub_uid,
                strategy_name=strat_cfg.get("name", strat_name),
                setup_type=setup_type_val,
                mode=trade_mode,
                features=features_payload,
            )
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

        try:
            setup_event = build_setup_context(
                trade_id=trade_id,
                symbol=symbol,
                account_label=account_label,
                strategy=strat_cfg.get("name", strat_name),
                features=features_payload,
                setup_type=setup_type_val,
                timeframe=tf_norm,
                ai_profile=strat_cfg.get("ai_profile") or None,
                extra={
                    "mode": trade_mode,
                    "sub_uid": sub_uid or None,
                    "client_trade_id": client_trade_id,
                    "source_trade_id": source_trade_id,
                    "order_id": trade_id,
                    "join_key": "orderId",
                },
            )
            publish_ai_event(setup_event)
            setup_logged = True
            bound.info("✅ LIVE setup_context emitted trade_id(orderId)=%s client_trade_id=%s source_trade_id=%s symbol=%s", trade_id, client_trade_id, source_trade_id, symbol)
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

        return

    # --- PAPER path ---
    if is_training_mode:
        paper_stop_pct = 0.0015
        paper_tp_mult = 1.5
    else:
        paper_stop_pct = 0.005
        paper_tp_mult = 2.0

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
    else:
        stop_price = price_f + stop_distance_f
        take_profit_price = max(0.0001, price_f - (paper_tp_mult * stop_distance_f))

    features_for_paper = {
        **(features_payload or base_features),
        "paper_stop_pct": float(paper_stop_pct),
        "paper_tp_mult": float(paper_tp_mult),
        "stop_price": float(stop_price),
        "take_profit_price": float(take_profit_price),
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "ts_open_ms": ts_open_ms,
    }

    if str(setup_type_val).strip().lower() == "unknown":
        setup_logged = False
        bound.warning(
            "label_quarantine: PAPER unknown setup_type; skipping log_features_at_open (trade_id=%s symbol=%s raw=%r reason=%s)",
            trade_id, symbol, setup_type_raw, st_reason
        )
    else:
        try:
            log_features_at_open(
                trade_id=trade_id,
                ts_open_ms=ts_open_ms,
                symbol=symbol,
                sub_uid=sub_uid,
                strategy_name=strat_cfg.get("name", strat_name),
                setup_type=setup_type_val,
                mode=trade_mode,
                features=features_for_paper,
            )
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

    if str(setup_type_val).strip().lower() == "unknown":
        setup_logged = False
        bound.warning(
            "label_quarantine: PAPER unknown setup_type; skipping ai_event publish (trade_id=%s symbol=%s raw=%r reason=%s)",
            trade_id, symbol, setup_type_raw, st_reason
        )
        try:
            emit_ai_decision(
                trade_id=client_trade_id,
                client_trade_id=client_trade_id,
                source_trade_id=source_trade_id,
                symbol=symbol,
                account_label=account_label,
                sub_uid=sub_uid,
                strategy_id=strat_id,
                strategy_name=strat_cfg.get("name", strat_name),
                timeframe=tf_norm,
                side=str(side),
                mode=trade_mode,
                allow=True,
                decision_code="LABEL_QUARANTINE_PAPER",
                reason=f"unknown_setup_type_quarantined (raw={setup_type_raw!r} reason={st_reason})",
                ai_score=features_payload.get("ai_score"),
                size_multiplier=float(size_multiplier_applied) if size_multiplier_applied != 1.0 else None,
                extra={
                    "stage": "paper_label_quarantine",
                    "setup_type_raw": setup_type_raw,
                    "setup_type_norm": setup_type_val,
                    "setup_type_reason": st_reason,
                    "timeframe_norm": tf_norm,
                    "timeframe_reason": tf_reason,
                    "trade_id_source": trade_id_source,
                    "source_trade_id": source_trade_id,
                },
            )
        except Exception:
            pass
    else:
        try:
            setup_event = build_setup_context(
                trade_id=trade_id,
                symbol=symbol,
                account_label=account_label,
                strategy=strat_cfg.get("name", strat_name),
                features=features_for_paper,
                setup_type=setup_type_val,
                timeframe=tf_norm,
                ai_profile=strat_cfg.get("ai_profile") or None,
                extra={
                    "mode": trade_mode,
                    "sub_uid": sub_uid or None,
                    "client_trade_id": client_trade_id,
                    "source_trade_id": source_trade_id,
                    "join_key": "client_trade_id",
                },
            )
            publish_ai_event(setup_event)
            setup_logged = True
            bound.info("✅ PAPER setup_context emitted trade_id=%s source_trade_id=%s symbol=%s", trade_id, source_trade_id, symbol)
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

    try:
        broker = get_paper_broker(account_label=account_label, starting_equity=float(equity_val) if equity_val > 0 else 1000.0)
        broker.open_position(
            symbol=symbol,
            side=paper_side,
            entry_price=price_f,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            setup_type=setup_type_val,
            timeframe=tf_norm,
            features=features_for_paper,
            extra={
                "mode": trade_mode,
                "sub_uid": sub_uid or None,
                "strategy_name": strat_cfg.get("name", strat_name),
                "setup_logged_by_executor": setup_logged,
                "client_trade_id": client_trade_id,
                "source_trade_id": source_trade_id,
            },
            trade_id=trade_id,
            log_setup=(not setup_logged),
        )

        bound.info(
            "PAPER entry [%s]: %s %s qty=%s @ ~%s (risk_pct=%s stop=%s tp=%s trade_id=%s source_trade_id=%s setup_logged=%s)",
            strat_id, symbol, paper_side, qty_capped, price_f, eff_risk_pct, stop_price, take_profit_price, trade_id, source_trade_id, setup_logged
        )
    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:


        pass  # auto-fix: empty except block


def _normalize_order_side(signal_side: str) -> str:
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
) -> Optional[str]:
    client = get_trade_client(sub_uid or None)
    try:
        order_side = _normalize_order_side(signal_side)
    except ValueError as e:
        bound_log.error("cannot normalize side %r for %s: %r", signal_side, symbol, e)
        return None

    order_link_id = trade_id
    success = False
    start_ms = started_ms or int(time.time() * 1000)
    order_id_out: Optional[str] = None

    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=order_side,
            qty=qty,
            orderType="Market",
            orderLinkId=order_link_id,
        )
        bound_log.info("LIVE entry executed [%s %s]: %s %s qty=%s client_trade_id=%s resp=%r", mode, strat, symbol, order_side, qty, trade_id, resp)
        success = True

        try:
            result = resp.get("result") if isinstance(resp, dict) else None
            r = result or {}

            order_id = (r.get("orderId") or r.get("order_id") or r.get("orderID"))
            if order_id:
                order_id_out = str(order_id)
            else:
                order_id_out = str(order_link_id)

            status = (r.get("orderStatus") or r.get("order_status") or "New")
            order_type = (r.get("orderType") or r.get("order_type") or "Market")

            price_str = str(r.get("price") or price)
            qty_str = str(r.get("qty") or qty)
            cum_exec_qty = str(r.get("cumExecQty") or r.get("cum_exec_qty") or 0)
            cum_exec_value = str(r.get("cumExecValue") or r.get("cum_exec_value") or 0)
            cum_exec_fee = str(r.get("cumExecFee") or r.get("cum_exec_fee") or 0)

            record_order_event(
                account_label=account_label,
                symbol=symbol,
                order_id=str(order_id_out),
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
                raw={"api_response": r, "sub_uid": sub_uid, "mode": mode, "strategy": strat},
            )
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

        try:
            tg_send(f"🚀 Entry placed [{mode}/{strat}] {symbol} {order_side} qty={qty} client_trade_id={trade_id} order_id={order_id_out}")
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

    except Exception as e:
        pass  # auto-fix: empty except block
    except Exception as e:
        order_id_out = None

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
                extra={"sub_uid": sub_uid or None, "account_label": account_label, "qty": qty, "price": price, "success": success, "order_id": order_id_out, "client_trade_id": order_link_id},
            )
            if duration > LATENCY_WARN_MS:
                log.warning("High executor latency for %s (%s): %d ms (threshold=%d ms)", symbol, strat, duration, LATENCY_WARN_MS)
                try:
                    tg_send(f"⚠️ High executor latency [{mode}/{strat}] {symbol} {duration} ms (threshold={LATENCY_WARN_MS} ms)")
                except Exception:
                    pass
        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:

            pass  # auto-fix: empty except block

    return order_id_out


async def executor_loop() -> None:
    pos = load_cursor()

    if EXEC_CURSOR_SELF_HEAL:
        healed = _cursor_heal_to_line_boundary(pos)
        if healed != pos:
            log.info("cursor self-heal: %s -> %s (SIGNAL_FILE=%s)", pos, healed, str(SIGNAL_FILE))
            pos = healed
            save_cursor(pos)

    log.info("executor_v2 starting at cursor=%s (EXEC_DRY_RUN=%s)", pos, EXEC_DRY_RUN)

    last_idle_log = time.time()

    while True:
        try:
            record_heartbeat("executor_v2")

            if not SIGNAL_FILE.exists():
                await asyncio.sleep(0.5)
                continue

            file_stat = SIGNAL_FILE.stat()
            file_size = file_stat.st_size

            if pos > file_size:
                log.info("Signal file truncated (size=%s, cursor=%s). Resetting cursor to 0.", file_size, pos)
                pos = 0
                save_cursor(pos)

            if EXEC_CURSOR_SELF_HEAL and pos > 0 and file_size > 0 and pos < file_size:
                healed = _cursor_heal_to_line_boundary(pos)
                if healed != pos:
                    log.info("cursor runtime-heal: %s -> %s (size=%s)", pos, healed, file_size)
                    pos = healed
                    save_cursor(pos)

            processed = 0
            with SIGNAL_FILE.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    try:
                        line = raw.decode("utf-8").strip()
                    except Exception as e:
                        pass  # auto-fix: empty except block
                    except Exception as e:
                        continue
                    if not line:
                        continue
                    await asyncio.sleep(0)
                    await process_signal_line(line)
                    processed += 1
                    save_cursor(pos)

            now = time.time()
            if (now - last_idle_log) >= float(EXEC_IDLE_HEARTBEAT_SEC):
                try:
                    age_s = now - file_stat.st_mtime
                except Exception:
                    age_s = -1.0
                log.info(
                    "idle: processed=%s cursor=%s file_size=%s file_age=%.2fs",
                    processed, pos, file_size, float(age_s)
                )
                last_idle_log = now

            await asyncio.sleep(0.25)

        except Exception as e:
            pass  # auto-fix: empty except block
        except Exception as e:
            await asyncio.sleep(1.0)


def main() -> None:
    try:
        asyncio.run(executor_loop())
    except KeyboardInterrupt:
        log.info("executor_v2 stopped by user")


if __name__ == "__main__":
    main()
