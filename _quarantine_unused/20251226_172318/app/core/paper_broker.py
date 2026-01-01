#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — PaperBroker v1.1 (executor-compatible + idempotent outcome emission)

Fixes / Upgrades
----------------
- Adds PaperBroker class wrapper so executor_v2 can call:
    • PaperBroker.load_or_create(...)
    • broker.open_position(...)
    • broker.update_price(...)
- Keeps disk-based robustness.
- Adds close de-dupe so we don't emit duplicate outcomes after restarts:
    state/paper_closed_ids.json

Notes
-----
- This module is PAPER-only. It never touches live orders.
- OutcomeRecords are published via app.ai.ai_events_spine, which will
  merge them with SetupContext events (by trade_id) and compute R-multiples.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# ROOT + logger
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Global (shared) paper state files
PAPER_POSITIONS_PATH: Path = STATE_DIR / "paper_positions.json"
PAPER_TRADES_LOG_PATH: Path = STATE_DIR / "paper_trades.jsonl"

# Close de-dupe file (prevents double outcome emission across restarts)
PAPER_CLOSED_IDS_PATH: Path = STATE_DIR / "paper_closed_ids.json"

# Logging
try:
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
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

log = get_logger("paper_broker")

# ---------------------------------------------------------------------------
# AI events spine (OutcomeRecord emission)
# ---------------------------------------------------------------------------

try:
    from app.ai.ai_events_spine import (  # type: ignore
        build_outcome_record,
        publish_ai_event,
    )
    _AI_EVENTS_AVAILABLE = True
except Exception:  # pragma: no cover
    log.warning(
        "ai_events_spine not available; PaperBroker will NOT emit AI events. "
        "Check imports if you expected Setup/Outcome logging."
    )

def build_outcome_record(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    pnl_usd: float,
    r_multiple: Optional[float] = None,
    win: Optional[bool] = None,
    exit_reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Phase 4–compatible fallback OutcomeRecord.
    Matches ai_events_spine contract shape.
    """

    extra_payload = {
        "schema_version": "paper_trade_close_v1",
        "lifecycle_stage": "TRADE_CLOSED",
        "is_final": True,
        "is_final_authority": True,
        "exit_controller": "paper_broker",
        "pnl_kind": "realized",
    }

    if extra:
        extra_payload.update(extra)

    return {
        "event_type": "outcome_record",
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "pnl_usd": float(pnl_usd),
        "r_multiple": float(r_multiple) if r_multiple is not None else None,
        "win": bool(win) if win is not None else None,
        "exit_reason": exit_reason,
        "extra": extra_payload,
    }


    def publish_ai_event(event: Dict[str, Any]) -> None:
        return None

    _AI_EVENTS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _safe_price(x: Any) -> float:
    try:
        v = float(x)
        if v <= 0:
            return 0.0
        return v
    except Exception:
        return 0.0

_FINALIZED_TRADES_PATH = Path("data/finalized_trades.json")


def _load_finalized_trades() -> set[str]:
    try:
        if _FINALIZED_TRADES_PATH.exists():
            with _FINALIZED_TRADES_PATH.open("r", encoding="utf-8") as f:
                return set(json.load(f))
    except Exception as e:
        log.error("Failed to load finalized trades registry: %r", e)
    return set()


def _save_finalized_trades(trade_ids: set[str]) -> None:
    try:
        _FINALIZED_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _FINALIZED_TRADES_PATH.open("w", encoding="utf-8") as f:
            json.dump(sorted(trade_ids), f, indent=2)
    except Exception as e:
        log.error("Failed to save finalized trades registry: %r", e)

_CLOSED_TRADES_PATH = Path("data/closed_trades.json")


def _load_closed_trades() -> Dict[str, int]:
    try:
        if _CLOSED_TRADES_PATH.exists():
            with _CLOSED_TRADES_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.error("Failed to load closed trades registry: %r", e)
    return {}


def _save_closed_trades(registry: Dict[str, int]) -> None:
    try:
        _CLOSED_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _CLOSED_TRADES_PATH.open("w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2)
    except Exception as e:
        log.error("Failed to save closed trades registry: %r", e)


def _mark_closed_once(trade_id: str, ts_ms: int) -> bool:
    """
    Returns True if this is the FIRST final close for trade_id.
    Returns False if already finalized (idempotent guard).
    """
    registry = _load_closed_trades()

    if trade_id in registry:
        return False

    registry[trade_id] = int(ts_ms)
    _save_closed_trades(registry)
    return True

def _load_positions() -> Dict[str, Dict[str, Any]]:
    if not PAPER_POSITIONS_PATH.exists():
        return {}
    try:
        raw = PAPER_POSITIONS_PATH.read_text(encoding="utf-8")
        if not raw.strip():
            return {}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        pos = data.get("positions")
        if not isinstance(pos, dict):
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for tid, pd in pos.items():
            if isinstance(tid, str) and isinstance(pd, dict):
                out[tid] = pd
        return out
    except Exception as e:
        log.warning("Failed to load PAPER positions: %r", e)
        return {}


def _save_positions(positions: Dict[str, Dict[str, Any]]) -> None:
    payload = {"version": 1, "updated_ms": _now_ms(), "positions": positions}
    try:
        PAPER_POSITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAPER_POSITIONS_PATH.write_text(
            json.dumps(payload, separators=(",", ":")),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning("Failed to save PAPER positions: %r", e)


def _append_trade_log(row: Dict[str, Any]) -> None:
    try:
        PAPER_TRADES_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PAPER_TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    except Exception as e:
        log.warning("Failed to append paper trade log: %r", e)


def _load_closed_ids() -> Dict[str, int]:
    """
    Returns {trade_id: ts_ms_closed}
    """
    if not PAPER_CLOSED_IDS_PATH.exists():
        return {}
    try:
        raw = PAPER_CLOSED_IDS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_closed_ids(d: Dict[str, int]) -> None:
    try:
        PAPER_CLOSED_IDS_PATH.write_text(
            json.dumps(d, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning("Failed to save closed-ids registry: %r", e)


def _mark_closed_once(trade_id: str, ts_ms: int) -> bool:
    """
    Returns True if this trade_id was newly marked closed.
    Returns False if it was already closed (dup).
    """
    try:
        reg = _load_closed_ids()
        if trade_id in reg:
            return False
        reg[trade_id] = int(ts_ms)
        _save_closed_ids(reg)
        return True
    except Exception:
        # If registry fails, we still prefer emitting (best effort)
        return True


# ---------------------------------------------------------------------------
# Outcome emission
# ---------------------------------------------------------------------------

def _emit_outcome_for_closed_position(
    pos: Dict[str, Any],
    *,
    exit_price: float,
    exit_reason: str,
    ts_ms: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    
    

    trade_id = str(pos.get("trade_id") or "").strip()
    symbol = str(pos.get("symbol") or "").strip().upper()
    side = str(pos.get("side") or "").title()  # "Buy" / "Sell"
    qty = _safe_float(pos.get("qty"))
    entry_price = _safe_price(pos.get("entry_price"))

    account_label = str(pos.get("account_label") or "main").strip()
    strategy = str(pos.get("strategy") or "unknown_strategy").strip()

    if not trade_id or not symbol or qty <= 0 or entry_price <= 0:
        log.warning(
            "Skipping outcome emission for malformed PAPER position: trade_id=%r symbol=%r qty=%r entry_price=%r",
            trade_id,
            symbol,
            qty,
            entry_price,
        )
        return

    exit_price = _safe_price(exit_price)
    if exit_price <= 0:
        log.warning(
            "Skipping outcome emission for trade_id=%s symbol=%s: invalid exit_price=%r",
            trade_id,
            symbol,
            exit_price,
        )
        return

    ts_close = ts_ms if ts_ms is not None else _now_ms()

    # ✅ idempotent: never emit twice for same trade_id
    if not _mark_closed_once(trade_id, ts_close):
        log.warning("Duplicate close suppressed for trade_id=%s symbol=%s", trade_id, symbol)
        return

    if side == "Buy":
        pnl_usd = (exit_price - entry_price) * qty
    elif side == "Sell":
        pnl_usd = (entry_price - exit_price) * qty
    else:
        log.warning(
            "Unknown side %r for PAPER trade_id=%s symbol=%s; assuming no PnL.",
            side,
            trade_id,
            symbol,
        )
        pnl_usd = 0.0

    base_extra: Dict[str, Any] = {
    "mode": pos.get("mode"),
    "sub_uid": pos.get("sub_uid"),
    "entry_ts_ms": pos.get("entry_ts_ms"),
    "exit_ts_ms": ts_close,
    "stop_price": pos.get("stop_price"),
    "tp_price": pos.get("tp_price"),
    "meta": pos.get("meta") or {},
    "paper_risk_usd": pos.get("risk_usd"),
    }

    extra = {
    **(extra or {}),
    **base_extra,
    # Phase 4 final lifecycle authority
    "schema_version": "final_trade_close_v1",
    "lifecycle_stage": "EXIT_MANAGEMENT",
    "is_final_authority": True,
    "exit_controller": "paper_broker",
    }


    event = build_outcome_record(
        trade_id=trade_id,
        symbol=symbol,
        account_label=account_label,
        strategy=strategy,
        pnl_usd=float(pnl_usd),
        r_multiple=None,  # ai_events_spine computes from SetupContext.features.risk_usd
        win=None,
        exit_reason=exit_reason,
        extra=extra,
    )

    try:
        publish_ai_event(event)
    except Exception as e:
        log.warning(
            "Failed to publish AI OutcomeRecord for trade_id=%s symbol=%s: %r",
            trade_id,
            symbol,
            e,
        )

    try:
        row = {
            "ts_ms": ts_close,
            "event": "close",
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "account_label": account_label,
            "strategy": strategy,
            "exit_reason": exit_reason,
            "pnl_usd": float(pnl_usd),
        }
        _append_trade_log(row)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Functional API (kept)
# ---------------------------------------------------------------------------

def open_paper_position(
    *,
    trade_id: str,
    symbol: str,
    side: str,
    entry_price: float,
    qty: float,
    account_label: str,
    strategy: str,
    mode: str = "PAPER",
    sub_uid: Optional[str] = None,
    stop_price: Optional[float] = None,
    tp_price: Optional[float] = None,
    risk_usd: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    trade_id = str(trade_id or "").strip()
    symbol = str(symbol or "").upper().strip()
    side_norm = str(side or "").title().strip()  # "Buy" / "Sell"
    account_label = str(account_label or "main").strip()
    strategy = str(strategy or "unknown_strategy").strip()
    mode = str(mode or "PAPER").strip()

    entry_price_f = _safe_price(entry_price)
    qty_f = _safe_float(qty)
    if not trade_id or not symbol or qty_f <= 0 or entry_price_f <= 0:
        log.warning(
            "Refusing to open PAPER position: invalid inputs trade_id=%r symbol=%r qty=%r entry_price=%r",
            trade_id, symbol, qty, entry_price,
        )
        return

    ts_open = ts_ms if ts_ms is not None else _now_ms()

    positions = _load_positions()
    pos: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side_norm,
        "qty": qty_f,
        "entry_price": entry_price_f,
        "entry_ts_ms": ts_open,
        "account_label": account_label,
        "strategy": strategy,
        "mode": mode,
        "sub_uid": str(sub_uid) if sub_uid is not None else None,
        "stop_price": _safe_price(stop_price) if stop_price is not None else None,
        "tp_price": _safe_price(tp_price) if tp_price is not None else None,
        "risk_usd": _safe_float(risk_usd) if risk_usd is not None else None,
        "meta": meta or {},
    }

    positions[trade_id] = pos
    _save_positions(positions)

    try:
        row = {
            "ts_ms": ts_open,
            "event": "open",
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side_norm,
            "qty": qty_f,
            "entry_price": entry_price_f,
            "account_label": account_label,
            "strategy": strategy,
            "mode": mode,
        }
        _append_trade_log(row)
    except Exception:
        pass

    log.info(
        "Opened PAPER position trade_id=%s symbol=%s side=%s qty=%s entry=%s account_label=%s strategy=%s",
        trade_id, symbol, side_norm, qty_f, entry_price_f, account_label, strategy,
    )


def close_paper_position(
    trade_id: str,
    *,
    exit_price: float,
    exit_reason: str = "manual_close",
    ts_ms: Optional[int] = None,
) -> None:
    trade_id = str(trade_id or "").strip()
    if not trade_id:
        return

    positions = _load_positions()
    pos = positions.pop(trade_id, None)
    if pos is None:
        log.warning("close_paper_position called for unknown trade_id=%s", trade_id)
        return

    _save_positions(positions)

    # ------------------------------------------------------------------
    # Phase 4: PAPER BROKER is the FINAL lifecycle authority
    # ------------------------------------------------------------------
    final_lifecycle = {
        "schema_version": "final_trade_close_v1",
        "lifecycle_stage": "TRADE_CLOSED",
        "is_final_authority": True,
        "close_emitter": "paper_broker",
        "execution_mode": "paper",
        "outcome_confidence": "HIGH",
    }

    _emit_outcome_for_closed_position(
        pos,
        exit_price=_safe_price(exit_price),
        exit_reason=exit_reason,
        ts_ms=ts_ms,
        extra=final_lifecycle,   # 🔴 THIS IS THE KEY LINE
    )


def on_price_tick(*, symbol: str, price: float, ts_ms: Optional[int] = None) -> None:
    symbol = str(symbol or "").upper().strip()
    price_f = _safe_price(price)
    if not symbol or price_f <= 0:
        return

    ts = ts_ms if ts_ms is not None else _now_ms()

    positions = _load_positions()
    if not positions:
        return

    changed = False

    for trade_id, pos in list(positions.items()):
        try:
            if str(pos.get("symbol") or "").upper() != symbol:
                continue

            side = str(pos.get("side") or "").title()
            sl = pos.get("stop_price")
            tp = pos.get("tp_price")

            exit_reason: Optional[str] = None
            exit_price_f: float = price_f

            if side == "Buy":
                if isinstance(sl, (int, float)) and _safe_price(sl) > 0 and price_f <= _safe_price(sl):
                    exit_reason = "sl_hit"
                    exit_price_f = _safe_price(sl)
                elif isinstance(tp, (int, float)) and _safe_price(tp) > 0 and price_f >= _safe_price(tp):
                    exit_reason = "tp_hit"
                    exit_price_f = _safe_price(tp)

            elif side == "Sell":
                if isinstance(sl, (int, float)) and _safe_price(sl) > 0 and price_f >= _safe_price(sl):
                    exit_reason = "sl_hit"
                    exit_price_f = _safe_price(sl)
                elif isinstance(tp, (int, float)) and _safe_price(tp) > 0 and price_f <= _safe_price(tp):
                    exit_reason = "tp_hit"
                    exit_price_f = _safe_price(tp)

            if not exit_reason:
                continue

            positions.pop(trade_id, None)
            changed = True

            _emit_outcome_for_closed_position(
                pos,
                exit_price=exit_price_f,
                exit_reason=exit_reason,
                ts_ms=ts,
            )

            log.info(
                "Closed PAPER position trade_id=%s symbol=%s reason=%s exit_price=%s",
                trade_id, symbol, exit_reason, exit_price_f,
            )

        except Exception as e:
            log.warning("Error processing PAPER tick for trade_id=%s symbol=%s: %r", trade_id, symbol, e)

    if changed:
        _save_positions(positions)


def list_open_positions() -> Dict[str, Dict[str, Any]]:
    return _load_positions()


# ---------------------------------------------------------------------------
# ✅ Executor-compatible class wrapper
# ---------------------------------------------------------------------------

class PaperBroker:
    """
    Compatibility wrapper for executor_v2.

    executor_v2 expects:
      - PaperBroker.load_or_create(account_label=..., starting_equity=...)
      - broker.open_position(...)
    """

    def __init__(self, account_label: str, starting_equity: float) -> None:
        self.account_label = str(account_label or "main").strip()
        self.starting_equity = float(starting_equity or 0.0)

    @classmethod
    def load_or_create(cls, *, account_label: str, starting_equity: float) -> "PaperBroker":
        # Nothing heavy to load; disk state is global in this version.
        return cls(account_label=account_label, starting_equity=starting_equity)

    def open_position(
        self,
        *,
        symbol: str,
        side: str,
        entry_price: float,
        stop_price: float,
        take_profit_price: float,
        setup_type: str,
        timeframe: str,
        features: Dict[str, Any],
        extra: Optional[Dict[str, Any]] = None,
        trade_id: str,
        log_setup: bool = False,
    ) -> None:
        """
        Matches the call signature used in executor_v2 right now.

        NOTE:
          - log_setup is handled by executor (you already fixed that logic).
          - We do NOT emit setup here. We only manage paper trade lifecycle.
        """
        # executor passes side as "long"/"short" here; normalize to Buy/Sell
        s = str(side or "").strip().lower()
        side_api = "Buy" if s in ("long", "buy") else "Sell"

        risk_usd = None
        try:
            # Prefer risk_usd from features if present
            risk_usd = (features or {}).get("risk_usd")
        except Exception:
            risk_usd = None

        open_paper_position(
            trade_id=str(trade_id),
            symbol=str(symbol),
            side=side_api,
            entry_price=float(entry_price),
            qty=float((features or {}).get("qty") or (features or {}).get("size") or 0.0),
            account_label=str((features or {}).get("account_label") or self.account_label),
            strategy=str((extra or {}).get("strategy_name") or (features or {}).get("strategy_name") or "unknown_strategy"),
            mode=str((extra or {}).get("mode") or (features or {}).get("trade_mode") or "PAPER"),
            sub_uid=str((extra or {}).get("sub_uid") or (features or {}).get("sub_uid") or "") or None,
            stop_price=float(stop_price),
            tp_price=float(take_profit_price),
            risk_usd=_safe_float(risk_usd) if risk_usd is not None else None,
            meta={
                "setup_type": setup_type,
                "timeframe": timeframe,
                "features": features or {},
                "extra": extra or {},
                "setup_logged": bool(log_setup),
            },
            ts_ms=int((features or {}).get("ts_open_ms") or _now_ms()),
        )

    def update_price(self, *, symbol: str, price: float, ts_ms: Optional[int] = None) -> None:
        on_price_tick(symbol=symbol, price=price, ts_ms=ts_ms)

    def close(self, *, trade_id: str, exit_price: float, exit_reason: str = "manual_close") -> None:
        close_paper_position(trade_id, exit_price=exit_price, exit_reason=exit_reason)
