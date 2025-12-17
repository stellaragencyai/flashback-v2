#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Paper Broker (LEARN_DRY engine, v1.3)

Patch v1.3 (2025-12-13)
-----------------------
✅ Publishes PAPER positions into the canonical positions bus:
    state/positions_bus.json

Why:
- tp_sl_manager reads positions_bus.json
- executor_v2 in PAPER mode opens positions via PaperBroker
- previously PaperBroker only wrote state/paper/<account_label>.json
  so TP/SL never saw paper positions (positions=0 forever)

This patch makes paper trades visible system-wide without touching executor or tp_sl_manager.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

import yaml  # type: ignore

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None

try:
    from app.ai.ai_events_spine import (  # type: ignore
        build_outcome_record,
        publish_ai_event,
    )
except Exception:  # pragma: no cover
    def build_outcome_record(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {}
    def publish_ai_event(*args: Any, **kwargs: Any) -> None:  # type: ignore
        pass

log = get_logger("paper_broker")


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Dataclasses for in-memory paper positions / account state
# ---------------------------------------------------------------------------

Side = Literal["long", "short"]


@dataclass
class PaperPosition:
    trade_id: str
    symbol: str
    side: Side
    entry_price: float
    size: float
    risk_usd: float
    stop_price: float
    take_profit_price: float

    setup_type: Optional[str]
    timeframe: Optional[str]
    ai_profile: Optional[str]

    opened_ms: int
    closed_ms: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl_usd: float = 0.0
    r_multiple: Optional[float] = None


@dataclass
class PaperAccountState:
    account_label: str
    strategy_name: str
    ai_profile: Optional[str]
    risk_pct: float
    equity: float
    starting_equity: float
    created_ms: int
    updated_ms: int
    open_positions: List[PaperPosition]
    closed_trades: List[PaperPosition]


# ---------------------------------------------------------------------------
# Helpers to load strategy config
# ---------------------------------------------------------------------------

def _load_strategy_for_label(account_label: str) -> Dict[str, Any]:
    """
    Load the strategy block from config/strategies.yaml for the given account_label.
    """
    cfg_path = ROOT / "config" / "strategies.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"strategies.yaml not found at {cfg_path}")

    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    subs = data.get("subaccounts") or []

    for sub in subs:
        if str(sub.get("account_label")) == account_label:
            return sub

    for sub in subs:
        if str(sub.get("name")) == account_label:
            return sub

    raise ValueError(f"No strategy config found for account_label={account_label!r}")


# ---------------------------------------------------------------------------
# Positions bus writer (canonical visibility for TP/SL Manager)
# ---------------------------------------------------------------------------

_POSITIONS_BUS_PATH: Path = ROOT / "state" / "positions_bus.json"
_POSITIONS_BUS_PATH.parent.mkdir(parents=True, exist_ok=True)


def _safe_read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "")
    except Exception:
        return None


def _safe_write_json(path: Path, obj: Any) -> None:
    try:
        path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    except Exception as e:
        log.warning("Failed to write %s: %r", path, e)


def _paper_position_to_bus_row(account_label: str, pos: PaperPosition) -> Dict[str, Any]:
    """
    Minimal bus row schema that downstream readers can tolerate.
    We intentionally keep this flat and boring.
    """
    return {
        "trade_id": str(pos.trade_id),
        "symbol": str(pos.symbol),
        "account_label": str(account_label),
        "side": "Buy" if pos.side == "long" else "Sell",
        "position_side": "LONG" if pos.side == "long" else "SHORT",
        "qty": float(pos.size),
        "size": float(pos.size),
        "avg_price": float(pos.entry_price),
        "entry_price": float(pos.entry_price),
        "stop_price": float(pos.stop_price),
        "take_profit_price": float(pos.take_profit_price),
        "risk_usd": float(pos.risk_usd),
        "opened_ms": int(pos.opened_ms),
        "mode": "PAPER",
        "source": "paper_broker",
        "setup_type": pos.setup_type,
        "timeframe": pos.timeframe,
        "ai_profile": pos.ai_profile,
    }


def _publish_positions_bus(account_label: str, open_positions: List[PaperPosition]) -> None:
    """
    Publish PAPER positions into state/positions_bus.json alongside any existing rows.

    Rules:
    - We replace ALL prior PAPER rows for this account_label
    - We do NOT touch non-PAPER rows (live/other sources)
    - This makes TP/SL Manager see paper positions without needing modifications
    """
    now_ms = _now_ms()
    existing = _safe_read_json(_POSITIONS_BUS_PATH)

    if isinstance(existing, dict):
        rows = existing.get("positions") or []
    elif isinstance(existing, list):
        rows = existing
    else:
        rows = []

    # Keep non-paper rows and paper rows from other accounts
    kept: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        is_paper = str(r.get("mode") or "").upper() == "PAPER" or str(r.get("source") or "") == "paper_broker"
        same_acct = str(r.get("account_label") or "") == account_label
        if is_paper and same_acct:
            continue
        kept.append(r)

    new_rows = [_paper_position_to_bus_row(account_label, p) for p in open_positions]

    out = {
        "ts_ms": now_ms,
        "source": "paper_broker",
        "positions": kept + new_rows,
    }
    _safe_write_json(_POSITIONS_BUS_PATH, out)


# ---------------------------------------------------------------------------
# PaperBroker
# ---------------------------------------------------------------------------

class PaperBroker:
    """
    PaperBroker manages simulated positions & equity for one account_label.
    """

    def __init__(self, state: PaperAccountState, state_path: Path) -> None:
        self._state = state
        self._state_path = state_path

    @classmethod
    def load_or_create(
        cls,
        account_label: str,
        *,
        starting_equity: float = 1000.0,
    ) -> "PaperBroker":
        paper_dir = ROOT / "state" / "paper"
        paper_dir.mkdir(parents=True, exist_ok=True)
        state_path = paper_dir / f"{account_label}.json"

        strategy = _load_strategy_for_label(account_label)
        strategy_name = str(strategy.get("name") or account_label)
        ai_profile = strategy.get("ai_profile")
        risk_pct = float(strategy.get("risk_pct") or 0.0)

        if state_path.exists():
            try:
                raw = json.loads(state_path.read_text(encoding="utf-8") or "{}")
            except Exception:
                raw = {}

            try:
                open_positions = [
                    PaperPosition(**pos) for pos in raw.get("open_positions") or []
                ]
                closed_trades = [
                    PaperPosition(**pos) for pos in raw.get("closed_trades") or []
                ]
            except Exception:
                open_positions = []
                closed_trades = []

            equity = float(raw.get("equity") or starting_equity)
            starting_equity_loaded = float(raw.get("starting_equity") or starting_equity)
            created_ms = int(raw.get("created_ms") or _now_ms())
            updated_ms = int(raw.get("updated_ms") or _now_ms())

            state = PaperAccountState(
                account_label=account_label,
                strategy_name=strategy_name,
                ai_profile=ai_profile,
                risk_pct=risk_pct,
                equity=equity,
                starting_equity=starting_equity_loaded,
                created_ms=created_ms,
                updated_ms=updated_ms,
                open_positions=open_positions,
                closed_trades=closed_trades,
            )
            log.info(
                "Loaded existing paper ledger for %s (equity=%.2f, open=%d, closed=%d)",
                account_label,
                state.equity,
                len(state.open_positions),
                len(state.closed_trades),
            )

            broker = cls(state, state_path)
            # ✅ Publish bus visibility on load (so TP/SL sees existing open paper positions)
            _publish_positions_bus(account_label, state.open_positions)
            return broker

        now = _now_ms()
        state = PaperAccountState(
            account_label=account_label,
            strategy_name=strategy_name,
            ai_profile=ai_profile,
            risk_pct=risk_pct,
            equity=float(starting_equity),
            starting_equity=float(starting_equity),
            created_ms=now,
            updated_ms=now,
            open_positions=[],
            closed_trades=[],
        )
        broker = cls(state, state_path)
        broker._save()
        _publish_positions_bus(account_label, state.open_positions)
        log.info(
            "Created new paper ledger for %s (starting_equity=%.2f, risk_pct=%.4f)",
            account_label,
            starting_equity,
            risk_pct,
        )
        return broker

    def _save(self) -> None:
        self._state.updated_ms = _now_ms()
        payload: Dict[str, Any] = {
            "account_label": self._state.account_label,
            "strategy_name": self._state.strategy_name,
            "ai_profile": self._state.ai_profile,
            "risk_pct": self._state.risk_pct,
            "equity": self._state.equity,
            "starting_equity": self._state.starting_equity,
            "created_ms": self._state.created_ms,
            "updated_ms": self._state.updated_ms,
            "open_positions": [asdict(p) for p in self._state.open_positions],
            "closed_trades": [asdict(p) for p in self._state.closed_trades],
        }
        try:
            self._state_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning(
                "[paper_broker] Failed to save paper state for %s: %r",
                self._state.account_label,
                e,
            )

    @property
    def account_label(self) -> str:
        return self._state.account_label

    @property
    def equity(self) -> float:
        return self._state.equity

    @property
    def risk_pct(self) -> float:
        return self._state.risk_pct

    def list_open_positions(self) -> List[PaperPosition]:
        return list(self._state.open_positions)

    def list_closed_trades(self) -> List[PaperPosition]:
        return list(self._state.closed_trades)

    def _generate_trade_id(self, symbol: str) -> str:
        suffix = uuid.uuid4().hex[:10]
        return f"{self._state.account_label}-{symbol}-{suffix}"

    def open_position(
        self,
        *,
        symbol: str,
        side: Side,
        entry_price: float,
        stop_price: float,
        take_profit_price: float,
        setup_type: Optional[str],
        timeframe: Optional[str],
        features: Dict[str, Any],
        extra: Optional[Dict[str, Any]] = None,
        trade_id: Optional[str] = None,
        log_setup: bool = False,
    ) -> PaperPosition:
        if entry_price <= 0 or stop_price <= 0:
            raise ValueError("entry_price and stop_price must be > 0")

        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            raise ValueError("stop_price must differ from entry_price")

        features_ext = dict(features or {})

        if "risk_usd" in features_ext and features_ext["risk_usd"] is not None:
            try:
                risk_amount = float(features_ext["risk_usd"])
            except Exception:
                risk_amount = self._state.equity * max(self._state.risk_pct, 0.0)
        else:
            risk_amount = self._state.equity * max(self._state.risk_pct, 0.0)
            features_ext["risk_usd"] = float(risk_amount)

        if risk_amount <= 0:
            log.warning(
                "[paper_broker] risk_amount=0 for %s; opening position with zero risk.",
                self._state.account_label,
            )

        if "size" in features_ext and features_ext["size"] is not None:
            try:
                size_val = float(features_ext["size"])
            except Exception:
                size_val = risk_amount / stop_distance if stop_distance > 0 else 0.0
        elif "qty" in features_ext and features_ext["qty"] is not None:
            try:
                size_val = float(features_ext["qty"])
            except Exception:
                size_val = risk_amount / stop_distance if stop_distance > 0 else 0.0
        else:
            size_val = risk_amount / stop_distance if stop_distance > 0 else 0.0
            features_ext["size"] = float(size_val)

        trade_id_final = trade_id or self._generate_trade_id(symbol)
        now = _now_ms()

        if log_setup:
            try:
                from app.ai.ai_events_spine import (  # type: ignore
                    build_setup_context,
                    publish_ai_event as _pub_setup,
                )

                setup_event = build_setup_context(
                    trade_id=trade_id_final,
                    symbol=symbol,
                    account_label=self._state.account_label,
                    strategy=self._state.strategy_name,
                    features=features_ext,
                    setup_type=setup_type,
                    timeframe=timeframe,
                    ai_profile=self._state.ai_profile,
                    extra=extra,
                )
                _pub_setup(setup_event)
            except Exception as e:
                log.warning("[paper_broker] Optional setup logging failed: %r", e)

        pos = PaperPosition(
            trade_id=trade_id_final,
            symbol=symbol,
            side=side,
            entry_price=float(entry_price),
            size=float(size_val),
            risk_usd=float(risk_amount),
            stop_price=float(stop_price),
            take_profit_price=float(take_profit_price),
            setup_type=setup_type,
            timeframe=timeframe,
            ai_profile=self._state.ai_profile,
            opened_ms=now,
        )
        self._state.open_positions.append(pos)
        self._save()

        # ✅ Publish to canonical bus so tp_sl_manager sees it
        _publish_positions_bus(self._state.account_label, self._state.open_positions)

        log.info(
            "[paper_broker] OPEN %s %s side=%s size=%.4f entry=%.4f sl=%.4f tp=%.4f risk_usd=%.2f",
            self._state.account_label,
            symbol,
            side,
            size_val,
            entry_price,
            stop_price,
            take_profit_price,
            risk_amount,
        )
        return pos

    def _close_position(
        self,
        pos: PaperPosition,
        *,
        exit_price: float,
        exit_reason: str,
    ) -> None:
        if exit_price <= 0:
            raise ValueError("exit_price must be > 0")

        equity_before = float(self._state.equity)

        if pos.side == "long":
            pnl = (exit_price - pos.entry_price) * pos.size
        else:
            pnl = (pos.entry_price - exit_price) * pos.size

        r_mult: Optional[float] = None
        if pos.risk_usd is not None and pos.risk_usd != 0:
            r_mult = pnl / pos.risk_usd

        now_ms = _now_ms()
        pos.exit_price = float(exit_price)
        pos.exit_reason = exit_reason
        pos.closed_ms = now_ms
        pos.pnl_usd = float(pnl)
        pos.r_multiple = r_mult

        self._state.equity += float(pnl)
        equity_after = float(self._state.equity)

        trade_duration_ms: Optional[int] = None
        if pos.opened_ms is not None:
            trade_duration_ms = now_ms - pos.opened_ms

        outcome_event = build_outcome_record(
            trade_id=pos.trade_id,
            symbol=pos.symbol,
            account_label=self._state.account_label,
            strategy=self._state.strategy_name,
            pnl_usd=float(pnl),
            r_multiple=r_mult,
            win=(r_mult is not None and r_mult > 0),
            exit_reason=exit_reason,
            extra={
                "side": pos.side,
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "risk_usd": pos.risk_usd,
                "setup_type": pos.setup_type,
                "timeframe": pos.timeframe,
                "ai_profile": pos.ai_profile,
                "opened_ms": pos.opened_ms,
                "closed_ms": pos.closed_ms,
                "trade_duration_ms": trade_duration_ms,
                "equity_before": equity_before,
                "equity_after": equity_after,
                "mode": "PAPER",
                "source": "paper_broker",
            },
        )
        publish_ai_event(outcome_event)

        self._state.open_positions = [
            p for p in self._state.open_positions if p.trade_id != pos.trade_id
        ]
        self._state.closed_trades.append(pos)
        self._save()

        # ✅ Publish updated bus after close
        _publish_positions_bus(self._state.account_label, self._state.open_positions)

        log.info(
            "[paper_broker] CLOSE %s %s side=%s exit=%.4f pnl=%.2f R=%s reason=%s equity=%.2f",
            self._state.account_label,
            pos.symbol,
            pos.side,
            exit_price,
            pnl,
            f"{r_mult:.2f}" if r_mult is not None else "None",
            exit_reason,
            self._state.equity,
        )

    def update_price(self, symbol: str, price: float) -> None:
        if price <= 0:
            return

        to_close: List[Any] = []

        for pos in list(self._state.open_positions):
            if pos.symbol != symbol:
                continue

            if pos.side == "long":
                if price >= pos.take_profit_price:
                    to_close.append((pos, "tp_hit"))
                elif price <= pos.stop_price:
                    to_close.append((pos, "sl_hit"))
            else:  # short
                if price <= pos.take_profit_price:
                    to_close.append((pos, "tp_hit"))
                elif price >= pos.stop_price:
                    to_close.append((pos, "sl_hit"))

        for pos, reason in to_close:
            self._close_position(pos, exit_price=price, exit_reason=reason)


def main() -> None:
    log.info(
        "PaperBroker main() called. This module is intended for import/use by "
        "Executor, not as a standalone loop."
    )
    while True:
        try:
            record_heartbeat("paper_broker")
        except Exception:
            pass
        time.sleep(15)


if __name__ == "__main__":
    main()
