#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Paper Broker (LEARN_DRY engine)

Purpose:
- Executor_v2 in PAPER mode opens simulated positions via PaperBroker
- tp_sl_manager reads state/positions_bus.json
- This module publishes PAPER positions to the canonical positions bus so TP/SL can "see" them
- Includes a small CLI for testing:
    --force-close-all
    --poke-price
"""

from __future__ import annotations

import argparse
import inspect
import json
import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal, Tuple

import yaml  # type: ignore


# ----------------------------
# ROOT / logger (fail-soft)
# ----------------------------
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
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("paper_broker")


# ----------------------------
# Types / dataclasses
# ----------------------------
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


def _now_ms() -> int:
    return int(time.time() * 1000)


# ----------------------------
# Strategy config loader
# ----------------------------
def _load_strategy_for_label(account_label: str) -> Dict[str, Any]:
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


# ----------------------------
# Positions bus writer
# ----------------------------
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
    now_ms = _now_ms()
    existing = _safe_read_json(_POSITIONS_BUS_PATH)

    if isinstance(existing, dict):
        rows = existing.get("positions") or []
    elif isinstance(existing, list):
        rows = existing
    else:
        rows = []

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
    out = {"ts_ms": now_ms, "source": "paper_broker", "positions": kept + new_rows}
    _safe_write_json(_POSITIONS_BUS_PATH, out)


# ----------------------------
# AI event (setup) publish (optional)
# ----------------------------
def _maybe_publish_setup_context(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    features: Dict[str, Any],
    setup_type: Optional[str],
    timeframe: Optional[str],
    ai_profile: Optional[str],
    extra: Optional[Dict[str, Any]],
) -> None:
    try:
        from app.ai.ai_events_spine import build_setup_context, publish_ai_event  # type: ignore
        evt = build_setup_context(
            trade_id=trade_id,
            symbol=symbol,
            account_label=account_label,
            strategy=strategy,
            features=features,
            setup_type=setup_type,
            timeframe=timeframe,
            ai_profile=ai_profile,
            extra=extra,
        )
        publish_ai_event(evt)
    except Exception as e:
        log.warning("[paper_broker] Optional setup_context publish failed: %r", e)


# ----------------------------
# Outcome writer (v1) (fail-soft, signature-adaptive)
# ----------------------------
def _maybe_write_outcome_v1_from_close(
    *,
    account_label: str,
    strategy: str,
    trade_id: str,
    symbol: str,
    side: Side,
    qty: float,
    entry_px: float,
    opened_ms: int,
    exit_px: float,
    closed_ms: int,
    fees_usd: float,
    mode: str,
    close_reason: str,
    pnl_usd: float,
    r_multiple: Optional[float],
    setup_type: Optional[str],
    timeframe: Optional[str],
    ai_profile: Optional[str],
) -> None:
    try:
        from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
    except Exception:
        return

    fn = write_outcome_from_paper_close  # type: ignore
    try:
        sig = inspect.signature(fn)
        params = set(sig.parameters.keys())
    except Exception:
        params = set()

    payload: Dict[str, Any] = {
        "account_label": account_label,
        "strategy": strategy,
        "trade_id": trade_id,
        "symbol": symbol,
        "entry_side": "Buy" if side == "long" else "Sell",
        "entry_qty": float(qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ms),
        "exit_px": float(exit_px),
        "exit_qty": float(qty),
        "closed_ts_ms": int(closed_ms),
        "fees_usd": float(fees_usd),
        "mode": str(mode),
        "close_reason": str(close_reason),
        "pnl_usd": float(pnl_usd),
        "r_multiple": r_multiple,
        "setup_type": setup_type,
        "timeframe": timeframe,
        "ai_profile": ai_profile,
    }

    # If signature is known, only pass accepted keys.
    # If we couldn't read the signature, we try a conservative minimal set.
    if params:
        call_kwargs = {k: v for k, v in payload.items() if k in params}
    else:
        call_kwargs = {
            "account_label": account_label,
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_side": payload["entry_side"],
            "entry_qty": payload["entry_qty"],
            "entry_px": payload["entry_px"],
            "opened_ts_ms": payload["opened_ts_ms"],
            "exit_px": payload["exit_px"],
            "exit_qty": payload["exit_qty"],
            "closed_ts_ms": payload["closed_ts_ms"],
            "fees_usd": payload["fees_usd"],
            "mode": payload["mode"],
            "close_reason": payload["close_reason"],
        }

    try:
        fn(**call_kwargs)  # type: ignore
        log.info("[paper_broker] ✅ outcomes.v1 wrote trade_id=%s", trade_id)
    except Exception as e:
        log.warning("[paper_broker] outcomes.v1 writer failed: %r", e)


# ----------------------------
# PaperBroker core
# ----------------------------
class PaperBroker:
    def __init__(self, state: PaperAccountState, state_path: Path) -> None:
        self._state = state
        self._state_path = state_path

    @classmethod
    def load_or_create(cls, account_label: str, *, starting_equity: float = 1000.0) -> "PaperBroker":
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
                open_positions = [PaperPosition(**pos) for pos in (raw.get("open_positions") or [])]
                closed_trades = [PaperPosition(**pos) for pos in (raw.get("closed_trades") or [])]
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
                account_label, state.equity, len(state.open_positions), len(state.closed_trades)
            )
            broker = cls(state, state_path)
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
            account_label, starting_equity, risk_pct
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
        self._state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

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
        risk_amount = self._state.equity * max(self._state.risk_pct, 0.0)
        if "risk_usd" in features_ext and features_ext["risk_usd"] is not None:
            try:
                risk_amount = float(features_ext["risk_usd"])
            except Exception:
                pass
        features_ext["risk_usd"] = float(risk_amount)

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
            _maybe_publish_setup_context(
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
        _publish_positions_bus(self._state.account_label, self._state.open_positions)

        log.info(
            "[paper_broker] OPEN %s %s side=%s size=%.4f entry=%.4f sl=%.4f tp=%.4f risk_usd=%.2f",
            self._state.account_label, symbol, side, size_val, entry_price, stop_price, take_profit_price, risk_amount
        )
        return pos

    def _close_position(self, pos: PaperPosition, *, exit_price: float, exit_reason: str) -> None:
        if exit_price <= 0:
            raise ValueError("exit_price must be > 0")

        if pos.side == "long":
            pnl = (exit_price - pos.entry_price) * pos.size
        else:
            pnl = (pos.entry_price - exit_price) * pos.size

        risk_per_unit = abs(pos.entry_price - pos.stop_price)
        r_mult: Optional[float] = None
        if risk_per_unit > 0 and pos.size > 0:
            r_mult = pnl / (risk_per_unit * pos.size)

        pos.closed_ms = _now_ms()
        pos.exit_price = float(exit_price)
        pos.exit_reason = str(exit_reason)
        pos.pnl_usd = float(pnl)
        pos.r_multiple = r_mult

        self._state.equity += float(pnl)

        # move to closed
        self._state.open_positions = [p for p in self._state.open_positions if p.trade_id != pos.trade_id]
        self._state.closed_trades.append(pos)
        self._save()
        _publish_positions_bus(self._state.account_label, self._state.open_positions)

        # Try writing outcome (fail-soft)
        _maybe_write_outcome_v1_from_close(
            account_label=self._state.account_label,
            strategy=self._state.strategy_name,
            trade_id=pos.trade_id,
            symbol=pos.symbol,
            side=pos.side,
            qty=pos.size,
            entry_px=pos.entry_price,
            opened_ms=pos.opened_ms,
            exit_px=float(exit_price),
            closed_ms=int(pos.closed_ms or _now_ms()),
            fees_usd=0.0,
            mode="PAPER",
            close_reason=str(exit_reason),
            pnl_usd=float(pnl),
            r_multiple=r_mult,
            setup_type=pos.setup_type,
            timeframe=pos.timeframe,
            ai_profile=pos.ai_profile,
        )

        log.info(
            "[paper_broker] CLOSE %s %s side=%s exit=%.4f pnl=%.2f R=%s reason=%s equity=%.2f",
            self._state.account_label, pos.symbol, pos.side, exit_price, pnl, str(r_mult), exit_reason, self._state.equity
        )

    def update_price(self, symbol: str, price: float) -> None:
        if price <= 0:
            return
        to_close: List[Tuple[PaperPosition, str]] = []
        for pos in list(self._state.open_positions):
            if pos.symbol != symbol:
                continue
            if pos.side == "long":
                if price >= pos.take_profit_price:
                    to_close.append((pos, "tp_hit"))
                elif price <= pos.stop_price:
                    to_close.append((pos, "sl_hit"))
            else:
                if price <= pos.take_profit_price:
                    to_close.append((pos, "tp_hit"))
                elif price >= pos.stop_price:
                    to_close.append((pos, "sl_hit"))

        for pos, reason in to_close:
            self._close_position(pos, exit_price=price, exit_reason=reason)

    def force_close_all(self, *, exit_price_mode: str = "tp", reason: str = "forced") -> int:
        count = 0
        for pos in list(self._state.open_positions):
            if exit_price_mode.lower() == "tp":
                px = float(pos.take_profit_price)
            elif exit_price_mode.lower() == "sl":
                px = float(pos.stop_price)
            else:
                px = float(pos.entry_price)
            self._close_position(pos, exit_price=px, exit_reason=reason)
            count += 1
        return count


# ----------------------------
# CLI
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(prog="paper_broker", add_help=True)
    ap.add_argument("--account", required=False, default="flashback01", help="account_label (e.g. flashback01)")
    ap.add_argument("--starting-equity", required=False, type=float, default=1000.0)
    ap.add_argument("--force-close-all", action="store_true")
    ap.add_argument("--exit-price-mode", required=False, default="tp", choices=["tp", "sl", "entry"])
    ap.add_argument("--reason", required=False, default="tp_forced")
    ap.add_argument("--poke-price", action="store_true")
    ap.add_argument("--symbol", required=False, default=None)
    ap.add_argument("--price", required=False, type=float, default=None)

    args = ap.parse_args()

    broker = PaperBroker.load_or_create(args.account, starting_equity=float(args.starting_equity))

    if args.poke_price:
        if not args.symbol or not args.price:
            raise SystemExit("FAIL: --poke-price requires --symbol and --price")
        broker.update_price(args.symbol, float(args.price))
        print(f"OK: poke_price {args.account} {args.symbol} {args.price}")
        return

    if args.force_close_all:
        n = broker.force_close_all(exit_price_mode=args.exit_price_mode, reason=args.reason)
        log.info("PaperBroker CLI: force-closed %d positions for %s", n, args.account)
        return

    log.info("PaperBroker CLI: nothing to do (use --force-close-all or --poke-price).")


if __name__ == "__main__":
    main()
