#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Test Signal Emitter v2.0

Purpose
-------
Append one or many well-formed signals into signals/observed.jsonl so that:

    • executor_v2
    • strategies.yaml
    • AI classifier + policy gate
    • PaperBroker
    • paper_price_daemon

can all be exercised end-to-end in DRY mode.

This does NOT talk to Bybit directly. It only writes to the signal bus.

Schema (what executor_v2 expects at minimum)
-------------------------------------------
Each line in signals/observed.jsonl is a JSON object with at least:

    {
      "symbol": "BTCUSDT",
      "timeframe": "5m",
      "setup_type": "breakout_high",
      "side": "buy",           # "buy"/"sell" or "long"/"short"
      "price": 43050.0,
      "ts": 1765426000000,
      "source": "emit_test_signal",
      "regime_hint": "trend_up | range | high_vol | low_vol",
      "ai_tag": "...",
      "note": "optional"
    }

Anything extra is carried into the AI feature payload via executor_v2
because it embeds the raw `signal` dict into features.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

from app.core.config import settings

ROOT: Path = settings.ROOT
SIGNAL_FILE: Path = ROOT / "signals" / "observed.jsonl"
SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)

# Preset setup types we'll use for synthetic training
BTC_SETUPS: List[str] = [
    "breakout_high",
    "breakout_fakeout",
    "trend_continuation",
    "range_fade",
    "scalp_pullback",
    "scalp_reversal",
]

ETH_SETUPS: List[str] = [
    "scalp_pullback",
    "scalp_reversal",
    "breakout_range",
    "trend_continuation",
]


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def build_signal(
    *,
    symbol: str,
    timeframe: str,
    setup_type: str,
    side: str,
    price: float,
    regime_hint: str = "unknown",
    vol_hint: str = "unknown",
    tag: str = "manual_batch",
    note: str | None = None,
) -> Dict[str, Any]:
    """
    Build a single signal dict in the exact schema executor_v2 expects.
    """
    # Normalize side for executor_v2 (accepts "buy"/"sell"/"long"/"short")
    side_norm = str(side).strip().lower()
    if side_norm not in ("buy", "sell", "long", "short"):
        raise ValueError(f"Unsupported side={side!r}; use buy/sell/long/short")

    tf_norm = str(timeframe).strip().lower()
    # Accept "5", "5m", "15", "15m" → normalize to "5m"/"15m"
    if tf_norm in ("5", "5m"):
        tf_norm = "5m"
    elif tf_norm in ("15", "15m"):
        tf_norm = "15m"

    sig: Dict[str, Any] = {
        "symbol": str(symbol).upper(),
        "timeframe": tf_norm,
        "setup_type": setup_type,
        "side": side_norm,
        "price": float(price),
        "ts": _now_ms(),
        "source": "emit_test_signal",
        # Hints for AI learning (these will be embedded into features)
        "regime_hint": regime_hint,
        "volatility_hint": vol_hint,
        "ai_tag": tag,
    }

    if note:
        sig["note"] = note

    return sig


def append_signals(signals: List[Dict[str, Any]]) -> None:
    """
    Append a list of signals to signals/observed.jsonl (one JSON per line).
    """
    with SIGNAL_FILE.open("a", encoding="utf-8") as f:
        for sig in signals:
            line = json.dumps(sig, separators=(",", ":"))
            f.write(line + "\n")

    print(f"Appended {len(signals)} signal(s) to {SIGNAL_FILE}")


def emit_single_from_args(args: argparse.Namespace) -> None:
    """
    Emit exactly one signal from CLI args.
    """
    sig = build_signal(
        symbol=args.symbol,
        timeframe=args.timeframe,
        setup_type=args.setup_type,
        side=args.side,
        price=args.price,
        regime_hint=args.regime,
        vol_hint=args.vol,
        tag="cli_single",
        note=args.note,
    )
    append_signals([sig])
    print("Single signal emitted:")
    print(json.dumps(sig, indent=2))


def emit_default_batch() -> None:
    """
    Emit a small, rich batch of BTC + ETH signals across 5m & 15m
    with a mix of long/short and different setup_types.

    This is ideal for:
        - Waking up multiple strategies at once
        - Generating diverse setups for AI dry-run learning
    """
    base_btc = 43050.0
    base_eth = 2255.0

    signals: List[Dict[str, Any]] = []

    # BTC 5m: breakout / fakeout pair
    signals.append(
        build_signal(
            symbol="BTCUSDT",
            timeframe="5m",
            setup_type="breakout_high",
            side="buy",
            price=base_btc + random.uniform(-20, 20),
            regime_hint="trend_up",
            vol_hint="high_vol",
            tag="batch_btc",
            note="BTC 5m breakout_high long",
        )
    )
    signals.append(
        build_signal(
            symbol="BTCUSDT",
            timeframe="5m",
            setup_type="breakout_fakeout",
            side="sell",
            price=base_btc + random.uniform(-30, 10),
            regime_hint="choppy",
            vol_hint="high_vol",
            tag="batch_btc",
            note="BTC 5m breakout_fakeout short",
        )
    )

    # BTC 15m: trend continuation
    signals.append(
        build_signal(
            symbol="BTCUSDT",
            timeframe="15m",
            setup_type="trend_continuation",
            side="buy",
            price=base_btc + random.uniform(-40, 40),
            regime_hint="trend_up",
            vol_hint="medium_vol",
            tag="batch_btc",
            note="BTC 15m trend_continuation long",
        )
    )

    # ETH 5m: scalp pullback & scalp reversal
    signals.append(
        build_signal(
            symbol="ETHUSDT",
            timeframe="5m",
            setup_type="scalp_pullback",
            side="buy",
            price=base_eth + random.uniform(-5, 5),
            regime_hint="trend_up",
            vol_hint="high_vol",
            tag="batch_eth",
            note="ETH 5m scalp_pullback long",
        )
    )
    signals.append(
        build_signal(
            symbol="ETHUSDT",
            timeframe="5m",
            setup_type="scalp_reversal",
            side="sell",
            price=base_eth + random.uniform(-7, 2),
            regime_hint="overextended",
            vol_hint="high_vol",
            tag="batch_eth",
            note="ETH 5m scalp_reversal short",
        )
    )

    # ETH 15m: breakout_range
    signals.append(
        build_signal(
            symbol="ETHUSDT",
            timeframe="15m",
            setup_type="breakout_range",
            side="buy",
            price=base_eth + random.uniform(-10, 10),
            regime_hint="range_break",
            vol_hint="medium_vol",
            tag="batch_eth",
            note="ETH 15m breakout_range long",
        )
    )

    append_signals(signals)
    print("Default batch emitted (BTC + ETH, 5m + 15m).")


def emit_random_batch(args: argparse.Namespace) -> None:
    """
    Emit N random signals across BTC/ETH and 5m/15m.

    This is useful when you want to flood the DRY system with
    diverse setups and see how AI + risk logic respond.
    """
    symbols = ["BTCUSDT", "ETHUSDT"]
    timeframes = ["5m", "15m"]

    signals: List[Dict[str, Any]] = []
    for _ in range(args.count):
        symbol = random.choice(symbols)
        tf = random.choice(timeframes)

        if symbol == "BTCUSDT":
            setup = random.choice(BTC_SETUPS)
            base_price = 43050.0
        else:
            setup = random.choice(ETH_SETUPS)
            base_price = 2255.0

        side = random.choice(["buy", "sell"])

        # Small price jitter to avoid identical entries
        price = base_price * (1.0 + random.uniform(-0.002, 0.002))

        regime = random.choice(
            ["trend_up", "trend_down", "range", "choppy", "news_spike"]
        )
        vol = random.choice(["low_vol", "medium_vol", "high_vol"])

        sig = build_signal(
            symbol=symbol,
            timeframe=tf,
            setup_type=setup,
            side=side,
            price=price,
            regime_hint=regime,
            vol_hint=vol,
            tag="random_batch",
            note=f"random {symbol} {tf} {setup} {side}",
        )
        signals.append(sig)

    append_signals(signals)
    print("Random batch emitted.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit one or many test signals into signals/observed.jsonl"
    )

    sub = parser.add_subparsers(dest="mode", required=False)

    # single: emit exactly one signal from CLI parameters
    single = sub.add_parser("single", help="emit a single custom signal")
    single.add_argument("--symbol", default="BTCUSDT")
    single.add_argument("--timeframe", default="5m")
    single.add_argument("--setup-type", default="test_manual")
    single.add_argument("--side", default="buy")
    single.add_argument("--price", type=float, default=43000.0)
    single.add_argument("--regime", default="unknown")
    single.add_argument("--vol", default="unknown")
    single.add_argument("--note", default=None)

    # batch: deterministic BTC+ETH set (good for quick tests)
    sub.add_parser("batch", help="emit a default BTC+ETH batch")

    # random: N random signals
    random_p = sub.add_parser("random", help="emit N random signals")
    random_p.add_argument("--count", type=int, default=10)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Default behavior: emit the BTC+ETH batch
    if not args.mode:
        emit_default_batch()
        return

    if args.mode == "single":
        emit_single_from_args(args)
    elif args.mode == "batch":
        emit_default_batch()
    elif args.mode == "random":
        emit_random_batch(args)
    else:
        raise SystemExit(f"Unknown mode={args.mode!r}")


if __name__ == "__main__":
    main()
