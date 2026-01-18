from __future__ import annotations

import argparse
import os
import time
import uuid
from typing import Any, Dict

from app.sim.paper_broker import PaperBroker


def _safe_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        s = str(x).strip()
        if s.lower() in ("none", "null"):
            return ""
        return s
    except Exception:
        return ""


def _normalize_timeframe(tf: Any, default: str = "5m") -> str:
    s = ""
    try:
        s = str(tf).strip().lower()
    except Exception:
        s = ""
    if not s:
        return default
    if s.endswith(("m", "h", "d", "w")):
        return s
    if s.isdigit():
        return f"{s}m"
    return s or default


def _pick_account_label(cli_account: str | None) -> str:
    """
    Priority:
      1) CLI --account / --label
      2) env ACCOUNT_LABEL
      3) env FLASHBACK_ACCOUNT (alias some scripts use)
      4) default flashback01
    """
    if cli_account:
        v = _safe_str(cli_account)
        if v:
            return v

    v = _safe_str(os.getenv("ACCOUNT_LABEL", ""))
    if v:
        return v

    v = _safe_str(os.getenv("FLASHBACK_ACCOUNT", ""))
    if v:
        return v

    return "flashback01"


def main() -> None:
    ap = argparse.ArgumentParser(description="Open one PAPER position for a specific Flashback subaccount (test utility).")
    ap.add_argument("--account", "--label", dest="account", default=None, help="Account label, e.g. flashback09")
    ap.add_argument("--symbol", default=None, help="Symbol (default: env PAPER_TEST_SYMBOL or XRPUSDT)")
    ap.add_argument("--side", default=None, help="long/short (default: env PAPER_TEST_SIDE or long)")
    ap.add_argument("--timeframe", default=None, help="Timeframe like 5m/15m/1h (default: env PAPER_TEST_TIMEFRAME or 5m)")
    ap.add_argument("--setup-type", default=None, help="Setup type (default: env PAPER_TEST_SETUP_TYPE or scalp)")
    ap.add_argument("--entry", default=None, help="Entry price (default: env PAPER_TEST_ENTRY_PRICE or 1.0000)")
    ap.add_argument("--sl", default=None, help="Stop price (default: env PAPER_TEST_STOP_PRICE or 0.9900)")
    ap.add_argument("--tp", default=None, help="Take profit price (default: env PAPER_TEST_TP_PRICE or 1.0100)")
    args = ap.parse_args()

    # Choose account label deterministically
    account_label = _pick_account_label(args.account)

    # Load ledger for that account label
    broker = PaperBroker.load_or_create(account_label, starting_equity=1000.0)

    # Defaults can be overridden via CLI, otherwise env
    symbol = (_safe_str(args.symbol) or _safe_str(os.getenv("PAPER_TEST_SYMBOL", "XRPUSDT")) or "XRPUSDT").upper()
    side_raw = (_safe_str(args.side) or _safe_str(os.getenv("PAPER_TEST_SIDE", "long")) or "long").lower()
    timeframe = _normalize_timeframe(_safe_str(args.timeframe) or os.getenv("PAPER_TEST_TIMEFRAME", "5m"), default="5m")
    setup_type = _safe_str(args.setup_type) or _safe_str(os.getenv("PAPER_TEST_SETUP_TYPE", "scalp")) or "scalp"

    entry_price = float(_safe_str(args.entry) or os.getenv("PAPER_TEST_ENTRY_PRICE", "1.0000"))
    stop_price = float(_safe_str(args.sl) or os.getenv("PAPER_TEST_STOP_PRICE", "0.9900"))
    take_profit_price = float(_safe_str(args.tp) or os.getenv("PAPER_TEST_TP_PRICE", "1.0100"))

    # Map side into expected enum values used by the broker ("long"/"short" is typical)
    side = "long" if side_raw in ("long", "buy") else ("short" if side_raw in ("short", "sell") else side_raw)

    # Features payload used downstream by learning/bucketization
    features: Dict[str, Any] = {
        "source": "paper_open_one_test",
        "memory_fingerprint": "",
        "timeframe": timeframe,
        "setup_type": setup_type,
    }

    # Extra metadata used for join/debugging
    trade_id = f"{account_label}-{symbol}-{uuid.uuid4().hex[:10]}"
    extra: Dict[str, Any] = {
        "mode": "PAPER",
        "join_key": "trade_id",
        "requested_by": "paper_open_one_test",
        "ts_ms": int(time.time() * 1000),
    }

    print(f"[paper_open_one_test] USING account_label={account_label} symbol={symbol} side={side} tf={timeframe} setup={setup_type}")

    opened = broker.open_position(
        symbol=symbol,
        side=side,  # type: ignore[arg-type]
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
        setup_type=setup_type,
        timeframe=timeframe,
        features=features,
        extra=extra,
        trade_id=trade_id,
        log_setup=True,  # IMPORTANT: ensures setup context gets recorded
    )

    print("OK: OPENED")
    print("  account_label:", account_label)
    print("  trade_id     :", opened.trade_id if hasattr(opened, "trade_id") else trade_id)
    print("  symbol       :", symbol)
    print("  side         :", side)
    print("  timeframe    :", timeframe)
    print("  setup_type   :", setup_type)


if __name__ == "__main__":
    main()

