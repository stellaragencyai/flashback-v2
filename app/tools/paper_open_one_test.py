# -*- coding: utf-8 -*-
from __future__ import annotations

from app.sim.paper_broker import PaperBroker

def main() -> None:
    broker = PaperBroker.load_or_create("flashback01", starting_equity=1000.0)

    # Simple deterministic open: long with TP above and SL below.
    pos = broker.open_position(
        symbol="XRPUSDT",
        side="long",
        entry_price=1.0,
        stop_price=0.99,
        take_profit_price=1.01,
        setup_type="scalp",
        timeframe="5m",
        features={
            "schema_version": "setup_features_v1",
            "symbol": "XRPUSDT",
            "timeframe": "5m",
            "setup_type": "scalp",
            "side": "buy",
            "qty": 1.0,
            "size": 1.0,
            "risk_usd": 1.0,
        },
        extra={
            "mode": "PAPER",
            "join_key": "client_trade_id",
        },
        trade_id=None,
        log_setup=False,
    )

    print("OK: OPENED trade_id=", pos.trade_id)

if __name__ == "__main__":
    main()
