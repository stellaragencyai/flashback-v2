from __future__ import annotations
import os
from pathlib import Path

def main() -> None:
    os.environ["ACCOUNT_LABEL"] = "flashback04"
    os.environ["AI_EVENTS_DIR"] = r".\state\ai_events_flashback04"

    from app.sim.paper_broker import PaperBroker

    b = PaperBroker.load_or_create("flashback04", starting_equity=1000.0)
    pos = b.open_position(
        symbol="XRPUSDT",
        side="long",
        entry_price=1.0,
        stop_price=0.99,
        take_profit_price=1.01,
        setup_type="scalp",
        timeframe="15m",
        features={"integrity_test": True},
        extra={"mode": "PAPER", "join_key": "client_trade_id"},
    )
    b.force_close_all(exit_price_mode="tp", reason="selftest_outcome_lane")

    lane = Path(r".\state\ai_events_flashback04\outcomes.v1.jsonl")
    if not lane.exists():
        raise SystemExit("FAIL: lane outcomes.v1.jsonl missing")

    tail = lane.read_text(encoding="utf-8").splitlines()[-1]
    if '"close_reason": "selftest_outcome_lane"' not in tail:
        raise SystemExit("FAIL: lane file did not get the new row")

    print("OK: lane outcome writer works")

if __name__ == "__main__":
    main()
