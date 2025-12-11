from app.ai.ai_events_spine import build_setup_context, publish_ai_event
from app.core.bus_types import ai_events_bus

ev = build_setup_context(
    trade_id="TEST_TRADE_123",
    symbol="BTCUSDT",
    account_label="main",
    strategy="TestStrategy",
    features={"foo": 1.23, "bar": 9.87},
    extra={"mode": "PAPER", "sub_uid": "524630315", "timeframe": "15"},
)

publish_ai_event(ev)
print("Pushed event. Bus size now:", len(ai_events_bus))
