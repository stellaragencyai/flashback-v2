from app.comms.alerts import (
    alert_trade_open,
    alert_trade_close,
    alert_error,
)
from time import time

_LAST_SENT = {}

def _dedupe(key, ttl=3):
    now = time()
    if key in _LAST_SENT and now - _LAST_SENT[key] < ttl:
        return False
    _LAST_SENT[key] = now
    return True

def on_trade_open(event):
    key = f"open:{event['account']}:{event['symbol']}"
    if not _dedupe(key): return
    alert_trade_open(
        event["account"],
        event["symbol"],
        event["side"],
        event["qty"],
        event["price"],
    )

def on_trade_close(event):
    key = f"close:{event['account']}:{event['symbol']}"
    if not _dedupe(key): return
    alert_trade_close(
        event["account"],
        event["symbol"],
        event["pnl"],
    )

def on_error(msg):
    if not _dedupe(f"err:{msg}", ttl=10): return
    alert_error(msg)
