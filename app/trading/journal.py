from time import time
from app.state.store import upsert_subaccount, get_subaccount

def record_trade(label, pnl, confidence, intent):
    sa = get_subaccount(label)

    trade = {
        "ts": time(),
        "pnl": pnl,
        "intent": intent,
        "confidence": confidence
    }

    trades = sa.get("trades", [])[-49:]
    trades.append(trade)

    wins = [t for t in trades if t["pnl"] > 0]

    upsert_subaccount(label, {
        "trades": trades,
        "trade_count": len(trades),
        "win_rate": round(len(wins)/len(trades),2) if trades else 0.0,
        "confidence": confidence,
        "ai_intent": intent,
        "last_trade_ts": trade["ts"],
        "learning_phase": "exploit" if confidence > 0.7 else "observe"
    })
