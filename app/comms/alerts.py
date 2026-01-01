from app.comms.telegram_bus_v1 import send_alert

def alert_trade_open(subaccount, symbol, side, qty, price):
    send_alert(
        subaccount,
        f"🚀 TRADE OPENED\n"
        f"Account: {subaccount}\n"
        f"Symbol: {symbol}\n"
        f"Side: {side}\n"
        f"Qty: {qty}\n"
        f"Price: {price}"
    )

def alert_trade_close(subaccount, symbol, pnl):
    send_alert(
        subaccount,
        f"🏁 TRADE CLOSED\n"
        f"Account: {subaccount}\n"
        f"Symbol: {symbol}\n"
        f"PNL: {pnl}"
    )

def alert_error(message):
    send_alert("notif", f"❌ ERROR\n{message}")
