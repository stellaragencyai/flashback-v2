from app.state.store import load_state

def hydrate_dashboard_rows():
    state = load_state()
    rows = []

    for label,sa in state["subaccounts"].items():
        rows.append({
            "account": label,
            "status": sa["status"],
            "pnl_pct": sa["pnl_pct"],
            "drawdown": sa["drawdown_pct"],
            "confidence": sa["confidence"],
            "risk": sa["risk"],
            "learning": sa["learning_phase"],
            "intent": sa["ai_intent"],
            "exposure": sa["exposure"],
            "win_rate": sa["win_rate"],
            "trades": len(sa["trades"]),
            "health": sa["health"],
            "locked": sa["locked"]
        })

    return rows
