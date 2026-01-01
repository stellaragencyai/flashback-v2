def emit_pilot_input_decision(setup_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Hardened pilot INPUT decision emitter.
    Guarantees schema_version == 1 and a usable decision row for the enforcer.
    """
    if pilot_decide is None:
        return None

    try:
        raw = pilot_decide(setup_event)
    except Exception as e:
        log.warning("pilot_decide crashed (non-fatal): %r", e)
        raw = None

    if not isinstance(setup_event, dict):
        return None

    trade_id = str(setup_event.get("trade_id") or "").strip()
    if not trade_id:
        return None

    client_trade_id = str(setup_event.get("client_trade_id") or trade_id).strip()
    source_trade_id = setup_event.get("source_trade_id")

    symbol = str(setup_event.get("symbol") or "").strip().upper()
    account_label = str(setup_event.get("account_label") or "").strip()
    timeframe = str(setup_event.get("timeframe") or "").strip()

    d = raw if isinstance(raw, dict) else {}

    decision = str(
        d.get("decision")
        or d.get("decision_code")
        or d.get("code")
        or d.get("action")
        or "COLD_START"
    ).strip()

    allow = d.get("allow")
    sm = d.get("size_multiplier")

    if sm is None:
        pa = d.get("proposed_action") if isinstance(d.get("proposed_action"), dict) else {}
        sm = pa.get("size_multiplier")

    gates = d.get("gates") if isinstance(d.get("gates"), dict) else {}
    reason = str(d.get("reason") or gates.get("reason") or "pilot_input").strip()

    if allow is None:
        allow = decision in ("ALLOW_TRADE", "COLD_START")

    try:
        sm_f = float(sm) if sm is not None else (0.25 if decision == "COLD_START" else (1.0 if allow else 0.0))
    except Exception:
        sm_f = 0.25 if decision == "COLD_START" else (1.0 if allow else 0.0)

    if sm_f < 0:
        sm_f = 0.0
    if allow and sm_f <= 0:
        sm_f = 1.0

    row = {
        "schema_version": 1,
        "ts": int(time.time() * 1000),
        "trade_id": trade_id,
        "client_trade_id": client_trade_id,
        "source_trade_id": source_trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "timeframe": timeframe,
        "decision": decision,
        "allow": bool(allow),
        "size_multiplier": float(sm_f),
        "gates": {"reason": reason},
        "meta": {"source": "pilot_input_normalized"},
    }

    _append_decision(row)
    return row
