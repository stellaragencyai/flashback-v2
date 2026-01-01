# -*- coding: utf-8 -*-
from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Add env flags near other SIG_* parsing (idempotent)
if "SIG_ALLOW_FALLBACK" not in s:
    needle = 'SIG_USE_STRATEGIES = _parse_bool(os.getenv("SIG_USE_STRATEGIES"), True)\n'
    if needle not in s:
        raise SystemExit("PATCH FAILED: could not find SIG_USE_STRATEGIES env line.")
    s = s.replace(
        needle,
        needle +
        'SIG_ALLOW_FALLBACK = _parse_bool(os.getenv("SIG_ALLOW_FALLBACK"), False)\n'
        'SIG_FALLBACK_FANOUT = _parse_bool(os.getenv("SIG_FALLBACK_FANOUT"), False)\n'
    )

# 2) Replace the fanout block (broadcast same side to every sub) with per-sub evaluation
pattern = re.compile(r"""
            side\s*=\s*None.*?
            # 1\)\s*Strategy setups.*?
            if\s+side\s+is\s+None:\s*\n.*?
            if\s+side\s+is\s+None:\s*\n.*?
            last_signal_bar\[key\]\s*=\s*bar_ts\s*\n
            total_signals_this_loop\s*\+=\s*1\s*\n
            tf_label\s*=\s*tf_display\(tf\)\s*\n
            applicable_strats:.*?\n
            if\s+applicable_strats:.*?
            else:.*?
            tg_info\(msg\)\s*\n
            regime_tags\s*=\s*\[reason\]\s*\n
            base_extra\s*=\s*\{.*?\}\s*\n
""", re.S | re.X)

m = pattern.search(s)
if not m:
    raise SystemExit("PATCH FAILED: could not find expected fanout block. File likely changed.")

replacement = r"""
            # Decide and emit signals per strategy/sub, not one signal broadcast to all.
            tf_label = tf_display(tf)
            matched_any = False

            # 1) Strategy setups (per-sub)
            if strat_list:
                for strat in strat_list:
                    raw_obj = strat.get("raw")
                    sub_uid = str(strat.get("sub_uid"))
                    strat_name = strat.get("name", f"sub-{sub_uid}")
                    automation_mode = strat.get("automation_mode")

                    setup_types = _get_strat_attr(raw_obj, "setup_types", []) or []
                    if not isinstance(setup_types, list):
                        setup_types = []

                    side = None
                    reason = ""
                    debug = {"regime": regime_ind, "signal_origin": "strategy"}

                    for setup in setup_types:
                        logic_fn = SETUP_LOGIC.get(str(setup))
                        if not logic_fn:
                            continue
                        s_side, s_reason = logic_fn(candles)
                        if not s_side:
                            continue
                        if not passes_regime_filters(raw_obj, regime_ind):
                            continue
                        side = s_side
                        reason = f"{setup}:{s_reason}"
                        debug["setup"] = setup
                        break

                    if not side:
                        continue

                    matched_any = True
                    total_signals_this_loop += 1

                    msg = (
                        f"?? *Signal Engine v2* - {symbol} / {tf_label}\n"
                        f"Side: *{side}*\n"
                        f"Strategy: `{strat_name}`\n"
                        f"Sub UID: `{sub_uid}`\n"
                        f"Last close: `{last_close}`\n"
                        f"Reason: `{reason}`\n"
                        f"(No orders placed here; executors handle trades.)"
                    )
                    tg_info(msg)

                    regime_tags = [reason]
                    base_extra = {"engine": "signal_engine_v2", "raw_debug": debug, "tf_raw": tf}

                    try:
                        sub_label = stratreg.get_sub_label(sub_uid) if _HAS_STRATEGY_REGISTRY else None
                    except Exception:
                        sub_label = None

                    extra = dict(base_extra)
                    extra.update(
                        {
                            "strategy_name": strat_name,
                            "strategy_automation_mode": automation_mode,
                            "sub_uid": sub_uid,
                            "sub_label": sub_label,
                            "strategy_raw": raw_obj,
                        }
                    )

                    signal_id = log_signal_from_engine(
                        symbol=symbol,
                        timeframe=tf_label,
                        side=side,
                        source="signal_engine_v2",
                        confidence=None,
                        stop_hint=None,
                        owner="AUTO_STRATEGY",
                        sub_uid=sub_uid,
                        strategy_role=strat_name,
                        regime_tags=regime_tags,
                        extra=extra,
                    )
                    print(
                        f"[SIGNAL] {symbol} {tf_label} {side} | strategy={strat_name} sub_uid={sub_uid} | "
                        f"signal_id={signal_id} | reason={reason}"
                    )

                    append_signal_jsonl(
                        symbol=symbol,
                        side_text=side,
                        tf_label=tf_label,
                        bar_ts=bar_ts,
                        price=last_close,
                        reason=reason,
                        debug=debug,
                        sub_uid=sub_uid,
                        strategy_name=strat_name,
                    )

            # 2) Fallback only if allowed, and only when nothing matched anywhere for this (symbol, tf, bar)
            if (not matched_any) and SIG_ALLOW_FALLBACK:
                simple_side, simple_debug = compute_simple_signal(candles)
                if simple_side:
                    side = simple_side
                    reason = f"fallback:{simple_debug.get('reason')}"
                    debug = dict(simple_debug)
                    debug["signal_origin"] = "fallback"
                    debug["regime"] = regime_ind

                    total_signals_this_loop += 1

                    msg = (
                        f"?? *Signal Engine v2* - {symbol} / {tf_label}\n"
                        f"Side: *{side}*\n"
                        f"Last close: `{last_close}`\n"
                        f"Reason: `{reason}`\n"
                        f"(Fallback mode)\n"
                        f"(No orders placed here; executors handle trades.)"
                    )
                    tg_info(msg)

                    regime_tags = [reason]
                    base_extra = {"engine": "signal_engine_v2", "raw_debug": debug, "tf_raw": tf}

                    if SIG_FALLBACK_FANOUT and strat_list:
                        for strat in strat_list:
                            raw_obj = strat.get("raw")
                            sub_uid = str(strat.get("sub_uid"))
                            strat_name = strat.get("name", f"sub-{sub_uid}")
                            automation_mode = strat.get("automation_mode")

                            try:
                                sub_label = stratreg.get_sub_label(sub_uid) if _HAS_STRATEGY_REGISTRY else None
                            except Exception:
                                sub_label = None

                            extra = dict(base_extra)
                            extra.update(
                                {
                                    "strategy_name": strat_name,
                                    "strategy_automation_mode": automation_mode,
                                    "sub_uid": sub_uid,
                                    "sub_label": sub_label,
                                    "strategy_raw": raw_obj,
                                }
                            )

                            signal_id = log_signal_from_engine(
                                symbol=symbol,
                                timeframe=tf_label,
                                side=side,
                                source="signal_engine_v2",
                                confidence=None,
                                stop_hint=None,
                                owner="AUTO_STRATEGY",
                                sub_uid=sub_uid,
                                strategy_role=strat_name,
                                regime_tags=regime_tags,
                                extra=extra,
                            )
                            print(
                                f"[SIGNAL] {symbol} {tf_label} {side} | strategy={strat_name} sub_uid={sub_uid} | "
                                f"signal_id={signal_id} | reason={reason}"
                            )

                            append_signal_jsonl(
                                symbol=symbol,
                                side_text=side,
                                tf_label=tf_label,
                                bar_ts=bar_ts,
                                price=last_close,
                                reason=reason,
                                debug=debug,
                                sub_uid=sub_uid,
                                strategy_name=strat_name,
                            )
                    else:
                        extra = dict(base_extra)
                        extra.update({"strategy_name": None, "strategy_automation_mode": None})

                        signal_id = log_signal_from_engine(
                            symbol=symbol,
                            timeframe=tf_label,
                            side=side,
                            source="signal_engine_v2",
                            confidence=None,
                            stop_hint=None,
                            owner="AUTO_STRATEGY",
                            sub_uid=None,
                            strategy_role="GENERIC_SIGNAL_ENGINE",
                            regime_tags=regime_tags,
                            extra=extra,
                        )
                        print(
                            f"[SIGNAL] {symbol} {tf_label} {side} | strategy=GENERIC | signal_id={signal_id} | reason={reason}"
                        )

                        append_signal_jsonl(
                            symbol=symbol,
                            side_text=side,
                            tf_label=tf_label,
                            bar_ts=bar_ts,
                            price=last_close,
                            reason=reason,
                            debug=debug,
                            sub_uid=None,
                            strategy_name=None,
                        )

            # Mark bar as processed for this (symbol, tf) regardless
            last_signal_bar[key] = bar_ts
"""

s = s[:m.start()] + replacement + s[m.end():]

p.write_text(s, encoding="utf-8")
print("OK: patched signal_engine.py to emit per-sub strategy signals and stop accidental fallback fanout.")