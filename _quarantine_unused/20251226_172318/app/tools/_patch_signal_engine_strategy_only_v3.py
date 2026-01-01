# -*- coding: utf-8 -*-
"""
Patch signal_engine.py (v3 robust anchors):
- Add SIG_ALLOW_FALLBACK + SIG_FALLBACK_FANOUT env parsing (if missing)
- Replace fanout emission block with strategy-only emission + gated fallback
- (Optional) add User-Agent to first requests.get(...) if none exists
"""

from __future__ import annotations

from pathlib import Path
import re

FILE = Path(r"app\bots\signal_engine.py")

def die(msg: str) -> None:
    raise SystemExit(msg)

def main() -> None:
    if not FILE.exists():
        die(f"PATCH FAILED: missing {FILE}")

    s = FILE.read_text(encoding="utf-8", errors="ignore")

    # 1) Add env flags near SIG_USE_STRATEGIES parsing
    if "SIG_ALLOW_FALLBACK" not in s:
        needle = 'SIG_USE_STRATEGIES = _parse_bool(os.getenv("SIG_USE_STRATEGIES"), True)\n'
        if needle not in s:
            die("PATCH FAILED: could not find SIG_USE_STRATEGIES env parse line.")
        insert = (
            'SIG_USE_STRATEGIES = _parse_bool(os.getenv("SIG_USE_STRATEGIES"), True)\n'
            'SIG_ALLOW_FALLBACK = _parse_bool(os.getenv("SIG_ALLOW_FALLBACK"), False)\n'
            'SIG_FALLBACK_FANOUT = _parse_bool(os.getenv("SIG_FALLBACK_FANOUT"), False)\n'
        )
        s = s.replace(needle, insert, 1)

    # 2) Optional 403 hardening: add a default User-Agent if file doesn't mention one anywhere
    if ("User-Agent" not in s) and ("requests.get" in s) and ("fetch_recent_klines" in s):
        rgx = re.compile(r"requests\.get\((?P<head>.*?)(?P<tail>,\s*timeout\s*=\s*\d+.*?\))", re.S)
        m = rgx.search(s)
        if m:
            head = m.group("head")
            tail = m.group("tail")
            if "headers=" not in head:
                repl = 'requests.get(' + head + 'headers={"User-Agent":"Mozilla/5.0"}, ' + tail
                s = s[:m.start()] + repl + s[m.end():]

    # 3) Replace the signal decision/emission block
    start_m = re.search(
        r'\n\s*side\s*=\s*None\n\s*reason\s*=\s*""\n\s*debug\s*:\s*Dict\[str,\s*Any\]\s*=\s*\{\}\n',
        s
    )
    if not start_m:
        die("PATCH FAILED: could not locate side/reason/debug init block.")

    # Search for the generic append_signal_jsonl(...) call *after* start_m
    tail = s[start_m.start():]
    end_m = re.search(
        r'append_signal_jsonl\(\s*\n'
        r'\s*symbol=symbol,\s*\n'
        r'\s*side_text=side,\s*\n'
        r'\s*tf_label=tf_label,\s*\n'
        r'\s*bar_ts=bar_ts,\s*\n'
        r'\s*price=last_close,\s*\n'
        r'\s*reason=reason,\s*\n'
        r'\s*debug=debug,\s*\n'
        r'\s*sub_uid=None,\s*\n'
        r'\s*strategy_name=None,\s*\n'
        r'\s*\)\s*\n',
        tail,
        re.S
    )
    if not end_m:
        die("PATCH FAILED: could not locate generic append_signal_jsonl(sub_uid=None) block after start anchor.")

    start = start_m.start()
    end = start_m.start() + end_m.end()

    replacement = r'''
            side = None
            reason = ""
            debug: Dict[str, Any] = {}

            tf_label = tf_display(tf)

            # Collect per-strategy matches (NO global fanout)
            matched: List[Dict[str, Any]] = []

            # 1) Strategy setups with regime gating (object-safe) - per sub/strategy
            if strat_list:
                for strat in strat_list:
                    raw_obj = strat.get("raw")
                    setup_types = _get_strat_attr(raw_obj, "setup_types", []) or []
                    if not isinstance(setup_types, list):
                        setup_types = []

                    for setup in setup_types:
                        logic_fn = SETUP_LOGIC.get(str(setup))
                        if not logic_fn:
                            continue
                        s_side, s_reason = logic_fn(candles)
                        if not s_side:
                            continue

                        if not passes_regime_filters(raw_obj, regime_ind):
                            continue

                        m_debug: Dict[str, Any] = {"setup": setup, "regime": regime_ind, "signal_origin": "strategy"}
                        matched.append({"strat": strat, "side": s_side, "reason": f"{setup}:{s_reason}", "debug": m_debug})
                        break  # one setup per strategy per bar

            # 2) Fallback (only if allowed) and ONLY if nothing matched
            fallback_payload = None
            if (not matched) and SIG_ALLOW_FALLBACK:
                simple_side, simple_debug = compute_simple_signal(candles)
                if simple_side:
                    fb_debug = dict(simple_debug)
                    fb_debug["signal_origin"] = "fallback"
                    fb_debug["regime"] = regime_ind
                    fallback_payload = {"side": simple_side, "reason": f"fallback:{simple_debug.get('reason')}", "debug": fb_debug}

            # If nothing at all, mark bar processed and continue
            if (not matched) and (fallback_payload is None):
                last_signal_bar[key] = bar_ts
                continue

            # Mark bar processed for this (symbol, tf) regardless
            last_signal_bar[key] = bar_ts

            # Emit matched strategies (strategy-only)
            if matched:
                for item in matched:
                    strat = item["strat"]
                    side = item["side"]
                    reason = item["reason"]
                    debug = item["debug"]

                    sub_uid = str(strat.get("sub_uid"))
                    strat_name = strat.get("name", f"sub-{sub_uid}")
                    automation_mode = strat.get("automation_mode")

                    msg = (
                        f"📡 *Signal Engine v2* - {symbol} / {tf_label}\n"
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
                            "strategy_raw": strat.get("raw") or strat,
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
                        f"[SIGNAL] {symbol} {tf_label} {side} | "
                        f"strategy={strat_name} sub_uid={sub_uid} | signal_id={signal_id} | reason={reason}"
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

                    total_signals_this_loop += 1

            # Emit fallback (generic or fanout, depending on env)
            elif fallback_payload is not None:
                side = fallback_payload["side"]
                reason = fallback_payload["reason"]
                debug = fallback_payload["debug"]

                msg = (
                    f"📡 *Signal Engine v2* - {symbol} / {tf_label}\n"
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
                    for strat in (strat_list or []):
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
                                "strategy_raw": strat.get("raw") or strat,
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
                            f"[SIGNAL] {symbol} {tf_label} {side} | "
                            f"strategy={strat_name} sub_uid={sub_uid} | signal_id={signal_id} | reason={reason}"
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

                        total_signals_this_loop += 1

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
                    print(f"[SIGNAL] {symbol} {tf_label} {side} | strategy=GENERIC | signal_id={signal_id} | reason={reason}")

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

                    total_signals_this_loop += 1
'''

    s2 = s[:start] + replacement + s[end:]
    if s2 == s:
        die("PATCH FAILED: no changes applied (unexpected).")

    FILE.write_text(s2, encoding="utf-8", newline="\n")
    print("OK: patched signal_engine.py (strategy-only emission + fallback gating) [v3].")

if __name__ == "__main__":
    main()
