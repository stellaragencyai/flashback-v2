from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

def main():
    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # Insert a hard outcome-write block near the end of _close_position, right before logging the CLOSE line.
    # We anchor on the CLOSE log.info call line.
    anchor = r'(?m)^\s*log\.info\(\s*$'
    m = re.search(anchor, s)
    if not m:
        raise SystemExit("FAIL: could not find log.info anchor in _close_position")

    # Make sure we're patching inside _close_position by locating that function block first
    fn_pat = r"(?ms)^    def _close_position\(\s*\n.*?\n(?=^    def\s|\Z)"
    fn = re.search(fn_pat, s)
    if not fn:
        raise SystemExit("FAIL: could not find PaperBroker._close_position block")

    block = fn.group(0)
    if "OUTCOME_V1_WRITE_ATTEMPT" in block:
        print("OK: outcome write block already present (skipping)")
        return

    # Inject immediately before the CLOSE log.info inside _close_position block
    inject_pat = r'(?m)^\s*log\.info\(\s*$'
    inject_point = re.search(inject_pat, block)
    if not inject_point:
        raise SystemExit("FAIL: could not locate injection point inside _close_position")

    inject = r'''
        # --- OUTCOME_V1_WRITE_ATTEMPT (paper close -> outcomes.v1.jsonl) ---
        try:
            if write_outcome_from_paper_close is not None:
                # We pass the PaperPosition and computed pnl/r metrics where possible.
                # The writer is expected to be fail-soft, but we log loudly if it errors.
                try:
                    write_outcome_from_paper_close(
                        account_label=self._state.account_label,
                        strategy=self._state.strategy_name,
                        trade_id=str(pos.trade_id),
                        symbol=str(pos.symbol),
                        side=str(pos.side),
                        entry_price=float(pos.entry_price),
                        exit_price=float(exit_price),
                        stop_price=float(pos.stop_price),
                        take_profit_price=float(pos.take_profit_price),
                        size=float(pos.size),
                        pnl_usd=float(pnl),
                        r_multiple=r_mult,
                        win=(r_mult is not None and r_mult > 0),
                        exit_reason=str(exit_reason),
                        opened_ms=int(pos.opened_ms),
                        closed_ms=int(pos.closed_ms or _now_ms()),
                        timeframe=pos.timeframe,
                        setup_type=pos.setup_type,
                        ai_profile=pos.ai_profile,
                        equity_before=float(equity_before),
                        equity_after=float(equity_after),
                    )
                    log.info("[paper_broker] ✅ outcomes.v1 write attempted trade_id=%s", str(pos.trade_id))
                except TypeError as te:
                    # Signature mismatch: writer changed. Make it obvious.
                    log.warning("[paper_broker] OUTCOME writer signature mismatch: %r", te)
                except Exception as e:
                    log.warning("[paper_broker] OUTCOME writer failed: %r", e)
            else:
                log.warning("[paper_broker] OUTCOME writer missing (write_outcome_from_paper_close=None)")
        except Exception as e:
            log.warning("[paper_broker] OUTCOME write wrapper failed: %r", e)
'''

    block2 = block[:inject_point.start()] + inject + block[inject_point.start():]
    s2 = s[:fn.start()] + block2 + s[fn.end():]

    TARGET.write_text(s2, encoding="utf-8")
    print("OK: patched paper_broker.py (forced outcome write on close, noisy)")

if __name__ == "__main__":
    main()
