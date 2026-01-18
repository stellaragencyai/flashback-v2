from pathlib import Path

p = Path("app/sim/paper_broker.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)
text = "".join(lines)

def die(msg: str):
    raise SystemExit("FAIL: " + msg)

# If already patched, bail.
if "PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE" in text and "_maybe_publish_outcome_record" in text:
    die("looks already patched")

# ----------------------------
# 1) Insert helper after setup_context warning block (anchor on known string)
# ----------------------------
needle_setup = "[paper_broker] Optional setup_context publish failed"
idx = None
for i, ln in enumerate(lines):
    if needle_setup in ln:
        idx = i
        break
if idx is None:
    die("could not find anchor warning line for setup_context publish")

# find end of containing block/function
end = None
for j in range(idx + 1, len(lines)):
    ln = lines[j]
    if ln.startswith("def ") or ln.startswith("class ") or (ln.strip() and not ln.startswith((" ", "\t", "#"))):
        end = j
        break
if end is None:
    end = len(lines)

helper = r'''
def _maybe_publish_outcome_record(
    *,
    account_label: str,
    strategy: str,
    trade_id: str,
    symbol: str,
    side: Side,
    qty: float,
    entry_px: float,
    opened_ms: int,
    exit_px: float,
    closed_ms: int,
    fees_usd: float,
    mode: str,
    close_reason: str,
    pnl_usd: float,
    r_multiple: Optional[float],
    setup_type: Optional[str],
    timeframe: Optional[str],
    ai_profile: Optional[str],
) -> bool:
    """Publish canonical outcome_record into ai_events_spine (single-writer)."""
    try:
        from app.ai.ai_events_spine import build_outcome_record, publish_ai_event  # type: ignore
        evt = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy),
            pnl_usd=float(pnl_usd),
            r_multiple=float(r_multiple) if r_multiple is not None else None,
            win=(float(pnl_usd) > 0.0),
            exit_reason=str(close_reason),
            timeframe=timeframe,
            extra={
                "mode": str(mode),
                "entry_side": "Buy" if side == "long" else "Sell",
                "entry_qty": float(qty),
                "entry_px": float(entry_px),
                "opened_ts_ms": int(opened_ms),
                "exit_side": "Sell" if side == "long" else "Buy",
                "exit_qty": float(qty),
                "exit_px": float(exit_px),
                "closed_ts_ms": int(closed_ms),
                "fees_usd": float(fees_usd),
                "setup_type": setup_type,
                "timeframe": timeframe,
                "ai_profile": ai_profile,
                "close_reason": str(close_reason),
            },
        )
        publish_ai_event(evt)
        log.info("[paper_broker] ✅ outcome_record published to spine trade_id=%s", trade_id)
        return True
    except Exception as e:
        log.warning("[paper_broker] outcome_record publish failed: %r", e)
        return False
'''

lines = lines[:end] + [helper] + lines[end:]
text = "".join(lines)

# ----------------------------
# 2) Replace entire _maybe_write_outcome_v1_from_close function
# ----------------------------
L = text.splitlines(True)

start = None
for i, ln in enumerate(L):
    if ln.startswith("def _maybe_write_outcome_v1_from_close"):
        start = i
        break
if start is None:
    die("could not locate def _maybe_write_outcome_v1_from_close")

end = None
for j in range(start + 1, len(L)):
    ln = L[j]
    if ln.startswith("def ") or ln.startswith("class ") or (ln.strip() and not ln.startswith((" ", "\t", "#"))):
        end = j
        break
if end is None:
    end = len(L)

replacement = r'''def _maybe_write_outcome_v1_from_close(
    *,
    account_label: str,
    strategy: str,
    trade_id: str,
    symbol: str,
    side: Side,
    qty: float,
    entry_px: float,
    opened_ms: int,
    exit_px: float,
    closed_ms: int,
    fees_usd: float,
    mode: str,
    close_reason: str,
    pnl_usd: float,
    r_multiple: Optional[float],
    setup_type: Optional[str],
    timeframe: Optional[str],
    ai_profile: Optional[str],
) -> None:
    # --- Single-writer: publish to ai_events_spine first (join happens there) ---
    published = _maybe_publish_outcome_record(
        account_label=account_label,
        strategy=strategy,
        trade_id=trade_id,
        symbol=symbol,
        side=side,
        qty=qty,
        entry_px=entry_px,
        opened_ms=opened_ms,
        exit_px=exit_px,
        closed_ms=closed_ms,
        fees_usd=fees_usd,
        mode=mode,
        close_reason=close_reason,
        pnl_usd=pnl_usd,
        r_multiple=r_multiple,
        setup_type=setup_type,
        timeframe=timeframe,
        ai_profile=ai_profile,
    )
    if published:
        return

    # Optional fallback: legacy outcomes.v1 direct writer (OFF by default)
    # Enable only if you explicitly set:
    #   $env:PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE = "true"
    import os
    allow_fallback = str(os.getenv("PAPER_BROKER_OUTCOME_V1_FALLBACK_WRITE", "false")).strip().lower() in ("1","true","yes","y","on")
    if not allow_fallback:
        return

    try:
        from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
    except Exception:
        return

    # Signature-adaptive call (fail-soft)
    try:
        import inspect
        fn = write_outcome_from_paper_close  # type: ignore
        try:
            sig = inspect.signature(fn)
            params = set(sig.parameters.keys())
        except Exception:
            params = set()

        payload = {
            "account_label": account_label,
            "strategy": strategy,
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_side": "Buy" if side == "long" else "Sell",
            "entry_qty": float(qty),
            "entry_px": float(entry_px),
            "opened_ts_ms": int(opened_ms),
            "exit_px": float(exit_px),
            "exit_qty": float(qty),
            "closed_ts_ms": int(closed_ms),
            "fees_usd": float(fees_usd),
            "mode": str(mode),
            "close_reason": str(close_reason),
            "pnl_usd": float(pnl_usd),
            "r_multiple": r_multiple,
            "setup_type": setup_type,
            "timeframe": timeframe,
            "ai_profile": ai_profile,
        }

        if params:
            call_kwargs = {k: v for k, v in payload.items() if k in params}
            fn(**call_kwargs)  # type: ignore
        else:
            # conservative minimal set
            fn(
                account_label=account_label,
                trade_id=trade_id,
                symbol=symbol,
                entry_side=payload["entry_side"],
                entry_qty=payload["entry_qty"],
                entry_px=payload["entry_px"],
                opened_ts_ms=payload["opened_ts_ms"],
                exit_px=payload["exit_px"],
                exit_qty=payload["exit_qty"],
                closed_ts_ms=payload["closed_ts_ms"],
                fees_usd=payload["fees_usd"],
                mode=payload["mode"],
                close_reason=payload["close_reason"],
            )  # type: ignore

        log.info("[paper_broker] ✅ outcomes.v1 wrote trade_id=%s", trade_id)
    except Exception as e:
        log.warning("[paper_broker] outcomes.v1 writer failed: %r", e)
'''

new_text = "".join(L[:start]) + replacement + "".join(L[end:])
p.write_text(new_text, encoding="utf-8")
print("OK: paper_broker single-writer patch applied (v2: full-function replace)")
